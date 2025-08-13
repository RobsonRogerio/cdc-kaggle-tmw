import os
import zipfile
import boto3
import shutil
from kaggle.api.kaggle_api_extended import KaggleApi
from pyspark.sql import SparkSession

# ===== CONFIGURAÇÕES =====
LOCAL_BASE_FOLDER = r"D:\Cursos\Lakehouse_TMW\cdc-kaggle\data"
LOCAL_ZIP_PATH = os.path.join(LOCAL_BASE_FOLDER, "kaggle_download.zip")
LOCAL_ACTUAL_FOLDER = os.path.join(LOCAL_BASE_FOLDER, "actual")

BUCKET_NAME = "treinamento-tmw"
S3_ACTUAL_PREFIX = "raw/data/actual/"
S3_LAST_PREFIX = "raw/data/last/"

DATASET_NAME = "teocalvo/teomewhy-loyalty-system"

# Inicializa cliente S3 e Kaggle API
s3_client = boto3.client("s3")
kaggle_api = KaggleApi()
spark = SparkSession.builder.appName("csv_to_parquet_upload").getOrCreate()

def download_kaggle_dataset():
    print("Autenticando Kaggle API...")
    kaggle_api.authenticate()

    print(f"Baixando dataset {DATASET_NAME} para {LOCAL_ZIP_PATH} ...")
    kaggle_api.dataset_download_files(DATASET_NAME, path=LOCAL_BASE_FOLDER, unzip=False, force=True)
    
    # Captura o nome do arquivo real baixado
    downloaded_zip = os.path.join(LOCAL_BASE_FOLDER, DATASET_NAME.split("/")[1] + ".zip")
    
    # Remove o arquivo kaggle_download.zip se existir antes de renomear
    if os.path.exists(LOCAL_ZIP_PATH):
        os.remove(LOCAL_ZIP_PATH)
    
    # Renomeia para kaggle_download.zip (mantém compatibilidade com o resto do código)
    os.rename(downloaded_zip, LOCAL_ZIP_PATH)

    print(f"Download concluído! Arquivo salvo em {LOCAL_ZIP_PATH}")

def extract_zip():
    print(f"Extraindo arquivos para {LOCAL_ACTUAL_FOLDER} ...")
    if os.path.exists(LOCAL_ACTUAL_FOLDER):
        # Limpa pasta atual antes da extração
        shutil.rmtree(LOCAL_ACTUAL_FOLDER)
    os.makedirs(LOCAL_ACTUAL_FOLDER, exist_ok=True)

    with zipfile.ZipFile(LOCAL_ZIP_PATH, 'r') as zip_ref:
        zip_ref.extractall(LOCAL_ACTUAL_FOLDER)
    print("Extração concluída!")

def list_s3_files(prefix):
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix)
    files = []
    for page in pages:
        for obj in page.get('Contents', []):
            files.append(obj['Key'])
    return files

def move_s3_objects(source_prefix, dest_prefix):
    print(f"Movendo arquivos existentes de s3://{BUCKET_NAME}/{source_prefix} para s3://{BUCKET_NAME}/{dest_prefix} ...")
    files = list_s3_files(source_prefix)
    for key in files:
        filename = key.split('/')[-1]
        copy_source = {'Bucket': BUCKET_NAME, 'Key': key}
        new_key = dest_prefix + filename

        # Copia arquivo para destino last
        s3_client.copy_object(Bucket=BUCKET_NAME, CopySource=copy_source, Key=new_key)
        # Deleta arquivo antigo da pasta actual
        s3_client.delete_object(Bucket=BUCKET_NAME, Key=key)
        print(f"✓ {filename} movido para last")
    if not files:
        print("Nenhum arquivo antigo encontrado para mover.")
    print("Movimentação concluída!")

def convert_and_upload_parquet():
    parquet_folder = os.path.join(LOCAL_BASE_FOLDER, "parquet_temp")
    if os.path.exists(parquet_folder):
        shutil.rmtree(parquet_folder)
    os.makedirs(parquet_folder, exist_ok=True)

    for filename in os.listdir(LOCAL_ACTUAL_FOLDER):
        if not filename.lower().endswith(".csv"):
            print(f"⚠ Ignorando {filename}")
            continue
        if filename.lower() == "database.db":
            print(f"⚠ Ignorando {filename}")
            continue

        csv_path = os.path.join(LOCAL_ACTUAL_FOLDER, filename)
        table_name = os.path.splitext(filename)[0]

        print(f"Lendo {filename} com Spark...")
        df = spark.read.csv(csv_path, header=True, sep=";")

        parquet_path = os.path.join(parquet_folder, table_name)
        print(f"Convertendo para Parquet: {parquet_path}")
        df.write.mode("overwrite").parquet(parquet_path)

    # Upload para o S3
    print(f"Enviando arquivos Parquet para s3://{BUCKET_NAME}/{S3_ACTUAL_PREFIX} ...")
    for root, _, files in os.walk(parquet_folder):
        for file in files:
            local_file = os.path.join(root, file)
            relative_path = os.path.relpath(local_file, parquet_folder)
            s3_key = os.path.join(S3_ACTUAL_PREFIX, relative_path).replace("\\", "/")
            s3_client.upload_file(local_file, BUCKET_NAME, s3_key)
            print(f"✓ {file} enviado como Parquet")
    print("Upload concluído!")

def convert_and_upload_cdc():
    LOCAL_CDC_FOLDER = os.path.join(LOCAL_BASE_FOLDER, "cdc")  # pasta local gerada pelo main.py
    PARQUET_CDC_FOLDER = os.path.join(LOCAL_BASE_FOLDER, "cdc_parquet_temp")
    S3_CDC_PREFIX = "raw/data/cdc/"

    # Limpa pasta temporária
    if os.path.exists(PARQUET_CDC_FOLDER):
        shutil.rmtree(PARQUET_CDC_FOLDER)
    os.makedirs(PARQUET_CDC_FOLDER, exist_ok=True)

    for filename in os.listdir(LOCAL_CDC_FOLDER):
        if not filename.lower().endswith(".csv"):
            print(f"⚠ Ignorando {filename}")
            continue

        csv_path = os.path.join(LOCAL_CDC_FOLDER, filename)
        table_name = os.path.splitext(filename)[0]

        print(f"[CDC] Lendo {filename} com Spark...")
        df = spark.read.csv(csv_path, header=True, sep=";")

        parquet_path = os.path.join(PARQUET_CDC_FOLDER, table_name)
        print(f"[CDC] Convertendo para Parquet: {parquet_path}")
        df.write.mode("overwrite").parquet(parquet_path)

    # Upload para o S3
    print(f"[CDC] Enviando arquivos Parquet para s3://{BUCKET_NAME}/{S3_CDC_PREFIX} ...")
    for root, _, files in os.walk(PARQUET_CDC_FOLDER):
        for file in files:
            local_file = os.path.join(root, file)
            relative_path = os.path.relpath(local_file, PARQUET_CDC_FOLDER)
            s3_key = os.path.join(S3_CDC_PREFIX, relative_path).replace("\\", "/")
            s3_client.upload_file(local_file, BUCKET_NAME, s3_key)
            print(f"✓ [CDC] {file} enviado como Parquet")
    print("[CDC] Upload concluído!")

def convert_and_upload_cdc():
    LOCAL_CDC_FOLDER = os.path.join(LOCAL_BASE_FOLDER, "cdc")  # pasta local gerada pelo main.py
    PARQUET_CDC_FOLDER = os.path.join(LOCAL_BASE_FOLDER, "cdc_parquet_temp")
    S3_CDC_PREFIX = "raw/data/cdc/"

    # Limpa pasta temporária
    if os.path.exists(PARQUET_CDC_FOLDER):
        shutil.rmtree(PARQUET_CDC_FOLDER)
    os.makedirs(PARQUET_CDC_FOLDER, exist_ok=True)

    for filename in os.listdir(LOCAL_CDC_FOLDER):
        if not filename.lower().endswith(".csv"):
            print(f"⚠ Ignorando {filename}")
            continue

        csv_path = os.path.join(LOCAL_CDC_FOLDER, filename)
        table_name = os.path.splitext(filename)[0]

        print(f"[CDC] Lendo {filename} com Spark...")
        df = spark.read.csv(csv_path, header=True, sep=";")

        parquet_path = os.path.join(PARQUET_CDC_FOLDER, table_name)
        print(f"[CDC] Convertendo para Parquet: {parquet_path}")
        df.write.mode("overwrite").parquet(parquet_path)

    # Upload para o S3
    print(f"[CDC] Enviando arquivos Parquet para s3://{BUCKET_NAME}/{S3_CDC_PREFIX} ...")
    for root, _, files in os.walk(PARQUET_CDC_FOLDER):
        for file in files:
            local_file = os.path.join(root, file)
            relative_path = os.path.relpath(local_file, PARQUET_CDC_FOLDER)
            s3_key = os.path.join(S3_CDC_PREFIX, relative_path).replace("\\", "/")
            s3_client.upload_file(local_file, BUCKET_NAME, s3_key)
            print(f"✓ [CDC] {file} enviado como Parquet")
    print("[CDC] Upload concluído!")

def main():
    download_kaggle_dataset()
    extract_zip()
    move_s3_objects(S3_ACTUAL_PREFIX, S3_LAST_PREFIX)
    convert_and_upload_parquet()
    convert_and_upload_cdc()
    print("\n✅ Processo completo finalizado!")

if __name__ == "__main__":
    main()