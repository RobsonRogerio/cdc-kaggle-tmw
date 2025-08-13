import os
import zipfile
import boto3
import shutil
from kaggle.api.kaggle_api_extended import KaggleApi
from pyspark.sql import SparkSession

# ============================================================
# CONFIGURAÇÕES BÁSICAS
# ============================================================

LOCAL_BASE_FOLDER = r"D:\Cursos\Lakehouse_TMW\cdc-kaggle\data"
LOCAL_ZIP_PATH = os.path.join(LOCAL_BASE_FOLDER, "kaggle_download.zip")

# Pasta actual e cdc serão tratadas localmente
FOLDERS = {
    "actual": {
        "local": os.path.join(LOCAL_BASE_FOLDER, "actual"),
        "s3_prefix": "raw/data/actual/"
    },
    "cdc": {
        "local": os.path.join(LOCAL_BASE_FOLDER, "cdc"),
        "s3_prefix": "raw/data/cdc/"
    }
}

BUCKET_NAME = "treinamento-tmw"
DATASET_NAME = "teocalvo/teomewhy-loyalty-system"

# ============================================================
# INICIALIZAÇÃO DE CLIENTES
# ============================================================

s3_client = boto3.client("s3")
kaggle_api = KaggleApi()
spark = SparkSession.builder.appName("csv_to_parquet_upload").getOrCreate()

# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def download_kaggle_dataset():
    """Baixa o dataset do Kaggle como zip."""
    print("Autenticando Kaggle API...")
    kaggle_api.authenticate()
    print(f"Baixando dataset {DATASET_NAME} ...")
    kaggle_api.dataset_download_files(DATASET_NAME, path=LOCAL_BASE_FOLDER, unzip=False, force=True)

    downloaded_zip = os.path.join(LOCAL_BASE_FOLDER, DATASET_NAME.split("/")[1] + ".zip")

    if os.path.exists(LOCAL_ZIP_PATH):
        os.remove(LOCAL_ZIP_PATH)

    os.rename(downloaded_zip, LOCAL_ZIP_PATH)
    print(f"Download concluído! Arquivo: {LOCAL_ZIP_PATH}")

def extract_zip():
    """Extrai o zip para a pasta 'actual' local."""
    actual_folder = FOLDERS["actual"]["local"]
    print(f"Extraindo arquivos para {actual_folder} ...")
    if os.path.exists(actual_folder):
        shutil.rmtree(actual_folder)
    os.makedirs(actual_folder, exist_ok=True)

    with zipfile.ZipFile(LOCAL_ZIP_PATH, 'r') as zip_ref:
        zip_ref.extractall(actual_folder)

    print("Extração concluída!")

def list_s3_files(prefix):
    """Lista arquivos em um prefixo do S3."""
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix)
    files = []
    for page in pages:
        for obj in page.get('Contents', []):
            files.append(obj['Key'])
    return files

def move_s3_objects(source_prefix, dest_prefix):
    """Move (copia e depois deleta) arquivos de um prefixo para outro no S3."""
    print(f"Movendo dados antigos de s3://{BUCKET_NAME}/{source_prefix} para s3://{BUCKET_NAME}/{dest_prefix} ...")
    files = list_s3_files(source_prefix)
    for key in files:
        filename = key.split('/')[-1]
        copy_source = {'Bucket': BUCKET_NAME, 'Key': key}
        new_key = dest_prefix + filename

        s3_client.copy_object(Bucket=BUCKET_NAME, CopySource=copy_source, Key=new_key)
        s3_client.delete_object(Bucket=BUCKET_NAME, Key=key)
        print(f"✓ {filename} movido para {dest_prefix}")
    if not files:
        print("Nenhum arquivo antigo encontrado.")
    print("Movimentação concluída!")

def convert_and_upload_parquet_for_folder(folder_key):
    """
    Converte CSV para Parquet e faz upload para o S3.
    Apenas para pastas locais (actual, cdc).
    """
    LOCAL_FOLDER = FOLDERS[folder_key]["local"]
    S3_PREFIX = FOLDERS[folder_key]["s3_prefix"]
    PARQUET_FOLDER = os.path.join(LOCAL_BASE_FOLDER, f"{folder_key}_parquet_temp")

    if os.path.exists(PARQUET_FOLDER):
        shutil.rmtree(PARQUET_FOLDER)
    os.makedirs(PARQUET_FOLDER, exist_ok=True)

    for filename in os.listdir(LOCAL_FOLDER):
        if not filename.lower().endswith(".csv"):
            print(f"[{folder_key}] Ignorando não-CSV: {filename}")
            continue
        if filename.lower() == "database.db":
            print(f"[{folder_key}] Ignorando: {filename}")
            continue
        
        csv_path = os.path.join(LOCAL_FOLDER, filename)
        table_name = os.path.splitext(filename)[0]

        print(f"[{folder_key}] Lendo {filename} com Spark...")
        df = spark.read.csv(csv_path, header=True, sep=";")

        parquet_path = os.path.join(PARQUET_FOLDER, table_name)
        print(f"[{folder_key}] Convertendo para Parquet: {parquet_path}")
        df.write.mode("overwrite").parquet(parquet_path)

    print(f"[{folder_key}] Fazendo upload para s3://{BUCKET_NAME}/{S3_PREFIX} ...")
    for root, _, files in os.walk(PARQUET_FOLDER):
        for file in files:
            local_file = os.path.join(root, file)
            relative_path = os.path.relpath(local_file, PARQUET_FOLDER)
            s3_key = os.path.join(S3_PREFIX, relative_path).replace("\\", "/")
            s3_client.upload_file(local_file, BUCKET_NAME, s3_key)
            print(f"✓ [{folder_key}] {file} enviado")
    print(f"[{folder_key}] Upload concluído!")

# ============================================================
# FLUXO PRINCIPAL
# ============================================================

def main():
    # 1️⃣ Baixa o dataset do Kaggle
    download_kaggle_dataset()

    # 2️⃣ Extrai para pasta 'actual'
    extract_zip()

    # 3️⃣ Copia os dados existentes no S3/actual para S3/last
    move_s3_objects("raw/data/actual/", "raw/data/last/")

    # 4️⃣ Converte e envia 'actual' e 'cdc' (pastas locais)
    for folder_key in FOLDERS:
        convert_and_upload_parquet_for_folder(folder_key)

    print("\n✅ Processo completo finalizado!")

if __name__ == "__main__":
    main()