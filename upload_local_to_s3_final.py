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

# Pastas locais e respectivos destinos no S3 (exceto 'last', que é gerada no S3)
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

# Mapeamento de nomes só para a pasta CDC
CDC_NAME_MAP = {
    "clientes": "customers",
    "transacao_produto": "transactions_product",
    "transacoes": "transactions",
    "transacao": "transactions"  # 🔹 Corrige pasta indevida "transacao"
}

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
    actual_folder = FOLDERS["actual"]["local"]
    print(f"Extraindo arquivos para {actual_folder} ...")
    if os.path.exists(actual_folder):
        shutil.rmtree(actual_folder)
    os.makedirs(actual_folder, exist_ok=True)

    with zipfile.ZipFile(LOCAL_ZIP_PATH, 'r') as zip_ref:
        zip_ref.extractall(actual_folder)

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
        
        # Nome original sem extensão
        original_name = os.path.splitext(filename)[0]
        # Remove sufixo de timestamp, se existir
        nome_limpo = original_name.split("_")[0]

        # Se for pasta CDC, aplica mapeamento de nomes
        if folder_key == "cdc":
            if nome_limpo not in CDC_NAME_MAP:
                print(f"[AVISO] Nome '{nome_limpo}' não está no mapa de CDC! Usando nome original.")
            table_name = CDC_NAME_MAP.get(nome_limpo, nome_limpo)
        else:
            table_name = nome_limpo

        csv_path = os.path.join(LOCAL_FOLDER, filename)

        print(f"[{folder_key}] Lendo {filename} com Spark...")
        df = spark.read.csv(csv_path, header=True, sep=";")

        parquet_path = os.path.join(PARQUET_FOLDER, table_name)
        print(f"[{folder_key}] Convertendo para Parquet: {parquet_path}")
        df.write.mode("overwrite").parquet(parquet_path)

    print(f"[{folder_key}] Fazendo upload para s3://{BUCKET_NAME}/{S3_PREFIX} ...")
    for root, _, files in os.walk(PARQUET_FOLDER):
        for file in files:
            local_file = os.path.join(root, file)
            table_name_path = os.path.relpath(root, PARQUET_FOLDER).replace("\\", "/")
            s3_key = os.path.join(S3_PREFIX, table_name_path, file).replace("\\", "/")
            s3_client.upload_file(local_file, BUCKET_NAME, s3_key)
            print(f"✓ [{folder_key}] {file} enviado para {s3_key}")
    print(f"[{folder_key}] Upload concluído!")

def print_s3_structure():
    """Lista o conteúdo esperado no S3 após execução."""
    print("\n📂 Estrutura final no S3:")
    s3_prefixes = [
        "raw/data/actual/",
        "raw/data/last/",
        "raw/data/cdc/customers/",
        "raw/data/cdc/transactions_product/",
        "raw/data/cdc/transactions/"
    ]
    for prefix in s3_prefixes:
        print(f"\n🗂 {prefix}")
        files = list_s3_files(prefix)
        if not files:
            print("  (vazio)")
        else:
            for key in files:
                print(f"  - {key}")

# ============================================================
# FLUXO PRINCIPAL
# ============================================================

def main():
    download_kaggle_dataset()
    extract_zip()
    move_s3_objects("raw/data/actual/", "raw/data/last/")

    for folder_key in FOLDERS:
        convert_and_upload_parquet_for_folder(folder_key)

    print("\n✅ Processo completo finalizado!")
    print_s3_structure()

if __name__ == "__main__":
    main()