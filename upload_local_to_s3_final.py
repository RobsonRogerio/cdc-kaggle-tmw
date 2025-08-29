import os
import zipfile
import boto3
import shutil
import re
from kaggle.api.kaggle_api_extended import KaggleApi
from pyspark.sql import SparkSession
import glob

# ============================================================
# CONFIGURAÇÕES BÁSICAS
# ============================================================

LOCAL_BASE_FOLDER = r"D:\\Cursos\\Lakehouse_TMW\\cdc-kaggle-tmw\\data"
LOCAL_ZIP_PATH = os.path.join(LOCAL_BASE_FOLDER, "kaggle_download.zip")

# Pastas locais e respectivos destinos no S3 (exceto 'last', que é gerada no S3)
FOLDERS = {
    "actual": {
        "local": os.path.join(LOCAL_BASE_FOLDER, "actual"),
        "s3_prefix": "raw/data/actual/"
    },
    "cdc": {
        "local": os.path.join(LOCAL_BASE_FOLDER, "cdc"),
        "s3_prefix": "raw/upsell/cdc/"
    }
}

BUCKET_NAME = "treinamento-tmw"
DATASET_NAME = "teocalvo/teomewhy-loyalty-system"

# Mapeamento de nomes só para a pasta CDC
CDC_NAME_MAP = {
    "clientes": "customers",
    "transacao_produto": "transactions_product",
    "transacoes": "transactions"
}

# ============================================================
# INICIALIZAÇÃO DE CLIENTES
# ============================================================

s3_client = boto3.client("s3")
kaggle_api = KaggleApi()
spark = SparkSession.builder \
    .appName("csv_to_parquet_upload") \
    .config("spark.hadoop.native.io", "false") \
    .config("spark.driver.extraLibraryPath", r"C:\hadoop\bin") \
    .getOrCreate()

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

    for filename in os.listdir(LOCAL_FOLDER):
        if not filename.lower().endswith(".csv"):
            print(f"[{folder_key}] Ignorando não-CSV: {filename}")
            continue
        if filename.lower() == "database.db":
            print(f"[{folder_key}] Ignorando: {filename}")
            continue

        original_name = os.path.splitext(filename)[0]
        nome_limpo = re.sub(r'(_\d{8}_\d{6}|_\d{8}|\d{14})$', '', original_name)

        print(f"[DEBUG] original_name: {original_name}, nome_limpo (sem timestamp): {nome_limpo}")

        if folder_key == "cdc":
            if nome_limpo not in CDC_NAME_MAP:
                print(f"[AVISO] Nome '{nome_limpo}' não está no mapa de CDC! Usando nome original.")
            table_name = CDC_NAME_MAP.get(nome_limpo, nome_limpo)
        else:
            table_name = nome_limpo

        print(f"[DEBUG] filename: {filename}, original_name: {original_name}, nome_limpo: {nome_limpo}, table_name: {table_name}")

        csv_path = os.path.join(LOCAL_FOLDER, filename)
        if not os.path.isfile(csv_path):
            print(f"[ERRO] Arquivo para leitura nao existe: {csv_path}")
            continue
        print(f"[{folder_key}] Lendo {filename} com Spark...")
        df = spark.read.csv(csv_path, header=True, sep=";")

        # Salvar CSV local (mantendo histórica e adicionando, sem apagar)
        if folder_key == "cdc":
            csv_output_file = os.path.join(FOLDERS["cdc"]["local"], f"{original_name}.csv")
            if os.path.exists(csv_output_file):
                print(f"[DEBUG] Arquivo já existe: {csv_output_file}, não será apagado nem sobrescrito.")
            else:
                # Para gerar o CSV somente se não existir ainda
                temp_csv_dir = os.path.join(LOCAL_BASE_FOLDER, "temp_csv_dir")
                if os.path.exists(temp_csv_dir):
                    shutil.rmtree(temp_csv_dir)

                df.coalesce(1).write.mode("overwrite").option("header", True).csv(temp_csv_dir)

                csv_files = glob.glob(os.path.join(temp_csv_dir, "*.csv"))
                if csv_files:
                    shutil.move(csv_files[0], csv_output_file)
                    print(f"[{folder_key}] Salvo CSV local: {csv_output_file}")
                else:
                    print(f"[ERRO] Nenhum CSV encontrado no diretório temporário {temp_csv_dir}")
                shutil.rmtree(temp_csv_dir)

        # Gerar parquet temporário para upload direto ao S3 e apagar depois
        print(f"[{folder_key}] Convertendo para Parquet e enviando direto para S3 em: {S3_PREFIX}{table_name}/")

        s3_tmp_dir = os.path.join(LOCAL_BASE_FOLDER, "tmp_s3_parquet")
        if os.path.exists(s3_tmp_dir):
            shutil.rmtree(s3_tmp_dir)
        os.makedirs(s3_tmp_dir)

        tmp_parquet_path = os.path.join(s3_tmp_dir, "data")
        df.write.mode("overwrite").parquet(tmp_parquet_path)

        for root, _, files in os.walk(tmp_parquet_path):
            for file in files:
                local_file = os.path.join(root, file)
                s3_key = os.path.join(S3_PREFIX, table_name, file).replace("\\", "/")
                s3_client.upload_file(local_file, BUCKET_NAME, s3_key)
                print(f"✓ [{folder_key}] {file} enviado para {s3_key}")

        shutil.rmtree(s3_tmp_dir)

    print(f"[{folder_key}] Upload concluído!")

def print_s3_structure():
    """Lista o conteúdo esperado no S3 após execução."""
    print("\n📂 Estrutura final no S3:")
    s3_prefixes = [
        "raw/data/actual/",
        "raw/data/last/",
        "raw/upsell/cdc/customers/",
        "raw/upsell/cdc/transactions_product/",
        "raw/upsell/cdc/transactions/"
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