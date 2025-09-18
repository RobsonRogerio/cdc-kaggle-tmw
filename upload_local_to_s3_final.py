# ============================================================
# IMPORTS
# ============================================================

# Bibliotecas padrão do Python
import os
import shutil
import zipfile
import sys
import subprocess
import glob
import re
from datetime import datetime

# AWS / Kaggle
import boto3
from kaggle.api.kaggle_api_extended import KaggleApi

# PySpark
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DecimalType
)
from pyspark.sql.functions import col, lit

import subprocess
import sys

def run_main_py(timeout=300):  # timeout padrão: 5 minutos
    print("Executando main.py...")
    try:
        # stdout=sys.stdout / stderr=sys.stderr fazem o log aparecer em tempo real no console
        result = subprocess.run(
            [sys.executable, "main.py"],
            stdout=sys.stdout,
            stderr=sys.stderr,
            text=True,
            check=True,
            timeout=timeout
        )
        print("\n✅ main.py executado com sucesso.")
    except subprocess.TimeoutExpired:
        print(f"\n⏱️ Erro: main.py demorou mais de {timeout} segundos e foi finalizado.")
    except subprocess.CalledProcessError as e:
        print("\n❌ Erro ao executar main.py:")
        print(e)
        sys.exit(1)

# ============================================================
# CONFIGURAÇÕES BÁSICAS
# ============================================================

LOCAL_BASE_FOLDER = r"D:\Cursos\Lakehouse_TMW\cdc-kaggle-tmw\data"
LOCAL_ZIP_PATH = os.path.join(LOCAL_BASE_FOLDER, "kaggle_download.zip")

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

CDC_NAME_MAP = {
    "clientes": "customers",
    "transacao_produto": "transactions_product",
    "transacoes": "transactions",
    "produtos": "products"
}

# ============================================================
# SCHEMAS DAS TABELAS (datas como string)
# ============================================================

SCHEMA_CLIENTES = StructType([
    StructField("IdCliente", StringType(), True),
    StructField("FlEmail", LongType(), True),
    StructField("FlTwitch", LongType(), True),
    StructField("FlYouTube", LongType(), True),
    StructField("FlBlueSky", LongType(), True),
    StructField("FlInstagram", LongType(), True),
    StructField("QtdePontos", LongType(), True),
    StructField("DtCriacao", StringType(), True),
    StructField("DtAtualizacao", StringType(), True),
    StructField("op", StringType(), True)
])

SCHEMA_TRANSACOES = StructType([
    StructField("IdTransacao", StringType(), True),
    StructField("IdCliente", StringType(), True),
    StructField("DtCriacao", StringType(), True),
    StructField("QtdePontos", StringType(), True),
    StructField("DescSistemaOrigem", StringType(), True),
    StructField("op", StringType(), True)
])

SCHEMA_TRANSACAO_PRODUTO = StructType([
    StructField("IdTransacaoProduto", StringType(), True),
    StructField("IdTransacao", StringType(), True),
    StructField("IdProduto", StringType(), True),
    StructField("QtdeProduto", LongType(), True),
    StructField("VlProduto", DecimalType(12, 2), True),
    StructField("op", StringType(), True)
])

SCHEMA_PRODUTOS = StructType([
    StructField("IdProduto", StringType(), True),
    StructField("DescNomeProduto", StringType(), True),
    StructField("DescDescricaoProduto", StringType(), True),
    StructField("DescCategoriaProduto", StringType(), True),
    StructField("op", StringType(), True)
])

TABELA_SCHEMA = {
    "customers": SCHEMA_CLIENTES,
    "transactions_product": SCHEMA_TRANSACAO_PRODUTO,
    "transactions": SCHEMA_TRANSACOES,
    "products": SCHEMA_PRODUTOS
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

# ============================================================
# FUNÇÃO PRINCIPAL DE CONVERSÃO E UPLOAD
# ============================================================

def convert_and_upload_parquet_for_folder(folder_key):
    LOCAL_FOLDER = FOLDERS[folder_key]["local"]
    S3_PREFIX = FOLDERS[folder_key]["s3_prefix"]

    for filename in os.listdir(LOCAL_FOLDER):
        if not filename.lower().endswith(".csv") or filename.lower() == "database.db":
            continue

        original_name = os.path.splitext(filename)[0]
        nome_limpo = re.sub(r'(_\d{8}_\d{6}|_\d{8}|\d{14})$', '', original_name)

        table_name = CDC_NAME_MAP.get(nome_limpo, nome_limpo) if folder_key == "cdc" else nome_limpo
        csv_path = os.path.join(LOCAL_FOLDER, filename)
        if not os.path.isfile(csv_path):
            continue

        # Leitura CSV com schema definido apenas para CDC
        if folder_key == "cdc":
            schema = TABELA_SCHEMA.get(table_name)
            if schema is None:
                continue
            df = spark.read.csv(csv_path, header=True, sep=";", schema=schema)
        else:
            df = spark.read.csv(csv_path, header=True, sep=";")

        # Adiciona change_timestamp
        if folder_key == "cdc":
            df = df.withColumn("change_timestamp", lit(datetime.utcnow()))

        # =================================================================
        # Conversão de colunas de quantidade para bigint
        # =================================================================
        for col_name in df.columns:
            if col_name.lower().startswith("qtde"):
                # Converte string "1.0" → double → bigint
                df = df.withColumn(col_name, col(col_name).cast("double").cast("bigint"))

        # ---- Ajuste específico para transações ----
        if folder_key == "cdc" and table_name == "transactions":
            df = df.withColumn("QtdePontos_cast", col("QtdePontos").cast("long"))

            print("\n🔎 Amostra de QtdePontos antes/depois do cast:")
            df.select("QtdePontos", "QtdePontos_cast").show(20, truncate=False)

            # Verifica se houve valores inválidos
            nulls = df.filter(col("QtdePontos").isNotNull() & col("QtdePontos_cast").isNull()).count()
            if nulls > 0:
                print(f"⚠️ {nulls} valores de QtdePontos não puderam ser convertidos para long.")

            # Substitui a coluna original pela convertida
            df = df.drop("QtdePontos").withColumnRenamed("QtdePontos_cast", "QtdePontos")
        # ------------------------------------------

        # Converte para Parquet e envia para S3
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
        "raw/upsell/cdc/transactions/",
        "raw/upsell/cdc/products/"
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
    run_main_py()
    download_kaggle_dataset()
    extract_zip()
    move_s3_objects("raw/data/actual/", "raw/data/last/")

    for folder_key in FOLDERS:
        convert_and_upload_parquet_for_folder(folder_key)

    print("\n✅ Processo completo finalizado!")
    #print_s3_structure()

if __name__ == "__main__":
    main()