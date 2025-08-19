import subprocess
import sys
import os
import boto3
import shutil
import re
from pyspark.sql import SparkSession
import glob

LOCAL_BASE_FOLDER = r"D:\\Cursos\\Lakehouse_TMW\\cdc-kaggle-tmw\\data"
BUCKET_NAME = "treinamento-tmw"

FOLDERS = {
    "cdc": {
        "local": os.path.join(LOCAL_BASE_FOLDER, "cdc"),
        "s3_prefix": "raw/data/cdc/"
    }
}

CDC_NAME_MAP = {
    "clientes": "customers",
    "transacao_produto": "transactions_product",
    "transacoes": "transactions"
}

s3_client = boto3.client("s3")
spark = SparkSession.builder.appName("cdc_upload_only").getOrCreate()


def run_main_py():
    print("Executando main.py...")
    try:
        # Substitua 'python' por 'python3' se precisar, e ajuste o caminho se main.py estiver em pasta diferente
        result = subprocess.run([sys.executable, 'main.py'], capture_output=True, text=True, check=True)
        print("main.py executado com sucesso.")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print("Erro ao executar main.py:")
        print(e.stdout)
        print(e.stderr)
        sys.exit(1)


def upload_cdc_to_s3():
    LOCAL_FOLDER = FOLDERS["cdc"]["local"]
    S3_PREFIX = FOLDERS["cdc"]["s3_prefix"]

    for filename in os.listdir(LOCAL_FOLDER):
        if not filename.lower().endswith(".csv"):
            print(f"[cdc] Ignorando não-CSV: {filename}")
            continue

        original_name = os.path.splitext(filename)[0]
        nome_limpo = re.sub(r'(_\d{8}_\d{6}|_\d{8}|\d{14})$', '', original_name)

        if nome_limpo not in CDC_NAME_MAP:
            print(f"[cdc][AVISO] Nome '{nome_limpo}' não está no mapa CDC. Usando nome original.")
        table_name = CDC_NAME_MAP.get(nome_limpo, nome_limpo)

        csv_path = os.path.join(LOCAL_FOLDER, filename)
        if not os.path.isfile(csv_path):
            print(f"[cdc][ERRO] Arquivo não encontrado: {csv_path}")
            continue

        print(f"[cdc] Lendo {filename} com Spark para converter e enviar ao S3...")
        df = spark.read.csv(csv_path, header=True, sep=";")

        # Diretório temporário para parquet
        tmp_parquet_dir = os.path.join(LOCAL_BASE_FOLDER, "tmp_upload_parquet")
        if os.path.exists(tmp_parquet_dir):
            shutil.rmtree(tmp_parquet_dir)
        os.makedirs(tmp_parquet_dir)

        parquet_path = os.path.join(tmp_parquet_dir, "data")
        df.write.mode("overwrite").parquet(parquet_path)

        for root, _, files in os.walk(parquet_path):
            for file in files:
                local_file = os.path.join(root, file)
                s3_key = os.path.join(S3_PREFIX, table_name, file).replace("\\", "/")
                s3_client.upload_file(local_file, BUCKET_NAME, s3_key)
                print(f"✓ [cdc] {file} enviado para {s3_key}")

        shutil.rmtree(tmp_parquet_dir)

    print("[cdc] Upload dos arquivos CDC concluído!")


if __name__ == "__main__":
    run_main_py()         # Executa o main.py para gerar os arquivos CDC localmente
    upload_cdc_to_s3()    # Executa upload dos arquivos gerados para S3