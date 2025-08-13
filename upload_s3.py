import os
import boto3

# ===== CONFIGURAÇÕES =====
# Caminho local da pasta "actual"
LOCAL_FOLDER = r"D:\Cursos\Lakehouse_TMW\cdc-kaggle\data\actual"

# Nome do bucket e prefixo no S3
BUCKET_NAME = "treinamento-tmw"
S3_PREFIX = "raw/data/actual/"

# =================================
# Inicia cliente S3 (usa credenciais já configuradas no AWS CLI)
s3_client = boto3.client("s3")

def upload_files():
    # Lista todos os arquivos na pasta local
    for filename in os.listdir(LOCAL_FOLDER):

        # Ignora o arquivo database.db
        if filename.lower() == "database.db":
            print(f"⚠ Ignorando {filename}")
            continue
        
        local_path = os.path.join(LOCAL_FOLDER, filename)

        # pula subpastas, só envia arquivos
        if not os.path.isfile(local_path):
            continue

        # Monta caminho final no S3
        s3_key = S3_PREFIX + filename

        print(f"Enviando {local_path} para s3://{BUCKET_NAME}/{s3_key} ...")
        s3_client.upload_file(local_path, BUCKET_NAME, s3_key)
        print(f"✓ {filename} enviado com sucesso!")

if __name__ == "__main__":
    upload_files()
    print("\n✅ Upload finalizado!")
