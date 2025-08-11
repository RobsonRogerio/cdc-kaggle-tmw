import datetime
import json
import os
import shutil
import time

import dotenv
import pandas as pd


print(dotenv.load_dotenv(".env"))

with open("config.json", "r") as f:
    CONFIG = json.load(f)
    
KAGGLE_USERNAME = os.getenv("KAGGLE_USERNAME")
KAGGLE_KEY = os.getenv("KAGGLE_KEY")


def get_update_lines(df_last, df_actual, pk, date_field):

    df_update = df_last.merge(
        df_actual,
        how="left",
        on=[pk],
        suffixes=('_x', '_y')
    )

    update_flag = df_update[date_field + '_y'] > df_update[date_field + '_x']
    ids_updated = df_update[update_flag][pk].tolist()

    df_update = df_actual[df_actual[pk].isin(ids_updated)].copy()
    df_update["op"] = "U"
    return df_update


def get_insert_lines(df_last, df_actual, pk):
    df_insert = df_actual[~df_actual[pk].isin(df_last[pk])].copy()
    df_insert["op"] = "I"
    return df_insert


def get_delete_lines(df_last, df_actual, pk):
    df_delete = df_last[~df_last[pk].isin(df_actual[pk])].copy()
    df_delete["op"] = "D"
    return df_delete


def create_cdc(df_last, df_actual, pk, date_field):
    df_update = get_update_lines(df_last, df_actual, pk, date_field)
    df_insert = get_insert_lines(df_last, df_actual, pk)
    df_delete = get_delete_lines(df_last, df_actual, pk)
    df_cdc = pd.concat([df_update, df_insert, df_delete], ignore_index=True)
    return df_cdc

def process_cdc(tables):
    print("Processando CDC de todas tabelas...")

    for t in tables:
        file_last = f"./data/last/{t['name']}.csv"
        file_actual = f"./data/actual/{t['name']}.csv"

        # Leitura da versão anterior
        print(f"[LOG] Tentando ler versão anterior: {file_last}")
        skipped_lines_last = 0
        try:
            df_last = pd.read_csv(file_last, sep=t["sep"], on_bad_lines='skip')
        except pd.errors.ParserError as e:
            print(f"[ERRO] ParserError em {file_last}: {e}")
            continue
        except Exception as e:
            print(f"[ERRO] Falha ao ler {file_last}: {e}")
            continue

        # Leitura da versão atual
        print(f"[LOG] Tentando ler versão atual: {file_actual}")
        try:
            df_actual = pd.read_csv(file_actual, sep=t["sep"], on_bad_lines='skip')
        except pd.errors.ParserError as e:
            print(f"[ERRO] ParserError em {file_actual}: {e}")
            continue
        except Exception as e:
            print(f"[ERRO] Falha ao ler {file_actual}: {e}")
            continue

        # Processa o CDC
        df_cdc = create_cdc(df_last, df_actual, t["pk"], t["date_field"])

        if df_cdc.shape[0] == 0:
            print(f"Nenhuma alteração encontrada para a tabela {t['name']}.")
            continue

        if not os.path.exists("./data/cdc"):
            os.makedirs("./data/cdc")

        now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"./data/cdc/{t['name']}_{now}.csv"

        print(f"[LOG] Salvando CDC de {t['name']} em {filename}")
        df_cdc.to_csv(filename, index=False, sep=t["sep"])

    print("CDC processado com sucesso!")


def download_kaggle_dataset(dataset_name):
    
    from kaggle.api.kaggle_api_extended import KaggleApi
    api = KaggleApi()
    api.authenticate()

    print(f"Baixando o dataset {dataset_name} para ./data/actual/...")
    api.dataset_download_files(dataset_name, path="data/actual/", unzip=True)
    print("Download concluído!")


def move_from_actual_to_last():
    
    actual_path = "./data/actual"
    last_path = "./data/last"

    if not os.path.exists(last_path):
        os.makedirs(last_path)

    print("Movendo arquivos de ./data/actual para ./data/last...")

    for item in os.listdir(actual_path):
        source = os.path.join(actual_path, item)
        destination = os.path.join(last_path, item)
        shutil.move(source, destination)

    print("Arquivos movidos com sucesso!")

def main():

    timer = CONFIG["timer"]["value"]
    
    if CONFIG["timer"]["unit"] == "minutes":
        timer *= 60
    elif CONFIG["timer"]["unit"] == "hours":
        timer *= 3600
    elif CONFIG["timer"]["unit"] == "days":
        timer *= 86400

    while True:
        dataset_name = CONFIG["dataset_name"]
        
        move_from_actual_to_last()
        download_kaggle_dataset(dataset_name)
        process_cdc(CONFIG["tables"])

        time.sleep(timer)
    

if __name__ == "__main__":
    main()