import csv
import glob
import json
import os
from sqlalchemy import MetaData, Table, create_engine, bindparam, UnicodeText
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, truncate_value

def get_images(json):

    images = []
    if isinstance(json['image'], list):
        for item in json['image']:
            if 'image' in item:
                image = item['image']
                images.append(image)

    return images

def find_record_csv(path_folder):
    """Procura o arquivo que contenha 'record.csv' no seu nome"""
    csv_files = glob.glob(os.path.join(path_folder, "*record.csv"))

    if not csv_files:
        print("Nenhum arquivo que contenha 'record.csv' foi encontrado.")
        return None

    csv_file = csv_files[0]
    print(f"✅ Arquivo encontrado: {csv_file}")

    return csv_file

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

metadata = MetaData()
historico_tbl = Table("Histórico de Clientes", metadata, schema=f"schema_{sid}", autoload_with=engine)
Base = declarative_base()

class Historico(Base):
    __table__ = historico_tbl

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

print("Sucesso! Inicializando migração de Históricos...")

csv_files = find_record_csv(path_file)

csv.field_size_limit(100000000000000)

df = pd.read_csv(csv_files, sep=None, engine='python')

log_folder = path_file

df["eventblock_pack"] = df["eventblock_pack"].astype(str).str.replace(r'^json::', '', regex=True)

log_folder = path_file
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for idx, row in df.iterrows():

    if idx % 1000 == 0 or idx == len(df):
        print(f"Processados: {idx} | Inseridos: {inserted_cont} | Não inseridos: {not_inserted_cont} | Concluído: {round((idx / len(df)) * 100, 2)}%")    

    date_str = f'{row["date"]} {row["start_time"]}'
    if not is_valid_date(date_str, '%Y-%m-%d %H:%M:%S'):
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Data inválida'
        not_inserted_data.append(row_dict)
        continue
    else:
        date = date_str

    if row['patient_id'] == None or row['patient_id'] == '' or row['patient_id'] == 'None':
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Paciente vazio'
        not_inserted_data.append(row_dict)
        continue
    else:
        id_patient = row["patient_id"]

    if not pd.isna(row["eventblock_pack"]) and isinstance(row["eventblock_pack"], str):
        try:
            json_data = json.loads(row["eventblock_pack"])
            if 'image' in json_data:
                images = get_images(json_data)
            else:
                not_inserted_cont +=1
                row_dict = row.to_dict()
                row_dict['Motivo'] = 'Não tem imagem no JSON'
                not_inserted_data.append(row_dict)
                continue
        except json.JSONDecodeError:
            print(f"Erro ao decodificar JSON na linha {idx + 2}. Pulando...")
            not_inserted_cont +=1
            row_dict = row.to_dict()
            row_dict['Motivo'] = 'JSON inválido'
            not_inserted_data.append(row_dict)
            continue
    else:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Campo de texto vazio ou inválido'
        not_inserted_data.append(row_dict)
        continue

    for image in images:
        
        existing_register = exists(session, image, "Classe", Historico)
        if existing_register:
            not_inserted_cont +=1
            row_dict = row.to_dict()
            row_dict['Motivo'] = 'Histórico já existe'
            not_inserted_data.append(row_dict)
            continue
        
        new_image = Historico(
            Histórico=image,
            Data=date,
            Classe=image
        )

        setattr(new_image, "Id do Cliente", row["patient_id"])
        # setattr(new_image, "Id do Histórico", 0 - count)
        setattr(new_image, "Id do Usuário", 0)

        log_data.append({
            # "Id do Histórico": row["pk"],
            "Id do Cliente": row["patient_id"],
            "Data": date,
            "Histórico": image,
            "Id do Usuário": 0,
            })
        
        session.add(new_image)

        inserted_cont+=1

    if inserted_cont % 1000 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos anexos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} anexos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_record_images.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_record_images.xlsx")