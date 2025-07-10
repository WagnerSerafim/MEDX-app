import glob
import json
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
import csv
from datetime import datetime
from utils.utils import is_valid_date, exists, create_log

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")

print("Conectando no Banco de Dados...")
DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine)

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

HistoricoClientes = getattr(Base.classes, "Histórico de Clientes")

print("Sucesso! Inicializando migração de Históricos...")

todos_arquivos = glob.glob(f'{path_file}/metadados-anexo-*.csv')
schedule_files = glob.glob(f'{path_file}/atendimento-*.csv')

csv.field_size_limit(1000000)
df = pd.read_csv(todos_arquivos[0], sep=',')
df = df.replace('None', '')

csv.field_size_limit(1000000)
df_schedule = pd.read_csv(schedule_files[0], sep=',')
df_schedule = df_schedule.replace('None', '')

schedule_lookup = {row['Atendimento']:row['Prontuario'] for _,row in df_schedule.iterrows() if pd.notna(row['Atendimento']) and pd.notna(row['Prontuario'])}

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0
id_cont = 0

for _, row in df.iterrows():

    if row['Arquivo'] in [None, '', 'None'] or pd.isna(row['Arquivo']):
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Histórico vazio ou inválido'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    else:
        record = row['Arquivo']

    date = '01/01/1900 00:00'

    if row['Atendimento'] in [None, '', 'None'] or pd.isna(row['Atendimento']):
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do atendimento vazio ou inválido'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    else:
        id_patient = schedule_lookup.get(row['Atendimento'], None)

    classe_record = row['Arquivo']
    
    new_record = HistoricoClientes(
        Histórico = record,
        Data = date,
        Classe = classe_record
    )
    # setattr(new_record, "Id do Histórico", id_record)
    setattr(new_record, "Id do Cliente", id_patient)
    setattr(new_record, "Id do Usuário", 0)
    
    log_data.append({
        # "Id do Histórico": id_record,
        "Id do Cliente": id_patient,
        "Data": date,
        "Histórico": record,
        "Id do Usuário": 0,
    })
    session.add(new_record)
    inserted_cont += 1

    if inserted_cont % 1000 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos históricos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} históricos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_record_metadados.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_record_metadados.xlsx")
