import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
import csv
from datetime import datetime
from utils.utils import is_valid_date, exists, create_log

def get_record():
    record = ''

    if row['qp'] not in [None, '', 'None'] and not pd.isna(row['qp']):
        record += f"Queixa Principal: {row['qp']}<br>"
    
    if row['hda'] not in [None, '', 'None'] and not pd.isna(row['hda']):
        record += f"HDA: {row['hda']}<br>"
    
    if row['hpp'] not in [None, '', 'None'] and not pd.isna(row['hpp']):
        record += f"HPP: {row['hpp']}<br>"

    if row['hf'] not in [None, '', 'None'] and not pd.isna(row['hf']):
        record += f"HF: {row['hf']}<br>"
    
    if row['especifico'] not in [None, '', 'None'] and not pd.isna(row['especifico']):
        record += f"Específico: {row['especifico']}<br>"

    if row['condulta'] not in [None, '', 'None'] and not pd.isna(row['condulta']):
        record += f"Conduta: {row['condulta']}<br>"

    return record

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

todos_arquivos = glob.glob(f'{path_file}/consultas.csv')

csv.field_size_limit(1000000)
df = pd.read_csv(todos_arquivos[0], sep=';', engine='python', encoding='ISO-8859-1', dtype=str)
df = df.replace('None', '')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0
id_cont = 4832

for idx, row in df.iterrows():

    id_cont += 1

    if exists(session, id_cont, "Id do Histórico", HistoricoClientes):
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Histórico já existe'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    else:
        id_record = id_cont

    record = get_record()
    if record in [None, '', 'None'] or pd.isna(record):
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Histórico vazio'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    date_str = f'{row['data'][:4]}-{row['data'][4:6]}-{row['data'][6:]} {row['hora'][:2]}:{row['hora'][2:4]}:{row['hora'][4:]}'
    if is_valid_date(date_str, '%Y-%m-%d %H:%M:%S'):
        date = date_str
    else:
        date = '01/01/1900 00:00'

    if row['ident'] in [None, '', 'None'] or pd.isna(row['ident']):
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do paciente vazio'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    else:
        id_patient = int(row['ident'])
    
    new_record = HistoricoClientes(
        Histórico=record,
        Data=date
    )
    setattr(new_record, "Id do Histórico", id_record)
    setattr(new_record, "Id do Cliente", id_patient)
    setattr(new_record, "Id do Usuário", 0)
    
    log_data.append({
        "Id do Histórico": id_record,
        "Id do Cliente": id_patient,
        "Data": date,
        "Histórico": record,
        "Id do Usuário": 0,
    })
    session.add(new_record)
    inserted_cont+=1

    if inserted_cont % 1000 == 0:
        session.commit()

    if (idx + 1) % 1000 == 0 or (idx + 1) == len(df):
        print(f"Processados {idx + 1} de {len(df)} registros ({(idx + 1) / len(df) * 100:.2f}%)")

session.commit()

print(f"{inserted_cont} novos históricos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} históricos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_record_consulta.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_record_consulta.xlsx")
