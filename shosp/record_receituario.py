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

def get_record(row):
    record = ''

    if not (row['Item'] in [None, '', 'None'] or pd.isna(row['Item'])):
        record += f"Item: {row['Item']}<br>"
    
    if not (row['Dosagem'] in [None, '', 'None'] or pd.isna(row['Dosagem'])):
        record += f"Dosagem: {row['Dosagem']}<br>"

    if not (row['Frequencia'] in [None, '', 'None'] or pd.isna(row['Frequencia'])):
        record += f"Frequencia: {row['Frequencia']}<br>"
    
    if not (row['Observacao'] in [None, '', 'None'] or pd.isna(row['Observacao'])):
        record += f"Observacao: {row['Observacao']}<br>"
    
    if not (row['Quantidade'] in [None, '', 'None'] or pd.isna(row['Quantidade'])):
        record += f"Quantidade: {row['Quantidade']}<br>"
    
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

todos_arquivos = glob.glob(f'{path_file}/receituario-*.csv')

csv.field_size_limit(1000000)
df = pd.read_csv(todos_arquivos[0], sep=',')
df = df.replace('None', '')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0
id_cont = 8715

for _, row in df.iterrows():

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

    record = get_record(row)
    if record == '':
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Histórico vazio'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    if is_valid_date(str(row['Data']), '%Y-%m-%d'):
        date = f'{str(row['Data'])} 00:00'
    else:
        date = '01/01/1900 00:00'

    if row['Prontuario'] in [None, '', 'None'] or pd.isna(row['Prontuario']):
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do paciente vazio'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    else:
        id_patient = row['Prontuario']
    
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

session.commit()

print(f"{inserted_cont} novos históricos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} históricos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_record_receituario.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_record_receituario.xlsx")
