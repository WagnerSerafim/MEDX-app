import csv
import glob
import json
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import urllib
from striprtf.striprtf import rtf_to_text
from utils.utils import create_log, is_valid_date, exists
    
def get_record(row):
    record = ""

    if row['queixa'] != "" and row['queixa'] != "." and row['queixa'] != "," and row['queixa'] != None:
        record += f"Consulta Queixa: {row['queixa']}<br>"

    if row['conduta'] != "" and row['conduta'] != "." and row['conduta'] != "," and row['conduta'] != None:
        record = f"Consulta Conduta: {row['conduta']}<br>"

    return record

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho do arquivo que contém os dados: ")

print("Conectando no Banco de Dados...")
DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine)

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

HistoricoClientes = getattr(Base.classes, "Histórico de Clientes")
Contatos = getattr(Base.classes, "Contatos")

print("Sucesso! Inicializando migração de consulta MySmartClinic...")

log_folder = path_file
csv_file = glob.glob(f'{path_file}/consulta.csv')

csv.field_size_limit(10**6)
df = pd.read_csv(csv_file[0], sep=";", encoding="ISO-8859-1")
df = df.fillna(value="")

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for index, row in df.iterrows():

    existing_patient = session.query(Contatos).filter(getattr(Contatos, "Referências")==row["id_paciente"]).first()
    if existing_patient:
        id_patient = getattr(existing_patient, "Id do Cliente")
    else:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Paciente não encontrado'
        not_inserted_data.append(row_dict)
        continue

    record = get_record(row)
    if record == "":
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Histórico vazio ou inválido'
        not_inserted_data.append(row_dict)
        continue
    
    record = record.replace('_x000D_', '<br>')
    

    if is_valid_date(row['data_criacao'], '%Y-%m-%d %H:%M:%S'):
        date = row['data_criacao']
    else:
        date = '01/01/1900 00:00' 
    
    new_record = HistoricoClientes(
        Histórico=record,
        Data=date
    )
    # setattr(new_record, "Id do Histórico", (0-row["ID_Anam"]))
    setattr(new_record, "Id do Cliente", id_patient)
    setattr(new_record, "Id do Usuário", 0)
    
    log_data.append({
        # "Id do Histórico": (0-row["ID_Anam"]),
        "Id do Cliente": id_patient,
        "Data": date,
        "Histórico": record,
        "Id do Usuário": 0,
    })
    
    session.add(new_record)
    inserted_cont+=1

    if inserted_cont % 10000 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos históricos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} históricos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_record_consulta.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_record_consulta.xlsx")