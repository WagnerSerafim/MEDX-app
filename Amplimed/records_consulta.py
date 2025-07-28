import csv
import json
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import urllib
from utils.utils import create_log, exists

def get_date(json_dict):
    if json_dict["dtconsulta"] not in ['', '<br>', None]:
        date = json_dict["dtconsulta"] + " 00:00"
    else: 
        date = "01/01/1900 00:00"
    return date


def get_record(json_dict):
    record = ""
    if json_dict["queixa"] not in ['', '<br>', None]:
        record+=f"Queixa: {json_dict["queixa"]}<br>"
    
    if json_dict["listpro"] not in ['', '<br>', None]:
        record+=f"Listpro: {json_dict["listpro"]}<br>"

    if json_dict["plantrat"] not in ['', '<br>', None]:
        record+=f"Plantrat: {json_dict["plantrat"]}<br>"
    
    if json_dict["conduta"] not in ['', '<br>', None]:
        record+=f"Conduta: {json_dict["conduta"]}<br>"

    if json_dict['descfis'] not in ['', '<br>', None]:
        record+=f"Descfis: {json_dict['descfis']}<br>"
    
    return record

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase= input("Informe o DATABASE: ")
path_folder = input("Informe o caminho do arquivo: ")

print("Iniciando a conexão com o banco de dados...")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine) 

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

HistoricoClientes = getattr(Base.classes, "Histórico de Clientes")
Contatos = getattr(Base.classes, "Contatos")

print("Carregando dados de consulta...")

json_file = os.path.join(path_folder, "consulta.json")
with open(json_file, 'r', encoding='utf-8') as file:
    json_data = json.load(file)
                     
log_folder = path_folder

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

print("Iniciando a inserção dos históricos...")

for dict in json_data:

    patient = exists(session, dict["codp"], "Id do Cliente", Contatos)
    if not patient:
        not_inserted_cont += 1
        dict['Motivo'] = 'Id do paciente não encontrado'
        dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(dict)
        continue
    id_patient = getattr(patient, "Id do Cliente")

    record = get_record(dict)
    if record == "":
        not_inserted_cont += 1
        dict['Motivo'] = 'Histórico vazio'
        dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(dict)
        continue

    date = get_date(dict)

    new_record = HistoricoClientes(
        Histórico=record,
        Data=date,
    )

    setattr(new_record, "Id do Cliente", id_patient)
    setattr(new_record, "Id do Usuário", 0)

    log_data.append({
        "Id no backup": dict["codcon"],
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

create_log(log_data, log_folder, "log_inserted_record_consulta.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_record_consulta.xlsx")
