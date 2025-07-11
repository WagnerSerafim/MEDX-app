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

Agenda = getattr(Base.classes, "Agenda")
Contatos = getattr(Base.classes, "Contatos")

print("Carregando dados de consulta...")

json_file = os.path.join(path_folder, "eventos.json")
with open(json_file, 'r', encoding='utf-8') as file:
    json_data = json.load(file)
                     
log_folder = path_folder

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

print("Iniciando a inserção dos agendamentos...")

for dict in json_data:

    patient = exists(session, dict["codp"], "Referências", Contatos)
    if not patient:
        not_inserted_cont += 1
        dict['Motivo'] = 'Id do paciente não encontrado'
        dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(dict)
        continue
    id_patient = getattr(patient, "Id do Cliente")

    description = dict.get("title", "")
    start_time = dict.get("start", "")
    end_time = dict.get("end", "")
    id_user = dict.get("codu")

    new_scheduling = Agenda()

    setattr(new_scheduling, "Vinculado a", id_patient)
    setattr(new_scheduling, "Id do Usuário", id_user)
    setattr(new_scheduling, "Descrição", description)
    setattr(new_scheduling, "Início", start_time)
    setattr(new_scheduling, "Final", end_time)
    setattr(new_scheduling, "Status", 1)

    log_data.append({
        'Vinculado a': id_patient,
        'Id do Usuário': id_user,
        'Descrição': description,
        'Início': start_time,
        'Final': end_time,
        'Status': 1,       
        })

    session.add(new_scheduling)
    inserted_cont+=1

    if inserted_cont % 1000 == 0:
        session.commit()

session.commit()
print(f"{inserted_cont} novos históricos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} históricos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_schedule_eventos.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_schedule_eventos.xlsx")
