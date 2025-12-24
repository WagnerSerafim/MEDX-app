import csv
import glob
import json
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, verify_nan
from datetime import datetime, timedelta


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

Agenda = getattr(Base.classes, "Agenda")

print("Sucesso! Inicializando migração de Agendamentos...")

todos_arquivos = glob.glob(f'{path_file}/Relatorio-de-Agendamento*.csv')

csv.field_size_limit(1000000000)  
df = pd.read_csv(todos_arquivos[0], sep=';', engine='python', encoding='latin1')
print(df.columns)

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0
id_scheduling_cont = 0

for idx, row in df.iterrows():

    id_scheduling_cont -= 1

    scheduling = exists(session, id_scheduling_cont, 'Id do Agendamento', Agenda)
    if scheduling:
        patient = verify_nan(row['PACIENTE'])
        service = verify_nan(row['SERVIï¿½O'])
        description = f"Paciente: {patient} - Serviço: {service}" if service else f"Paciente: {patient}"
        old_description = getattr(scheduling, 'Descrição')
        setattr(scheduling, 'Descrição', description)
    else:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do agendamento não existe'
        row_dict['Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        not_inserted_data.append(row_dict)
        continue
    
    log_data.append({
        "Id do Agendamento": id_scheduling_cont,
        "Vinculado a": getattr(scheduling, 'Vinculado a'),
        "Descrição Atualizada:": description,
        "Descrição Anterior:": old_description,
        "Status" : 1
    })

    inserted_cont+=1

    if inserted_cont % 1000 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos agendamentos foram atualizados com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} agendamentos não foram atualizados, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_updated_relatorio_agendamento.xlsx")
create_log(not_inserted_data, log_folder, "log_not_updated_relatorio_agendamento.xlsx")
