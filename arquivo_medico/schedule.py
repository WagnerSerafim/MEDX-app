import csv
import glob
import json
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log
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

todos_arquivos = glob.glob(f'{path_file}/reserva.csv')

csv.field_size_limit(1000000000)  
df = pd.read_csv(todos_arquivos[0], sep=';', engine='python', encoding='ISO-8859-1', dtype=str)
df = df.replace('None', '')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0
id_scheduling_cont = 4766

for idx, row in df.iterrows():

    id_scheduling_cont += 1

    if exists(session, id_scheduling_cont, 'Id do Agendamento', Agenda):
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do agendamento já existe'
        row_dict['Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        not_inserted_data.append(row_dict)
        continue
    else:
        id_scheduling = id_scheduling_cont
    
    date_str = f'{row['ano']}-{row['mes']}-{row['dia']} {int(row['hora'])//60}:{int(row['hora'])%60}:00'
    if is_valid_date(date_str, '%Y-%m-%d %H:%M:%S'):
        start_time = date_str
        end_str = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S') + timedelta(minutes=30)
        end_time = end_str.strftime('%Y-%m-%d %H:%M:%S')
    else:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Data ou Hora inválida'
        row_dict['Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        not_inserted_data.append(row_dict)
        continue

    if row['prontuario'] in [None, '', 'None'] or pd.isna(row['prontuario']):
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do paciente vazio'
        not_inserted_data.append(row_dict)
        continue
    else:
        id_patient = int(row['prontuario']) + 1731

    id_user = 609143270

    description = f'{row['paciente']} - {row['tipo']}'

    new_schedulling = Agenda(
        Descrição=description,
        Início=start_time,
        Final=end_time,
        Status=1,
    )

    setattr(new_schedulling, "Id do Agendamento", id_scheduling)
    setattr(new_schedulling, "Vinculado a", id_patient)
    setattr(new_schedulling, "Id do Usuário", id_user)
    
    log_data.append({
        "Id do Agendamento": id_scheduling,
        "Vinculado a": id_patient,
        "Id do Usuário": id_user,
        "Início": start_time,
        "Final": end_time,
        "Descrição": description,
        "Status" : 1
    })

    session.add(new_schedulling)

    inserted_cont+=1

    if inserted_cont % 1000 == 0:
        session.commit()

    if (idx + 1) % 1000 == 0 or (idx + 1) == len(df):
        print(f"Processados {idx + 1} de {len(df)} registros ({(idx + 1) / len(df) * 100:.2f}%)")


session.commit()

print(f"{inserted_cont} novos agendamentos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} agendamentos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_schedule_reserva.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_schedule_reserva.xlsx")
