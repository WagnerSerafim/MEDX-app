import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, verify_nan
import csv

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase= input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ") 

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

print("Conectando no Banco de dados...")

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine) 

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Agenda = getattr(Base.classes, "Agenda")
Contatos = getattr(Base.classes, "Contatos")

print("Sucesso! Inicializando migração de Agendamentos...")

csv.field_size_limit(10000000000000)

todos_arquivos = glob.glob(f'{path_file}/attendances.csv')

df = pd.read_csv(todos_arquivos[0])
df = df.replace('None', '')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for _, row in df.iterrows():

    id_patient = verify_nan(row["patient_id"])
    if id_patient == "":
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do paciente vazio ou inválido'
        not_inserted_data.append(row_dict)
        continue
    id_patient = int(id_patient)

    type_schedule = verify_nan(row['type'])
    observation = verify_nan(row['observation'])
    patient = exists(session, id_patient, "Id do Cliente", Contatos)
    if not patient:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do paciente vinculado não existe no banco de dados'
        not_inserted_data.append(row_dict)
        continue
    else:
        description = f"{patient.Nome} {type_schedule} {observation}"

    id_scheduling = verify_nan(row["id"])
    if id_scheduling == "":
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Agendamento vazio ou nulo'
        not_inserted_data.append(row_dict)
        continue
    id_scheduling = int(id_scheduling)

    exists_row = session.query(Agenda).filter(getattr(Agenda, 'Id do Agendamento') == id_scheduling).first()
    if exists_row:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id já existe no banco de dados'
        not_inserted_data.append(row_dict)
        continue

    if not pd.isna(row['start_date']) and is_valid_date(row['start_date'], '%Y-%m-%d %H:%M:%S'):
        if isinstance(row['start_date'], datetime):
            start_time = row['start_date'].strftime('%Y-%m-%d %H:%M:%S')
        else:
            start_time = row['start_date']
    else:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Data ou Hora inválida'
        not_inserted_data.append(row_dict)
        continue

    if not pd.isna(row['end_date']) and is_valid_date(row['end_date'], '%Y-%m-%d %H:%M:%S'):
        if isinstance(row['end_date'], datetime):
            end_time = row['end_date'].strftime('%Y-%m-%d %H:%M:%S')
        else:
            end_time = row['end_date']
    else:
        start_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        end_dt = start_dt + timedelta(minutes=30)
        end_time = end_dt.strftime('%Y-%m-%d %H:%M:%S')

    user = verify_nan(row['user_id'])
    if user == "":
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do paciente vazio ou inválido'
        not_inserted_data.append(row_dict)
        continue

    new_schedulling = Agenda(
        Descrição=description,
        Início=start_time,
        Final=end_time,
        Status=1,
    )

    setattr(new_schedulling, "Id do Agendamento", row["id"])
    setattr(new_schedulling, "Vinculado a", row["patient_id"])
    setattr(new_schedulling, "Id do Usuário", user)
    
    log_data.append({
        "Id do Agendamento": row["id"],
        "Vinculado a": row["patient_id"],
        "Id do Usuário": user,
        "Início": start_time,
        "Final": end_time,
        "Descrição": description,
        "Status" : 1
    })

    session.add(new_schedulling)

    inserted_cont+=1

    if inserted_cont % 1000 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos agendamentos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} agendamentos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_scheduling_attendances.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_scheduling_attendances.xlsx")
