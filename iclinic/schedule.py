import csv
import glob
import json
import os
from sqlalchemy import MetaData, Table, create_engine, bindparam, UnicodeText
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, truncate_value, verify_nan
from datetime import datetime, timedelta


def find_scheduling_csv(path_folder):
    """Procura o arquivo que contenha 'scheduling.csv' no seu nome"""
    csv_files = glob.glob(os.path.join(path_folder, "*scheduling.csv"))

    if not csv_files:
        print("Nenhum arquivo que contenha 'scheduling.csv' foi encontrado.")
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
agenda_tbl = Table("Agenda", metadata, schema=f"schema_{sid}", autoload_with=engine)
Base = declarative_base()

class Agenda(Base):
    __table__ = agenda_tbl

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

print("Sucesso! Inicializando migração de Agenda...")

csv_files = find_scheduling_csv(path_file)

csv.field_size_limit(1000000)

df = pd.read_csv(csv_files, sep=',', engine='python', dtype=str)

log_folder = path_file

log_folder = path_file
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for idx, row in df.iterrows():

    id_scheduling = verify_nan(row["pk"])
    if id_scheduling == None:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Agendamento vazio'
        not_inserted_data.append(row_dict)
        continue

    existing_register = exists(session, row["pk"], "Id do Agendamento", Agenda)
    if existing_register:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Agendamento já existe'
        not_inserted_data.append(row_dict)
        continue
        

    id_patient = verify_nan(row["patient_id"])
    if id_patient == None:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Paciente vazio'
        not_inserted_data.append(row_dict)
        continue

    date = verify_nan(row["date"])
    if date == None:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Data vazia'
        not_inserted_data.append(row_dict)
        continue

    start_time_str = verify_nan(row["start_time"])
    if start_time_str == None:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Hora de início vazia'
        not_inserted_data.append(row_dict)
        continue

    date_str = f'{date} {start_time_str}'
    if is_valid_date(date_str, '%Y-%m-%d %H:%M:%S'):
        start_time = date_str
        start_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        end_dt = start_dt + timedelta(minutes=30)
        end_time = end_dt.strftime('%Y-%m-%d %H:%M:%S')
    else:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Data ou Hora inválida'
        not_inserted_data.append(row_dict)
        continue

    id_user = verify_nan(row["physician_id"])
    if id_user == None:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Usuário vazio'
        not_inserted_data.append(row_dict)
        continue
    
    desc = verify_nan(row["description"])
    description = f"{row["patient_name"]}"
    if desc:
        description += f" - {desc}"
    
    start_time = f"{row["date"]} {row["start_time"]}"
    end_time = f"{row["date"]} {row["end_time"]}"

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

session.commit()

print(f"{inserted_cont} novos agendamentos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} agendamentos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_scheduling_schedulling.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_scheduling_schedulling.xlsx")
