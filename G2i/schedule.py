import csv
import glob
import json
import os
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
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

metadata = MetaData()
agenda_tbl = Table("Agenda", metadata, schema=f"schema_{sid}", autoload_with=engine)
contatos_tbl = Table("Contatos", metadata, schema=f"schema_{sid}", autoload_with=engine)

Base = declarative_base()

class Agenda(Base):
    __table__ = agenda_tbl

class Contatos(Base):
    __table__ = contatos_tbl

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

print("Sucesso! Inicializando migração de Agendamentos...")

extension_file = glob.glob(f'{path_file}/agendas.json')

with open(extension_file[0], 'r') as f:
    df = pd.read_json(f)

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont = 0
not_inserted_data = []
not_inserted_cont = 0

for idx, row in df.iterrows():

    id_scheduling = verify_nan(row["id"])
    if id_scheduling is None:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Agendamento vazio'
        not_inserted_data.append(row_dict)
        continue

    existing_record = exists(session, id_scheduling, "Id do Agendamento", Agenda)
    
    ini_str = verify_nan(row["inicio"])
    if ini_str is None:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Data de Início vazia'
        not_inserted_data.append(row_dict)
        continue
    else:
        ini_obj = datetime.strptime(ini_str, '%Y-%m-%d %H:%M:%S')
        start_time = ini_obj.strftime('%Y-%m-%d %H:%M:%S')
        if not is_valid_date(start_time, '%Y-%m-%d %H:%M:%S'):
            not_inserted_cont +=1
            row_dict = row.to_dict()
            row_dict['Motivo'] = 'Data de Início inválida'
            not_inserted_data.append(row_dict)
            continue

    fim_str = verify_nan(row["fim"])
    if fim_str is None:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Data de Fim vazia'
        not_inserted_data.append(row_dict)
        continue
    else:
        fim_obj = datetime.strptime(fim_str, '%Y-%m-%d %H:%M:%S')
        end_time = fim_obj.strftime('%Y-%m-%d %H:%M:%S')
        if not is_valid_date(end_time, '%Y-%m-%d %H:%M:%S'):
            not_inserted_cont +=1
            row_dict = row.to_dict()
            row_dict['Motivo'] = 'Data de Fim inválida'
            not_inserted_data.append(row_dict)
            continue

    id_patient = verify_nan(row["cliente_id"])
    if id_patient == "" or id_patient == None or id_patient == 'None':
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do paciente vazio'
        not_inserted_data.append(row_dict)
        continue

    patient = exists(session, id_patient, "Id do Cliente", Contatos)
    if not patient:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Paciente não encontrado no Banco de Dados'
        not_inserted_data.append(row_dict)
        continue
    patient_name = getattr(patient, "Nome", None)
    procedure = verify_nan(row['procedimento'])

    if patient_name:
        description = f"Paciente: {patient_name}"
        if procedure:
            description += f" | Procedimento: {procedure}"

    user = verify_nan(row['grupo_id'])

    new_schedulling = Agenda(
        Descrição=description,
        Início=start_time,
        Final=end_time,
        Status=1,
    )

    setattr(new_schedulling, "Id do Agendamento", id_scheduling)
    setattr(new_schedulling, "Vinculado a", id_patient)
    setattr(new_schedulling, "Id do Usuário", user)
    
    log_data.append({
        "Id do Agendamento": id_scheduling,
        "Vinculado a": id_patient,
        "Id do Usuário": 1,
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

create_log(log_data, log_folder, "log_inserted_agendas.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_agendas.xlsx")
