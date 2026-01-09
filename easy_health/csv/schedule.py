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

extension_file = glob.glob(f'{path_file}/schedules*.csv')

df = pd.read_csv(extension_file[0], sep=';', encoding='utf-8', quotechar='"')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont = 0
not_inserted_data = []
not_inserted_cont = 0
id_scheduling = 0

for idx, row in df.iterrows():

    id_scheduling -= 1
    
    ini_str = verify_nan(row["AG. INÍCIO"])
    if ini_str is None:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Data de Início vazia'
        not_inserted_data.append(row_dict)
        continue
    else:
        ini_obj = datetime.strptime(ini_str, '%d/%m/%Y %H:%M:%S')
        start_time = ini_obj.strftime('%Y-%m-%d %H:%M:%S')
        if not is_valid_date(start_time, '%Y-%m-%d %H:%M:%S'):
            not_inserted_cont +=1
            row_dict = row.to_dict()
            row_dict['Motivo'] = 'Data de Início inválida'
            not_inserted_data.append(row_dict)
            continue

    fim_str = verify_nan(row["AG. FIM"])
    if fim_str is None:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Data de Fim vazia'
        not_inserted_data.append(row_dict)
        continue
    else:
        fim_obj = datetime.strptime(fim_str, '%d/%m/%Y %H:%M:%S')
        end_time = fim_obj.strftime('%Y-%m-%d %H:%M:%S')
        if not is_valid_date(end_time, '%Y-%m-%d %H:%M:%S'):
            not_inserted_cont +=1
            row_dict = row.to_dict()
            row_dict['Motivo'] = 'Data de Fim inválida'
            not_inserted_data.append(row_dict)
            continue

    id_patient = verify_nan(row["PACIENTE_ID"])
    if id_patient == "" or id_patient == None or id_patient == 'None':
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do paciente vazio'
        not_inserted_data.append(row_dict)
        continue

    patient_name = verify_nan(row['PACIENTE'])
    procedure = verify_nan(row['PROCEDIMENTO AG.'])

    if patient_name:
        description = f"Paciente: {patient_name}"
        if procedure:
            description += f" | Procedimento: {procedure}"

    user = verify_nan(row['DOUTOR_ID'])

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

create_log(log_data, log_folder, "log_inserted_schedules.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_schedules.xlsx")
