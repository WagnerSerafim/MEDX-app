import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import exists, create_log, is_valid_date, verify_nan
from datetime import datetime

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

print("Sucesso! Inicializando migração de Agenda...")

extension_file = glob.glob(f'{path_file}/agenda.csv')

df = pd.read_csv(extension_file[0], sep=';')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for _, row in df.iterrows():

    id_scheduling = verify_nan(row['ID'])
    if id_scheduling == '':
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = f'Id do agendamento é um valor inválido ou vazio'
        row_dict['TimeStamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    existings_scheduling = exists(session, id_scheduling, 'Id do Agendamento', Agenda)
    if existings_scheduling:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = f'Id {id_scheduling} já existe no Banco de Dados'
        row_dict['TimeStamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    id_patient = verify_nan(row['ID_PACIENTE'])
    if id_patient == '':
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = f'Id do paciente é um valor inválido ou vazio'
        row_dict['TimeStamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    id_user = verify_nan(row['ID_PROFISSIONAL'])
    if id_user == '':
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = f'Id do usuario é um valor inválido ou vazio'
        row_dict['TimeStamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    
    start_time = verify_nan(row['DATA_HORA_INICIO'])
    if start_time == '':
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = f'Horario de inicio é um valor inválido ou vazio'
        row_dict['TimeStamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    
    if is_valid_date(start_time, '%Y-%m-%d %H:%M:%S'):
        begining_hour = start_time
    else:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = f'A data de início {row['DATA_HORA_INICIO']} é um valor inválido'
        row_dict['TimeStamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    end_time = verify_nan(row['DATA_HORA_FIM'])
    if end_time == '':
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = f'Horario final é um valor inválido ou vazio'
        row_dict['TimeStamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    
    if is_valid_date(end_time, '%Y-%m-%d %H:%M:%S'):
        ending_hour = end_time
    else:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = f'A data final {row['DATA_HORA_FIM']} é um valor inválido'
        row_dict['TimeStamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    patient = verify_nan(row['NOME_PACIENTE'])
    procedure = verify_nan(row['PROCEDIMENTO'])
    schedule_status = verify_nan(row['AGENDA_STATUS'])
    description = f'{patient} {procedure} {schedule_status}'.strip()

    new_scheduling = Agenda()

    setattr(new_scheduling, "Id do Agendamento", id_scheduling)
    setattr(new_scheduling, "Vinculado a", id_patient)
    setattr(new_scheduling, 'Id do Usuário', id_user)
    setattr(new_scheduling, 'Início', begining_hour)
    setattr(new_scheduling, 'Final', ending_hour)
    setattr(new_scheduling, 'Descrição', description)
    setattr(new_scheduling, 'Status', 1)
    
    log_data.append({
        'Id do Agendamento': id_scheduling,
        'Vinculado a': id_patient,
        'Id do Usuário': id_user,
        'Início': begining_hour,
        'Final': ending_hour,
        'Descrição': description,
        'Status': 1,
        'TimeStamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

    session.add(new_scheduling)

    inserted_cont += 1
    if inserted_cont % 1000 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos agendamentos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} agendamentos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_schedule_agenda.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_schedule_agenda.xlsx")
