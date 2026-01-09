import glob
import os
from sqlalchemy import MetaData, Table, create_engine, bindparam, UnicodeText
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime, timedelta
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, limpar_numero, verify_nan
import csv

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase= input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ") 

print("Conectando no Banco de dados...")

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

todos_arquivos = glob.glob(f'{path_file}/scheduling_CLINICA.xlsx')
df = pd.read_excel(todos_arquivos[0], engine='openpyxl')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont = 0
not_inserted_data = []
not_inserted_cont = 0

for idx, row in df.iterrows():

    if idx % 1000 == 0 or idx == len(df):
        print(f"Processados: {idx} | Inseridos: {inserted_cont} | Não inseridos: {not_inserted_cont} | Concluído: {round((idx / len(df)) * 100, 2)}%")

    user = 1

    id_schedule = verify_nan(row["Id"])
    if id_schedule == None:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do agendamento vazio ou inválido'
        not_inserted_data.append(row_dict)
        continue

    existing_record = exists(session, id_schedule, "Id do Agendamento", Agenda)
    if existing_record:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Agendamento já existe no banco de dados'
        not_inserted_data.append(row_dict)
        continue

    id_patient = verify_nan(row["FichaPacienteId"])
    if id_patient == None:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do paciente vazio ou inválido'
        not_inserted_data.append(row_dict)
        continue
    
    observation = verify_nan(row['Observacao'])
    patient_name = verify_nan(row['NomePaciente'])
    description = ''

    if patient_name:
        description = f"{patient_name}"
        if observation:
            description += f" - {observation}"

    date_obj = verify_nan(row['DataHora'])
    if date_obj == None:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Data do agendamento vazio'
        not_inserted_data.append(row_dict)
        continue
    else:
        start_time = date_obj.strftime('%Y-%m-%d %H:%M:%S')
        duration = limpar_numero(verify_nan(row['duracao']))
        if duration == None:
            duration = 30
        end_time_obj = date_obj + timedelta(minutes=int(duration))
        end_time = end_time_obj.strftime('%Y-%m-%d %H:%M:%S')

        if not is_valid_date(start_time, '%Y-%m-%d %H:%M:%S') or not is_valid_date(end_time, '%Y-%m-%d %H:%M:%S'):
            not_inserted_cont += 1
            row_dict = row.to_dict()
            row_dict['Motivo'] = 'Data do agendamento ou horário inválido'
            not_inserted_data.append(row_dict)
            continue

    new_schedulling = Agenda(
        Descrição=description,
        Início=start_time,
        Final=end_time,
        Status=1,
    )

    setattr(new_schedulling, "Id do Agendamento", id_schedule)
    setattr(new_schedulling, "Vinculado a", id_patient)
    setattr(new_schedulling, "Id do Usuário", user)
    
    log_data.append({
        "Id do Agendamento": id_schedule,
        "Vinculado a": id_patient,
        "Id do Usuário": user,
        "Início": start_time,
        "Final": end_time,
        "Descrição": description,
        "Status": 1
    })

    session.add(new_schedulling)

    inserted_cont += 1

    if inserted_cont % 1000 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos agendamentos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} agendamentos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_schedule_scheduling.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_schedule_scheduling.xlsx")
