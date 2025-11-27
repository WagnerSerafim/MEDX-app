import glob
import os
from sqlalchemy import MetaData, Table, create_engine, bindparam, UnicodeText
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime, timedelta
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, verify_nan
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

Base = declarative_base()

class Agenda(Base):
    __table__ = agenda_tbl

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

print("Sucesso! Inicializando migração de Agendamentos...")

csv.field_size_limit(100000000)
todos_arquivos = glob.glob(f'{path_file}/Agendas.csv')

df = pd.read_csv(todos_arquivos[0], sep=';', dtype=str, encoding='latin1', quotechar='"')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for idx, row in df.iterrows():

    if idx % 1000 == 0 or idx == len(df):
        print(f"Processados: {idx} | Inseridos: {inserted_cont} | Não inseridos: {not_inserted_cont} | Concluído: {round((idx / len(df)) * 100, 2)}%")

    id_scheduling = verify_nan(row["tbID"])
    if id_scheduling == None:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Agendamento vazio ou nulo'
        not_inserted_data.append(row_dict)
        continue

    exists_row = exists(session, id_scheduling, "Id do Agendamento", Agenda)
    if exists_row:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id já existe no banco de dados'
        not_inserted_data.append(row_dict)
        continue

    id_patient = verify_nan(row["tbFicha"])
    if id_patient == None:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do paciente vazio ou inválido'
        not_inserted_data.append(row_dict)
        continue

    patient = verify_nan(row["tbNome"])
    description = patient
    obs = verify_nan(row["tbObservacao"])
    if obs: 
        description += f" - {obs}"
    phone = verify_nan(row["tbCelular"])
    if phone:
        description += f" - {phone}"

    if description == None or description.strip() == "":
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Descrição vazia ou inválida'
        not_inserted_data.append(row_dict)
        continue

    date_obj = verify_nan(row['tbData'])
    date_obj = datetime.strptime(date_obj, '%d/%m/%Y') if date_obj else None   

    hour_obj = verify_nan(row['tbHora'])
    hour_obj = datetime.strptime(hour_obj, '%d/%m/%Y %H:%M:%S') if hour_obj else None

    start_time = datetime.combine(date_obj.date(), hour_obj.time())
    end_time = start_time + timedelta(minutes=30)

    user_id = verify_nan(row["tbMedico"])
    if user_id == 6497:
        user = 1866366057
    elif user_id == 6496 or user_id == 1:
        user = 1
    else:
        user = user_id

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

create_log(log_data, log_folder, "log_inserted_Agendas.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_Agendas.xlsx")
