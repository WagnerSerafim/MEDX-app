import glob
import os
from sqlalchemy import MetaData, Table, create_engine, bindparam, UnicodeText
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, exists, verify_nan
from datetime import datetime, timedelta

def get_user(user):
    if user == 54:
        return 1
    elif user == 64:
        return 674111404
    else:
        return user

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

todos_arquivos = glob.glob(f'{path_file}/tblAgenda.xlsx')

df = pd.read_excel(todos_arquivos[0])
df = df.replace('None', '')

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

    user_int = verify_nan(row['intProfissionalId'])
    user = get_user(user_int)
    
    id_patient = verify_nan(row['intClienteId'])
    if id_patient == None:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do paciente vazio'
        row_dict['TimeStamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    patient = exists(session, id_patient, "Id do Cliente", Contatos)
    if not patient:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do paciente não existe no banco de dados'
        row_dict['TimeStamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    
    description = patient.Nome

    start_time = verify_nan(row['datAgendamento'])
    if start_time == None:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Data de início vazia'
        row_dict['TimeStamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    start_time_obj = pd.to_datetime(start_time)
    end_time_obj = start_time_obj + timedelta(minutes=10)
    end_time = end_time_obj.strftime("%Y-%m-%d %H:%M:%S")

    new_schedulling = Agenda(
        Descrição=description,
        Início=start_time,
        Final=end_time,
        Status=1,
    )

    # setattr(new_schedulling, "Id do Agendamento", id_scheduling)
    setattr(new_schedulling, "Vinculado a", id_patient)
    setattr(new_schedulling, "Id do Usuário", user)
    
    log_data.append({
        # "Id do Agendamento": id_scheduling,
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

create_log(log_data, log_folder, "log_inserted_tblAgenda.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_tblAgenda.xlsx")
