import glob
import os
from sqlalchemy import MetaData, Table, create_engine, bindparam, UnicodeText
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime, timedelta
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, verify_nan

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

print("Sucesso! Inicializando migração de Históricos...")

todos_arquivos = glob.glob(f'{path_file}/TB_AGENDA*.json')

with open(todos_arquivos[0], 'r', encoding='utf-8') as f:
    df = pd.read_json(todos_arquivos[0], encoding='utf-8')

df = pd.DataFrame(df.iloc[:, 0].to_list())

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

    id_scheduling = verify_nan(row['ID_AGENDA'])

    if exists(session, id_scheduling, 'Id do Agendamento', Agenda):
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do agendamento já existe'
        row_dict['Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        not_inserted_data.append(row_dict)
        continue
    
    start_time_str = verify_nan(row['INICIO'])
    if start_time_str == None:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Data ou Hora de início vazia'
        row_dict['Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        not_inserted_data.append(row_dict)
        continue

    start_time_str = datetime.strptime(start_time_str, '%Y-%m-%dT%H:%M:%S.%fZ')
    start_time = start_time_str.strftime('%Y-%m-%d %H:%M:%S')
    
    end_time_str = verify_nan(row['FIM'])
    if end_time_str == None:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Data ou Hora de fim vazia'
        row_dict['Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        not_inserted_data.append(row_dict)
        continue

    end_time_str = datetime.strptime(end_time_str, '%Y-%m-%dT%H:%M:%S.%fZ')
    end_time = start_time_str.strftime('%Y-%m-%d %H:%M:%S')

    if row['ID_PACIENTE'] in [None, '', 'None'] or pd.isna(row['ID_PACIENTE']):
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do paciente vazio'
        not_inserted_data.append(row_dict)
        continue
    else:
        id_patient = row['ID_PACIENTE']

    id_user = 1

    description = f'{row['NOME']} - {row['MOTIVO']}'

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

create_log(log_data, log_folder, "log_inserted_TB_AGENDA.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_TB_AGENDA.xlsx")
