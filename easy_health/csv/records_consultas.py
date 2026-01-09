from datetime import datetime
import glob
import json
import os
from sqlalchemy import MetaData, Table, create_engine, bindparam, UnicodeText
from sqlalchemy.orm import declarative_base, sessionmaker
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
historico_tbl = Table("Histórico de Clientes", metadata, schema=f"schema_{sid}", autoload_with=engine)

Base = declarative_base()

class Historico(Base):
    __table__ = historico_tbl

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

print("Sucesso! Inicializando migração de Históricos...")

todos_arquivos = glob.glob(f'{path_file}/consultas*.csv')
schedule_files = glob.glob(f'{path_file}/atendimento*.csv')

df = pd.read_csv(todos_arquivos[0], sep=';', encoding='utf-8', quotechar='"')

df_schedule = pd.read_csv(schedule_files[0], sep=';', encoding='utf-8', quotechar='"')


schedules = {}
for _, row in df_schedule.iterrows():
    schedules[row["CÓDIGO ATENDIMENTO"]] = [row["CÓDIGO PACIENTE"], row['DATA INICIAL']]

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont = 0
not_inserted_data = []
not_inserted_cont = 0

for idx, row in df.iterrows():

    try:
        schedule = schedules[row["CÓDIGO ATENDIMENTO"]]
    except:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Agendamento não encontrado para o histórico'
        not_inserted_data.append(row_dict)
        continue

    id_record = row['CÓDIGO']
    if id_record in ['', None]:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Histórico vazio ou inválido'
        not_inserted_data.append(row_dict)
        continue
    else:
        existing_record = exists(session, id_record, 'Id do Histórico', Historico)
        if existing_record:
            not_inserted_cont +=1
            row_dict = row.to_dict()
            row_dict['Motivo'] = 'Histórico já existe no banco de dados'
            not_inserted_data.append(row_dict)
            continue

    record = f'{row['ANAMNESE']}'
    if row['AVALIAÇÃO'] not in ['', None]:
        record += f'<br> {row['AVALIAÇÃO']}'
    if id_record == 4227318:
        print(f"Debug RECORD: {record}")
        print(f"Debug AVALIACAO: {row['ANAMNESE']}")
        print(f"Debug ANAMNESE: {row['AVALIAÇÃO']}")

    if record == "":
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Histórico vazio ou inválido'
        not_inserted_data.append(row_dict)
        continue

    date_str = schedule[1]
    if date_str in ['', None]:
        date = '1900-01-01'
    else:
        date_obj = datetime.strptime(date_str, '%d/%m/%Y %H:%M:%S')
        date = date_obj.strftime('%Y-%m-%d %H:%M:%S')
        if not is_valid_date(date, '%Y-%m-%d %H:%M:%S'):
            date = '1900-01-01'

    id_patient = schedule[0]
    if id_patient in ['', None]:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do paciente vazio'
        not_inserted_data.append(row_dict)
        continue
    
    new_record = Historico(
        Data=date,
    )
    setattr(new_record, "Histórico", bindparam(None, value=record, type_=UnicodeText()))
    setattr(new_record, "Id do Histórico", id_record)
    setattr(new_record, "Id do Cliente", id_patient)
    setattr(new_record, "Id do Usuário", 0)
    
    log_data.append({
        "Id do Histórico": id_record,
        "Id do Cliente": id_patient,
        "Data": date,
        "Histórico": record,
        "Id do Usuário": 0,
    })
    session.add(new_record)
    inserted_cont+=1

    if inserted_cont % 1000 == 0:
        session.commit()

    if (idx + 1) % 1000 == 0 or (idx + 1) == len(df):
        print(f"Processados {idx + 1} de {len(df)} registros ({(idx + 1) / len(df) * 100:.2f}%)")


session.commit()

print("Migração concluída! Gerando logs...")
print(f"{inserted_cont} novos históricos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} históricos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_record_consultas.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_record_consultas.xlsx")
