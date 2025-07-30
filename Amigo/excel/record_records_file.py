import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import urllib
from utils.utils import create_log, is_valid_date, exists, truncate_value, verify_nan

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine)

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

HistoricoClientes = getattr(Base.classes, "Histórico de Clientes")

print("Sucesso! Inicializando migração de Históricos...")

todos_arquivos = glob.glob(f'{path_file}/records_file.xlsx')
record_path = glob.glob(f'{path_file}/records.xlsx')

df = pd.read_excel(todos_arquivos[0])
df = df.replace('None', '')

df_records = pd.read_excel(record_path[0])
df_records = df_records.replace('None', '')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

record_lookup = {row['id']: row['patient_id'] for _, row in df_records.iterrows()}

for idx, row in df.iterrows():

    if idx % 1000 == 0 or idx == len(df):
        print(f"Processados: {idx} | Inseridos: {inserted_cont} | Não inseridos: {not_inserted_cont} | Concluído: {round((idx / len(df)) * 100, 2)}%")

    record_id = verify_nan(row['id'])
    if record_id == "":
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Histórico é vazio ou nulo'
        not_inserted_data.append(row_dict)
        continue

    existing_record = exists(session, record_id, "Id do Histórico", HistoricoClientes)
    if existing_record:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Histórico já existe no banco de dados'
        not_inserted_data.append(row_dict)
        continue

    id_patient = row["patient_id"]
    if id_patient == "" or id_patient == None or id_patient == 'None' or pd.isna(id_patient):
        if row['record_id'] in record_lookup:
            id_patient = record_lookup[row['record_id']]
        else:
            not_inserted_cont += 1
            row_dict = row.to_dict()
            row_dict['Motivo'] = 'Id do paciente vazio e não encontrado no arquivo records.xlsx'
            not_inserted_data.append(row_dict)
            continue
    
    record = verify_nan(row['name'])
    if record == "":
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Histórico vazio'
        not_inserted_data.append(row_dict)
        continue
    
    classe_url = truncate_value(verify_nan(row['url']), 100)
    if classe_url == "":
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Classe é vazia ou nula'
        not_inserted_data.append(row_dict)
        continue

    if isinstance(row['created_at'], datetime):
        date_str = row['created_at'].strftime('%Y-%m-%d %H:%M:%S')
        if is_valid_date(date_str, '%Y-%m-%d %H:%M:%S'):
            date = date_str
        else:
            date = '01/01/1900 00:00'
    else:
        if is_valid_date(row['created_at'], '%d-%m-%Y %H:%M:%S'):
            date = row['created_at']
        else:
            date = '01/01/1900 00:00'

    new_record = HistoricoClientes(
        Histórico = record,
        Data = date,
        Classe = classe_url
    )

    setattr(new_record, "Id do Histórico", record_id)
    setattr(new_record, "Id do Cliente", id_patient)
    setattr(new_record, "Id do Usuário", 0)

    log_data.append({
        "Id do Histórico": record_id,
        "Id do Cliente": id_patient,
        "Data": date,
        "Histórico": record,
        "Classe": classe_url,
        "Id do Usuário": 0,
    })
    session.add(new_record)
    inserted_cont+=1

    if inserted_cont % 100 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos históricos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} históricos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_record_file.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_record_file.xlsx")