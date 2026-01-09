import glob
import os
from sqlalchemy import MetaData, Table, create_engine, bindparam, UnicodeText
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, verify_nan
import csv

def get_record(row):
    record = ''

    procedure = verify_nan(row['Procedimento'])
    if procedure != None:
        record += f"Procedimento: {procedure}<br>"
    sessions = verify_nan(row['Sessões'])
    if sessions != None:
        record += f"Sessões: {sessions}<br>"
    quantity = verify_nan(row['Quantidade'])
    if quantity != None:
        record += f"Quantidade: {quantity}<br>"
    frequency = verify_nan(row['Frequência'])
    if frequency != None:
        record += f"Frequência: {frequency}<br>"
    preparation = verify_nan(row['Preparação'])
    if preparation != None:
        record += f"Preparação: {preparation}<br>"

    return record

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")

print("Conectando no Banco de dados...")

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

csv.field_size_limit(10000000)
todos_arquivos = glob.glob(f'{path_file}/Planos.csv')
print(todos_arquivos)

df = pd.read_csv(todos_arquivos[0], sep=';', engine='python', quotechar='"', encoding='latin1')
print(df.columns)
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

    # record_id = verify_nan(row['Código'])
    # if record_id == None:
    #     not_inserted_cont += 1
    #     row_dict = row.to_dict()
    #     row_dict['Motivo'] = 'Id do Histórico é vazio ou nulo'
    #     not_inserted_data.append(row_dict)
    #     continue

    # existing_record = exists(session, record_id, "Id do Histórico", Historico)
    # if existing_record:
    #     not_inserted_cont +=1
    #     row_dict = row.to_dict()
    #     row_dict['Motivo'] = 'Histórico já existe no banco de dados'
    #     not_inserted_data.append(row_dict)
    #     continue

    id_patient = verify_nan(row['Código do paciente'])
    if id_patient == None:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Paciente é vazio ou nulo'
        not_inserted_data.append(row_dict)
        continue

    record = get_record(row)
    if record == '':
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Histórico vazio'
        not_inserted_data.append(row_dict)
        continue

    if is_valid_date(row['Data de criação'], "%Y-%m-%d %H:%M:%S"):
        date = row['Data de criação']
    else:
        date = '1900-01-01 00:00:00'

    new_record = Historico(
        Data=date,
    )
    setattr(new_record, "Histórico", bindparam(None, value=record, type_=UnicodeText()))
    # setattr(new_record, "Id do Histórico", record_id)
    setattr(new_record, "Id do Cliente", id_patient)
    setattr(new_record, "Id do Usuário", 0)

    log_data.append({
        # "Id do Histórico": record_id,
        "Id do Cliente": id_patient,
        "Data": date,
        "Histórico": record,
        "Id do Usuário": 0,
    })
    session.add(new_record)
    inserted_cont+=1

    if inserted_cont % 1000 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos históricos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} históricos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_records_Planos.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_records_Planos.xlsx")