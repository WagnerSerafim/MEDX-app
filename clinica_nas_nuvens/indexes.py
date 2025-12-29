from datetime import datetime
import glob
import os
import re
from sqlalchemy import MetaData, Table, UnicodeText, bindparam, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import DataError, IntegrityError
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, truncate_value, verify_nan
import csv

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

print("Sucesso! Inicializando migração de Histórico...")

todos_arquivos = glob.glob(f'{path_file}/ARQUIVO.csv')
pasta_files = glob.glob(f'{path_file}/PASTA_ARQUIVO.csv')

csv.field_size_limit(1000000)
df = pd.read_csv(todos_arquivos[0], sep=',', encoding='utf-8', quotechar='"')
df_pasta = pd.read_csv(pasta_files[0], sep=',', encoding='utf-8', quotechar='"')
pasta_lookup = {}

for idx, row in df_pasta.iterrows():
    pasta_lookup[row['codpastaarquivo']] = {
        'id_patient': verify_nan(row['codpaciente']),
        'data': verify_nan(row['dataHoraCriacao']),
        'nome_arquivo': verify_nan(row['nome']),
    }

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for idx, row in df.iterrows():

    pasta_infos = pasta_lookup.get(row['codpastaarquivo'])
    if not pasta_infos:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id da Pasta de Arquivos vazio'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    if idx % 1000 == 0 or idx == len(df):
        print(f"Processados: {idx} | Inseridos: {inserted_cont} | Não inseridos: {not_inserted_cont} | Concluído: {round((idx / len(df)) * 100, 2)}%")

    id_record = verify_nan(row['codarquivo'])
    if id_record is None:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Prontuário vazio'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    if exists(session, id_record, "Id do Histórico", Historico):
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Histórico já existe'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    record = pasta_infos.get('nome_arquivo')
    if record is None:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Histórico vazio ou inválido'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    date_str = pasta_infos.get('data')
    if date_str is None:
        print("Data não encontrada, 1900-01-01 será usada.")
        date = '1900-01-01 00:00'
    else:
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
            date = date_obj.strftime("%Y-%m-%d %H:%M:%S")
            if not is_valid_date(date, '%Y-%m-%d %H:%M:%S'):
                date = '1900-01-01 00:00'
        except ValueError:
            try:
                date_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M")
                date = date_obj.strftime("%Y-%m-%d %H:%M")
                if not is_valid_date(date, '%Y-%m-%d %H:%M'):
                    date = '1900-01-01 00:00'
            except:
                date = '1900-01-01 00:00'

    id_patient = pasta_infos.get('id_patient')
    if id_patient is None:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do paciente vazio'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    classe = verify_nan(row['urlstorage'][1:])
    if classe is None:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Classe vazia'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    if len(classe) > 100:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Classe maior que 100 caracteres'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    
    new_record = Historico(
        Data=date,
    )
    setattr(new_record, "Histórico", bindparam(None, value=record, type_=UnicodeText()))
    setattr(new_record, "Id do Histórico", id_record)
    setattr(new_record, "Id do Cliente", id_patient)
    setattr(new_record, "Id do Usuário", 0)
    setattr(new_record, "Classe", classe)
    
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

session.commit()

print(f"{inserted_cont} novos históricos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} históricos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_indexes_ARQUIVOS.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_indexes_ARQUIVOS.xlsx")
