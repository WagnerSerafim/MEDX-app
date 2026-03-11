from datetime import datetime
import glob
import os
import re
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, truncate_value, verify_nan

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")

print("Conectando no Banco de Dados...")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

metadata = MetaData()
contatos_tbl = Table("Contatos", metadata, schema=f"schema_{sid}", autoload_with=engine)

Base = declarative_base()

class Contatos(Base):
    __table__ = contatos_tbl

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

print("Sucesso! Inicializando atualização de Contatos...")

file = "C:\\Users\\WJSur\\Documents\\migracao\\36492_Marcelo_Dorotea_dos_Santos_SISTEMA_NAO_INFORMADO\\CADASTRO_PACIENTES_MIGRACAO.xlsx"
df = pd.read_excel(file, engine="openpyxl")

log_folder = "C:\\Users\\WJSur\\Documents\\migracao\\36492_Marcelo_Dorotea_dos_Santos_SISTEMA_NAO_INFORMADO"

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for idx,row in df.iterrows():

    if idx % 1000 == 0 or idx == len(df):
        print(f"Processados: {idx} | Inseridos: {inserted_cont} | Não inseridos: {not_inserted_cont} | Concluído: {round((idx / len(df)) * 100, 2)}%")

    id_patient = verify_nan(row["id"])
    if id_patient == None:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Cliente vazio'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    birth = verify_nan(row['nascimento'])
    if not birth:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Nascimento vazio na planilha'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    patient = exists(session, id_patient, 'Id do Cliente', Contatos)
    if not patient:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Cliente não existe no banco'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    
    if not is_valid_date(birth, '%Y-%m-%d'):
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Data Inválida'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    setattr(patient, 'Nascimento', birth)

    log_data.append({
        "Id do Cliente": id_patient,
        "Nome": row['nome'],
        "Nova Data de Nascimento": birth,
        "TimeStamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

    inserted_cont+=1

session.commit()

print(f"{inserted_cont} novos contatos foram atualizados com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} contatos não foram atualizados, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_updated_birthday.xlsx")
create_log(not_inserted_data, log_folder, "log_not_updated_birthday.xlsx")

    