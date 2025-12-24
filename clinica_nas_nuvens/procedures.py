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
procedure_tbl = Table("Procedimentos", metadata, schema=f"schema_{sid}", autoload_with=engine)

Base = declarative_base()

class Procedimentos(Base):
    __table__ = procedure_tbl

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

print("Sucesso! Inicializando migração de Procedimentos...")

todos_arquivos = glob.glob(f'{path_file}/TIPO_PROCEDIMENTO.csv')

csv.field_size_limit(1000000)
df = pd.read_csv(todos_arquivos[0], sep=',', encoding='utf-8', quotechar='"', on_bad_lines='skip')

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

    id_procedure = verify_nan(row['codtipoprocedimento'])
    if id_procedure is None:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do procedimento vazio'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    procedure = verify_nan(row['nome'])
    if not procedure:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Nome do procedimento vazio'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    if len(procedure) > 100:
        procedure = truncate_value(procedure, 100)
    
    new_procedure = Procedimentos(
        Procedimento=procedure,
        Custo = 0,
        Produto = 0,
        Sessões = 0,
        Comissao = 0,
    )
    setattr(new_procedure, "Id do Procedimento", id_procedure)
    setattr(new_procedure, "Preço Base", 0)
    log_data.append({
        "Id do Procedimento": id_procedure,
        "Procedimento": procedure,

    })
    session.add(new_procedure)
    inserted_cont+=1

    if inserted_cont % 1000 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos procedimentos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} procedimentos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_procedures_TIPO_PROCEDIMENTO.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_procedures_TIPO_PROCEDIMENTO.xlsx")
