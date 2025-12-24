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
estoque_tbl = Table("Estoque", metadata, schema=f"schema_{sid}", autoload_with=engine)

Base = declarative_base()

class Estoque(Base):
    __table__ = estoque_tbl

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

print("Sucesso! Inicializando migração de Estoque...")

todos_arquivos = glob.glob(f'{path_file}/EST_PRODUTO.csv')

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

    id_stock = verify_nan(row['codigo'])
    if id_stock is None:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do item do estoque vazio'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    item = verify_nan(row['nome'])
    if not item:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Nome do item vazio'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    if len(item) > 100:
        item = truncate_value(item, 100)
    
    new_stock = Estoque(
        Item=item,
    )
    setattr(new_stock, "Id do Item", id_stock)
    
    log_data.append({
        "Id do Item": id_stock,
        "Item": item,
    })
    session.add(new_stock)
    inserted_cont+=1

    if inserted_cont % 1000 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos itens do estoque foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} itens do estoque não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_stock_EST_PRODUTO.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_stock_EST_PRODUTO.xlsx")
