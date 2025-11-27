import csv
import glob
import os
from sqlalchemy import MetaData, Table, create_engine, bindparam, UnicodeText
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
import urllib
from utils.utils import create_log, verify_nan
from datetime import datetime

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase= input("Informe o DATABASE: ")
path_file = input("Informe o caminho do arquivo: ")

print("Iniciando a conexão com o banco de dados...")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)
metadata = MetaData()
autodocs_tbl = Table("Autodocs", metadata, schema=f"schema_{sid}", autoload_with=engine)

Base = declarative_base()

class Autodocs(Base):
    __table__ = autodocs_tbl

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

print("Sucesso! Inicializando migração de Fórmulas...")

csv.field_size_limit(10000000000000)

todos_arquivos = glob.glob(f'{path_file}/t_modeloslaudos.csv')

df = pd.read_csv(todos_arquivos[0], sep=',', engine='python', quotechar='"')
df = df.replace('None', '')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

nome_biblioteca_pai = f"Laudos Migração"

autodocs_pai = Autodocs(Pai=0, Biblioteca=nome_biblioteca_pai)
session.add(autodocs_pai)
session.commit()
id_pai = getattr(autodocs_pai, "Id do Texto")

print(f"Id do Texto do AUTODOCS pai criado: {id_pai}")

for idx, row in df.iterrows():

    if idx % 1000 == 0 or idx == len(df):
        print(f"Processados: {idx} | Inseridos: {inserted_cont} | Não inseridos: {not_inserted_cont} | Concluído: {round((idx / len(df)) * 100, 2)}%")

    text = verify_nan(row["texto"])
    if text == None:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Texto vazio ou nulo'
        not_inserted_data.append(row_dict)
        continue
        

    name = verify_nan(row["nome"])
    if name == None:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Nome da fórmula vazio ou nulo'
        not_inserted_data.append(row_dict)
        continue

    new_autodocs = Autodocs(
        Texto=text,
        Biblioteca=name,
        Pai=id_pai
    )
    setattr(new_autodocs, "Texto", bindparam(None, value=text, type_=UnicodeText()))

    log_data.append({
        "Texto": text,
        "Biblioteca": name,
        "Pai": id_pai
    })

    session.add(new_autodocs)
    inserted_cont += 1

    if inserted_cont % 1000 == 0:
        session.commit()

session.commit()
print(f"{inserted_cont} novos receituários foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} receituários não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_modelos_laudos.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_modelos_laudos.xlsx")