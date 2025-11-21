import csv
import glob
import json
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import create_log, verify_nan
from datetime import datetime


sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase= input("Informe o DATABASE: ")
path_file = input("Informe o caminho do arquivo: ")

print("Iniciando a conexão com o banco de dados...")

DATABASE_URL = f"mssql+pyoiledbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine) 

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Autodocs = getattr(Base.classes, "Autodocs")

print("Sucesso! Inicializando migração de Fórmulas...")

csv.field_size_limit(10000000000000)

todos_arquivos = glob.glob(f'{path_file}/formulas.csv')

df = pd.read_csv(todos_arquivos[0], sep=';', engine='python', quotechar="'", on_bad_lines='warn', escapechar='\\')
df = df.replace('None', '')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

data_hoje = datetime.now().strftime("%d/%m/%Y")
nome_biblioteca_pai = f"Documentos Migração {data_hoje}"

autodocs_pai = Autodocs(Pai=0, Biblioteca=nome_biblioteca_pai)
session.add(autodocs_pai)
session.commit()
id_pai = getattr(autodocs_pai, "Id do Texto")

print(f"Id do Texto do AUTODOCS pai criado: {id_pai}")

for idx, row in df.iterrows():

    if idx % 1000 == 0 or idx == len(df):
        print(f"Processados: {idx} | Inseridos: {inserted_cont} | Não inseridos: {not_inserted_cont} | Concluído: {round((idx / len(df)) * 100, 2)}%")

    text = verify_nan(row["Descricao"])
    if text == None:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Texto vazio ou nulo'
        not_inserted_data.append(row_dict)
        continue
        

    name = verify_nan(row["Titulo"])
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

create_log(log_data, log_folder, "log_inserted_formulas.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_formulas.xlsx")