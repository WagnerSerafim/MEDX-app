import json
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import create_log
from datetime import datetime


sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase= input("Informe o DATABASE: ")
path_folder = input("Informe o caminho do arquivo: ")

print("Iniciando a conexão com o banco de dados...")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine) 

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Autodocs = getattr(Base.classes, "Autodocs")

print("Carregando dados do arquivo JSON...")

json_file = os.path.join(path_folder, "documentos.json")
with open(json_file, 'r', encoding='utf-8') as file:
    json_data = json.load(file)

log_folder = path_folder

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

print("Iniciando a inserção dos Autodocs...")

for dict in json_data:

    if dict.get("conteudo") in ['', '<br>', None]:
        not_inserted_cont += 1
        dict['Motivo'] = 'Conteúdo vazio'
        dict['Timestamp'] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(dict)
        continue

    if dict.get("nome") in ['', '<br>', None]:
        not_inserted_cont += 1
        dict['Motivo'] = 'Nome vazio'
        dict['Timestamp'] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(dict)
        continue

    new_autodocs = Autodocs(
        Texto=dict["conteudo"],
        Biblioteca=dict["nome"],
        Pai=id_pai
    )
    setattr(new_autodocs, "Id do Texto", 0 - dict['codd'])

    log_data.append({
        "Texto": dict["conteudo"],
        "Biblioteca": dict["nome"],
        "Pai": id_pai
    })

    session.add(new_autodocs)
    inserted_cont += 1

    if inserted_cont % 1000 == 0:
        session.commit()

session.commit()
print(f"{inserted_cont} novos históricos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} históricos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_autodocs_documentos.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_autodocs_documentos.xlsx")