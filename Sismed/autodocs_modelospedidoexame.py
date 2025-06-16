import glob
import json
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import exists, create_log


sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase= input("Informe o DATABASE: ")
path_file = input("Informe a pasta do arquivo JSON: ").strip()          

print("Conectando no Banco de Dados...")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine) 

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Autodocs = getattr(Base.classes, "Autodocs")

print("Sucesso! Inicializando migração...")

json_file = glob.glob(f'{path_file}/modelospedidoexame.json')

with open(json_file[0], 'r', encoding='utf-8') as file:
    json_data = json.load(file)

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for dict in json_data:

    new_autodocs = Autodocs(
        Texto=dict["MOPEEXtexto"],
        Biblioteca=dict["MOPEEXnome"],
        Pai = -51757203
    )

    log_data.append({
        "Texto":dict["MOPEEXtexto"],
        "Biblioteca":dict["MOPEEXnome"],
        "Pai": -51757203
        })

    session.add(new_autodocs)

    inserted_cont+=1
    if inserted_cont % 100 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos modelos de pedidos de exames foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} modelos de pedidos de exames não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_autodocs_modelospedidoexame.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_autodocs_modelospedidoexame.xlsx")