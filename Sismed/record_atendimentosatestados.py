import csv
import glob
import json
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import urllib
from utils.utils import exists, create_log

def get_date(json_dict):
    if json_dict["ATESTAdatahora"] != "" and json_dict["ATESTAdatahora"] != None:
        date = json_dict["ATESTAdatahora"]
    else: 
        date = "01/01/1900 00:00"
    return date


def get_record(json_dict):
    record = ""
    if json_dict["ATESTAtexto"] != "" and json_dict["ATESTAtexto"] != None:
        record = json_dict["ATESTAtexto"]
    
    return record

    

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase= input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta: ")                   

print("Conectando no Banco de Dados...")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine) 

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

HistoricoClientes = getattr(Base.classes, "Histórico de Clientes")

print("Sucesso! Inicializando migração...")

json_file = glob.glob(f'{path_file}/atendimentosatestados.json')

with open(json_file[0], 'r', encoding='utf-8') as file:
    json_data = json.load(file)
                     
log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont = 0
not_inserted_data = []
not_inserted_cont = 0

for dict in json_data:

    record = get_record(dict)
    if record == "":
        continue

    date = get_date(dict)

    new_record = HistoricoClientes(
        Histórico=record,
        Data=date,
    )

    setattr(new_record, "Id do Cliente", dict["PACIENcodigo"])
    setattr(new_record, "Id do Histórico", -5396-dict["ATESTAcodigo"])
    setattr(new_record, "Id do Usuário", 0)

    log_data.append({
        "Id do Histórico": dict["ATESTAcodigo"],
        "Id do Cliente": dict["PACIENcodigo"],
        "Data": date,
        "Histórico": record,
        "Id do Usuário": 0,
        })

    session.add(new_record)

    inserted_cont+=1
    if inserted_cont % 100 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos historicos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} historicos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_record_atendimentosatestados.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_record_atendimentosatestados.xlsx")