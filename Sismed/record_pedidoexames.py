import csv
import json
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import urllib

def get_date(json_dict):
    if json_dict["PEDEXAdatahora"] != "" and json_dict["PEDEXAdatahora"] != None:
        date = json_dict["PEDEXAdatahora"]
    else: 
        date = "01/01/1900 00:00"
    return date


def get_record(json_dict):
    record = ""
    if json_dict["PEDEXAtexto"] != "" and json_dict["PEDEXAtexto"] != None:
        record = json_dict["PEDEXAtexto"]
    
    return record

    

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase= input("Informe o DATABASE: ")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine) 

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

HistoricoClientes = getattr(Base.classes, "Histórico de Clientes")

json_file = input("Informe a pasta do arquivo JSON: ").strip()                     
log_folder = os.path.dirname(json_file)

with open(json_file, 'r', encoding='utf-8') as file:
    json_data = json.load(file)

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []

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
    setattr(new_record, "Id do Histórico", 100000-dict["PEDEXAcodigo"])
    setattr(new_record, "Id do Usuário", 0)

    log_data.append({
        "Id do Histórico": dict["PEDEXAcodigo"],
        "Id do Cliente": dict["PACIENcodigo"],
        "Data": date,
        "Histórico": record,
        "Id do Usuário": 0,
        })

    session.add(new_record)

session.commit()

print("Históricos inseridos com sucesso!")

session.close()

log_df = pd.DataFrame(log_data)
log_file_path = os.path.join(log_folder, "record_pedidoexames_log.xlsx")
log_df.to_excel(log_file_path, index=False)