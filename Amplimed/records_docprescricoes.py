import json
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib

def get_date(json_dict):
    if json_dict["dtconsulta"] != "":
        date = json_dict["dtconsulta"] + " 00:00"
    else: 
        date = "01/01/1900 00:00"
    return date


def get_record(json_consulta,presc):
    record = ""
    for consulta in json_consulta:
        if presc["codcon"] == consulta["codcon"]:
            record = f"Prescrição: {presc["conteudo"]}"
            date = get_date(consulta)
            codp = consulta["codp"]
            break
    
    return record, date, codp

    

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

json_file = input("Informe a pasta do arquivo JSON consulta: ")
with open(json_file, 'r', encoding='utf-8') as file:
    json_consulta = json.load(file)

json_file = input("Informe a pasta do arquivo JSON documentosprescricoes: ")
with open(json_file, 'r', encoding='utf-8') as file:
    json_data = json.load(file)
                     
log_folder = input("Informe a pasta onde deseja salvar o arquivo de log: ").strip()

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []

for dict in json_data:

    record,date,codp = get_record(json_consulta, dict)
    if record == "":
        continue

    new_record = HistoricoClientes(
        Histórico=record,
        Data=date,
    )

    setattr(new_record, "Id do Cliente", codp)
    # setattr(new_record, "Id do Histórico", 0-dict["codcon"])
    setattr(new_record, "Id do Usuário", 0)

    log_data.append({
        # "Id do Histórico": dict["codcon"],
        "Id do Cliente": codp,
        "Data": date,
        "Histórico": record,
        "Id do Usuário": 0,
        })

    session.add(new_record)

session.commit()

print("Históricos inseridos com sucesso!")

session.close()

log_df = pd.DataFrame(log_data)
log_file_path = os.path.join(log_folder, "record_log.xlsx")
log_df.to_excel(log_file_path, index=False)