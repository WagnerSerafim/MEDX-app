import json
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import urllib

def get_valid_date(json_dict):
    
    if pd.isna(json_dict) or json_dict in ["", "0000-00-00"]:
        return "01/01/1900"
    
    try:
        date_obj = datetime.strptime(str(json_dict), "%Y-%m-%d")
        if 1900 <= date_obj.year <= 2100:
            return date_obj.strftime("%Y/%m/%d")
        else:
            return "01/01/1900"
    except ValueError:
        return "01/01/1900"


log_data = []

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase= input("Informe o DATABASE: ")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine) 

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Agenda = Base.classes.Agenda

json_file = input("Informe a pasta do arquivo JSON: ").strip()                     
log_folder = os.path.dirname(json_file)
patients = f"{os.path.dirname(json_file)}/pac_fun_for.json"

with open(json_file, 'r', encoding='utf-8') as file:
    json_data = json.load(file)

with open(patients, 'r', encoding='utf-8') as file:
    patients_data = json.load(file)

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

for dict in json_data:

    if pd.isna(dict["ATENDIcodigo"]) or dict["ATENDIcodigo"] == "":
        continue

    elif pd.isna(dict["ATENDIdataentrada"]) or dict["ATENDIdataentrada"] == "":
        continue

    elif pd.isna(dict["ATENDIhoraentrada"]) or dict["ATENDIhoraentrada"] == "":
        continue

    elif pd.isna(dict["MEDICOcodigo"]) or dict["MEDICOcodigo"] == "":
        continue
    
    name = ""
    for patient in patients_data:
        if dict["PACIENcodigo"] == patient["PACIENcodigo"]:
            name = patient["PACIENnome"]
            break

    description = f"{name} {dict["ATENDIobservacao"]}"
    if description == "":
        description = "Agendamento sem descrição no backup"
    
    start_date = get_valid_date(dict["ATENDIdataentrada"])
    end_date = get_valid_date(dict["ATENDIdatasaida"])

    start_time = f"{start_date} {dict["ATENDIhoraentrada"]}"
    start_time = datetime.strptime(start_time, "%Y/%m/%d %H:%M")

    end_time = f"{end_date} {dict["ATENDIhorasaida"]}"
    end_time = datetime.strptime(end_time, "%Y/%m/%d %H:%M")

    new_schedulling = Agenda(
        Descrição=description,
        Início=start_time,
        Final=end_time,
        Status=1,
    )

    setattr(new_schedulling, "Id do Agendamento", dict["ATENDIcodigo"])
    setattr(new_schedulling, "Vinculado a", dict["PACIENcodigo"])
    setattr(new_schedulling, "Id do Usuário", dict["MEDICOcodigo"])
    
    log_data.append({
        "Id do Agendamento": dict["ATENDIcodigo"],
        "Vinculado a": dict["PACIENcodigo"],
        "Id do Usuário": dict["MEDICOcodigo"],
        "Início": start_time,
        "Final": end_time,
        "Descrição": description,
        "Status" : 1
    })

    session.add(new_schedulling)

session.commit()

print("Novos agendamentos inseridos com sucesso!")

session.close()

log_df = pd.DataFrame(log_data)
log_file_path = os.path.join(log_folder, "log_schedulling_atendimentos.xlsx")
log_df.to_excel(log_file_path, index=False)
