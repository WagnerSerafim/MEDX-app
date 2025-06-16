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

Agenda = getattr(Base.classes, "Agenda")

print("Sucesso! Inicializando migração...")

json_file = glob.glob(f'{path_file}/atendimentos.json')
patients = glob.glob(f'{path_file}/pac_fun_for.json')

with open(json_file[0], 'r', encoding='utf-8') as file:
    json_data = json.load(file)
                     
log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont = 0
not_inserted_data = []
not_inserted_cont = 0

with open(patients[0], 'r', encoding='utf-8') as file:
    patients_data = json.load(file)

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

patients_lookup = {patient["PACIENcodigo"]: patient for patient in patients_data}

for dict in json_data:

    existing_scheduling = exists(session, dict["ATENDIcodigo"], "Id do Agendamento", Agenda)
    if existing_scheduling:
        not_inserted_cont += 1
        dict["Motivo"] = "Id do Agendamento já existe"
        not_inserted_data.append(dict)
        continue

    if pd.isna(dict["ATENDIcodigo"]) or dict["ATENDIcodigo"] == "":
        not_inserted_cont += 1
        dict["Motivo"] = "Id do Agendamento é vazio ou nulo"
        not_inserted_data.append(dict)
        continue

    if pd.isna(dict["ATENDIdataentrada"]) or dict["ATENDIdataentrada"] == "":
        not_inserted_cont += 1
        dict["Motivo"] = "Data de entrada é vazia ou nula"
        not_inserted_data.append(dict)
        continue

    if pd.isna(dict["ATENDIhoraentrada"]) or dict["ATENDIhoraentrada"] == "":
        not_inserted_cont += 1
        dict["Motivo"] = "Hora de entrada é vazia ou nula"
        not_inserted_data.append(dict)
        continue

    if pd.isna(dict["MEDICOcodigo"]) or dict["MEDICOcodigo"] == "":
        not_inserted_cont += 1
        dict["Motivo"] = "Id do Médico é vazio ou nulo"
        not_inserted_data.append(dict)
        continue

    if pd.isna(dict["PACIENcodigo"]) or dict["PACIENcodigo"] == "":
        not_inserted_cont += 1
        dict["Motivo"] = "Id do Paciente é vazio ou nulo"
        not_inserted_data.append(dict)
        continue
    
    paciente = patients_lookup.get(dict["PACIENcodigo"])
    name = paciente["PACIENnome"] if paciente else ""

    description = f"{name} {dict.get('ATENDIobservacao', '')}".strip()
    if not description:
        description = "Agendamento sem descrição no backup"
    
    start_date = get_valid_date(dict["ATENDIdataentrada"])
    end_date = get_valid_date(dict["ATENDIdatasaida"])

    try:
        start_time = datetime.strptime(f"{start_date} {dict['ATENDIhoraentrada']}", "%Y/%m/%d %H:%M")
        end_time = datetime.strptime(f"{end_date} {dict['ATENDIhorasaida']}", "%Y/%m/%d %H:%M")
    except Exception as e:
        not_inserted_cont += 1
        dict['Motivo'] = f'Data ou hora inválida: {e}'
        not_inserted_data.append(dict)
        continue

    new_scheduling = Agenda(
        Descrição=description,
        Início=start_time,
        Final=end_time,
        Status=1,
    )

    setattr(new_scheduling, "Id do Agendamento", dict["ATENDIcodigo"])
    setattr(new_scheduling, "Vinculado a", dict["PACIENcodigo"])
    setattr(new_scheduling, "Id do Usuário", dict["MEDICOcodigo"])
    
    log_data.append({
        "Id do Agendamento": dict["ATENDIcodigo"],
        "Vinculado a": dict["PACIENcodigo"],
        "Id do Usuário": dict["MEDICOcodigo"],
        "Início": start_time,
        "Final": end_time,
        "Descrição": description,
        "Status" : 1
    })

    session.add(new_scheduling)
    inserted_cont+=1
    if inserted_cont % 100 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos agendamentos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} agendamentos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_scheduling_atendimentos.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_scheduling_atendimentos.xlsx")
