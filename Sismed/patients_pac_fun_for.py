import csv
import json
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, truncate_value 

def replace_null_with_empty_string(data):
    if isinstance(data, dict): 
        return {key: replace_null_with_empty_string(value) for key, value in data.items()}
    elif isinstance(data, list):  
        return [replace_null_with_empty_string(item) for item in data]
    elif data is None:  
        return ""
    else:
        return data    

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase= input("Informe o DATABASE: ")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine) 

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Contatos = getattr(Base.classes, "Contatos")

json_file = input("Informe a pasta do arquivo JSON: ").strip()                     
log_folder = os.path.dirname(json_file)
cities_path = f"{os.path.dirname(json_file)}/cidades.json"

with open(json_file, 'r', encoding='utf-8') as file:
    json_data = json.load(file)
    json_data = replace_null_with_empty_string(json_data)

with open(cities_path, 'r', encoding='utf-8') as file:
    json_cities = json.load(file)
    json_cities = replace_null_with_empty_string(json_cities)

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []

for dict in json_data:
    city = ""

    if dict["CIDADEcodigo"] != "" and dict["CIDADEcodigo"] != None:
        for city_dict in json_cities:
            if dict["CIDADEcodigo"] == city_dict["CIDADEcodigo"]:
                city = city_dict["CIDADEnome"]
                break

    if is_valid_date(dict["PACIENdatanascimento"], "%Y-%m-%d"):
        birthday = dict['PACIENdatanascimento']
    else:
        birthday = '01/01/1900'

    sex = "M" if pd.isna(dict["PACIENsexo"]) or dict["PACIENsexo"] == "" else dict["PACIENsexo"].upper()

    if pd.isna(dict["PACIENend_numero"]) or dict["PACIENend_numero"] == "":
        address = dict["PACIENend_rua"]
    else:
        number = str(dict["PACIENend_numero"]) 
        address = f"{dict['PACIENend_rua']} {number}"

    new_patient = Contatos(
        Nome=truncate_value(dict["PACIENnome"], 50),
        Nascimento=birthday,
        Sexo=sex,
        RG=truncate_value(dict["PACIENrg"], 25),
        Celular=truncate_value(dict["PACIENcontato"], 25),
        Email=truncate_value(dict["PACIENend_email"], 100),
        Profissão=truncate_value(dict["PACIENprofissao"], 25),
        Observações=dict["PACIENalertaspc"],
        Pai = dict["PACIENpai"],
        Mãe = dict["PACIENmae"]
    )

    setattr(new_patient, "Id do Cliente", dict["PACIENcodigo"])
    setattr(new_patient, "CPF/CGC", truncate_value(dict["PACIENcpf"], 25))
    setattr(new_patient, "Cep Residencial", truncate_value(dict["PACIENend_cep"], 10))
    setattr(new_patient, "Endereço Residencial", truncate_value(address, 50))
    setattr(new_patient, "Endereço Comercial", truncate_value(dict["PACIENend_complemento"], 50))
    setattr(new_patient, "Bairro Residencial", truncate_value(dict["BAIRROnome"], 25))
    setattr(new_patient, "Cidade Residencial", truncate_value(city, 25))
    
    log_data.append({
        "Id do Cliente": dict["PACIENcodigo"],
        "Nascimento": birthday,
        "Sexo": sex,
        "RG": truncate_value(dict["PACIENrg"], 25),
        "CPF/CGC": dict["PACIENcpf"],
        "Celular": dict["PACIENcontato"],
        "Email": dict["PACIENend_email"],
        "Profissão": dict["PACIENprofissao"],
        "Observações": dict["PACIENalertaspc"],
        "Cep Residencial": dict["PACIENend_cep"],
        "Endereço Residencial": address,
        "Bairro Residencial": truncate_value(dict["BAIRROnome"], 25),
        "Cidade Residencial": city,
        "Pai": dict["PACIENpai"],
        "Mãe": dict["PACIENmae"]
    })

    session.add(new_patient)

session.commit()

print("Novos contatos inseridos com sucesso!")

session.close()

log_df = pd.DataFrame(log_data)
log_file_path = os.path.join(log_folder, "patients_log.xlsx")
log_df.to_excel(log_file_path, index=False)
