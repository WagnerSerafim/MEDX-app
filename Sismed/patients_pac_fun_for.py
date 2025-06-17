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
path_file = input("Informe a pasta do arquivo JSON: ").strip()

print("Conectando no Banco de Dados...")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine) 

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Contatos = getattr(Base.classes, "Contatos")

print("Sucesso! Inicializando migração...")

log_folder = path_file

patients_path = glob.glob(f'{path_file}/pac_fun_for.json')
cities_path = glob.glob(f'{path_file}/cidades.json')
phones_path = glob.glob(f'{path_file}/telefones.json')

with open(patients_path[0], 'r', encoding='utf-8') as file:
    json_data = json.load(file)
    json_data = replace_null_with_empty_string(json_data)

with open(cities_path[0], 'r', encoding='utf-8') as file:
    json_cities = json.load(file)
    json_cities = replace_null_with_empty_string(json_cities)

with open(phones_path[0], 'r', encoding='utf-8') as file:
    json_phones = json.load(file)
    json_phones = replace_null_with_empty_string(json_phones)

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

patient_phones = {}

for phone in json_phones:
    patient_id = phone["TELEFOcodigopmsfnc"]
    phone_number = phone["TELEFOnumero"]
    
    if patient_id not in patient_phones:
        patient_phones[patient_id] = []

    patient_phones[patient_id].append(phone_number)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0


for dict in json_data:
    city = ""
    celular = ""
    telefone_residencial = ""

    existing_patient = exists(session, dict["PACIENcodigo"], "Id do Cliente", Contatos)
    if existing_patient:
        not_inserted_cont += 1
        dict['Motivo'] = 'Id do Cliente já existe'
        not_inserted_data.append(dict)
        continue
    else:
        patient_id = dict["PACIENcodigo"]
    
    patient_phone_list = patient_phones.get(patient_id, [])

    if len(patient_phone_list) > 0:
        cellphone = patient_phone_list[0]
    if len(patient_phone_list) > 1:
        home_phone = patient_phone_list[1]

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
        number = str(int(dict["PACIENend_numero"])) 
        address = f"{dict['PACIENend_rua']} {number}"

    name = truncate_value(dict["PACIENnome"], 50)
    rg = truncate_value(str(dict["PACIENrg"]), 25)
    email = truncate_value(dict["PACIENend_email"], 100)
    occupation = truncate_value(dict["PACIENprofissao"], 25)
    observation = dict["PACIENalertaspc"]
    mother = dict["PACIENmae"]
    father = dict["PACIENpai"]
    cpf = truncate_value(str(dict["PACIENcpf"]), 25)
    convenio_id = dict["CONVENcodigo"]
    spouse = dict["PACIENesposo"]
    cep = dict["PACIENend_cep"]
    complement = truncate_value(dict["PACIENend_complemento"], 50)
    neighbourhood = truncate_value(dict["BAIRROnome"], 25)

    new_patient = Contatos(
        Nome = name,
        Nascimento = birthday,
        Sexo = sex,
        RG = rg,
        Celular = cellphone,
        Email = email,
        Profissão = occupation,
        Observações = observation,
        Pai = father,
        Mãe = mother,
        Cônjugue = spouse
        
    )

    setattr(new_patient, "Id do Cliente", patient_id)
    setattr(new_patient, "CPF/CGC", cpf)
    setattr(new_patient, "Cep Residencial", cep)
    setattr(new_patient, "Endereço Residencial", truncate_value(address, 50))
    setattr(new_patient, "Endereço Comercial", complement)
    setattr(new_patient, "Bairro Residencial", neighbourhood)
    setattr(new_patient, "Cidade Residencial", truncate_value(city, 25))
    setattr(new_patient, "Telefone Residencial", truncate_value(home_phone, 25))
    setattr(new_patient, "Id da Assinatura", patient_id)
    setattr(new_patient, "Id do Convênio", convenio_id)
    
    log_data.append({
        "Id do Cliente": patient_id,
        "Id da Assinatura": patient_id,
        "Nascimento": birthday,
        "Sexo": sex,
        "RG": rg,
        "CPF/CGC":cpf,
        "Celular": cellphone,
        "Email": email,
        "Profissão": occupation,
        "Observações": observation,
        "Cep Residencial": cep,
        "Endereço Residencial": address,
        "Bairro Residencial": neighbourhood,
        "Cidade Residencial": city,
        "Pai": father,
        "Mãe": mother,
        "Cônjugue": spouse,
        "Telefone Residencial": home_phone,
        "Id do Convênio": convenio_id
    })

    session.add(new_patient)

    inserted_cont+=1
    if inserted_cont % 100 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos contatos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} contatos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_patients_pacientes.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_patients_pacientes.xlsx")
