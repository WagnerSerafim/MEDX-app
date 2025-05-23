import os
import json
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
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
dbase = input("Informe o DATABASE: ")
json_file = input("Informe a pasta do arquivo JSON: ").strip()

print("Conectando no Banco de Dados...")
DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine)

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Contatos = getattr(Base.classes, "Contatos")

print("Sucesso! Inicializando migração de Contatos...")

log_folder = os.path.dirname(json_file)
cities_path = f"{os.path.dirname(json_file)}/cidades.json"
phones_path = f"{os.path.dirname(json_file)}/telefones.json"

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

try:
    with open(json_file, 'r', encoding='utf-8') as file:
        json_data = json.load(file)
        json_data = replace_null_with_empty_string(json_data)
except PermissionError:
    print(f"Permission denied: Unable to access '{json_file}'. Please check the file permissions.")
    exit(1)
except FileNotFoundError:
    print(f"File not found: '{json_file}'. Please provide a valid JSON file path.")
    exit(1)

with open(cities_path, 'r', encoding='utf-8') as file:
    json_cities = json.load(file)
    json_cities = replace_null_with_empty_string(json_cities)

with open(phones_path, 'r', encoding='utf-8') as file:
    json_phones = json.load(file)
    json_phones = replace_null_with_empty_string(json_phones)

# Create a lookup dictionary for cities
city_lookup = {city_dict["CIDADEcodigo"]: city_dict["CIDADEnome"] for city_dict in json_cities}

# Create a lookup dictionary for phones
phones_lookup = {}
for phone in json_phones:
    if phone["TELEFOpmsfnc"] == "P":
        patient_id = phone["TELEFOcodigopmsfnc"]
        if patient_id not in phones_lookup:
            phones_lookup[patient_id] = []
        phones_lookup[patient_id].append(phone["TELEFOnumero"])

log_data = []
inserted_cont = 0
not_inserted_data = []
not_inserted_cont = 0

for dict in json_data:
    city = city_lookup.get(dict["CIDADEcodigo"], "")

    phones = phones_lookup.get(dict["PACIENcodigo"], [])
    first_phone = phones[0] if len(phones) > 0 else None
    second_phone = phones[1] if len(phones) > 1 else None
    other_phones = phones[2:] if len(phones) > 2 else []

    telephone = truncate_value(first_phone, 25) if first_phone else None
    cellphone = truncate_value(second_phone, 25) if second_phone else None
    other_phones_str = ", ".join(other_phones) if other_phones else ""

    observations = dict["PACIENalertaspc"] or ""
    if other_phones_str:
        observations += f" | Outros telefones: {other_phones_str}"

    if not dict["PACIENcodigo"]:
        not_inserted_cont += 1
        dict['Motivo'] = 'Id do Cliente vazio'
        not_inserted_data.append(dict)
        continue

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

    existing_record = exists(session, dict["PACIENcodigo"], "Id do Cliente", Contatos)
    if existing_record:
        not_inserted_cont += 1
        dict['Motivo'] = 'Id do Cliente já existe'
        not_inserted_data.append(dict)
        continue

    new_patient = Contatos(
        Nome=truncate_value(dict["PACIENnome"], 50),
        Nascimento=birthday,
        Sexo=sex,
        RG=truncate_value(dict["PACIENrg"], 25),
        Celular=cellphone,
        Email=truncate_value(dict["PACIENend_email"], 100),
        Profissão=truncate_value(dict["PACIENprofissao"], 25),
        Observações=observations,
        Pai=dict["PACIENpai"],
        Mãe=dict["PACIENmae"]
    )

    setattr(new_patient, "Id do Cliente", dict["PACIENcodigo"])
    setattr(new_patient, "Telefone Residencial", telephone)
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
        "Telefone": telephone,
        "Celular": cellphone,
        "Email": dict["PACIENend_email"],
        "Profissão": dict["PACIENprofissao"],
        "Observações": observations,
        "Cep Residencial": dict["PACIENend_cep"],
        "Endereço Residencial": address,
        "Bairro Residencial": truncate_value(dict["BAIRROnome"], 25),
        "Cidade Residencial": city,
        "Pai": dict["PACIENpai"],
        "Mãe": dict["PACIENmae"]
    })

    session.add(new_patient)
    inserted_cont += 1
    if inserted_cont % 10000 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos contatos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} contatos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_patients_pac_fun_for.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_patients_pac_fun_for.xlsx")
