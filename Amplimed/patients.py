from datetime import datetime
import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, replace_null_with_empty_string, truncate_value
import json

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

patients_path = glob.glob(f'{path_file}/pacientes.json')
cities_path = glob.glob(f'{path_file}/cidade.json')
state_path = glob.glob(f'{path_file}/estado.json')

with open(patients_path[0], 'r', encoding='utf-8') as file:
    json_data = json.load(file)
    json_data = replace_null_with_empty_string(json_data)

with open(cities_path[0], 'r', encoding='utf-8') as file:
    json_cities = json.load(file)
    json_cities = replace_null_with_empty_string(json_cities)

with open(state_path[0], 'r', encoding='utf-8') as file:
    json_state = json.load(file)
    json_state = replace_null_with_empty_string(json_state)


cities_lookup = {city['id']: city['nome'] for city in json_cities}

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0
idx = 0

for row in json_data:
    idx += 1

    if idx % 1000 == 0 or idx == len(json_data):
        print(f"Processados: {idx} | Inseridos: {inserted_cont} | Não inseridos: {not_inserted_cont} | Concluído: {round((idx / len(json_data)) * 100, 2)}%")

    if row["codp"] in [None, '', 'None'] or pd.isna(row["codp"]):
        not_inserted_cont +=1
        row_dict = row
        row_dict['Motivo'] = 'Id do Cliente vazio'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    else:
        id_patient = row["codp"] 

    existing_record = exists(session, id_patient, "Id do Cliente", Contatos)
    if existing_record:
        not_inserted_cont +=1
        row_dict = row
        row_dict['Motivo'] = 'Cliente já existe no banco de dados'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    
    if row['nome'] in [None, '', 'None'] or pd.isna(row['nome']):
        not_inserted_cont +=1
        row_dict = row
        row_dict['Motivo'] = 'Nome do Paciente vazio'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    else:
        name = row['nome']

    if is_valid_date(row["dtnasc"], "%Y-%m-%d"):
        birthday = row['dtnasc']
    else:
        birthday = '1900-01-01'

    if row['genero'] == 'Feminino':
        sex = 'F'
    else:
        sex = 'M'

    email = row['email'] if not pd.isna(row['email']) else ''
    cpf = row['cpf'] if not pd.isna(row['cpf']) else ''
    telephone = row['telf'] if not pd.isna(row['telf']) else ''
    cellphone = row['celular'] if not pd.isna(row['celular']) else ''
    observation = row['obs'] if not pd.isna(row['obs']) else ''
    marital_status = row['estadoci'] if not pd.isna(row['estadoci']) else ''
    occupation = row['profis'] if not pd.isna(row['profis']) else ''
    address = f"{row['endereco']} {row['numero'] if not pd.isna(row['numero']) else ''}" if not pd.isna(row['endereco']) else ''
    cep = row['cep'] if not pd.isna(row['cep']) else ''
    neighbourhood = row['bairro'] if not pd.isna(row['bairro']) else ''
    city = cities_lookup.get(row['idcidade'], '') if not pd.isna(row['idcidade']) else ''
    state = ''
    complement = row['comple'] if not pd.isna(row['comple']) else ''
    rg = row['rg'] if not pd.isna(row['rg']) else ''
    mother = row['nmae'] if not pd.isna(row['nmae']) else ''
    father = row['npai'] if not pd.isna(row['npai']) else ''
    # id_insurance = row['convenio'] if not pd.isna(row['convenio']) else ''

    new_patient = Contatos(
        Nome=truncate_value(name, 50),
        Nascimento=birthday,
        Sexo=sex,
        Celular=truncate_value(cellphone, 25),
        Email=truncate_value(email, 100),
    )

    setattr(new_patient, "Id do Cliente", id_patient)
    setattr(new_patient, "CPF/CGC", truncate_value(cpf, 25))
    setattr(new_patient, "Cep Residencial", truncate_value(cep, 10))
    setattr(new_patient, "Endereço Residencial", truncate_value(address, 50))
    setattr(new_patient, "Endereço Comercial", truncate_value(complement, 50))
    setattr(new_patient, "Bairro Residencial", truncate_value(neighbourhood, 25))
    setattr(new_patient, "Cidade Residencial", truncate_value(city, 25))
    setattr(new_patient, "Estado Residencial", truncate_value(state, 2))
    setattr(new_patient, "Telefone Residencial", truncate_value(telephone, 25))
    setattr(new_patient, "Profissão", truncate_value(occupation, 25))
    setattr(new_patient, "Pai", truncate_value(father, 50))
    setattr(new_patient, "Mãe", truncate_value(mother, 50))
    setattr(new_patient, "RG", truncate_value(rg, 25))
    setattr(new_patient, "Observações", observation)
    setattr(new_patient, "Estado Civil", truncate_value(marital_status, 25))
    # setattr(new_patient, "Id do Convênio", id_insurance)

    
    log_data.append({
        "Id do Cliente": id_patient,
        "Nome": name,
        "Nascimento": birthday,
        "Sexo": sex,
        "CPF/CGC": cpf,
        "RG" : rg,
        "Profissão": occupation,
        "Pai": father,
        "Mãe": mother,
        "Telefone Residencial": telephone,
        "Celular": cellphone,
        "Email": email,
        "Cep Residencial": cep,
        "Endereço Residencial": address,
        "Endereço Comercial": truncate_value(complement, 50),
        "Bairro Residencial": truncate_value(neighbourhood, 25),
        "Cidade Residencial": city,
        "Estado Residencial": state,
        "Observações": observation,
        "Estado Civil": marital_status,
        # "Id do Convênio": id_insurance,
        "TimeStamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

    session.add(new_patient)

    inserted_cont+=1
    if inserted_cont % 1000 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos contatos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} contatos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_patients.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_patients.xlsx")
