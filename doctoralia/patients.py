from datetime import datetime
import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, truncate_value
import csv

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")

print("Conectando no Banco de Dados...")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine)

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Contatos = getattr(Base.classes, "Contatos")

print("Sucesso! Inicializando migração de Contatos...")

csv.field_size_limit(1000000)
extension_file = glob.glob(f'{path_file}/patients.csv')

df = pd.read_csv(extension_file[0], sep=';', engine='python', encoding='utf-16')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for _, row in df.iterrows():

    if row["id"] in [None, '', 'None'] or pd.isna(row["id"]):
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Cliente vazio'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    else:
        id_patient = row["id"]

    existing_record = exists(session, id_patient, "Id do Cliente", Contatos)
    if existing_record:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Cliente já existe'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    
    if row['first name'] in [None, '', 'None'] or pd.isna(row['first name']):
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Nome do Paciente vazio'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    else:
        name = f'{row['first name']} {row['last name']}'

    if is_valid_date(row["date of birth"], "%Y-%m-%d"):
        birthday = row['date of birth']
    else:
        birthday = '1900-01-01'

    if row['gender'] == 'Female':
        sex = 'F'
    else:
        sex = 'M'

    if pd.isna(row['email']):
        email = None
    else:
        email = row['email']

    if pd.isna(row['document']):
        cpf = None
    else:
        cpf = row['document']

    if pd.isna(row['additional phone']):
        telephone = None
    else:
        telephone = row['additional phone']

    if pd.isna(row['phone']):
        cellphone = None
    else:
        cellphone = row['phone']
        cellphone = cellphone.replace("'","")

    if pd.isna(row['observations']):
        observation = None
    else:
        observation = row['observations']

    if pd.isna(row['marital status']):
        marital_status = None
    else:
        marital_status = row['marital status']
        if marital_status == 'Undefined':
            marital_status = 'Indefinido'

    if pd.isna(row['profession']):
        occupation = None
    else:
        occupation = row['profession']

    if pd.isna(row['address street']):
        address = None
    else:
        address = f'{row['address street']} {row['address number'] if not pd.isna(row['address number']) else ''}'

    if pd.isna(row['address postal code']):
        cep = None
    else:
        cep = row['address postal code']
    
    if pd.isna(row['address neighbordhood']):
        neighbourhood = None
    else:
        neighbourhood = row['address neighbordhood']

    if pd.isna(row['address city']):
        city = None
    else:
        city = row['address city']

    rg = None
    complement = None
    mother = None
    father = None

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
    setattr(new_patient, "Telefone Residencial", truncate_value(telephone, 25))
    setattr(new_patient, "Profissão", truncate_value(occupation, 25))
    setattr(new_patient, "Pai", truncate_value(father, 50))
    setattr(new_patient, "Mãe", truncate_value(mother, 50))
    setattr(new_patient, "RG", truncate_value(rg, 25))
    setattr(new_patient, "Observações", observation)
    setattr(new_patient, "Estado Civil", truncate_value(marital_status, 25))

    
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
        "Observações": observation,
        "Estado Civil": marital_status,
        "TimeStamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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

create_log(log_data, log_folder, "log_inserted_patients.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_patients.xlsx")
