import csv
import os
import glob
from sqlalchemy import MetaData, Table, create_engine, bindparam, UnicodeText
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, verify_nan

def truncate_value(value, max_length):
    if pd.isna(value):
        return None
    return str(value)[:max_length] 

def find_patient_csv(path_folder):
    csv_files = glob.glob(os.path.join(path_folder, "*patient.csv"))
    if not csv_files:
        print("Nenhum arquivo encontrado.")
        return None
    print(f"✅ Arquivo encontrado: {csv_files[0]}")
    return csv_files[0]


sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

metadata = MetaData()
contatos_tbl = Table("Contatos", metadata, schema=f"schema_{sid}", autoload_with=engine)
Base = declarative_base()

class Contatos(Base):
    __table__ = contatos_tbl

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

print("Sucesso! Inicializando migração de Contatos...")

csv_files = find_patient_csv(path_file)

csv.field_size_limit(100000000000000)

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont = 0
not_inserted_data = []
not_inserted_cont = 0

df = pd.read_csv(csv_files, sep=",", engine='python', quotechar='"')

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for index, row in df.iterrows():
    
    id_patient = verify_nan(row["patient_id"])
    if id_patient == None:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Cliente vazio'
        not_inserted_data.append(row_dict)
        continue

    existing_register = exists(session, row["patient_id"], "Id do Cliente", Contatos)
    if existing_register:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Cliente já existe'
        not_inserted_data.append(row_dict)
        continue
    
    name = verify_nan(row["name"])
    if name == None:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Nome do Cliente vazio'
        not_inserted_data.append(row_dict)
        continue

    if is_valid_date(row["birthdate"], "%Y-%m-%d"):
        birthday = row['birthdate']
    else:
        birthday = '01/01/1900'

    sex = "M" if pd.isna(row["gender"]) or row["gender"] == "" else row["gender"].upper()

    address = row["address"] if pd.isna(row["number"]) or row["number"] == "" else f"{row['address']} {str(row['number'])}"
    address = truncate_value(address, 50)

    name = truncate_value(name, 50)
    rg = truncate_value(str(row["rg"]), 25)
    cellphone = truncate_value(str(row["mobile_phone"]), 25)
    email = truncate_value(row["email"], 100)
    occupation = truncate_value(row["occupation"], 25)
    observation = verify_nan(row["observation"])
    home_phone = truncate_value(row["home_phone"], 25)
    cpf = truncate_value(str(row["cpf"]), 25)
    cep = truncate_value(str(row["zip_code"]), 10)
    neighborhood = truncate_value(row["neighborhood"], 25)
    city = truncate_value(row["city"], 25)
    state = truncate_value(row["state"], 2)
    country = truncate_value(row["country"], 50)
    complement = truncate_value(row["complement"], 50)

    novo_contato = Contatos(
        Nome=name,
        Nascimento=birthday,
        Sexo=sex,
        RG=rg,
        Celular=cellphone,
        Email=email,
        Profissão=occupation,
        Observações=observation
    )

    setattr(novo_contato, "Id do Cliente", id_patient)
    setattr(novo_contato, "CPF/CGC", cpf)
    setattr(novo_contato, "Telefone Residencial", home_phone)
    setattr(novo_contato, "Cep Residencial", cep)
    setattr(novo_contato, "Endereço Residencial", address)
    setattr(novo_contato, "Endereço Comercial", complement)
    setattr(novo_contato, "Bairro Residencial", neighborhood)
    setattr(novo_contato, "Cidade Residencial", city)
    setattr(novo_contato, "Estado Residencial", state)
    setattr(novo_contato, "País Residencial", country)

    log_data.append({
        "Nome": name,
        "Id do Cliente": id_patient,
        "Nascimento": birthday,
        "Sexo": sex,
        "RG": rg,
        "CPF/CGC": cpf,
        "Celular": cellphone,
        "Telefone Residencial": home_phone,
        "Email": email,
        "Profissão": occupation,
        "Observações": truncate_value(observation, 32000),
        "Cep Residencial": cep,
        "Endereço Residencial": address,
        "Endereço Comercial": complement,
        "Bairro Residencial": neighborhood,
        "Cidade Residencial": city,
        "Estado Residencial": state,
        "País Residencial": country
    })

    session.add(novo_contato)

    inserted_cont+=1
    if inserted_cont % 10000 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos contatos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} contatos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_patients_patients.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_patients_patients.xlsx")