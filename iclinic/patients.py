import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import urllib


def truncate_value(value, max_length):
    """Se o valor for maior que max_length, ele será truncado"""
    if pd.isna(value):
        return None
    return str(value)[:max_length] 

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase= input("Informe o DATABASE: ")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine) 

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Contatos = Base.classes.Contatos

log_folder = input("Informe a pasta onde deseja salvar o arquivo de log: ").strip()

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

patients_csv = input("Arquivo CSV de pacientes: ").strip()
df = pd.read_csv(patients_csv, sep=None, engine='python')
df = df.fillna(value="")

log_data = []

for index, row in df.iterrows():

    if pd.isna(row["birthdate"]) or row["birthdate"] == "":
        birthday = datetime.strptime("01/01/1900", "%d/%m/%Y")
    else:
        birthday = row["birthdate"]

    sex = "M" if pd.isna(row["gender"]) or row["gender"] == "" else row["gender"].upper()

    if pd.isna(row["number"]) or row["number"] == "":
        address = row["address"]
    else:
        number = str(row["number"]) 
        address = f"{row['address']} {number}"  


    novo_contato = Contatos(
        Nome=truncate_value(row["name"], 50),
        Nascimento=birthday,
        Sexo=sex,
        RG=truncate_value(row["rg"], 25),
        Celular=truncate_value(row["mobile_phone"], 25),
        Email=truncate_value(row["email"], 100),
        Profissão=truncate_value(row["occupation"], 25),
        Observações=row["observation"]
    )

    setattr(novo_contato, "Id do Cliente", row["patient_id"])
    setattr(novo_contato, "CPF/CGC", truncate_value(row["cpf"], 25))
    setattr(novo_contato, "Telefone Residencial", truncate_value(row["home_phone"], 25))
    setattr(novo_contato, "Cep Residencial", truncate_value(row["zip_code"], 10))
    setattr(novo_contato, "Endereço Residencial", truncate_value(address, 50))
    setattr(novo_contato, "Endereço Comercial", truncate_value(row["complement"], 50))
    setattr(novo_contato, "Bairro Residencial", truncate_value(row["neighborhood"], 25))
    setattr(novo_contato, "Cidade Residencial", truncate_value(row["city"], 25))
    setattr(novo_contato, "Estado Residencial", truncate_value(row["state"], 2))
    setattr(novo_contato, "País Residencial", truncate_value(row["country"], 50))
    
    log_data.append({
        "Id do Cliente": row["patient_id"],
        "Nascimento": birthday,
        "Sexo": sex,
        "RG": truncate_value(row["rg"], 25),
        "CPF/CGC": row["cpf"],
        "Celular": row["mobile_phone"],
        "Telefone Residencial": row["home_phone"],
        "Email": row["email"],
        "Profissão": row["occupation"],
        "Observações": row["observation"],
        "Cep Residencial": row["zip_code"],
        "Endereço Residencial": address,
        "Bairro Residencial": truncate_value(row["neighborhood"], 25),
        "Cidade Residencial": row["city"],
        "Estado Residencial": truncate_value(row["state"], 2),
        "País Residencial": truncate_value(row["country"], 50)
    })

    session.add(novo_contato)

session.commit()

print("Novos contatos inseridos com sucesso!")

session.close()

log_df = pd.DataFrame(log_data)
log_file_path = os.path.join(log_folder, "patients_log.xlsx")
log_df.to_excel(log_file_path, index=False)