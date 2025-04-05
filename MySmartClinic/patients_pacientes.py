import csv
import os
import re
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import urllib
from striprtf.striprtf import rtf_to_text


def truncate_value(value, max_length):
    """Se o valor for maior que max_length, ele será truncado"""
    if pd.isna(value):
        return None
    return str(value)[:max_length] 

def is_valid_date(date_str):
    """ Verifica se a data é válida e diferente de '0000-00-00' """
    if pd.isna(date_str) or date_str in ["", "0000-00-00"]:
        return False
    try:
        date_obj = datetime.strptime(str(date_str), "%Y-%m-%d") 
        return 1900 <= date_obj.year <= 2100  
    except ValueError:
        return False 

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase= input("Informe o DATABASE: ")
patients_csv = input("Informe o caminho de pacientes.csv: ").strip()

print("Conectando no Banco de Dados...")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine)

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Contatos = Base.classes.Contatos

print("Sucesso! Inicializando migração de pacientes MySmartClinic...\n")

log_folder = os.path.dirname(patients_csv)

csv.field_size_limit(10**6)
df = pd.read_csv(patients_csv, sep=";")
df = df.fillna(value="")

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
cont = 0

for index, row in df.iterrows():
    
    if row["id_paciente"] == None or row["id_paciente"] == '':
        continue
    else:
        id_patient = row["id_paciente"]
        # id_patient = re.sub(r'[a-f]','', id_patient)
    
    if row['nome'] == None or row['nome'] == '':
        continue
    else:
        name = row['nome']

    if not is_valid_date(row["data_nascimento"]):
        birthday = datetime.strptime("01/01/1900", "%d/%m/%Y")
    else:
        birthday = datetime.strptime(str(row["data_nascimento"]), "%Y-%m-%d")

    sex = row['sexo']
    if sex != 'M' and sex != 'F':
        sex = 'M'

    email = row['email']
    cpf = row['cpf']
    rg = row['rg']
    telephone = row['telefone']
    cellphone = row['celular']
    cep = row['cep']
    complement = row['complemento']
    neighbourhood = row['bairro']
    city = row['cidade']
    state = row['estado']
    occupation = row['profissao']
    mother = row['nome_mae']
    father = row['nome_pai']


    address = f"{row['endereco']} {row['numero']}"

    new_patient = Contatos(
        Nome=truncate_value(name, 50),
        Nascimento=birthday,
        Sexo=sex,
        Celular=truncate_value(cellphone, 25),
        Email=truncate_value(email, 100),
    )

    #setattr(new_patient, "Id do Cliente", id_patient)
    setattr(new_patient, "Referências", id_patient)
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

    
    log_data.append({
        #"Id do Cliente": id_patient,
        "Referências": id_patient,
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
    })

    session.add(new_patient)
    cont += 1
    if cont % 1000 == 0:
        session.commit()
        print(f"{cont} contatos inseridos...")

session.commit()

print(f"{cont} novos Contatos foram inseridos com sucesso!")

session.close()

log_df = pd.DataFrame(log_data)
log_file_path = os.path.join(log_folder, "log_patients_pacientes.xlsx")
log_df.to_excel(log_file_path, index=False)
