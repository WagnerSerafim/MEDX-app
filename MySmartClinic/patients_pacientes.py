import csv
import glob
import os
import re
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import urllib
from striprtf.striprtf import rtf_to_text
from utils.utils import create_log, is_valid_date, exists, truncate_value

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase= input("Informe o DATABASE: ")
path_file = input("Informe o caminho dos arquivos: ").strip()

print("Conectando no Banco de Dados...")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine)

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Contatos = Base.classes.Contatos

print("Sucesso! Inicializando migração de pacientes MySmartClinic...\n")

log_folder = os.path.dirname(path_file)
csv_file = glob.glob(f'{path_file}/pacientes.csv')

csv.field_size_limit(10**6)
df = pd.read_csv(csv_file[0], sep=";", encoding="ISO-8859-1")
df = df.fillna(value="")

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for index, row in df.iterrows():
    
    if row["id_paciente"] == None or row["id_paciente"] == '':
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict["Motivo"] = "ID do paciente está vazio"
        not_inserted_data.append(row_dict)
        continue
    else:
        id_patient = row["id_paciente"]
    
    if row['nome'] == None or row['nome'] == '':
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict["Motivo"] = "Nome do paciente está vazio"
        not_inserted_data.append(row_dict)
        continue
    else:
        name = row['nome']

    if is_valid_date(row["data_nascimento"], "%Y-%m-%d"):
        birthday = row['data_nascimento']
    else:
        birthday = '01/01/1900'

    if row['sexo'] != 'M' and row['sexo'] != 'F':
        sex = 'M'
    else:
        sex = row['sexo']

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
    
    inserted_cont+=1
    if inserted_cont % 10000 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos contatos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} contatos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_patients_pacientes.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_patients_pacientes.xlsx")

