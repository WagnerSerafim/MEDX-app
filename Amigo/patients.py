import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import urllib
from utils.utils import *

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")

print("Conectando no Banco de dados...")

try:
    DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

    engine = create_engine(DATABASE_URL)

    Base = automap_base()
    Base.prepare(autoload_with=engine)

    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()

    Contatos = Base.classes.Contatos

except Exception as e:
    print(f"Erro ao conectar ao banco de dados: {e}")
    exit()

print("Sucesso! Começando migração de pacientes...")

excel_file = glob.glob(f'{path_file}/patients.xlsx')
df = pd.read_excel(excel_file[0])
df = df.replace('None', '')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for idx, row in df.iterrows():

    if row['id'] in [None, '', 'None']:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Cliente vazio'
        not_inserted_data.append(row_dict)
        continue

    existing_patient = session.query(Contatos).filter(getattr(Contatos, "Id do Cliente")==row["id"]).first()
    if existing_patient:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Cliente já existe no Banco de Dados'
        not_inserted_data.append(row_dict)
        continue
    else: 
        id_patient = row["id"]

    if isinstance(row['born'], datetime):
        date_str = row['born'].strftime('%Y-%m-%d')
        if is_valid_date(date_str, '%Y-%m-%d'):
            birthday = date_str
        else:
            birthday = '01/01/1900'
    else:
        if is_valid_date(row['born'], '%Y-%m-%d'):
            birthday = row['born']
        else:
            birthday = '01/01/1900'

    if row['gender'] == "Feminino":
        sex = 'F'
    else:
        sex = "M"

    if row["address_number"] in [None, '', 'None'] or pd.isna(row['address_number']):
        address = row["address_address"]
    else:
        number = str(row["address_number"]) 
        address = f"{row['address_address']} {number}"  

    name = truncate_value(row["name"], 50)
    rg = truncate_value(str(row["rg"]), 25)
    cpf = truncate_value(str(row["cpf"]), 25)
    cellphone = row['contact_cellphone']
    email = truncate_value(row["email"], 100)
    occupation = truncate_value(row["jobrole"], 25)
    cep = truncate_value(row["address_cep"], 10)
    complement = truncate_value(row["address_complement"], 50)
    neighbourhood = truncate_value(row["address_district"], 25)
    city = truncate_value(row["address_city"], 25)
    father = truncate_value(row["father_name"], 50)
    mother = truncate_value(row["mother_name"], 50)
    telephone = truncate_value(row["contact_phone_home"], 25)
    observation = row["observation"]

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
    setattr(new_patient, "Endereço Residencial", truncate_value(address,50))
    setattr(new_patient, "Endereço Comercial", truncate_value(complement, 50))
    setattr(new_patient, "Bairro Residencial", truncate_value(neighbourhood, 25))
    setattr(new_patient, "Cidade Residencial", truncate_value(city, 25))
    setattr(new_patient, "Telefone Residencial", truncate_value(telephone, 25))
    setattr(new_patient, "Profissão", truncate_value(occupation, 25))
    setattr(new_patient, "Pai", truncate_value(father, 50))
    setattr(new_patient, "Mãe", truncate_value(mother, 50))
    setattr(new_patient, "RG", truncate_value(rg, 25))
    
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
        "Endereço Residencial": truncate_value(address,50),
        "Endereço Comercial": truncate_value(complement, 50),
        "Bairro Residencial": truncate_value(neighbourhood, 25),
        "Cidade Residencial": city,
    })

    session.add(new_patient)

    inserted_cont+=1

    if inserted_cont % 1000 == 0:
        session.commit()

    if (idx) % 1000 == 0 or (idx) == len(df):
        print(f"Processados {idx} de {len(df)} registros ({(idx) / len(df) * 100:.2f}%)")

session.commit()

print(f"{inserted_cont} novos contatos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} contatos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_patients.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_patients.xlsx")
