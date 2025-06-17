import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import urllib
from utils.utils import *
import math

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
count_id = 0

for _, row in df.iterrows():

    if 'ID' in df.columns:
        if row['ID'] in [None, '', 'None']:
            not_inserted_cont += 1
            row_dict = row.to_dict()
            row_dict['Motivo'] = 'Id do Cliente vazio'
            not_inserted_data.append(row_dict)
            continue

        existing_patient = session.query(Contatos).filter(getattr(Contatos, "Id do Cliente")==row["ID"]).first()
        if existing_patient:
            not_inserted_cont +=1
            row_dict = row.to_dict()
            row_dict['Motivo'] = 'Id do Cliente já existe no Banco de Dados'
            not_inserted_data.append(row_dict)
            continue
        else: 
            id_patient = row["ID"]
    else:
        count_id += 1
        id_patient = count_id
        existing_patient = session.query(Contatos).filter(getattr(Contatos, "Id do Cliente")==id_patient).first()
        if existing_patient:
            not_inserted_cont += 1
            row_dict = row.to_dict()
            row_dict['Motivo'] = 'Id do Cliente já existe no Banco de Dados'
            not_inserted_data.append(row_dict)
            continue

    if 'NASCIMENTO' in df.columns:
        if isinstance(row['NASCIMENTO'], datetime):
            birthday = row['NASCIMENTO'].strftime('%Y-%m-%d')
        else:
            # tenta converter string para datetime
            try:
                birthday_dt = pd.to_datetime(row['NASCIMENTO'], dayfirst=True, errors='coerce')
                if pd.isna(birthday_dt):
                    birthday = '1900-01-01'
                else:
                    birthday = birthday_dt.strftime('%Y-%m-%d')
            except Exception:
                birthday = '1900-01-01'
    else:
        birthday = '1900-01-01'

    if 'SEXO' in df.columns:
        if row['SEXO'] == "F":
            sex = "F"
        else:
            sex = "M"
    else:
        sex = "M"  

    if 'NUMERO' in df.columns and 'ENDERECO' in df.columns:
        if row["NUMERO"] in [None, '', 'None']:
            address = row["ENDERECO"]
        else:
            number = str(row["NUMERO"]) 
            address = f"{row['ENDERECO']} {number}"
    elif 'ENDERECO' in df.columns:
        address = row["ENDERECO"]
    else:
        address = ''

    if row['NOME'] in [None, '', 'None'] or pd.isna(row['NOME']):
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Nome vazio'
        not_inserted_data.append(row_dict)
        continue
    else:
        name = truncate_value(clean_value(row["NOME"]), 50)
    
        
    rg = truncate_value(clean_value(verify_column_exists("RG", df, row)), 25)

    cpf = truncate_value(clean_value(verify_column_exists("CPF", df, row)), 25)

    cellphone = truncate_value(clean_value(verify_column_exists("CELULAR", df, row)), 25)

    email = truncate_value(clean_value(verify_column_exists("EMAIL", df, row)), 100)

    occupation = truncate_value(clean_value(verify_column_exists("PROFISSAO", df, row)), 25)

    cep = truncate_value(clean_value(verify_column_exists("CEP", df, row)), 10)

    complement = truncate_value(clean_value(verify_column_exists("COMPLEMENTO", df, row)), 50)

    neighbourhood = truncate_value(clean_value(verify_column_exists("BAIRRO", df, row)), 25)

    city = truncate_value(clean_value(verify_column_exists("CIDADE", df, row)), 25)

    father = truncate_value(clean_value(verify_column_exists("PAI", df, row)), 50)

    mother = truncate_value(clean_value(verify_column_exists("MAE", df, row)), 50)

    home_phone = truncate_value(clean_value(verify_column_exists("TELEFONE", df, row)), 25)

    insurance = clean_value(verify_column_exists("CONVENIO", df, row))

    observation = clean_value(verify_column_exists("OBSERVACOES", df, row))
    address = truncate_value(clean_value(address), 50)

    new_patient = Contatos(
        Nome=name,
        Nascimento=birthday,
        Sexo=sex,
        Celular=cellphone,
        Email=email,
    )

    setattr(new_patient, "Id do Cliente", clean_value(id_patient))
    setattr(new_patient, "CPF/CGC", cpf)
    setattr(new_patient, "Cep Residencial", cep)
    setattr(new_patient, "Endereço Residencial", address)
    setattr(new_patient, "Endereço Comercial", complement)
    setattr(new_patient, "Bairro Residencial", neighbourhood)
    setattr(new_patient, "Cidade Residencial", city)
    setattr(new_patient, "Telefone Residencial", home_phone)
    setattr(new_patient, "Profissão", occupation)
    setattr(new_patient, "Pai", father)
    setattr(new_patient, "Mãe", mother)
    setattr(new_patient, "RG", rg)
    setattr(new_patient, "Observações", observation)
    
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
        "Telefone Residencial": home_phone,
        "Celular": cellphone,
        "Email": email,
        "Cep Residencial": cep,
        "Endereço Residencial": address,
        "Endereço Comercial": complement,
        "Bairro Residencial": neighbourhood,
        "Cidade Residencial": city,
        "Observações": observation
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
