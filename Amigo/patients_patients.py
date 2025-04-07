import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import urllib
from utils import *

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
patients_excel = input("Informe o caminho de patients.xlsx: ").strip()

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

log_folder = os.path.dirname(patients_excel)

df = pd.read_excel(patients_excel)
df = df.fillna(value="")

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
not_inserted_data = []  
repeated_ids_count = 0
total_patients = len(df)

for index, row in df.iterrows():

    if row["id"] == "" or row["id"] == None:
        print(f"ID do paciente não encontrado na linha {index + 2}. Pulando...")
        continue

    if row["name"] == "" or row["name"] == None:
        print(f"Nome do paciente não encontrado na linha {index + 2}. Pulando...")
        continue

    existing_patient = session.query(Contatos).filter(getattr(Contatos, "Id do Cliente")==row["id"]).first()

    if existing_patient:
        repeated_ids_count += 1
        not_inserted_data.append({
            "Id do Cliente": row["id"],
            "Nome": row["name"],
            "Motivo": "ID já existe"
        })
        continue     

    if not is_valid_date(row["born"]):
        birthday = "01/01/1900"
    else:
        birthday = datetime.strptime(str(row["born"]), "%Y-%m-%d")

    if row['gender'] == "Feminino":
        sex = 'F'
    elif row['gender'] == "Masculino":
        sex = 'M'
    else:
        sex = "M"

    if row["address_number"]== None or row["address_number"] == "":
        address = row["address_address"]
    else:
        number = str(row["address_number"]) 
        address = f"{row['address_address']} {number}"  

    id_patient = row["id"]
    name = truncate_value(row["name"], 50)
    rg = truncate_value(row["rg"], 25)
    cpf = truncate_value(row["cpf"], 25)
    cellphone = cellphone
    email = truncate_value(row["email"], 100)
    profession = truncate_value(row["jobrole"], 25)
    cep = truncate_value(row["address_cep"], 10)
    complement = truncate_value(row["address_complement"], 50)
    neighbourhood = truncate_value(row["address_district"], 25)
    city = truncate_value(row["address_city"], 25)
    father_name = truncate_value(row["father_name"], 50)
    mother_name = truncate_value(row["mother_name"], 50)
    telephone = truncate_value(row["contact_phone_home"], 25)
    observation = row["observation"]

    log = {
        "Id do Cliente": id_patient,
        "Nome": name,
        "Nascimento": birthday,
        "Sexo": sex,
        "RG": rg,
        "CPF/CGC": cpf,
        "Celular": cellphone,
        "Email": email,
        "Profissão": profession,
        "Cep Residencial": cep,
        "Endereço Residencial": address,
        "Endereço Comercial": complement,
        "Bairro Residencial": neighbourhood,
        "Cidade Residencial": city,
        "Mãe": mother_name,
        "Pai": father_name,
        "Observações": observation,
        "Telefone Residencial": telephone
    }
    try:
        novo_contato = Contatos(
            Nome=name,
            Nascimento=birthday,
            Sexo=sex,
            RG=rg,
            Celular=cellphone,
            Email=email,
            Profissão=profession
        )

        setattr(novo_contato, "Id do Cliente", id_patient)
        setattr(novo_contato, "CPF/CGC", cpf)
        setattr(novo_contato, "Cep Residencial", cep)
        setattr(novo_contato, "Endereço Residencial", address)
        setattr(novo_contato, "Endereço Comercial", complement)
        setattr(novo_contato, "Bairro Residencial", neighbourhood)
        setattr(novo_contato, "Cidade Residencial", city)
        setattr(novo_contato, "Pai", father_name)
        setattr(novo_contato, "Mãe", mother_name)
        setattr(novo_contato, "Observações", observation)
        setattr(novo_contato, "Telefone Residencial", telephone)
        
        log_data.append(log)

        session.add(novo_contato)
    
    except Exception as e:
        print(f"Erro ao inserir o paciente na linha {index + 2}: {e}\n\nLog paciente: {log}")
        not_inserted_data.append({
            "Id do Cliente": id_patient,
            "Nome": name,
            "Motivo": str(e)
        })
        continue

if repeated_ids_count > 0:
    print(f"Dentre {total_patients} pacientes, achamos {repeated_ids_count} ID's repetidos que não vão ser inseridos.")
    confirm_insert = input("Quer continuar com a migração mesmo assim? (Y/N): ").strip().upper()
    
    if confirm_insert == "Y":
        session.commit()
        print(f"Novos Contatos inseridos com sucesso! {total_patients - repeated_ids_count} registros inseridos.")
    else:
        print(f"Migração abortada.")
else:
    session.commit()
    print(f"Novos Contatos inseridos com sucesso! {total_patients} registros inseridos.")

session.close()

create_log(not_inserted_data, log_folder, "log_patients_notInserted.xlsx")
create_log(log_data, log_folder, "log_patients_patients.xlsx")

