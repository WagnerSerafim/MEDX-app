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

def is_valid_date(date_str):
    """ Verifica se a data é válida e diferente de '0000-00-00' """
    if pd.isna(date_str) or date_str in ["", "0000-00-00"]:
        return False
    try:
        date_obj = datetime.strptime(str(date_str), "%d-%m-%Y") 
        return 1900 <= date_obj.year <= 2100  
    except ValueError:
        return False 

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
patients_excel = input("Informe o caminho de patients.xlsx: ").strip()

print("Conectando no Banco de dados...\n")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine)

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Contatos = Base.classes.Contatos

print("Sucesso! Começando migração de pacientes...\n")

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
    existing_patient = session.query(Contatos).filter(getattr(Contatos, "Id do Cliente")==row["id"]).first()

    if existing_patient:
        repeated_ids_count += 1
        not_inserted_data.append({
            "Id do Cliente": row["id"],
            "Nome": row["name"]
        })
        continue 

    if not is_valid_date(row["born"]):
        birthday = datetime.strptime("01/01/1900", "%d/%m/%Y")
    else:
        birthday = datetime.strptime(str(row["born"]), "%Y-%m-%d")

    if row['gender'] == "Feminino":
        sex = 'F'
    elif row['gender'] == "Masculino":
        sex = 'M'
    else:
        sex = "M"

    if pd.isna(row["address_number"]) or row["address_number"] == "":
        address = row["address_address"]
    else:
        number = str(row["address_number"]) 
        address = f"{row['address_address']} {number}"  

    novo_contato = Contatos(
        Nome=truncate_value(row["name"], 50),
        Nascimento=birthday,
        Sexo=sex,
        RG=truncate_value(row["rg"], 25),
        Celular=truncate_value(row["contact_cellphone"], 25),
        Email=truncate_value(row["email"], 100),
        Profissão=truncate_value(row["jobrole"], 25)
    )

    setattr(novo_contato, "Id do Cliente", row["id"])
    setattr(novo_contato, "CPF/CGC", truncate_value(row["cpf"], 25))
    setattr(novo_contato, "Cep Residencial", truncate_value(row["address_cep"], 10))
    setattr(novo_contato, "Endereço Residencial", truncate_value(address, 50))
    setattr(novo_contato, "Endereço Comercial", truncate_value(row["address_complement"], 50))
    setattr(novo_contato, "Bairro Residencial", truncate_value(row["address_district"], 25))
    setattr(novo_contato, "Cidade Residencial", truncate_value(row["address_city"], 25))
    setattr(novo_contato, "Pai", truncate_value(row["mother_name"], 50))
    setattr(novo_contato, "Mãe", truncate_value(row["father_name"], 50))
    setattr(novo_contato, "Observações", row["observation"])
    setattr(novo_contato, "Telefone Residencial", truncate_value(row["contact_phone_home"], 25))
    
    log_data.append({
        "Id do Cliente": row["id"],
        "Nome": truncate_value(row["name"], 50),
        "Nascimento": birthday,
        "Sexo": sex,
        "RG": truncate_value(row["rg"], 25),
        "CPF/CGC": row["cpf"],
        "Celular": row["contact_cellphone"],
        "Email": row["email"],
        "Profissão": row["jobrole"],
        "Cep Residencial": row["address_cep"],
        "Endereço Residencial": address,
        "Endereço Comercial": truncate_value(row["address_complement"], 50),
        "Bairro Residencial": truncate_value(row["address_district"], 25),
        "Cidade Residencial": row["address_city"],
        "Mãe": truncate_value(row["mother_name"], 50),
        "Pai": truncate_value(row["father_name"], 50),
        "Observações": row['observation'],
        "Telefone Residencial": truncate_value(row["contact_phone_home"], 25)
    })

    session.add(novo_contato)

if repeated_ids_count > 0:
    print(f"Dentre {total_patients} pacientes, achamos {repeated_ids_count} ID's repetidos que não vão ser inseridos.")
    confirm_insert = input("Quer continuar com a migração mesmo assim? (Y/N): ").strip().upper()
    
    if confirm_insert == "Y":
        session.commit()
        print(f"Novos Contatos inseridos com sucesso! {total_patients - repeated_ids_count} registros inseridos.")
    else:
        print(f"Migração abortada.\n")
else:
    session.commit()
    print(f"Novos Contatos inseridos com sucesso! {total_patients} registros inseridos.")

session.close()

log_not_inserted_df = pd.DataFrame(not_inserted_data)
log_not_inserted_path = os.path.join(log_folder, "log_notInserted.xlsx")
log_not_inserted_df.to_excel(log_not_inserted_path, index=False)
