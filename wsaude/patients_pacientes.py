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
dbase= input("Informe o DATABASE: ")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine) 

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Contatos = Base.classes.Contatos

patients_excel = input("Caminho de pacientes.xlsx: ").strip()
log_folder = os.path.dirname(patients_excel)

df = pd.read_excel(patients_excel)
df = df.fillna(value="")

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
repeated_ids_count = 0

for index, row in df.iterrows():

    if not is_valid_date(row["Nascimento"]):
        birthday = datetime.strptime("01/01/1900", "%d/%m/%Y")
    else:
        birthday = datetime.strptime(str(row["Nascimento"]), "%d-%m-%Y")

    sex = "M" if pd.isna(row["Sexo"]) or row["Sexo"] == "" else row["Sexo"].upper()

    if pd.isna(row["Nº da Residência"]) or row["Nº da Residência"] == "":
        address = row["Logradouro"]
    else:
        number = str(row["Nº da Residência"]) 
        address = f"{row['Logradouro']} {number}"  

    existing_contact = session.query(Contatos).filter(getattr(Contatos, "Id do Cliente") == row["Nº Interno"]).first()

    if existing_contact:
        repeated_ids_count += 1
        continue

    novo_contato = Contatos(
        Nome=truncate_value(row["Nome"], 50),
        Nascimento=birthday,
        Sexo=sex,
        RG=truncate_value(row["RG"], 25),
        Celular=truncate_value(row["Celular"], 25),
        Email=truncate_value(row["E-mail"], 100),
        Profissão=truncate_value(row["Profissão"], 25)
    )

    setattr(novo_contato, "Id do Cliente", row["Nº Interno"])
    setattr(novo_contato, "CPF/CGC", truncate_value(row["CPF"], 25))
    setattr(novo_contato, "Cep Residencial", truncate_value(row["CEP"], 10))
    setattr(novo_contato, "Endereço Residencial", truncate_value(address, 50))
    setattr(novo_contato, "Endereço Comercial", truncate_value(row["Complemento do Logradouro"], 50))
    setattr(novo_contato, "Bairro Residencial", truncate_value(row["Bairro"], 25))
    setattr(novo_contato, "Cidade Residencial", truncate_value(row["Cidade"], 25))
    
    log_data.append({
        "Id do Cliente": row["Nº Interno"],
        "Nome": truncate_value(row["Nome"], 50),
        "Nascimento": birthday,
        "Sexo": sex,
        "RG": truncate_value(row["RG"], 25),
        "CPF/CGC": row["CPF"],
        "Celular": row["Celular"],
        "Email": row["E-mail"],
        "Profissão": row["Profissão"],
        "Cep Residencial": row["CEP"],
        "Endereço Residencial": address,
        "Endereço Comercial": truncate_value(row["Complemento do Logradouro"], 50),
        "Bairro Residencial": truncate_value(row["Bairro"], 25),
        "Cidade Residencial": row["Cidade"]
    })

    session.add(novo_contato)

session.commit()

print(f"Novos Contatos inseridos com sucesso! {len(df) - repeated_ids_count} registros inseridos.")
print(f"{repeated_ids_count} registros com ID repetido foram ignorados.")

session.close()

log_df = pd.DataFrame(log_data)
log_file_path = os.path.join(log_folder, "log_patients_pacientes.xlsx")
log_df.to_excel(log_file_path, index=False)
