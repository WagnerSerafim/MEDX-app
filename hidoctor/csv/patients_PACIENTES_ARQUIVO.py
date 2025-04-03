import csv
import os
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
        date_obj = datetime.strptime(str(date_str), "%d-%m-%Y") 
        return 1900 <= date_obj.year <= 2100  
    except ValueError:
        return False 

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase= input("Informe o DATABASE: ")
patients_excel = input("Informe o caminho de PACIENTES_ARQUIVO.xlsx: ").strip()

print("Conectando no Banco de Dados...\n")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine)

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Contatos = Base.classes.Contatos

print("\nSucesso! Inicializando migração de pacientes HiDoctor...\n")

log_folder = os.path.dirname(patients_excel)
df = pd.read_excel(patients_excel)
df = df.fillna(value="")

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
repeated_ids_count = 0

for index, row in df.iterrows():
    if not is_valid_date(row["Nascimento"]) or row['Nascimento'][0] == '#':
        birthday = datetime.strptime("01/01/1900", "%d/%m/%Y")
    else:
        birthday = datetime.strptime(str(row["Nascimento"]), "%Y-%m-%d")

    sex = row['SexoPaciente']
    if sex != 'M' and sex != 'F':
        sex = 'M'

    address = row['LogradouroPaciente']
    
    if row['PacObservacoes'] == None or row['PacObservacoes'] == '':
        observation = rtf_to_text(row['PacObservacoes'])
    else:
        observation = ""

    novo_contato = Contatos(
        Nome=truncate_value(row["Nome_Paciente"], 50),
        Nascimento=birthday,
        Sexo=sex,
        Celular=truncate_value(row["PacTelefones"], 25),
        Email=truncate_value(row["EMail"], 100),
    )

    setattr(novo_contato, "Id do Cliente", row["ID_Pac"])
    setattr(novo_contato, "CPF/CGC", truncate_value(row["CPFPaciente"], 25))
    setattr(novo_contato, "Cep Residencial", truncate_value(row["CEPPaciente"], 10))
    setattr(novo_contato, "Endereço Residencial", truncate_value(address, 50))
    setattr(novo_contato, "Endereço Comercial", truncate_value(row["ComplementoPaciente"], 50))
    setattr(novo_contato, "Bairro Residencial", truncate_value(row["BairroPaciente"], 25))
    setattr(novo_contato, "Cidade Residencial", truncate_value(row["CidadePaciente"], 25))
    setattr(novo_contato, "Observações", observation)
    
    log_data.append({
        "Id do Cliente": row["ID_Pac"],
        "Nome": truncate_value(row["Nome_Paciente"], 50),
        "Nascimento": birthday,
        "Sexo": sex,
        "CPF/CGC": row["CPFPaciente"],
        "Celular": row["PacTelefones"],
        "Email": row["EMail"],
        "Cep Residencial": row["CEPPaciente"],
        "Endereço Residencial": address,
        "Endereço Comercial": truncate_value(row["ComplementoPaciente"], 50),
        "Bairro Residencial": truncate_value(row["BairroPaciente"], 25),
        "Cidade Residencial": row["CidadePaciente"],
        "Observações": observation,
    })

    session.add(novo_contato)

session.commit()

print(f"Novos Contatos inseridos com sucesso!")

session.close()

log_df = pd.DataFrame(log_data)
log_file_path = os.path.join(log_folder, "log_patients_pacientes.xlsx")
log_df.to_excel(log_file_path, index=False)
