import os
import random
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd


def is_valid_date(date_str):
    """ Verifica se a data é válida e diferente de '0000-00-00' """
    if pd.isna(date_str) or date_str in ["", "0000-00-00"]:
        return False
    try:
        date_obj = datetime.strptime(str(date_str), "%d-%m-%Y") 
        return 1900 <= date_obj.year <= 2100  
    except ValueError:
        return False 

def truncate_value(value, max_length):
    """Se o valor for maior que max_length, ele será truncado"""
    if pd.isna(value):
        return None
    return str(value)[:max_length] 


DATABASE_URL = "mssql+pyodbc://Medizin_32373:658$JQxn@medxserver.database.windows.net:1433/MEDX31?driver=ODBC+Driver+17+for+SQL+Server"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine) 

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Contatos = Base.classes.Contatos

excel_file = input("Caminho do arquivo de pacientes em xlsx: ")
df = pd.read_excel(excel_file, sheet_name="Historico Pacientes")

df = df.fillna(value="")

inserted = []
log_data = []

for index, row in df.iterrows():
    try:
        if row["Paciente"] not in inserted:
            inserted.append(row["Paciente"])
        else:
            continue

        if not is_valid_date(row["nascimento"]):
            birthday = datetime.strptime("01/01/1900", "%d/%m/%Y")
        else:
            birthday = datetime.strptime(str(row["nascimento"]), "%d-%m-%Y")

        if pd.isna(row["numero"]) or row["numero"] == "S.N":
            address = row["endereco"]
        else:
            number = str(row["numero"]) 
            address = f"{row['endereco']} {number}"  

        for _,item in row.items():
            if pd.isna(item) or item == "" :
                item = None

        novo_contato = Contatos(
            Nome=truncate_value(row["Paciente"], 50),
            Nascimento=birthday,
            Sexo="M",
            Celular=truncate_value(row["celular"], 25),
            Email=truncate_value(row["email"], 100),
        )

        setattr(novo_contato, "Id do Cliente", index+1)
        setattr(novo_contato, "CPF/CGC", truncate_value(row["cpf"], 25))
        setattr(novo_contato, "Cep Residencial", truncate_value(row["cep"], 10))
        setattr(novo_contato, "Endereço Residencial", truncate_value(address, 50))
        setattr(novo_contato, "Bairro Residencial", truncate_value(row["bairro"], 25))
        setattr(novo_contato, "Cidade Residencial", truncate_value(row["cidade"], 25))
        
        log_data.append({
            "Id do Cliente": index,
            "Nascimento": birthday,
            "Sexo": "M",
            "CPF/CGC": truncate_value(row["cpf"], 25),
            "Celular": truncate_value(row["celular"], 25),
            "Email": truncate_value(row["email"], 100),
            "Cep Residencial": truncate_value(row["cep"], 10),
            "Endereço Residencial": truncate_value(address, 50),
            "Bairro Residencial": truncate_value(row["bairro"], 25),
            "Cidade Residencial": truncate_value(row["cidade"], 25)
        })

        session.add(novo_contato)

    except Exception as e:
        print(f"❌ Erro ao inserir ID Cliente: {index}, Nome: {row['Paciente']}. Erro: {e}")

session.commit()

print("Novos contatos inseridos com sucesso!")

# log_df = pd.DataFrame(log_data)
# log_file_path = os.path.join(log_folder, "patients_log.xlsx")
# log_df.to_excel(log_file_path, index=False)

session.close()
