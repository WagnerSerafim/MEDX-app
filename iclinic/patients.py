from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd


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

df = pd.read_csv(r"C:\Users\Wagner Serafim\Documents\06-12-2024-patient.csv", sep=None, engine='python')

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
        RG=row["rg"][:25],
        Celular=row["mobile_phone"],
        Email=row["email"][:100],
        Profissão=row["occupation"],
        Observações=row["observation"]
    )

    setattr(novo_contato, "Id do Cliente", row["patient_id"])
    setattr(novo_contato, "CPF/CGC", row["cpf"])
    setattr(novo_contato, "Telefone Residencial", row["home_phone"])
    setattr(novo_contato, "Cep Residencial", row["zip_code"])
    setattr(novo_contato, "Endereço Residencial", address)
    setattr(novo_contato, "Endereço Comercial", row["complement"])
    setattr(novo_contato, "Bairro Residencial", row["neighborhood"][:25])
    setattr(novo_contato, "Cidade Residencial", row["city"])
    setattr(novo_contato, "Estado Residencial", row["state"][:2])
    setattr(novo_contato, "País Residencial", row["country"][:50])
    
    session.add(novo_contato)

session.commit()

print("Novos contatos inseridos com sucesso!")

session.close()
