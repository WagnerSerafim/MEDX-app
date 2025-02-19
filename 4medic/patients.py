from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd


def truncate_value(value, max_length):
    """Se o valor for maior que max_length, ele será truncado"""
    if pd.isna(value):
        return None
    return str(value)[:max_length]  # Mantém só os primeiros caracteres permitidos

sid = input("Informe o SoftwareID: ")
password = input("Informe a senha: ")
dbase= input("Informe o DATABASE: ")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(engine, reflect=True)

# Criar sessão
SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

# Acessar a classe correspondente à tabela "Contatos"
Contatos = Base.classes.Contatos

df = pd.read_excel(r"E:\Migracoes\4Medic\Schema_32292\backup_651340\backup_651340\csv\pacientes.xlsx")

for index,row in df.iterrows():

    if pd.isna(row["NASCIMENTO"]) or row["NASCIMENTO"] == "":
        birthday = datetime.strptime("01/01/1900 00:00", "%d/%m/%Y %H:%M")
    else:
        birthday = row["NASCIMENTO"]
    
    if pd.isna(row["SEXO"]) or row["SEXO"] == "":
        sex = "M"
    else:
        sex = row["SEXO"]


    new_contact = Contatos(
        Nome = truncate_value(row["NOME"], 50),
        Nascimento = birthday,
        Sexo = sex,
        RG = truncate_value(row["RG"], 25),
        Email = truncate_value(row["EMAIL"], 100)
    )
    setattr(new_contact, "Id do Cliente", row["ID_PACIENTE"])
    setattr(new_contact, "CPF/CGC", truncate_value(row["CPF"], 25))
    session.add(new_contact)

session.commit()

print("Novos contato inserido com sucesso!")

session.close()
