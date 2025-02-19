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

DATABASE_URL = "mssql+pyodbc://Medizin_32373:658$JQxn@medxserver.database.windows.net:1433/MEDX31?driver=ODBC+Driver+17+for+SQL+Server"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(engine, reflect=True)

# Criar sessão
SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

# Acessar a classe correspondente à tabela "Contatos"
Contatos = Base.classes.Contatos

df = pd.read_csv(r"C:\Users\Wagner Serafim\Documents\06-12-2024-patient.csv", sep=None, engine='python')

for index,row in df.iterrows():

    if pd.isna(row["birthdate"]) or row["birthdate"] == "":
        birthday = datetime.strptime("01/01/1900", "%d/%m/%Y")
    else:
        birthday = row["birthdate"]
    
    if pd.isna(row["gender"]) or row["gender"] == "":
        sex = "M"
    else:
        sex = row["gender"]


    novo_contato = Contatos(
        Nome = truncate_value(row["name"], 50),
        Nascimento = birthday,
        Sexo = sex
    )
    setattr(novo_contato, "Id do Cliente", row["patient_id"])
    print(f"Nome: {row['name']} ({len(str(row['name']))} caracteres)")
    session.add(novo_contato)

session.commit()

print("Novos contato inserido com sucesso!")

# Fechar sessão
session.close()
