from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd

# Configuração do banco de dados
DATABASE_URL = "mssql+pyodbc://Medizin_32373:658$JQxn@medxserver.database.windows.net:1433/MEDX31?driver=ODBC+Driver+17+for+SQL+Server"

# Criar engine
engine = create_engine(DATABASE_URL)

# Habilitar mapeamento automático (reflection)
Base = automap_base()
Base.prepare(engine, reflect=True)  # Descobrir tabelas automaticamente

# Criar sessão
SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

# Acessar a classe correspondente à tabela "Contatos"
Contatos = Base.classes.Contatos  # Nome da tabela no banco

df = pd.read_csv("C:\Users\WJSur\Documents\iclinic_files\06-12-2024-patient.csv")

for index,item in df.items():

    if item["birthdate"][index] == "":
        birthday = datetime.strptime("01/01/1900", "%d/%m/%Y")
    else:
        birthday = item["birthdate"][index]
    
    if item["gender"][index] == "":
        sex = "M"
    else:
        sex = item["gender"][index]


    novo_contato = Contatos(
        patient_id = item["patient_id"][index],
        name = item["name"][index],
        birthdate = birthday,
        gender = sex
    )

    session.add(novo_contato)

session.commit()

print("Novos contato inserido com sucesso!")

# Fechar sessão
session.close()
