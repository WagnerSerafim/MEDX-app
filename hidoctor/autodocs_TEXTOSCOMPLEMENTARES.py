import json
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib


sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase= input("Informe o DATABASE: ")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine) 

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Autodocs = getattr(Base.classes, "Autodocs")

text_excel = input("Informe o caminho do arquivo TEXTOSCOMPLEMENTARES.xlsx: ").strip()                     
log_folder = os.path.dirname(text_excel)

df = pd.read_excel(text_excel)

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
cont = 0
father = int(input("ID do Pai: "))

for _,row in df.iterrows():

    text = row['TextoComplementar']
    Biblioteca = row['NomeTextoComplementar']


    new_autodocs = Autodocs(
        Texto=row['TextoComplementar'],
        Biblioteca=row['NomeTextoComplementar'],
        Pai = father
    )

    log_data.append({
        "Texto":row['TextoComplementar'],
        "Biblioteca":row['NomeTextoComplementar'],
        "Pai": father
        })

    cont+=1
    session.add(new_autodocs)

session.commit()

print(f"Um total de {cont} receituários foram inseridos com sucesso!")

session.close()

log_df = pd.DataFrame(log_data)
log_file_path = os.path.join(log_folder, "autodocs_modelosreceita_log.xlsx")
log_df.to_excel(log_file_path, index=False)