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

json_file = input("Informe a pasta do arquivo JSON: ").strip()                     
log_folder = os.path.dirname(json_file)

with open(json_file, 'r', encoding='utf-8') as file:
    json_data = json.load(file)

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []

for dict in json_data:

    new_autodocs = Autodocs(
        Texto=dict["MODRECtexto"],
        Biblioteca=dict["MODRECnome"],
        Pai = -1231858396
    )

    log_data.append({
        "Texto":dict["MODRECtexto"],
        "Biblioteca":dict["MODRECnome"],
        "Pai": -1231858396
        })

    session.add(new_autodocs)

session.commit()

print("Autodocs inseridos com sucesso!")

session.close()

log_df = pd.DataFrame(log_data)
log_file_path = os.path.join(log_folder, "autodocs_modelosreceita_log.xlsx")
log_df.to_excel(log_file_path, index=False)