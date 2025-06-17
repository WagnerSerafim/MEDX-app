import csv
import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import create_log, exists

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase= input("Informe o DATABASE: ")
path_file = input("Informe o caminho dos arquivos: ").strip()

print("Conectando no Banco de Dados...")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine)

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Contatos = Base.classes.Contatos

print("Sucesso! Inicializando atualização de pacientes...\n")

log_folder = os.path.dirname(path_file)
csv_file = glob.glob(f'{path_file}/patients.csv')

csv.field_size_limit(10**6)
df = pd.read_csv(csv_file[0], sep=";", encoding="utf-16")
df = df.fillna(value="")

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for index, row in df.iterrows():
    
    patient_to_update = session.query(Contatos).filter(getattr(Contatos, "Id do Cliente") == row["id"]).first()
    if not patient_to_update:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Paciente não encontrado no banco de dados'
        not_inserted_data.append(row_dict)
        continue
    else:
        cellphone = row["phone"].replace("'", "")
        dict = {
            "Id do Cliente": getattr(patient_to_update, "Id do Cliente"),
            "Celular": f"Atualizado de {getattr(patient_to_update, "Celular")} para {cellphone}",
            "Motivo": "Atualizado com sucesso"
        }
        setattr(patient_to_update, "Celular", cellphone)

        if getattr(patient_to_update, "CPF/CGC") in [None, ""]:
            dict["CPF/CGC"] = f"Atualizado de {getattr(patient_to_update, 'CPF/CGC')} para {row['document']}"
            setattr(patient_to_update, "CPF/CGC", row["document"])
        
    dict["Timestamp"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    log_data.append(dict)

    inserted_cont += 1
    if inserted_cont % 100 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} contatos foram atualizados com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} contatos não foram atualizados, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_patients.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_patients.xlsx")

