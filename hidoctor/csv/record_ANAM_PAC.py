import json
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import urllib
from striprtf.striprtf import rtf_to_text

def is_valid_date(date_str):
    """ Verifica se a data é válida e diferente de '00-00-0000' """
    if pd.isna(date_str) or date_str in ["", "00-00-0000 00:00"]:
        return False
    try:
        date_obj = datetime.strptime(str(date_str), "%d-%m-%Y %H:%M") 
        return 1900 <= date_obj.year <= 2100  
    except ValueError:
        return False

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
excel_file = input("Informe o caminho do arquivo ANAM_PAC.xlsx: ")

print("Conectando no Banco de Dados...")
DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine)

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

HistoricoClientes = getattr(Base.classes, "Histórico de Clientes")

print("Sucesso! Inicializando migração de anamneses HiDoctor...")

log_folder = os.path.dirname(excel_file)

df = pd.read_excel(excel_file)
df = df.fillna(value="")

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
cont=0
cont_commit = 0
for index, row in df.iterrows():
    
    record = row['Texto_Anamnese']
    if record == "":
        continue
    
    date = ""

    try:
        if is_valid_date(row['Anam_Date']):
            date = f"{row["Anam_Date"]}"
    except Exception as e:
        print(f"Erro ao processar a data {row['Anam_Date']}: {e}")
        date = datetime.strptime("01/01/1900 00:00", "%d/%m/%Y %H:%M") 
    
    new_record = HistoricoClientes(
        Histórico=record,
        Data=row["Anam_Date"]
    )
    setattr(new_record, "Id do Cliente", row["ID_Pac"])
    setattr(new_record, "Id do Usuário", 0)
    setattr(new_record, "Id do Histórico", (0-row["ID_Anam"]))
    
    log_data.append({
        "Id do Histórico": (0-row["ID_Anam"]),
        "Id do Cliente": row["ID_Pac"],
        "Data": date,
        "Histórico": record,
        "Id do Usuário": 0,
    })
    cont+=1
    session.add(new_record)

session.commit()

print(f"{cont} novos Históricos foram inseridos com sucesso!")

session.close()

log_df = pd.DataFrame(log_data)
log_file_path = os.path.join(log_folder, "log_record_ANAM_PAC.xlsx")
log_df.to_excel(log_file_path, index=False)
