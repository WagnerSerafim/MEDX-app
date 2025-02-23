import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import urllib


def not_inserted(row,index):
    dict = {
        "Linha": index+2,
        "Id do Agendamento": row["pk"],
        "Vinculado a": row["patient_id"],
        "Id do Usuário": row["physicina_id"],
        "Início": f"{row["date"]} {row["start_time"]}",
        "Final": f"{row["date"]} {row["end_time"]}",
        "Descrição": f"{row["patient_name"]} {row["description"]}"
    }
           
    return dict

log_data = []
notInserted = []

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase= input("Informe o DATABASE: ")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine) 

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Agenda = Base.classes.Agenda

schedulling_csv = input("Arquivo CSV de agendamentos: ").strip()
df = pd.read_csv(schedulling_csv, sep=None, engine='python')
df = df.fillna(value="")

log_folder = input("Informe a pasta onde deseja salvar o arquivo de log: ").strip()

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

for index, row in df.iterrows():

    if pd.isna(row["pk"]) or row["pk"] == "":
        notInserted.append(not_inserted(row,index))
        continue

    elif pd.isna(row["date"]) or row["date"] == "":
        notInserted.append(not_inserted(row,index))
        continue

    elif pd.isna(row["start_time"]) or row["start_time"] == "":
        notInserted.append(not_inserted(row,index))
        continue

    elif pd.isna(row["physician_id"]) or row["physician_id"] == "":
        notInserted.append(not_inserted(row,index))
        continue

    description = f"{row["patient_name"]} {row["description"]}"
    if description == "":
        description = "Agendamento sem descrição no backup"
    
    start_time = f"{row["date"]} {row["start_time"]}"
    end_time = f"{row["date"]} {row["end_time"]}"

    new_schedulling = Agenda(
        Descrição=description,
        Início=start_time,
        Final=end_time,
        Status=1,
    )

    setattr(new_schedulling, "Id do Agendamento", row["pk"])
    setattr(new_schedulling, "Vinculado a", row["patient_id"])
    setattr(new_schedulling, "Id do Usuário", row["physician_id"])
    
    log_data.append({
        "Id do Agendamento": row["pk"],
        "Vinculado a": row["patient_id"],
        "Id do Usuário": row["physicina_id"],
        "Início": start_time,
        "Final": end_time,
        "Descrição": description,
        "Status" : 1
    })

    session.add(new_schedulling)

session.commit()

print("Novos contatos inseridos com sucesso!")

session.close()

log_df = pd.DataFrame(log_data)
log_file_path = os.path.join(log_folder, "schedulling_log.xlsx")
log_df.to_excel(log_file_path, index=False)

notInserted_df = pd.DataFrame(notInserted)
log_file_path = os.path.join(log_folder, "notInserted_schedulling_log.xlsx")
notInserted_df.to_excel(log_file_path, index=False)