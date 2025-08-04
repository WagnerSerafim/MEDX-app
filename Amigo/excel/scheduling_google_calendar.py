import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import pandas as pd
import urllib

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase= input("Informe o DATABASE: ")
excel_file = input("Informe a pasta do arquivo google_calendar.xlsx: ").strip()   

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine) 

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Agenda = Base.classes.Agenda

                  
log_folder = os.path.dirname(excel_file)

df = pd.read_excel(excel_file)
df = df.fillna(value="")

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
cont = 0

for index, row in df.iterrows():
    cont+=1
    if row["Start Date"] == "" or row['Start Date']==None:
        continue

    description = f"{row['Summary']}"

    start_time = row['Start Date']
    end_time = row['End Date']

    user = -2003140179

    new_schedulling = Agenda(
        Descrição=description,
        Início=start_time,
        Final=end_time,
        Status=1,
    )

    setattr(new_schedulling, "Id do Agendamento", cont)
    setattr(new_schedulling, "Vinculado a", 1)
    setattr(new_schedulling, "Id do Usuário", user)
    
    log_data.append({
        "Id do Agendamento": cont,
        "Vinculado a": 1,
        "Id do Usuário": user,
        "Início": start_time,
        "Final": end_time,
        "Descrição": description,
        "Status" : 1
    })

    session.add(new_schedulling)

session.commit()

print(f"Novos agendamentos inseridos com sucesso!")

session.close()

log_df = pd.DataFrame(log_data)
log_file_path = os.path.join(log_folder, "log_scheduling_google_calendar.xlsx")
log_df.to_excel(log_file_path, index=False)
