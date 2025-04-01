import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import pandas as pd
import urllib

def get_valid_date(date_str):
    
    if pd.isna(date_str) or date_str in ["", "0000-00-00"]:
        return "01/01/1900"
    
    try:
        date_obj = datetime.strptime(str(date_str), "%d-%m-%Y")
        if 1900 <= date_obj.year <= 2100:
            return date_obj.strftime("%d/%m/%Y")
        else:
            return "01/01/1900"
    except ValueError:
        return "01/01/1900"


sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase= input("Informe o DATABASE: ")
excel_file = input("Informe a pasta do arquivo attendances.xlsx: ").strip()   

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

for index, row in df.iterrows():

    if row["id"] == "" or row['id']==None:
        continue

    if row["start_date"] == "" or row['start_date']==None:
        continue

    if row['patient_id'] == "" or row['patient_id']==None:
        continue

    description = f"{row['type']} {row["observation"]}"

    start_time = row['start_date']
    end_time = row['end_date']

    user = 0
    if row['user_id'] == 59902:
        user = -2074285693
    elif row['user_id'] == 113208:
        user = -624345335
    elif row['user_id'] == 140779:
        user = -704157762
    else:
        user = 1

    new_schedulling = Agenda(
        Descrição=description,
        Início=start_time,
        Final=end_time,
        Status=1,
    )

    setattr(new_schedulling, "Id do Agendamento", row["id"])
    setattr(new_schedulling, "Vinculado a", row["patient_id"])
    setattr(new_schedulling, "Id do Usuário", user)
    
    log_data.append({
        "Id do Agendamento": row["id"],
        "Vinculado a": row["patient_id"],
        "Id do Usuário": 1,
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
log_file_path = os.path.join(log_folder, "log_scheduling_attendances.xlsx")
log_df.to_excel(log_file_path, index=False)
