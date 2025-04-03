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

print("Conectando no Banco de dados...\n")

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine) 

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Agenda = Base.classes.Agenda

print("Sucesso! Começando migração de agendamentos...\n")
                  
log_folder = os.path.dirname(excel_file)

df = pd.read_excel(excel_file)
df = df.fillna(value="")

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
cont = 0 
for index, row in df.iterrows():

    cont+=1

    id_schedule = cont
    id_pac = row['ID_Pac'][1:]
    beginning = row['DataHoraComp']
    
    if id_schedule == "" or id_schedule == None:
        continue

    if beginning == "" or beginning == None:
        continue

    if id_pac == "" or id_pac == None:
        continue

    description = f"{row['Descricao']} {row['Notas']}"
    beginning_datetime = datetime.strptime(str(beginning), '%Y-%m-%d %H:%M:%S')

    ending = beginning_datetime + timedelta(minutes=30)
    status = 1
    id_user = 1 #Não tem ID o profissional responsável pela agenda



    new_schedulling = Agenda(
        Descrição=description,
        Início=beginning,
        Final=ending,
        Status=1,
    )

    setattr(new_schedulling, "Id do Agendamento", id_schedule)
    setattr(new_schedulling, "Vinculado a", id_pac)
    setattr(new_schedulling, "Id do Usuário", id_user)
    
    log_data.append({
        "Id do Agendamento": id_schedule,
        "Vinculado a": id_pac,
        "Id do Usuário": 1,
        "Início": beginning,
        "Final": ending,
        "Descrição": description,
        "Status" : 1
    })

    session.add(new_schedulling)

session.commit()

print(f"Novos agendamentos inseridos com sucesso!")

session.close()

log_df = pd.DataFrame(log_data)
log_file_path = os.path.join(log_folder, "log_scheduling_COMPROMISSOSAGENDA.xlsx")
log_df.to_excel(log_file_path, index=False)
