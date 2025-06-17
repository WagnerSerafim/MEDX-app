import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase= input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ") 

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

print("Conectando no Banco de dados...")

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine) 

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Agenda = getattr(Base.classes, "Agenda")
Contatos = getattr(Base.classes, "Contatos")

print("Sucesso! Inicializando migração de Agendamentos...")

todos_arquivos = glob.glob(f'{path_file}/dados.xlsx')

df = pd.read_excel(todos_arquivos[0], sheet_name='agendamentos_export')
df = df.replace('None', '')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for _, row in df.iterrows():

    patient = exists(session, row["CD_PACIENTE"], "Id do Cliente", Contatos)
    if not patient:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do paciente vinculado não existe no banco de dados'
        not_inserted_data.append(row_dict)
        continue
    else:
        description = f"{patient.Nome}"
        patient_id = row['CD_PACIENTE']

    exists_row = session.query(Agenda).filter(getattr(Agenda, 'Id do Agendamento') == row["CD_AGENDAMENTO"]).first()
    if exists_row:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id já existe no banco de dados'
        not_inserted_data.append(row_dict)
        continue
    else:
        id_scheduling = row["CD_AGENDAMENTO"]
    
    start_time = row["DT_HR_INICIO"]

    end_time = row["DT_HR_FIM"]

    user = 1

    new_schedulling = Agenda(
        Descrição=description,
        Início=start_time,
        Final=end_time,
        Status=1,
    )

    setattr(new_schedulling, "Id do Agendamento", id_scheduling)
    setattr(new_schedulling, "Vinculado a", patient_id)
    setattr(new_schedulling, "Id do Usuário", user)
    
    log_data.append({
        "Id do Agendamento": id_scheduling,
        "Vinculado a": patient_id,
        "Id do Usuário": 1,
        "Início": start_time,
        "Final": end_time,
        "Descrição": description,
        "Status" : 1
    })

    session.add(new_schedulling)

    inserted_cont+=1

    if inserted_cont % 100 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos agendamentos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} agendamentos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_scheduling.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_scheduling.xlsx")
