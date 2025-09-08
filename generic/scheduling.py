import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, verify_nan

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

todos_arquivos = glob.glob(f'{path_file}/dados*.xlsx')

df = pd.read_excel(todos_arquivos[0], sheet_name='consultas')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for idx, row in df.iterrows():
    
    if idx % 1000 == 0 or idx == len(df):
        print(f"Processados: {idx} | Inseridos: {inserted_cont} | Não inseridos: {not_inserted_cont} | Concluído: {round((idx / len(df)) * 100, 2)}%")

    user = verify_nan(row['USUARIOID'])
    if not user:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do usuário vazio'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    patient = exists(session, row["NOME"], "Nome", Contatos)
    if not patient:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do paciente vinculado não existe no banco de dados'
        not_inserted_data.append(row_dict)
        continue
    else:
        obs = verify_nan(row['OBSERVACAO'])
        room = verify_nan(row['SALAS'])
        procedure = verify_nan(row['PROCEDIMENTOS'])
        description = f"{patient.Nome} {procedure} {obs} {room}".strip()
        patient_id = getattr(patient, "Id do Cliente")
    
    dt_obj = verify_nan(row['DATA'])
    dt = dt_obj.strftime('%Y-%m-%d')
    hour_begin = verify_nan(row['HORA_INICIO'])
    hour_end = verify_nan(row['HORA_FIM'])

    start = f'{dt} {hour_begin}'
    end = f'{dt} {hour_end}'

    if is_valid_date(start, '%Y-%m-%d %H:%M:%S'):
        start = start
    else:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Data de início inválida'
        not_inserted_data.append(row_dict)
        continue

    if is_valid_date(end, '%Y-%m-%d %H:%M:%S'):
        end = end
    else:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Data de fim inválida'
        not_inserted_data.append(row_dict)
        continue

    new_schedulling = Agenda(
        Descrição=description,
        Início=start,
        Final=end,
        Status=1,
    )

    setattr(new_schedulling, "Vinculado a", patient_id)
    setattr(new_schedulling, "Id do Usuário", user)
    
    log_data.append({
        "Vinculado a": patient_id,
        "Id do Usuário": user,
        "Início": start,
        "Final": end,
        "Descrição": description,
        "Status" : 1
    })

    session.add(new_schedulling)

    inserted_cont+=1

    if inserted_cont % 1000 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos agendamentos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} agendamentos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_scheduling.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_scheduling.xlsx")
