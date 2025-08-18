import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, verify_nan
from datetime import datetime, timedelta


sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")

print("Conectando no Banco de Dados...")
DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine)

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Agenda = getattr(Base.classes, "Agenda")

print("Sucesso! Inicializando migração de Agendamentos...")

todos_arquivos = glob.glob(f'{path_file}/dados*.xlsx')

df = pd.read_excel(todos_arquivos[0], sheet_name='COMPROMISSOSAGENDA')
df = df.replace('None', '')

df_other = pd.read_excel(todos_arquivos[0], sheet_name='COMPROMISSOSAGENDACONTATOS')
df_other = df_other.replace('None', '')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for idx, row in df.iterrows():

    if idx % 500 == 0 or idx == len(df):
        print(f"Processados: {idx} | Inseridos: {inserted_cont} | Não inseridos: {not_inserted_cont} | Concluído: {round((idx / len(df)) * 100, 2)}%")
    
    scheduling_id = verify_nan(row['Counter'])
    if not scheduling_id:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'ID Counter vazio'
        row_dict['TimeStamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        not_inserted_data.append(row_dict)
        continue

    existing_scheduling = exists(session, scheduling_id, "Id do Agendamento", Agenda)
    if existing_scheduling:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Agendamento já existe'
        row_dict['TimeStamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        not_inserted_data.append(row_dict)
        continue
    
    date_str = verify_nan(row['DataHoraComp'])
    if is_valid_date(date_str, '%Y-%m-%d %H:%M:%S'):
        start_time = date_str
        start_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        end_dt = start_dt + timedelta(minutes=30)
        end_time = end_dt.strftime('%Y-%m-%d %H:%M:%S')
    else:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Data ou Hora inválida'
        not_inserted_data.append(row_dict)
        continue

    id_patient = verify_nan(row["ID_Pac"][1:])
    if not id_patient:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do paciente vazio'
        not_inserted_data.append(row_dict)
        continue

    user = row['profissional_id']

    description = verify_nan(row['Descricao'])
    if row['Notas']:
        description += f" - {row['Notas']}"

    new_schedulling = Agenda(
        Descrição=description,
        Início=start_time,
        Final=end_time,
        Status=1,
    )

    setattr(new_schedulling, "Id do Agendamento", scheduling_id)
    setattr(new_schedulling, "Vinculado a", id_patient)
    setattr(new_schedulling, "Id do Usuário", user)
    
    log_data.append({
        "Id do Agendamento": scheduling_id,
        "Vinculado a": id_patient,
        "Id do Usuário": user,
        "Início": start_time,
        "Final": end_time,
        "Descrição": description,
        "Status" : 1
    })

    session.add(new_schedulling)

    inserted_cont+=1

    if inserted_cont % 500 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos agendamentos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} agendamentos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_scheduling_COMPROMISSOSAGENDA.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_scheduling_COMPROMISSOSAGENDA.xlsx")
