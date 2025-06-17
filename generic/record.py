import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import urllib
from utils.utils import create_log, is_valid_date, exists

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine)

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

HistoricoClientes = getattr(Base.classes, "Histórico de Clientes")

print("Sucesso! Inicializando migração de Históricos...")

todos_arquivos = glob.glob(f'{path_file}/dados.xlsx')

df = pd.read_excel(todos_arquivos[0], sheet_name='evolucoes_export')
df = df.replace('None', '')

df_scheduling = pd.read_excel(todos_arquivos[0], sheet_name='agendamentos_export')
df_scheduling = df_scheduling.replace('None', '')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

scheduling_lookup = {row['CD_AGENDAMENTO']: row['CD_PACIENTE'] for _, row in df_scheduling.iterrows()}

for _, row in df.iterrows():

    record_id = row['CD_EVOLUCAO']
    if record_id is None or record_id == "":
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Histórico é vazio ou nulo'
        not_inserted_data.append(row_dict)
        continue

    existing_record = exists(session, record_id, "Id do Histórico", HistoricoClientes)
    if existing_record:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Histórico já existe no banco de dados'
        not_inserted_data.append(row_dict)
        continue

    if not (row['CD_AGENDAMENTO'] in [None, ''] or pd.isna(row['CD_AGENDAMENTO'])):
        if row['CD_AGENDAMENTO'] in scheduling_lookup:
            id_patient = scheduling_lookup[row['CD_AGENDAMENTO']]
        else:
            not_inserted_cont += 1
            row_dict = row.to_dict()
            row_dict['Motivo'] = 'Id do paciente vazio ou não encontrado na tabela agendamentos_export'
            not_inserted_data.append(row_dict)
            continue
    else:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do agendamento vazio ou nulo'
        not_inserted_data.append(row_dict)
        continue
    
    record = row['DS_EVOLUCAO']
    if record in [None, '', 'None'] or pd.isna(record):
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Histórico vazio'
        not_inserted_data.append(row_dict)
        continue

    date = row['DT_HORA_EVOLUCAO']

    new_record = HistoricoClientes(
        Histórico = record,
        Data = date
    )

    setattr(new_record, "Id do Histórico", record_id)
    setattr(new_record, "Id do Cliente", id_patient)
    setattr(new_record, "Id do Usuário", 0)

    log_data.append({
        "Id do Histórico": record_id,
        "Id do Cliente": id_patient,
        "Data": date,
        "Histórico": record,
        "Id do Usuário": 0,
    })
    session.add(new_record)
    inserted_cont+=1

    if inserted_cont % 100 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos históricos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} históricos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_record.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_record.xlsx")