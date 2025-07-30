from datetime import datetime
import glob
import json
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log

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

HistoricoClientes = getattr(Base.classes, "Histórico de Clientes")

print("Sucesso! Inicializando migração de Históricos...")

todos_arquivos = glob.glob(f'{path_file}/dados.xlsx')

df = pd.read_excel(todos_arquivos[0], sheet_name='consultas')
df = df.replace('None', '')

df_schedule = pd.read_excel(todos_arquivos[0], sheet_name='atendimentos')
df_schedule = df_schedule.replace('None', '')

schedules = {}
for _, row in df_schedule.iterrows():
    schedules[row['CODIGOATENDIMENTO']] = [row['CODIGOPACIENTE'], row['DATAINICIAL']]

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for idx, row in df.iterrows():

    schedule = schedules[row['CODIGOATENDIMENTO']]

    id_record = row['CODIGO']
    if id_record in ['', None]:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Histórico vazio ou inválido'
        not_inserted_data.append(row_dict)
        continue
    else:
        existing_record = exists(session, id_record, 'Id do Histórico', HistoricoClientes)
        if existing_record:
            not_inserted_cont +=1
            row_dict = row.to_dict()
            row_dict['Motivo'] = 'Histórico já existe no banco de dados'
            not_inserted_data.append(row_dict)
            continue

    record = f'{row['ANAMNESE']}'
    if row['AVALIACAO'] not in ['', None]:
        record += f'<br> {row['AVALIACAO']}'

    if record == "":
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Histórico vazio ou inválido'
        not_inserted_data.append(row_dict)
        continue

    date_str = schedule[1]
    if date_str in ['', None]:
        date = '1900-01-01'
    else:
        date = date_str.strftime('%Y-%m-%d')

    id_patient = schedule[0]
    if id_patient in ['', None]:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do paciente vazio'
        not_inserted_data.append(row_dict)
        continue
    
    new_record = HistoricoClientes(
        Histórico=record,
        Data=date
    )
    setattr(new_record, "Id do Histórico", id_record)
    setattr(new_record, "Id do Cliente", id_patient)
    setattr(new_record, "Id do Usuário", 0)
    
    log_data.append({
        "Id do Histórico": id_record,
        "Id do Cliente": id_patient,
        "Data": date,
        "Histórico": record,
        "Id do Usuário": 0,
    })
    session.add(new_record)
    inserted_cont+=1

    if inserted_cont % 1000 == 0:
        session.commit()

    if (idx + 1) % 1000 == 0 or (idx + 1) == len(df):
        print(f"Processados {idx + 1} de {len(df)} registros ({(idx + 1) / len(df) * 100:.2f}%)")


session.commit()

print("Migração concluída! Gerando logs...")
print(f"{inserted_cont} novos históricos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} históricos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_record_consultas.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_record_consultas.xlsx")
