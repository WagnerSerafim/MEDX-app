import glob
import json
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log
from striprtf.striprtf import rtf_to_text
from datetime import datetime

def get_record(row):
    """
    A partir da linha do dataframe, retorna o histórico formatado.
    """
    try:
        record = ''
        record += f"Solicitação de Exame: {row['Descr']}"
    except:
        return ''
    
    return record


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

df = pd.read_excel(todos_arquivos[0], sheet_name='SolicExa')
df = df.replace('None', '')

# df_consult = pd.read_excel(todos_arquivos[0], sheet_name='Consulta')
# df_consult = df_consult.replace('None', '')

# df_consult = df_consult.sort_values(by='Seq', ascending=True)

# last_id = df_consult['Seq'].iloc[-1]

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont = 0
not_inserted_data = []
not_inserted_cont = 0
id_record = 0

for _, row in df.iterrows():
    id_record -= 1

    existing_record = exists(session, id_record, "Id do Histórico", HistoricoClientes)
    if existing_record:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Histórico já existe'
        not_inserted_data.append(row_dict)
        continue

    record = get_record(row)
    if record == "":
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Histórico vazio ou inválido'
        not_inserted_data.append(row_dict)
        continue

    record = record.replace('_x000D_', ' ')

    date_str = row['DataSist'].strftime('%Y-%m-%d %H:%M')
    if is_valid_date(date_str, '%Y-%m-%d %H:%M'):
        date = date_str
    else:
        date = '01/01/1900 00:00'

    id_patient = str(row["CodPaciente"])
    id_patient = id_patient[:-2]
    if id_patient == "" or id_patient == None or id_patient == 'None':
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
    inserted_cont += 1

    if inserted_cont % 1000 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos históricos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} históricos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_record_solicExa.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_record_solicExa.xlsx")
