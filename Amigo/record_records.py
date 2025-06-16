import glob
import json
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log

def get_info(json_str, record):
    message = ""  
    if isinstance(json_str, str):
        try:
            json_str = json.loads(json_str)
        except json.JSONDecodeError:
            message = "Erro ao processar JSON.<br>"
            return record, message

    if json_str.get("surgery_request_observation"):
        record += f"Informações Extras:<br>Motivo Cirurgia: {json_str['surgery_request_observation']}<br>"
        record += f"CID: {json_str.get('cid1', 'Não disponível')}<br><br>"
        for exam in json_str.get('exams', []):
            record += f"Nome da Cirurgia solicitada: {exam.get('name', 'Nome não informado')}"
            record += f"Quantidade solicitada: {exam.get('amount', 'Quantidade não informada')}"
    else:
        if json_str.get("exams"):
            record += f"Informações Extras:<br>"
            if json_str.get('clinical_indication'):
                record += f"Indicação Clínica: {json_str['clinical_indication']}<br><br>"
                for exam in json_str['exams']:
                    record += f"Nome do Exame solicitado: {exam.get('name', 'Nome não informado')}"
                    record += f"Quantidade solicitada: {exam.get('amount', 'Quantidade não informada')}"
        
        if json_str.get("telemedicine_consent_term"):
            record += f"Informações Extras:<br>Termo de Consentimento Telemedicina: {json_str['telemedicine_consent_term']}"
        
        if json_str.get("133899"):
            record += f"Informações Extras:{json_str['133899']}<br>"
            if json_str.get("133900"):
                record += f"<br>{json_str['133900']}<br>"
        
        if json_str.get("133835"):
            record += f"Informações Extras:{json_str['133835']}<br>"

        if json_str.get("131866"):
            record += f"Informações Extras:{json_str['131866']}<br>"
        
        if json_str.get("133950"):
            record += f"Informações Extras:{json_str['133950']}<br>"
            if json_str.get("133951"):
                record += f"<br>{json_str['133951']}<br>"
            
        if json_str.get("134871"):
            record += f"Informações Extras:{json_str['134871']}<br>"

    return record, message

def get_record(row):
    record = ""
    message = ""

    if not ((row["title"] in ['', None] or pd.isna(row['title'])) and (row["text"] in ['', None] or pd.isna(row['text'])) and (row["extra"] in ['', None] or pd.isna(row['extra']))):
        record += f"Tipo do histórico: {row['type']}<br>"

        if not (row["title"] in ['', None] or pd.isna(row['title'])):
            record += f"Título: {row['title']}<br><br>"
        
        if not (row["text"] in ['', None] or pd.isna(row['text'])):
            record += f"Texto: {row['text']}<br><br>"

        if not (row["extra"] in ['', None] or pd.isna(row['extra'])):
            extra = str(row["extra"])
            if not "[" == extra[0]:
                record_function, message = get_info(row['extra'], record)
                record += record_function
    
    return record, message

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

todos_arquivos = glob.glob(f'{path_file}/records.xlsx')

df = pd.read_excel(todos_arquivos[0])
df = df.replace('None', '')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for _, row in df.iterrows():

    record_id = row['id']
    if record_id is None or record_id == "":
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Histórico é vazio ou nulo'
        not_inserted_data.append(row_dict)
        continue

    existing_record = exists(session, record_id, "Id do Histórico", HistoricoClientes)
    if existing_record:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Histórico já existe no banco de dados'
        not_inserted_data.append(row_dict)
        continue

    id_patient = row["patient_id"]
    if id_patient == "" or id_patient == None or id_patient == 'None':
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do paciente vazio'
        not_inserted_data.append(row_dict)
        continue
    
    record, message = get_record(row)
    if record in [None, '', 'None', 'Tipo do histórico: MEDICINE<br>'] or message == "Erro ao processar JSON.": 
        not_inserted_cont += 1
        row_dict = row.to_dict()
        if message:
            row_dict['Motivo'] = message
        else:
            row_dict['Motivo'] = 'Histórico vazio ou apenas com tipo MEDICINE'
        not_inserted_data.append(row_dict)
        continue
    
    if isinstance(row['created_at'], datetime):
        date_str = row['created_at'].strftime('%Y-%m-%d %H:%M:%S')
        if is_valid_date(date_str, '%Y-%m-%d %H:%M:%S'):
            date = date_str
        else:
            date = '01/01/1900 00:00'
    else:
        if is_valid_date(row['created_at'], '%d-%m-%Y %H:%M:%S'):
            date = row['created_at']
        else:
            date = '01/01/1900 00:00'

    new_record = HistoricoClientes(
        Histórico=record,
        Data=date,
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

create_log(log_data, log_folder, "log_inserted_records.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_records.xlsx")
