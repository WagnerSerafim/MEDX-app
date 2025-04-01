import json
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import urllib

def is_valid_date(date_str):
    """ Verifica se a data é válida e diferente de '00-00-0000' """
    if pd.isna(date_str) or date_str in ["", "00-00-0000"]:
        return False
    try:
        date_obj = datetime.strptime(str(date_str), "%d-%m-%Y") 
        return 1900 <= date_obj.year <= 2100  
    except ValueError:
        return False 

def get_info(json_str, record):
    # Verifica se json_str é uma string e tenta carregá-la como um JSON
    if isinstance(json_str, str):
        try:
            json_str = json.loads(json_str)  # Convertendo a string JSON para um dicionário
        except json.JSONDecodeError:
            # Se não for um JSON válido, podemos retornar um erro ou simplesmente não adicionar nada
            record += "Erro ao processar JSON.<br>"
            return record

    # Agora, json_str é um dicionário, e você pode acessá-lo normalmente
    if json_str.get("surgery_request_observation"):
        record += f"Informações Extras:<br>Motivo Cirurgia: {json_str['surgery_request_observation']}<br>"
        record += f"CID: {json_str.get('cid1', 'Não disponível')}<br><br>"
        for exam in json_str.get('exams', []):
            record += f"Nome da Cirurgia solicitada: {exam.get('name', 'Nome não informado')}"
            record += f"Quantidade solicitada: {exam.get('amount', 'Quantidade não informada')}"
    else:
        if json_str.get("exams"):
            record += f"Informações Extras:<br>"
            if json_str.get('clinical_indication'):  # Usando .get() para evitar o erro KeyError
                record += f"Indicação Clínica: {json_str['clinical_indication']}<br><br>"
                for exam in json_str['exams']:
                    record += f"Nome do Exame solicitado: {exam.get('name', 'Nome não informado')}"
                    record += f"Quantidade solicitada: {exam.get('amount', 'Quantidade não informada')}"
        
        if json_str.get("telemedicine_consent_term"):
            record += f"Informações Extras:<br>Termo de Consentimento Telemedicina: {json_str['telemedicine_consent_term']}"

    return record



def get_record(row):
    record = ""

    if not ((row["title"]=="" or row["title"]==None) and (row["text"]=="" or row["text"]==None) and (row["extra"]=="" or row["extra"]==None)):
        record += f"Tipo do histórico: {row["type"]}<br>"

        if not (row["title"]=="" or row["title"]==None):
            record += f"Título: {row['title']}<br><br>"
        
        if not (row["text"]=="" or row["text"]==None):
            record += f"Texto: {row["text"]}<br><br>"

        if not (row["extra"]=="" or row["extra"]==None):
            if not "[" == row["extra"][0]:
                record += get_info(row['extra'], record)
    
    return record

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
excel_file = input("Informe o caminho do arquivo records.xlsx: ")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine)

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

HistoricoClientes = getattr(Base.classes, "Histórico de Clientes")


log_folder = os.path.dirname(excel_file)

df = pd.read_excel(excel_file)
df = df.fillna(value="")

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []

for index, row in df.iterrows():
    
    record = get_record(row)
    if record == "":
        continue
    
    date = ""

    try:
        if is_valid_date(row['created_at']):
            date = f"{row["created_at"]} 00:00"
    except Exception as e:
        print(f"Erro ao processar a data {row['created_at']}: {e}")
        date = datetime.strptime("01/01/1900 00:00", "%d/%m/%Y %H:%M") 

    new_record = HistoricoClientes(
        Histórico=record,
        Data=date,
    )

    setattr(new_record, "Id do Histórico", row['id'])
    setattr(new_record, "Id do Cliente", row["patient_id"])
    setattr(new_record, "Id do Usuário", 0)

    log_data.append({
        "Id do Histórico": row['id'],
        "Id do Cliente": row["patient_id"],
        "Data": date,
        "Histórico": record,
        "Id do Usuário": 0,
    })
    session.add(new_record)

session.commit()

print("Novos Históricos inseridos com sucesso!")

session.close()

log_df = pd.DataFrame(log_data)
log_file_path = os.path.join(log_folder, "log_record_records.xlsx")
log_df.to_excel(log_file_path, index=False)
