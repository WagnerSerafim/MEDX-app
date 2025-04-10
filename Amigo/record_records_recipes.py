import json
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import urllib

def is_valid_date(date_str, date_format):
    if date_str in ["", None]:
        return False
    try:
        if "/" in date_str:
            date_str = date_str.replace("/", "-")
        
        date_obj = datetime.strptime(str(date_str), date_format)
        
        if date_format in ["%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"]:
            if (1900 <= date_obj.year <= 2100) and (1 <= date_obj.month <= 12) and (1 <= date_obj.day <= 31) and \
               (0 <= date_obj.hour <= 23) and (0 <= date_obj.minute <= 59) and (0 <= date_obj.second <= 59):
                return True
        elif date_format in ["%d-%m-%Y %H:%M", "%Y-%m-%d %H:%M"]:
            if (1900 <= date_obj.year <= 2100) and (1 <= date_obj.month <= 12) and (1 <= date_obj.day <= 31) and \
               (0 <= date_obj.hour <= 23) and (0 <= date_obj.minute <= 59):
                return True
        else:
            if (1900 <= date_obj.year <= 2100) and (1 <= date_obj.month <= 12) and (1 <= date_obj.day <= 31):
                return True
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

    if not ((row["name"] == "" or row["name"] is None) and 
            (row["dose"] == "" or row["dose"] is None) and 
            (row["quantidade"] == "" or row["quantidade"] is None) and
            (row["posologia"] == "" or row["posologia"] is None) and
            (row["observation"] == "" or row["observation"] is None)):
         
        if not (row["type_extra"] == "" or row["type_extra"] is None):
            record += f"Tipo do histórico: {row['type_extra']}<br>"

        if not (row["name"] == "" or row["name"] is None):
            record += f"Nome: {row['name']}<br><br>"
        
        if not (row["dose"] == "" or row["dose"] is None):
            record += f"Dose: {row['dose']}<br><br>"

        if not (row["quantidade"] == "" or row["quantidade"] is None):
            record += f"Quantidade: {row['quantidade']}<br><br>"
        
        if not (row["posologia"] == "" or row["posologia"] is None):
            record += f"posologia: {row['posologia']}<br><br>"
        
        if not (row["observation"] == "" or row["observation"] is None):
            record += f"Observações: {row['observation']}<br><br>"
    
    return record


sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
excel_file = input("Informe o caminho do arquivo records_recipes.xlsx: ")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine)

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

HistoricoClientes = getattr(Base.classes, "Histórico de Clientes")


log_folder = os.path.dirname(excel_file)
record_file = os.path.dirname(excel_file)
record_file = os.path.join(record_file, "records.xlsx")

df = pd.read_excel(excel_file)
df_record = pd.read_excel(record_file)
df = df.fillna(value="")

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
repeated_ids_count = 0
total_patients = len(df)

merged_df = pd.merge(df, df_record, left_on='record_id', right_on='id', how='left', suffixes=('_extra', '_record'))
print(merged_df.columns)

for index, row in merged_df.iterrows():
    id_paciente = row['id_paciente']

    existing_register = session.query(HistoricoClientes).filter(getattr(HistoricoClientes, "Id do Histórico") == row["id_extra"]).first()

    if existing_register:
        repeated_ids_count += 1
        continue
    
    record = get_record(row)
    if record == "":
        continue

    date = ""
    try:
        date_str = row['created_at_extra'].strftime("%Y-%m-%d %H:%M")
        if is_valid_date(date_str[:16], "%Y-%m-%d %H:%M"):
            date = f"{date_str[:16]}"
    except Exception as e:
        print(f"Erro ao processar a data {row['created_at_extra']}: {e}")
        date = datetime.strptime("01/01/1900 00:00", "%d/%m/%Y %H:%M")

    new_record = HistoricoClientes(
        Histórico=record,
        Data=date,
    )

    setattr(new_record, "Id do Histórico", row['id_extra'])
    setattr(new_record, "Id do Cliente", id_paciente)
    setattr(new_record, "Id do Usuário", 0)

    log_data.append({
        "Id do Histórico": row['id_extra'],
        "Id do Cliente": id_paciente,
        "Data": date,
        "Histórico": record,
        "Id do Usuário": 0,
    })
    
    session.add(new_record)

session.commit()

print(f"Novos Históricos inseridos com sucesso! {repeated_ids_count} registros já existentes!")
session.close()

log_df = pd.DataFrame(log_data)
log_file_path = os.path.join(log_folder, "log_record_recipes.xlsx")
log_df.to_excel(log_file_path, index=False)
