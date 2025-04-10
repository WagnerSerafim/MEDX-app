import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import urllib


def truncate_value(value, max_length):
    """Se o valor for maior que max_length, ele será truncado"""
    if pd.isna(value):
        return None
    return str(value)[:max_length] 

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

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
excel_file = input("Informe o caminho do arquivo records_file.xlsx: ")

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
    
    record = row['name']
    if record == "":
        continue

    date = ""

    try:
        date_str = row['created_at'].strftime("%Y-%m-%d %H:%M")
        if is_valid_date(date_str[:16], "%Y-%m-%d %H:%M"):
            date = f"{date_str[:16]}"
    except Exception as e:
        print(f"Erro ao processar a data {row['created_at']}: {e}")
        date = datetime.strptime("01/01/1900 00:00", "%d/%m/%Y %H:%M") 

    new_record = HistoricoClientes(
        Histórico=record,
        Data=date,
        Classe = truncate_value(row['url'], 100)
    )

    setattr(new_record, "Id do Histórico", row['id'])
    setattr(new_record, "Id do Cliente", row["patient_id"])
    setattr(new_record, "Id do Usuário", 0)

    log_data.append({
        "Id do Histórico": row['id'],
        "Id do Cliente": row["patient_id"],
        "Data": date,
        "Histórico": record,
        "Classe": truncate_value(row['url'], 100),
        "Id do Usuário": 0,
    })
    session.add(new_record)

session.commit()

print("Novos anexos inseridos com sucesso!")

session.close()

log_df = pd.DataFrame(log_data)
log_file_path = os.path.join(log_folder, "log_record_records_file.xlsx")
log_df.to_excel(log_file_path, index=False)
