import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import urllib
from utils.utils import create_log, is_valid_date, exists
import json
import ast

def parse_date_br_to_sql(date_str):
    if pd.isna(date_str) or date_str in ['', None]:
        return None
    try:
        # Tenta converter do formato brasileiro para o formato SQL
        return datetime.strptime(date_str, "%d/%m/%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        try:
            # Tenta converter só a data, se não tiver hora
            return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
        except Exception:
            return None

def get_record(record):
    import re
    if pd.isna(record) or record in ['', 'None', None]:
        return None, None
    # Remove _x000D_ e espaços extras
    record = record.replace('_x000D_', '').replace('\n', '').replace('\r', '').strip()
    # Remove vírgulas antes de fechar colchetes/chaves (opcional, para casos de trailing comma)
    record = re.sub(r',(\s*[}\]])', r'\1', record)
    try:
        json_data = json.loads(record)
    except json.JSONDecodeError:
        try:
            json_data = json.loads(json.dumps(ast.literal_eval(record)))
        except Exception as e:
            print("DEBUG: Erro ao decodificar JSON:", e)
            return None, None
    if not json_data:
        print("DEBUG: json_data vazio")
        return None, None

    data = json_data.get('data', {})
    attributes = data.get('attributes', data)
    date = attributes.get('created_at') or attributes.get('prescriptionDate')
    record_str = ''
    medicamentos = attributes.get('medicamentos', [])
    if not medicamentos:
        print("DEBUG: medicamentos vazio para registro:", record)
    for medicamento in medicamentos:
        record_str += f"Nome: {medicamento.get('nome', '')} <br>"
        record_str += f"Posologia: {medicamento.get('posologia', '')}"
    return [record_str, date]

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

todos_arquivos = glob.glob(f'{path_file}/prescricoesmemed*.xlsx')

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
    record_info = get_record(row['Prescricao'])
    record = record_info[0]
    if record in [None, '', 'None'] or pd.isna(record):
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Histórico vazio'
        not_inserted_data.append(row_dict)
        continue

    date = parse_date_br_to_sql(record_info[1])  # <-- aqui faz a conversão

    id_patient = row['IdPaciente']

    new_record = HistoricoClientes(
        Histórico = record,
        Data = date
    )

    setattr(new_record, "Id do Cliente", id_patient)
    setattr(new_record, "Id do Usuário", 0)

    log_data.append({
        "Id do Cliente": id_patient,
        "Data": date,
        "Histórico": record,
        "Id do Usuário": 0,
    })
    session.add(new_record)
    inserted_cont+=1

    if inserted_cont % 1000 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos históricos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} históricos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_record_prescricaomemed.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_record_prescricaomemed.xlsx")

from datetime import datetime

def parse_date_br_to_sql(date_str):
    if pd.isna(date_str) or date_str in ['', None]:
        return None
    try:
        # Tenta converter do formato brasileiro para o formato SQL
        return datetime.strptime(date_str, "%d/%m/%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        try:
            # Tenta converter só a data, se não tiver hora
            return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
        except Exception:
            return None