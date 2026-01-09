import csv
import glob
import json
import os
from sqlalchemy import MetaData, Table, create_engine, bindparam, UnicodeText
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime, timedelta
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, truncate_value

def get_record(json):
    """Extraindo o histórico do JSON"""
    record = ""

    try:
        if json.get("block"):
            if record != "":
                record += "<br><br>"

            record = json["block"][0]["tab"] + "<br><br>"
            for dic in json["block"]:
                record +=  f"{dic['name']}: "
                if isinstance(dic["value"], list):
                    if isinstance(dic["value"][0], dict):
                        for item in dic["value"]:
                            record += f"Altura: {item.get('height', 'N/A')} <br>"
                            record += f"Peso: {item.get('weight', 'N/A')} <br>"
                            record += f"IMC: {item.get('imc', item.get('bmi_value', 'N/A'))} <br>"

                    else:
                        if len(dic["value"]) >1:
                            values = ", ".join(dic["value"])
                            record += f"{values} <br><br>"
                        elif len(dic["value"]) == 1:
                            record += f"{dic["value"][0]} <br><br>"
                        else:
                            continue
                else:
                    unity = dic.get("unity", "")
                    record += f"{dic["value"]} {unity} <br>"
        
        if json.get("aditional"):
            if record != "":
                record += "<br><br>"
            
            record += "Texto(s) adicional(ais): <br>"
            for dic in json["aditional"]:
                record += f"- {dic["aditional_text"]}"
        
        if json.get("attest"):
            if record != "":
                record += "<br><br>"
            
            record += "Atestado(s): <br><br>"
            for dic in json["attest"]:
                record += f"{dic["name"]} <br><br> {dic["value"]}"
        
        if json.get("prescription_v3"):
            if record != "":
                record += "<br><br>"

            record += "Prescrição(ões) V3: <br>"
            for prescription in json["prescription_v3"]:
                if prescription["items"].get("exams"):
                    for exam in prescription["items"]["exams"]:
                        if exam.get("clinicalIndication", "") != "":
                            record += f"Indicação Clínica: {exam['clinicalIndication']}"
                            for item in exam.get("examsItems", []):
                                record += f"<br><br>{item['term']} <br>Quantidade: {item['quantity']}"
                if prescription["items"].get("drugs"):
                    for drug in prescription["items"]["drugs"]:
                        if drug.get("name") and drug["name"] != "":
                            record += f"<br>Remédio: {drug['name']}"
                        if drug.get("composition") and drug["composition"] != "":
                            record += f"<br>Composição: {drug['composition']}"
                        if drug.get("posology") and drug["posology"] != "":
                            record += f"<br>Posologia: {drug['posology']}"
                        record += "<br><br>"
        
        if json.get("exam_request"):
            if record != "":
                record += "<br><br>"
            
            record += f"Pedido(s) de Exame(s):<br>"
            for request in json["exam_request"]:
                if request["clinical_indication"] == "":
                    record += f"Indicação Clínica: Não especificada<br>"
                else:
                    record += f"Indicação Clínica: {request["clinical_indication"]}<br>"
                if len(request["items"]) == 0:
                    continue
                else:
                    for item in request["items"]:
                        record += f"Exame: {item["text"]}<br>Quantidade: {item["quantity"]}<br><br>"
        
        if json.get("recipe"):
            if record != "":
                record += "<br><br>"

            for recipe in json['recipe']:
                if recipe['value'] != '' and recipe['value'] is not None:
                    record += f"Receita: {recipe['value']}<br>"
        
        if json.get('prescription_v1'):
            if record != "":
                record += "<br><br>"

            record += "Prescrição(ões) V1: <br>"
            for prescription in json["prescription_v1"]:
                if prescription.get('items'):
                    for item in prescription['items']:
                        if item['text'] != '' and item['text'] is not None:
                            record += f"Item : {item['text']}<br>"

                        if item['quantity'] != '' and item['quantity'] is not None:
                            record+= f"Quantidade: {item['quantity']}<br>"

                        if item['posology'] != '' and item['posology'] is not None:
                            record+= f"Posologia: {item['posology']}<br>"

    except Exception as e:
        return None, f"Erro ao processar o JSON {json}: {e}"
    
    return record, None

def find_record_csv(path_folder):
    """Procura o arquivo que contenha 'record.csv' no seu nome"""
    csv_files = glob.glob(os.path.join(path_folder, "*record.csv"))

    if not csv_files:
        print("Nenhum arquivo que contenha 'record.csv' foi encontrado.")
        return None

    csv_file = csv_files[0]
    print(f"✅ Arquivo encontrado: {csv_file}")

    return csv_file

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

metadata = MetaData()
historico_tbl = Table("Histórico de Clientes", metadata, schema=f"schema_{sid}", autoload_with=engine)
Base = declarative_base()

class Historico(Base):
    __table__ = historico_tbl

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

print("Sucesso! Inicializando migração de Históricos...")

csv_files = find_record_csv(path_file)

csv.field_size_limit(1000000)

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

df = pd.read_csv(csv_files, sep=",", engine='python', quotechar='"')

df["eventblock_pack"] = df["eventblock_pack"].astype(str).str.replace(r'^json::', '', regex=True)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for index, row in df.iterrows():

    exists_row = exists(session, row["pk"], "Id do Histórico", Historico)
    if exists_row:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id já existe no banco de dados'
        not_inserted_data.append(row_dict)
        continue
    else:
        id_record = row["pk"]

    date_str = f'{row["date"]} {row["start_time"]}'
    if is_valid_date(date_str, '%Y-%m-%d %H:%M:%S'):
        date = date_str
    else:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Data ou Hora inválida'
        not_inserted_data.append(row_dict)
        continue

    if not pd.isna(row["eventblock_pack"]) and isinstance(row["eventblock_pack"], str):
        try:
            json_data = json.loads(row["eventblock_pack"])
            record, error_message = get_record(json_data)

            if record is None or record == "":
                not_inserted_cont += 1
                row_dict = row.to_dict()
                row_dict['Motivo'] = error_message
                not_inserted_data.append(row_dict)
                continue
        
        except json.JSONDecodeError:
            print(f"Erro ao decodificar JSON na linha {index + 2}. Pulando...")
            not_inserted_cont += 1
            row_dict = row.to_dict()
            row_dict['Motivo'] = 'Erro ao decodificar JSON'
            not_inserted_data.append(row_dict)
            continue
    else:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Campo eventblock_pack vazio ou inválido'
        continue
    
    if row['patient_id'] == "" or row['patient_id'] == None or row['patient_id'] == 'None':
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do paciente vazio'
        not_inserted_data.append(row_dict)
        continue
    else:
        id_patient = row["patient_id"]

    new_record = Historico(
        Data=date,
    )
    setattr(new_record, "Histórico", bindparam(None, value=record, type_=UnicodeText()))
    # setattr(new_record, "Id do Histórico", record_id)
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

session.commit()

print(f"{inserted_cont} novos históricos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} históricos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_record_records.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_record_records.xlsx")
