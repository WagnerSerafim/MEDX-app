import glob
import json
import os
from sqlalchemy import MetaData, Table, create_engine, bindparam, UnicodeText
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, verify_nan
from striprtf.striprtf import rtf_to_text

def get_record(row):
    record = ''

    conmorpai = verify_nan(row['CONMORPAI'])
    if conmorpai:
        record += f"Histórico de doenças PAI: {conmorpai}<br><br>"

    conpregra = verify_nan(row['CONPREGRA'])
    if conpregra:
        record += f"CONPREGRA: {conpregra}<br><br>"
    
    conpredif = verify_nan(row['CONPREDIF'])
    if conpredif:
        record += f"CONPREDIF: {conpredif}<br><br>"
    
    conprepro = verify_nan(row['CONPREPRO'])
    if conprepro:
        record += f"CONPREPRO: {conprepro}<br><br>"
    
    conpreida = verify_nan(row['CONPREIDA'])
    if conpreida:
        record += f"Idade da gestante: {conpreida}<br><br>"
    
    conneocna = verify_nan(row['CONNEOCNA'])
    if conneocna:
        record += f"CONNEOCNA: {conneocna}<br><br>"
    
    conneopes = verify_nan(row['CONNEOPES'])
    if conneopes:
        record += f"Peso: {conneopes}kg<br><br>"
    
    conneoest = verify_nan(row['CONNEOEST'])
    if conneoest:
        record += f"Estatura: {conneoest}cm<br><br>"
    
    conneulin = verify_nan(row['CONNEULIN'])
    if conneulin:
        record += f"CONNEULIN: {conneulin}<br><br>"
    
    conneuati = verify_nan(row['CONNEUATI'])
    if conneuati:
        record += f"CONNEUATI: {conneuati}<br><br>"
    
    conneudis = verify_nan(row['CONNEUDIS'])
    if conneudis:
        record += f"CONNEUDIS: {conneudis}<br><br>"
    
    conneusex = verify_nan(row['CONNEUSEX'])
    if conneusex:
        record += f"CONNEUSEX: {conneusex}<br><br>"
    
    conneuesc = verify_nan(row['CONNEUESC'])
    if conneuesc:
        record += f"CONNEUESC: {conneuesc}<br><br>"
    
    conexafis = verify_nan(row['CONEXAFIS'])
    if conexafis:
        conexafis = rtf_to_text(conexafis)
        record += f"Exame Físico: {conexafis}<br><br>"
    
    conexacon = verify_nan(row['CONEXACON'])
    if conexacon:
        conexacon = rtf_to_text(conexacon)
        record += f"CONEXACON: {conexacon}<br><br>"
    
    conneopro = verify_nan(row['CONNEOPRO'])
    if conneopro:
        record += f"CONNEOPRO: {conneopro}<br><br>"
    
    conneoqtd = verify_nan(row['CONNEOQTD'])
    if conneoqtd:
        record += f"CONNEOQTD: {conneoqtd}<br><br>"
    
    conneodia = verify_nan(row['CONNEODIA'])
    if conneodia:
        record += f"CONNEODIA: {conneodia}<br><br>"
    
    conhabdes = verify_nan(row['CONHABDES'])
    if conhabdes:
        record += f"CONHABDES: {conhabdes}<br><br>"
    
    conexapes = verify_nan(row['CONEXAPES'])
    if conexapes:
        record += f"CONEXAPES: {conexapes}kg<br><br>"
    
    conexagap = verify_nan(row['CONEXAGAP'])
    if conexagap:
        record += f"CONEXAGAP: {conexagap}<br><br>"
    
    conexaest = verify_nan(row['CONEXAEST'])
    if conexaest:
        record += f"CONEXAEST: {conexaest}cm<br><br>"
    
    conexapec = verify_nan(row['CONEXAPEC'])
    if conexapec:
        record += f"CONEXAPEC: {conexapec}<br><br>"

    return record

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

metadata = MetaData()
historico_tbl = Table("Histórico de Clientes", metadata, schema=f"schema_{sid}", autoload_with=engine)
contatos_tbl = Table("Contatos", metadata, schema=f"schema_{sid}", autoload_with=engine)
Base = declarative_base()

class Historico(Base):
    __table__ = historico_tbl

class Contatos(Base):
    __table__ = contatos_tbl

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

print("Sucesso! Inicializando migração de Históricos...")

todos_arquivos = glob.glob(f'{path_file}/priconsult.json')

with open(todos_arquivos[0], 'r', encoding='utf-8') as f:
    df = pd.read_json(todos_arquivos[0], encoding='utf-8')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for idx, row in df.iterrows():

    if idx % 1000 == 0 or idx == len(df):
        print(f"Processados: {idx} | Inseridos: {inserted_cont} | Não inseridos: {not_inserted_cont} | Concluído: {round((idx / len(df)) * 100, 2)}%")

    id_resp = verify_nan(row["CONRES"])
    if id_resp == None:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do responsável vazio'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    id_resp = str(id_resp).lstrip('0')  # Remove os zeros à esquerda

    id_child = verify_nan(row["CONSEQ"])
    if id_child == None:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do filho vazio'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    id_child = str(id_child).lstrip('0')  # Remove os zeros à esquerda

    id_patient_str = f"{id_resp}-{id_child}"
    print(id_patient_str)


    patient = exists(session, id_patient_str, "Referências", Contatos)
    if not patient:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Cliente não encontrado no banco de dados'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    else:
        id_patient = getattr(patient, "Id do Cliente")
    
    record = get_record(row)
    if record == None:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Histórico vazio ou inválido'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    
    date_obj = verify_nan(row['CONDAT'])
    if date_obj == None:
        date = '1900-01-01'
    else:
        if is_valid_date(date_obj, '%Y-%m-%d'):
            date = date_obj
        
    new_record = Historico(
        Data=date,
    )
    setattr(new_record, "Histórico", bindparam(None, value=record, type_=UnicodeText()))
    # setattr(new_record, "Id do Histórico", record_id)
    setattr(new_record, "Id do Cliente", id_patient)
    setattr(new_record, "Id do Usuário", 0)

    log_data.append({
        # "Id do Histórico": record_id,
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

create_log(log_data, log_folder, "log_inserted_priconsult.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_priconsult.xlsx")
