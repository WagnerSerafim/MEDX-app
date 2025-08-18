import xml.etree.ElementTree as ET
import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
import csv
from utils.utils import exists, create_log
import base64
from striprtf.striprtf import rtf_to_text

def get_record(row):
    record = ''
    if not (row['PRESS'] in [None, ''] or pd.isna(row['PRESS'])):
        press = row['PRESS']
        padding = len(press) % 4
        if padding != 0:
            press += '=' * (4 - padding)
        decoded_content = base64.b64decode(press).decode('latin1')
        clean_text = rtf_to_text(decoded_content)
        record += f"Pressão: {clean_text}<br>"

    if not (row['PESO'] in [None, ''] or pd.isna(row['PESO'])):
        height = row['PESO']
        padding = len(height) % 4
        if padding != 0:
            height += '=' * (4 - padding)
        decoded_content = base64.b64decode(height).decode('latin1')
        clean_text = rtf_to_text(decoded_content)
        record += f"Peso: {clean_text}<br>"

    if not (row['DIABETICA'] in [None, ''] or pd.isna(row['DIABETICA'])):
        diabetic = row['DIABETICA']
        padding = len(diabetic) % 4
        if padding != 0:
            diabetic += '=' * (4 - padding)
        decoded_content = base64.b64decode(diabetic).decode('latin1')
        clean_text = rtf_to_text(decoded_content)
        record += f"Diabética: {clean_text}<br>"

    if not (row['HIPERT'] in [None, ''] or pd.isna(row['HIPERT'])):
        hipert = row['HIPERT']
        padding = len(hipert) % 4
        if padding != 0:
            hipert += '=' * (4 - padding)
        decoded_content = base64.b64decode(hipert).decode('latin1')
        clean_text = rtf_to_text(decoded_content)
        record += f"Hipertensiva: {clean_text}<br>"

    if not (row['PRI_OBS'] in [None, ''] or pd.isna(row['PRI_OBS'])):
        pri_obs = row['PRI_OBS']
        padding = len(pri_obs) % 4
        if padding != 0:
            pri_obs += '=' * (4 - padding)
        decoded_content = base64.b64decode(pri_obs).decode('latin1')
        clean_text = rtf_to_text(decoded_content)
        record += f"Primeira Observação: {clean_text}<br>"

    if not (row['QUEIXA'] in [None, ''] or pd.isna(row['QUEIXA'])):
        queixa = row['QUEIXA']
        padding = len(queixa) % 4
        if padding != 0:
            queixa += '=' * (4 - padding)
        decoded_content = base64.b64decode(queixa).decode('latin1')
        clean_text = rtf_to_text(decoded_content)
        record += f"Queixa: {clean_text}<br>"

    if not (row['ANTECED'] in [None, ''] or pd.isna(row['ANTECED'])):
        antecedentes = row['ANTECED']
        padding = len(antecedentes) % 4
        if padding != 0:
            antecedentes += '=' * (4 - padding)
        decoded_content = base64.b64decode(antecedentes).decode('latin1')
        clean_text = rtf_to_text(decoded_content)
        record += f"Antecedentes: {clean_text}<br>"
    
    if not (row['EXAMES'] in [None, ''] or pd.isna(row['EXAMES'])):
        exames = row['EXAMES']
        padding = len(exames) % 4
        if padding != 0:
            exames += '=' * (4 - padding)
        decoded_content = base64.b64decode(exames).decode('latin1')
        clean_text = rtf_to_text(decoded_content)
        record += f"Exames: {clean_text}<br>"

    if not (row['TRATAM'] in [None, ''] or pd.isna(row['TRATAM'])):
        tratamentos = row['TRATAM']
        padding = len(tratamentos) % 4
        if padding != 0:
            tratamentos += '=' * (4 - padding)
        decoded_content = base64.b64decode(tratamentos).decode('latin1')
        clean_text = rtf_to_text(decoded_content)
        record += f"Tratamentos: {clean_text}<br>"

    if not (row['OBS'] in [None, ''] or pd.isna(row['OBS'])):
        obs = row['OBS']
        padding = len(obs) % 4
        if padding != 0:
            obs += '=' * (4 - padding)
        decoded_content = base64.b64decode(obs).decode('latin1')
        clean_text = rtf_to_text(decoded_content)
        record += f"Observação: {clean_text}<br>"
    
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
Agenda = getattr(Base.classes, "Agenda")

print("Sucesso! Inicializando migração de Históricos...")

record_file = glob.glob(f'{path_file}/Atendimento.xml')

tree = ET.parse(record_file[0])
root = tree.getroot()

recorddata = root.find("recorddata")

registros = []

for row in recorddata.findall("row"):
    registro = {}
    for field in row.findall("field"):
        nome = field.attrib["name"]
        if "value" in field.attrib:
            valor = field.attrib["value"]
        elif field.text:
            valor = field.text.strip()
        else:
            valor = ""
        registro[nome] = valor
    registros.append(registro)

df = pd.DataFrame(registros)

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
updated_cont=0
not_updated_data = []
not_updated_cont = 0

for idx, row in df.iterrows():

    if idx % 1000 == 0 or idx == len(df):
        print(f"Processados: {idx} | Atualizados: {updated_cont} | Não Atualizados: {idx} | Concluído: {round((idx / len(df)) * 100, 2)}%")
    
    scheduling = exists(session, row['CD_AGEND'], "Id do Agendamento", Agenda)
    if not scheduling:
        not_updated_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Agendamento não existe'
        not_updated_data.append(row_dict)
        continue
    else:
        date_obj = getattr(scheduling, "Início")
    
    record = exists(session, row['CODIGO'], "Id do Histórico", HistoricoClientes)
    if not record:
        not_updated_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Histórico não existe'
        not_updated_data.append(row_dict)
        continue
    else:
        setattr(record, "Data", date_obj)
    
    log_data.append({
        "Id do Histórico": getattr(record, "Id do Histórico", None),
        "Id do Cliente": getattr(record, "Id do Cliente", None),
        "Data Antiga": getattr(record, "Data", None),
        "Data Nova": date_obj,
        "Id do Usuário": 0,
    })
    updated_cont+=1

    if updated_cont % 1000 == 0:
        session.commit()

session.commit()

print(f"{updated_cont} novos históricos foram inseridos com sucesso!")
if not_updated_cont > 0:
    print(f"{not_updated_cont} históricos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_updated_record.xlsx")
create_log(not_updated_data, log_folder, "log_not_updated_record.xlsx")