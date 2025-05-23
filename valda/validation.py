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
Contatos = getattr(Base.classes, "Contatos")

print("Sucesso! Inicializando migração de Históricos...")

todos_arquivos = glob.glob(f'{path_file}/ids_certos.xlsx')

df = pd.read_excel(todos_arquivos[0])
df = df.replace('None', '')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

found_list = []


for _, row in df.iterrows():

    existing_patient = exists(session, row['PacienteID'], "Referências", Contatos)
    if existing_patient == None:
        row_dict = row.to_dict()
        row_dict['Nome'] = ''
        row_dict['Status'] = 'Paciente não encontrado'
        found_list.append(row_dict)
        continue
    else:
        row_dict = row.to_dict()
        row_dict['Status'] = 'Paciente encontrado'
        row_dict['Nome'] = existing_patient.Nome
        found_list.append(row_dict)

session.close()

create_log(found_list, log_folder, "log_found_list.xlsx")
