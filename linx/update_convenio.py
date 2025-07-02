import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import exists, create_log, truncate_value
from datetime import datetime

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

Contatos = getattr(Base.classes, "Contatos")

print("Sucesso! Inicializando migração de Contatos...")

extension_file = glob.glob(f'{path_file}/PacientesClinicas.xlsx')

df = pd.read_excel(extension_file[0])

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for _, row in df.iterrows():

    patient = exists(session, row['PacienteID'], 'Id do Cliente', Contatos)
    if patient:
        if getattr(patient, 'Id do Convênio') not in [None, 'None', ''] and not pd.isna(getattr(patient, 'Id do Convênio')):
            not_inserted_cont += 1
            row_dict = row.to_dict()
            row_dict['Motivo'] = f'Paciente já possui um Convênio associado'
            not_inserted_data.append(row_dict)
            continue
        setattr(patient, 'Id do Convênio', row['PlanoID'])
    else:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = f'Paciente não encontrado no Banco de Dados'
        not_inserted_data.append(row_dict)
        continue
    
    log_data.append({
        'Id do Cliente': getattr(patient, 'Id do Cliente'),
        'Nome ': getattr(patient, 'Nome'),
        'Convênio atualizado para': getattr(patient, 'Id do Convênio'),
    })

    inserted_cont += 1
    if inserted_cont % 1000 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} contatos foram atualizados com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} contatos não foram atualizados, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_update_convenios.xlsx")
create_log(not_inserted_data, log_folder, "log_not_update_convenios.xlsx")
