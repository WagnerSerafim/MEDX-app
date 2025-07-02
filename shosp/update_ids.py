import glob
import json
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import exists, create_log
from datetime import datetime, timedelta


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
Convenios = getattr(Base.classes, "Convênios")

print("Sucesso! Inicializando atualização de Ids dos Contatos...")

todos_arquivos = glob.glob(f'{path_file}/dados.xlsx')

df = pd.read_excel(todos_arquivos[0], sheet_name='shosp_cadastro_paciente_michell')
df = df.replace('None', '')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0
count_insurace = 1
insurances = []

for _, row in df.iterrows():

    if row['Plano'] in ['None', None, ''] or pd.isna(row['Plano']):
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Convênio não foi verificado pois está vazio ou é inválido'
        not_inserted_data.append(row_dict)
    else:
        insurance = exists(session, row['Plano'], 'Convênio', Convenios)
        if not insurance:
            count_insurace += 1
            new_insurance = Convenios(
                Convênio = row['Plano']
            )
            setattr(new_insurance, 'Id do Convênio', count_insurace)
            session.add(new_insurance)
            session.commit()
            insurance = exists(session, row['Plano'], 'Convênio', Convenios)
            
    patient = exists(session, row['Nome'], 'Nome', Contatos)
    if not patient:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Esse nome não existe nos Contatos do banco'
        not_inserted_data.append(row_dict)
        continue
    else:
        setattr(patient, 'Id do Cliente', row['Pront.'])
        if insurance:
            setattr(patient, 'Id do Convênio', getattr(insurance, 'Id do Convênio'))
        session.add(patient)
        
        inserted_cont+=1

    if inserted_cont % 500 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos históricos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} históricos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_update_ids_patients.xlsx")
create_log(not_inserted_data, log_folder, "log_not_update_ids_patients.xlsx")
    

    



    
