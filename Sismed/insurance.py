import glob
import json
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import exists, create_log


sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase= input("Informe o DATABASE: ")
path_file = input("Informe a pasta do arquivo JSON: ")                    

print("Conectando no Banco de Dados...")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine) 

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Convenio = getattr(Base.classes, "Convênios")

print("Sucesso! Inicializando migração...")

extension_file = glob.glob(f'{path_file}/convenios.json')

log_folder = path_file

with open(extension_file[0], 'r', encoding='utf-8') as file:
    json_data = json.load(file)

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont = 0
not_inserted_data = []
not_inserted_cont = 0

for dict in json_data:

    existing_insurance = exists(session, dict["CONVENcodigo"], "Id do Convênio", Convenio)
    if existing_insurance:
        not_inserted_cont += 1
        dict['Motivo'] = 'Id do Convênio já existe'
        not_inserted_data.append(dict)
        continue
    else:
        insurance_id = dict["CONVENcodigo"]
        
    if dict['CONVENnome'] in ["", None, "None"] or pd.isna(dict['CONVENnome']):
        not_inserted_cont += 1
        dict['Motivo'] = 'Nome do Convênio vazio'
        not_inserted_data.append(dict)
        continue
    else:
        insurance_name = dict['CONVENnome']

    new_insurance = Convenio(
        Convênio = insurance_name,
        Observações = '',
        Código = ''
    )
    setattr(new_insurance, "Id do Convênio", insurance_id)
    setattr(new_insurance, "Código da Tabela TISS", '')

    log_data.append({
        "Id do Convênio": insurance_id,
        "Convênio": insurance_name,
        "Observações": '',
        "Código da Tabela TISS": '',
        "Código": ''
    })

    session.add(new_insurance)

    inserted_cont+=1
    if inserted_cont % 100 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos contatos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} contatos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_insurance.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_insurance.xlsx")