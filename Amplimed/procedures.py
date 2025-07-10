from datetime import datetime
import json
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import create_log

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase= input("Informe o DATABASE: ")
path_folder = input("Informe o caminho do arquivo: ")

print("Iniciando a conexão com o banco de dados...")
DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine) 

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Procedimentos = getattr(Base.classes, "Procedimentos")

print("Carregando dados de consulta...")

json_file = os.path.join(path_folder, "procedimentos.json")
with open(json_file, 'r', encoding='utf-8') as file:
    json_data = json.load(file)
                     
log_folder = path_folder

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

print("Iniciando a inserção dos dados...")
for dict in json_data:

    id_procedure = dict.get("codproc", None)
    if id_procedure is None or id_procedure in ['', '<br>', None]:
        not_inserted_cont += 1
        dict['Motivo'] = 'Id do procedimento não encontrado'
        dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(dict)
        continue
    if id_procedure == 1:
        not_inserted_cont += 1
        dict['Motivo'] = 'Id de Consulta'
        dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(dict)
        continue

    name = dict.get("nome", None)
    if name is None or name in ['', '<br>', None]:
        not_inserted_cont += 1
        dict['Motivo'] = 'Nome do procedimento não encontrado'
        dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(dict)
        continue

    new_procedure = Procedimentos()

    setattr(new_procedure, "Id do Procedimento", id_procedure)
    setattr(new_procedure, 'Procedimento', name)
    setattr(new_procedure, 'Custo', 0)
    setattr(new_procedure, 'Preço Base', 0)
    setattr(new_procedure, 'Produto', 0)
    setattr(new_procedure, 'Sessões', 0)
    setattr(new_procedure, 'Comissao', 0)

    log_data.append({
        'Id do Procedimento': id_procedure,
        'Procedimento': name,
        'Custo': 0,
        'Preço Base': 0,
        'Produto': 0,
        'Sessões': 0,
        'Comissao': 0
    })

    session.add(new_procedure)

    inserted_cont += 1
    if inserted_cont % 1000 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos procedimentos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} procedimentos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_procedimentos.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_procedimentos.xlsx")
