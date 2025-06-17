import glob
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
path_file = input("Informe o caminho da pasta: ")                    

print("Conectando no Banco de Dados...")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine) 

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

HistoricoClientes = getattr(Base.classes, "Histórico de Clientes")

print("Sucesso! Inicializando migração...")

todos_arquivos = glob.glob(f'{path_file}/relatorio_imagens.xlsx')

df = pd.read_excel(todos_arquivos[0])
df = df.replace('None', '')
                     
log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont = 0
not_inserted_data = []
not_inserted_cont = 0
count = 0

for _,row in df.iterrows():

    count += 1

    record_id = -9987 - count
    existing_record = exists(session,record_id, "Id do Histórico", HistoricoClientes)
    if existing_record:
        not_inserted_cont += 1
        dict['Motivo'] = 'Id do Histórico já existe'
        not_inserted_data.append(dict)
        continue

    record = row['Nome do arquivo']

    patient_id = row['Id do cliente']

    date = "1900-01-01 00:00:00"

    classe = row['Caminho do arquivo']

    new_record = HistoricoClientes(
        Histórico = record,
        Data = date,
        Classe = classe,
    )

    setattr(new_record, "Id do Cliente", patient_id)
    setattr(new_record, "Id do Histórico", record_id)
    setattr(new_record, "Id do Usuário", 0)

    log_data.append({
        "Id do Histórico": record_id,
        "Id do Cliente": patient_id,
        "Data": date,
        "Histórico": record,
        "Id do Usuário": 0,
        })

    session.add(new_record)

    inserted_cont+=1
    if inserted_cont % 100 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos historicos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} historicos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_image_imagensdocpacientes.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_image_imagensdocpacientes.xlsx")