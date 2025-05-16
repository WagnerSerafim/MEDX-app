import glob
import os
import re
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log

def extrair_nome(nome):
    match = re.search(r'\s\d+', nome) 
    if match:
        nome_limpo = nome[:match.start()] 
        return nome_limpo
    else:
        return nome

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

print("Sucesso! Inicializando migração de Anexos...")

log_folder = path_file
image_file = glob.glob(f'{path_file}/lista_arquivos.xlsx')

df = pd.read_excel(image_file[0])
df = df.fillna(value="")

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0
cont_id = 2000

for index, row in df.iterrows():

    name = extrair_nome(row['Nome Paciente'])

    existing_patient = exists(session, name, "Nome", Contatos)
    if existing_patient:
        id_patient = getattr(existing_patient, "Id do Cliente")
    else:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Nome de Paciente não encontrado'
        not_inserted_data.append(row_dict)
        continue

    record = row['Caminho arquivo']
    if record == "":
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Caminho do arquivo vazio ou inválido'
        not_inserted_data.append(row_dict)
        continue

    date = '01/01/1900 00:00'
    
    new_record = HistoricoClientes(
        Histórico=name,
        Data=date,
        Classe = record
    )
    setattr(new_record, "Id do Histórico", cont_id)
    setattr(new_record, "Id do Cliente", id_patient)
    setattr(new_record, "Id do Usuário", 0)
    
    
    log_data.append({
        "Id do Histórico": cont_id,
        "Id do Cliente": id_patient,
        "Data": date,
        "Classe": record,
        "Histórico": name,
        "Id do Usuário": 0,
    })

    cont_id += 1
    session.add(new_record)
    inserted_cont+=1

    if inserted_cont % 10000 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos anexos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} anexos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_images.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_images.xlsx")
