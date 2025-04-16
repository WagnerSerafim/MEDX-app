import glob
import json
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log

def get_record(row):
    """
    A partir da linha do dataframe, retorna o histórico formatado.
    """
    try:
        record = ''
        record += f'{row['Nome Paciente']}.pdf'
    except:
        return ''
    
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
Contatos = getattr(Base.classes, "Contatos")

print("Sucesso! Inicializando migração de Históricos...")

todos_arquivos = glob.glob(f'{path_file}/*arquivos.xlsx')

df = pd.read_excel(todos_arquivos[0])
df = df.replace('None', '')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for _, row in df.iterrows():

    patient = exists(session, row['Nome Paciente'], "Nome", Contatos)
    if not patient:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Paciente não encontrado'
        not_inserted_data.append(row_dict)
        continue
    else:
        id_patient = getattr(patient, "Id do Cliente")

    record = get_record(row)
    if record == "":
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Histórico vazio ou inválido'
        not_inserted_data.append(row_dict)
        continue

    date = '01/01/1900 00:00'
    classe = row['Caminho arquivo']
    if len(classe) > 100:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Classe muito longa'
        not_inserted_data.append(row_dict)
        continue
    
    # id_patient = row["PacienteId"]
    # if id_patient == "" or id_patient == None or id_patient == 'None':
    #     not_inserted_cont +=1
    #     row_dict = row.to_dict()
    #     row_dict['Motivo'] = 'Id do paciente vazio'
    #     not_inserted_data.append(row_dict)
    #     continue
    try:
        new_record = HistoricoClientes(
            Histórico=record,
            Data=date
        )
        # setattr(new_record, "Id do Histórico", (row['id']))
        setattr(new_record, "Id do Cliente", id_patient)
        setattr(new_record, "Id do Usuário", 0)
        setattr(new_record, "Classe", classe)
    
        log_data.append({
            # "Id do Histórico": (row['id']),
            "Id do Cliente": id_patient,
            "Data": date,
            "Histórico": record,
            "Id do Usuário": 0,
        })
        session.add(new_record)
        inserted_cont+=1

        if inserted_cont % 10000 == 0:
            session.commit()

    except Exception as e:
            not_inserted_cont +=1
            row_dict = row.to_dict()
            row_dict['Motivo'] = str(e)
            not_inserted_data.append(row_dict)
            continue
    
session.commit()

print(f"{inserted_cont} novos históricos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} históricos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_record_files.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_record_files.xlsx")
