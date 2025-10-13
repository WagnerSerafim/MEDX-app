from datetime import datetime
import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, verify_nan


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

print("Sucesso! Inicializando migração de Históricos...")

todos_arquivos = glob.glob(f'{path_file}/records.xlsx')

df = pd.read_excel(todos_arquivos[0])
df = df.replace('None', '')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0
count_id = 1
for idx, row in df.iterrows():

    if idx % 500 == 0 or idx == len(df):
        print(f"Processados: {idx} | Inseridos: {inserted_cont} | Não inseridos: {not_inserted_cont} | Concluído: {round((idx / len(df)) * 100, 2)}%")

    if 'ID' in df.columns:
        record_id = row['ID']
    else:
        record_id = count_id
        count_id += 1

    existing_record = exists(session, record_id, "Id do Histórico", HistoricoClientes)
    if existing_record:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Histórico já existe'
        not_inserted_data.append(row_dict)
        continue
    else:
        id_record = record_id

    record = verify_nan(row['HISTORICO'])
    record = record.replace('_x000D_', '<br>')
    if record == "":
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Histórico vazio ou inválido'
        not_inserted_data.append(row_dict)
        continue
        
    
    # if 'DATA' in df.columns:
    #     if isinstance(row['DATA'], datetime):
    #         date_obj = row['DATA'].strftime('%Y-%m-%d %H:%M:%S')
    #         date_str = f'{date_obj[:10]} {row['HORA']}'
    #         if is_valid_date(date_str, '%Y-%m-%d %H:%M'):
    #             date = date_str
    #         else:
    #             date = '1900-01-01'
    #     else:
    #         date_str = row['DATA'][:10]  
    #         if is_valid_date(date_str, '%Y-%m-%d'):
    #             date = f'{date_str} {row['HORA']}'
    #         else:
    #             not_inserted_cont +=1
    #             row_dict = row.to_dict()
    #             row_dict['Motivo'] = 'Data inválida'
    #             not_inserted_data.append(row_dict)
    #             continue
    # else:
    #     date = '1900-01-01 00:00'
    date = row['DATA']

    id_patient = row["PACIENTEID"]
    if id_patient == "" or id_patient == None or id_patient == 'None':
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do paciente vazio'
        not_inserted_data.append(row_dict)
        continue
    
    new_record = HistoricoClientes(
        Histórico=record,
        Data=date
    )
    setattr(new_record, "Id do Histórico", id_record)
    setattr(new_record, "Id do Cliente", id_patient)
    setattr(new_record, "Id do Usuário", 0)
    
    log_data.append({
        "Id do Histórico": id_record,
        "Id do Cliente": id_patient,
        "Data": date,
        "Histórico": record,
        "Id do Usuário": 0,
    })

    session.add(new_record)
    inserted_cont+=1

    if inserted_cont % 500 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos históricos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} históricos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_record.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_record.xlsx")
