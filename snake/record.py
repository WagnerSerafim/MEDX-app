import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import urllib
from utils.utils import create_log, exists, verify_nan, parse_us_datetime_to_sql
from datetime import datetime
    
sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine)

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

HistoricoClientes = getattr(Base.classes, "Histórico de Clientes")

print("Sucesso! Inicializando migração de Históricos...")

todos_arquivos = glob.glob(f'{path_file}/evolucaoclinica*.xlsx')

df = pd.read_excel(todos_arquivos[0])

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for idx, row in df.iterrows():
    
    if idx % 1000 == 0 or idx == len(df):
        print(f"Processados: {idx} | Inseridos: {inserted_cont} | Não inseridos: {not_inserted_cont} | Concluído: {round((idx / len(df)) * 100, 2)}%")

    id_record = verify_nan(row['IdEvolucaoClinica'])
    if id_record in ['', None]:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'IdEvolucaoClinica vazio'
        row_dict['TimeStamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        not_inserted_data.append(row_dict)
        continue

    existing_record = exists(session, id_record, "Id do Histórico", HistoricoClientes)
    if existing_record:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = f'Evolução Clínica com Id {id_record} já existe'
        row_dict['TimeStamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        not_inserted_data.append(row_dict)
        continue

    id_patient = verify_nan(row['IdPaciente'])
    if id_patient in ['', None]:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'IdPaciente vazio'
        row_dict['TimeStamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        not_inserted_data.append(row_dict)
        continue

    record  = verify_nan(row['Evolucao'])
    if record in [None, '', 'None'] or pd.isna(record):
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Histórico vazio'
        not_inserted_data.append(row_dict)
        continue

    date = parse_us_datetime_to_sql(row['DataInclusao'])

    new_record = HistoricoClientes(
        Histórico = record,
        Data = date
    )

    setattr(new_record, "Id do Cliente", id_patient)
    setattr(new_record, "Id do Usuário", 0)

    log_data.append({
        "Id do Cliente": id_patient,
        "Data": date,
        "Histórico": record,
        "Id do Usuário": 0,
    })
    session.add(new_record)
    inserted_cont+=1

    if inserted_cont % 1000 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos históricos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} históricos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_record_evolucaoclinica.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_record_evolucaoclinica.xlsx")