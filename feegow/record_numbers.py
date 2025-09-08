from datetime import datetime
import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, verify_nan

def get_record(row):
    """A partir da linha do dataframe, retorna o histórico formatado"""
    record = ''
    type_info = verify_nan(row['tipo_informacao'])
    resume_content = verify_nan(row['conteudo_resumo'])

    if not type_info in [None, '', 'None']:
        record += f'Tipo de histórico: {row["tipo_informacao"]}<br><br>'

    if not resume_content in [None, '', 'None']:
        record += f'Conteúdo do histórico: {row["conteudo_resumo"]}<br><br>'
    else:
        record = ''

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

print("Sucesso! Inicializando migração de Históricos...")

todos_arquivos = glob.glob(f'{path_file}/_*.xlsx')

dfs = []

for arquivo in todos_arquivos:
    df = pd.read_excel(arquivo)
    dfs.append(df)

df_main = pd.concat(dfs, ignore_index=True)

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for idx, row in df_main.iterrows():


    if idx % 1000 == 0 or idx == len(df):
        print(f"Processados: {idx} | Inseridos: {inserted_cont} | Não inseridos: {not_inserted_cont} | Concluído: {round((idx / len(df)) * 100, 2)}%")

    id_record = verify_nan(row['id'])
    if id_record in [None, '', 'None']:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Histórico vazio'
        row_dict['TimeStamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        not_inserted_data.append(row_dict)
        continue

    existing_record = exists(session, id_record, "Id do Histórico", HistoricoClientes)
    if existing_record:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Histórico já existe'
        not_inserted_data.append(row_dict)
        continue

    record = get_record(row)
    if record == "":
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Histórico vazio'
        not_inserted_data.append(row_dict)
        continue

    record = record.replace('_x000D_', ' ')

    id_patient = verify_nan(row['paciente_id'])
    if id_patient == "" or id_patient == None:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Paciente vazio'
        not_inserted_data.append(row_dict)
        continue

    data_hora = str(row['data_hora']) if row['data_hora'] is not None else ""
    if is_valid_date(data_hora, '%Y-%m-%d %H:%M:%S'):
        date = data_hora
    elif is_valid_date(data_hora, '%Y-%m-%d'):
        date = data_hora + ' 00:00:00'
    else:
        date = '01/01/1900 00:00'
 
    new_record = HistoricoClientes(
        Histórico=record,
        Data=date
    )
    setattr(new_record, "Id do Histórico", (row['id']))
    setattr(new_record, "Id do Cliente", id_patient)
    setattr(new_record, "Id do Usuário", 0)

    log_data.append({
        "Id do Histórico": (row['id']),
        "Id do Cliente": id_patient,
        "Data": date,
        "Histórico": record,
        "Id do Usuário": 0,
    })
    inserted_cont+=1

    
    session.add(new_record)

    if inserted_cont % 1000 == 0:
        session.commit()
    
session.commit()

print(f"{inserted_cont} novos históricos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} históricos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_record_numbers.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_record_numbers.xlsx")
