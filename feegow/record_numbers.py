import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log

def get_record(row):
    """A partir da linha do dataframe, retorna o histórico formatado"""
    record = ''
    if not (row['tipo_informacao'] == '' or row['tipo_informacao'] == None):
        record += f'Tipo de histórico: {row["tipo_informacao"]}<br><br>'

    if not (row['conteudo_resumo'] == '' or row['conteudo_resumo'] == None):
        record += f'Conteúdo do histórico: {row["conteudo_resumo"]}<br><br>'

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

    existing_record = exists(session, row['id'], "Id do Histórico", HistoricoClientes)
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

    id_patient = row['paciente_id']
    if id_patient == "" or id_patient == None:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Paciente vazio'
        not_inserted_data.append(row_dict)
        continue

    # Validação para data_hora
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

    if (idx + 1) % 1000 == 0 or (idx + 1) == len(df_main):
        print(f"Processados {idx + 1} de {len(df_main)} registros ({(idx + 1) / len(df_main) * 100:.2f}%)")

session.commit()

print(f"{inserted_cont} novos históricos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} históricos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_record_numbers.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_record_numbers.xlsx")
