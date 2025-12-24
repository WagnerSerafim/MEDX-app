from datetime import datetime
import glob
import os
from sqlalchemy import MetaData, Table, create_engine, bindparam, UnicodeText
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, verify_nan
from striprtf.striprtf import rtf_to_text

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")

print("Conectando no Banco de Dados...")
DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

metadata = MetaData()
historico_tbl = Table("Histórico de Clientes", metadata, schema=f"schema_{sid}", autoload_with=engine)

Base = declarative_base()

class Historico(Base):
    __table__ = historico_tbl

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

print("Sucesso! Inicializando migração de Históricos...")

todos_arquivos = glob.glob(f'{path_file}/dados*.xlsx')

df = pd.read_excel(todos_arquivos[0], sheet_name='CONS_PAC')
df = df.replace('None', '')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for idx, row in df.iterrows():

    if idx % 500 == 0 or idx == len(df):
        print(f"Processados: {idx} | Inseridos: {inserted_cont} | Não inseridos: {not_inserted_cont} | Concluído: {round((idx / len(df)) * 100, 2)}%")

    # record_id = verify_nan(row['ID_CONS'])
    # if not record_id:
    #     not_inserted_cont += 1
    #     row_dict = row.to_dict()
    #     row_dict['Motivo'] = 'ID_Anam vazio'
    #     row_dict['TimeStamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #     not_inserted_data.append(row_dict)
    #     continue

    # existing_record = exists(session, record_id, "Id do Histórico", Historico)
    # if existing_record:
    #     not_inserted_cont += 1
    #     row_dict = row.to_dict()
    #     row_dict['Motivo'] = 'Id do Histórico já existe'
    #     row_dict['TimeStamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #     not_inserted_data.append(row_dict)
    #     continue

    record = verify_nan(row['Text'])
    if record == None:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Histórico vazio ou inválido'
        row_dict['TimeStamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        not_inserted_data.append(row_dict)
        continue
    else:
        record = rtf_to_text(record)
        record = record.replace('_x000D_', '<br>')

    if is_valid_date(row['Date_Cons'], '%Y-%m-%d %H:%M:%S'):
        date = row['Date_Cons']
    else:
        date = '01/01/1900 00:00'

    id_patient = verify_nan(row["ID_Pac"])
    if id_patient == "" or id_patient == None or id_patient == 'None':
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do paciente vazio'
        row_dict['TimeStamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        not_inserted_data.append(row_dict)
        continue
    
    new_record = Historico(
        Data=date,
    )
    setattr(new_record, "Histórico", bindparam(None, value=record, type_=UnicodeText()))
    # setattr(new_record, "Id do Histórico", record_id)
    setattr(new_record, "Id do Cliente", id_patient)
    setattr(new_record, "Id do Usuário", 0)
    
    log_data.append({
        # "Id do Histórico": record_id,
        "Id do Cliente": id_patient,
        "Data": date,
        "Histórico": record,
        "Id do Usuário": 0,
        "TimeStamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })
    session.add(new_record)
    inserted_cont+=1

    if inserted_cont % 1000 == 0:
        session.commit()

session.commit()

print("Migração concluída! Gerando logs...")
print(f"{inserted_cont} novos históricos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} históricos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_record_CONS_PAC.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_record_CONS_PAC.xlsx")
