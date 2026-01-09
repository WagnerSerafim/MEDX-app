import glob
import os
from sqlalchemy import MetaData, Table, create_engine, bindparam, UnicodeText
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, verify_nan
import csv

def select_answer(num):
    num_answer = num
    if num_answer == 1:
        return "Sim"
    elif num_answer == 2:
        return "Não"
    elif num_answer == 3:
        return "Talvez"
    else:
        return None

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")

print("Conectando no Banco de dados...")

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

csv.field_size_limit(10000000)
todos_arquivos = glob.glob(f'{path_file}/Anamneses*respostas.csv')

df = pd.read_csv(todos_arquivos[0], sep=';', engine='python', quotechar='"', encoding='latin1', on_bad_lines='skip')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont = 0
not_inserted_data = []
not_inserted_cont = 0
count = 0

for idx, row in df.iterrows():

    if idx % 1000 == 0 or idx == len(df):
        print(f"Processados: {idx} | Inseridos: {inserted_cont} | Não inseridos: {not_inserted_cont} | Concluído: {round((idx / len(df)) * 100, 2)}%")

    if row['Tipo de pergunta'] != "Calculadora gestacional":
        continue

    cod_anam = str(verify_nan(row['Código da anamnese']))
    if cod_anam == 'None' or cod_anam == None:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Histórico é vazio ou nulo'
        not_inserted_data.append(row_dict)
        continue

    count_str = str(count + 1)
    record_id = int(cod_anam + count_str)

    existing_record = exists(session, record_id, "Id do Histórico", Historico)
    if existing_record:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Histórico já existe no banco de dados'
        not_inserted_data.append(row_dict)
        continue

    id_patient = verify_nan(row['Código do paciente'])
    if id_patient is None:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Paciente vazio'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    answer = verify_nan(row['Resposta'])
    if not answer or '"select":[]' in answer:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Resposta vazia'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    question = verify_nan(row['Pergunta'])
    if not question:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Pergunta vazia'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    if question in ['FUMA',"BEBE COM FREQUENCIA?","ESTÁ EM USO, JÁ USOU OU DESEJA USAR HORMONIOS?"]:
        answer_num = answer[12]
        print(f"Print:{answer_num} e {answer}")
        print("##################################################")
        answer = select_answer(int(answer_num))
    
    record = f"{question}: {answer}"

    if is_valid_date(row['Data de criação'], "%Y-%m-%d %H:%M:%S"):
        date = row['Data de criação']
    else:
        date = '1900-01-01 00:00:00'

    new_record = Historico(
        Data=date,
    )
    setattr(new_record, "Histórico", bindparam(None, value=record, type_=UnicodeText()))
    setattr(new_record, "Id do Histórico", record_id)
    setattr(new_record, "Id do Cliente", id_patient)
    setattr(new_record, "Id do Usuário", 0)

    log_data.append({
        "Id do Histórico": record_id,
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

create_log(log_data, log_folder, "log_inserted_records_Anamnese_Respostas.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_records_Anamnese_Respostas.xlsx")