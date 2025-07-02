import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import exists, create_log, is_valid_date
from datetime import datetime


def get_record(row):
    record = ''

    if not (row['Nome'] in ['None', None, ''] or pd.isna(row['Nome'])):
        record += f'Nome: {row['Nome']}<br>'
    
    if not (row['Resultado'] in ['None', None, ''] or pd.isna(row['Resultado'])):
        record += f'Resultado: {row['Resultado']}<br>'
    
    if not (row['Observacao'] in ['None', None, ''] or pd.isna(row['Observacao'])):
        record += f'Observação: {row['Observacao']}<br>'

    if not (row['Justificativa'] in ['None', None, ''] or pd.isna(row['Justificativa'])):
        record += f'Diagnóstico: {row['Justificativa']}<br>'

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

print("Sucesso! Inicializando migração de Histórico de Clientes...")

extension_file = glob.glob(f'{path_file}/ExamesPacientesItens.xlsx')

extension_file_patients = glob.glob(f'{path_file}/ExamesPacientes.xlsx')

df = pd.read_excel(extension_file[0])

df_patients = pd.read_excel(extension_file_patients[0])

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

patients_lookup = {patient['ExamePacienteID']:patient['PacienteID'] for _,patient in df_patients.iterrows()}

for _, row in df.iterrows():

    if row['ExamePacienteItemID'] in ['None', None, ''] or pd.isna(row['ExamePacienteItemID']):
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = f'Id {row['ExamePacienteItemID']} é um valor inválido ou vazio'
        row_dict['TimeStamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    existing_record = exists(session, int(row['ExamePacienteItemID']) + 74890, 'Id do Histórico', HistoricoClientes)
    if existing_record:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['TimeStamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        row_dict['Motivo'] = f'Id {row['ExamePacienteItemID']} já existe no Banco de Dados'
        not_inserted_data.append(row_dict)
        continue
    else:
        id_record = int(row['ExamePacienteItemID']) + 74890

    id_patient = patients_lookup.get(row['ExamePacienteID'], "")
    if id_patient == "" or id_patient == None or id_patient == 'None' or pd.isna(id_patient):
        if id_record in patients_lookup:
            id_patient = patients_lookup[id_record]
        else:
            not_inserted_cont += 1
            row_dict = row.to_dict()
            row_dict['Motivo'] = 'Id do paciente vazio e não encontrado no arquivo ExamesPacientes.xlsx'
            not_inserted_data.append(row_dict)
            continue

    record = get_record(row)
    if record == '':
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = f'O histórico é um valor inválido ou vazio'
        row_dict['TimeStamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue


    if row['DataSolicitacao'] in ['None', None, ''] or pd.isna(row['DataSolicitacao']):
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = f'A data {row['DataSolicitacao']} é um valor inválido ou vazio'
        row_dict['TimeStamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    else:
        data_str = row['DataSolicitacao']
        date = datetime.strptime(data_str, '%m/%d/%Y %I:%M:%S %p')
        date = date.strftime('%Y-%m-%d %H:%M:%S')
        if not is_valid_date(date, '%Y-%m-%d %H:%M:%S'):
            not_inserted_cont += 1
            row_dict = row.to_dict()
            row_dict['Motivo'] = f'A data inicial {row['DataSolicitacao']} é um valor inválido'
            row_dict['TimeStamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            not_inserted_data.append(row_dict)
            continue

    new_record = HistoricoClientes()

    setattr(new_record, "Id do Histórico", id_record)
    setattr(new_record, 'Histórico', record)
    setattr(new_record, 'Id do Cliente', id_patient)
    setattr(new_record, 'Id do Usuário', 0)
    setattr(new_record, 'Data', date)

    
    log_data.append({
        'Id do Histórico': id_record,
        'Histórico': record,
        'Data': date,
        'Id do Usuário': 0,
        'Id do Cliente': id_patient,
        'TimeStamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

    session.add(new_record)

    inserted_cont += 1
    if inserted_cont % 1000 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos históricos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} históricos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_ExamesPacientesItens.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_ExamesPacientesItens.xlsx")
