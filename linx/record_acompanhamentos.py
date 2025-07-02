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

    if not (row['Avaliacao'] in ['None', None, ''] or pd.isna(row['Avaliacao'])):
        record += f'Avaliação: {row['Avaliacao']}<br>'
    
    if not (row['Conduta'] in ['None', None, ''] or pd.isna(row['Conduta'])):
        record += f'Conduta: {row['Conduta']}<br>'
    
    if not (row['Impressao'] in ['None', None, ''] or pd.isna(row['Impressao'])):
        record += f'Impressão: {row['Impressao']}<br>'

    if not (row['Diagnostico'] in ['None', None, ''] or pd.isna(row['Diagnostico'])):
        record += f'Diagnóstico: {row['Diagnostico']}<br>'
    
    if not (row['HipoteseDiagnostica'] in ['None', None, ''] or pd.isna(row['HipoteseDiagnostica'])):
        record += f'Hipótese Diagnóstica: {row['HipoteseDiagnostica']}<br>'
    
    if not (row['Conduta'] in ['None', None, ''] or pd.isna(row['Conduta'])):
        record += f'Conduta: {row['Conduta']}<br>'
    
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

extension_file = glob.glob(f'{path_file}/SeguimentosAcompanhamentos.xlsx')

df = pd.read_excel(extension_file[0])

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for _, row in df.iterrows():

    if row['SeguimentoAcompanhamentoID'] in ['None', None, ''] or pd.isna(row['SeguimentoAcompanhamentoID']):
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = f'Id {row['SeguimentoAcompanhamentoID']} é um valor inválido ou vazio'
        row_dict['TimeStamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    existing_record = exists(session, row['SeguimentoAcompanhamentoID'], 'Id do Histórico', HistoricoClientes)
    if existing_record:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['TimeStamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        row_dict['Motivo'] = f'Id {row['SeguimentoAcompanhamentoID']} já existe no Banco de Dados'
        not_inserted_data.append(row_dict)
        continue
    else:
        id_record = row['SeguimentoAcompanhamentoID']

    if row['PacienteID'] in ['None', None, ''] or pd.isna(row['PacienteID']):
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = f'O PacienteID {row['PacienteID']} é um valor inválido ou vazio'
        row_dict['TimeStamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    else:
        id_patient = row['PacienteID']

    record = get_record(row)

    if row['DataConsulta'] in ['None', None, ''] or pd.isna(row['DataConsulta']):
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = f'A data {row['DataConsulta']} é um valor inválido ou vazio'
        row_dict['TimeStamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    else:
        data_str = row['DataConsulta']
        date = datetime.strptime(data_str, '%m/%d/%Y %I:%M:%S %p')
        date = date.strftime('%Y-%m-%d %H:%M:%S')
        if not is_valid_date(date, '%Y-%m-%d %H:%M:%S'):
            not_inserted_cont += 1
            row_dict = row.to_dict()
            row_dict['Motivo'] = f'A data inicial {row['DataConsulta']} é um valor inválido'
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

create_log(log_data, log_folder, "log_inserted_SeguimentosAcompanhamentos.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_SeguimentosAcompanhamentos.xlsx")
