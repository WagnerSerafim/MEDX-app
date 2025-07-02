import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import exists, create_log, is_valid_date
from datetime import datetime

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

Agenda = getattr(Base.classes, "Agenda")

print("Sucesso! Inicializando migração de Agenda...")

extension_file = glob.glob(f'{path_file}/Agendas.xlsx')

df = pd.read_excel(extension_file[0])

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for _, row in df.iterrows():

    if row['AgendaID'] in ['None', None, ''] or pd.isna(row['AgendaID']):
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = f'Id {row['AgendaID']} é um valor inválido ou vazio'
        row_dict['TimeStamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    existings_scheduling = exists(session, row['AgendaID'], 'Id do Agendamento', Agenda)
    if existings_scheduling:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = f'Id {row['AgendaID']} já existe no Banco de Dados'
        row_dict['TimeStamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    else:
        id_scheduling = row['AgendaID']

    if row['PacienteID'] in ['None', None, ''] or pd.isna(row['PacienteID']):
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = f'Id {row['PacienteID']} é um valor inválido ou vazio'
        row_dict['TimeStamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    else:
        id_patient = row['PacienteID']

    if row['UsuarioID'] in ['None', None, ''] or pd.isna(row['UsuarioID']):
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = f'Id {row['UsuarioID']} é um valor inválido ou vazio'
        row_dict['TimeStamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    else:
        id_user = row['UsuarioID']
    
    if row['HoraInicial'] in ['None', None, ''] or pd.isna(row['HoraInicial']):
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = f'A hora inicial {row['HoraInicial']} é um valor inválido ou vazio'
        row_dict['TimeStamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    else:
        data_str = row['HoraInicial']
        date = datetime.strptime(data_str, '%m/%d/%Y %I:%M:%S %p')
        date = date.strftime('%Y-%m-%d %H:%M:%S')
        if is_valid_date(date, '%Y-%m-%d %H:%M:%S'):
            begining_hour = date
        else:
            not_inserted_cont += 1
            row_dict = row.to_dict()
            row_dict['Motivo'] = f'A data inicial {row['HoraInicial']} é um valor inválido'
            row_dict['TimeStamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            not_inserted_data.append(row_dict)
            continue
    
    if row['HoraFinal'] in ['None', None, ''] or pd.isna(row['HoraFinal']):
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = f'A hora final {row['HoraFinal']} é um valor inválido ou vazio'
        row_dict['TimeStamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    else:
        data_str = row['HoraFinal']
        date = datetime.strptime(data_str, '%m/%d/%Y %I:%M:%S %p')
        date = date.strftime('%Y-%m-%d %H:%M:%S')
        if is_valid_date(date, '%Y-%m-%d %H:%M:%S'):
            ending_hour = date
        else:
            not_inserted_cont += 1
            row_dict = row.to_dict()
            row_dict['Motivo'] = f'A data final {row['HoraFinal']} é um valor inválido'
            row_dict['TimeStamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            not_inserted_data.append(row_dict)
            continue

    description = f'{row['Descricao']}'
    if not (row['Observacao'] in ['None', None, ''] or pd.isna(row['Observacao'])):
        description += f' - {row['Observacao']}'

    new_scheduling = Agenda()

    setattr(new_scheduling, "Id do Agendamento", id_scheduling)
    setattr(new_scheduling, "Vinculado a", id_patient)
    setattr(new_scheduling, 'Id do Usuário', id_user)
    setattr(new_scheduling, 'Início', begining_hour)
    setattr(new_scheduling, 'Final', ending_hour)
    setattr(new_scheduling, 'Descrição', description)
    setattr(new_scheduling, 'Status', 1)
    
    log_data.append({
        'Id do Agendamento': id_scheduling,
        'Vinculado a': id_patient,
        'Id do Usuário': id_user,
        'Início': begining_hour,
        'Final': ending_hour,
        'Descrição': description,
        'Status': 1,
        'TimeStamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

    session.add(new_scheduling)

    inserted_cont += 1
    if inserted_cont % 1000 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos agendamentos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} agendamentos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_Agendas.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_Agendas.xlsx")
