import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, verify_nan

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase= input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ") 

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

print("Conectando no Banco de dados...")

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine) 

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Agenda = getattr(Base.classes, "Agenda")
Contatos = getattr(Base.classes, "Contatos")

print("Sucesso! Inicializando migração de Agendamentos...")

todos_arquivos = glob.glob(f'{path_file}/schedule.xlsx')

df = pd.read_excel(todos_arquivos[0])

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

dict_user ={
    44: 2004908037,
    95: -2092618722,
    22: -1271409881,
    14: -1077557811,
    104: 1101594651,
    96: -1254069442,
    11: 661097330,
    69: 1110988994,
    100: 742075895,
    99: 318778042,
    9: -1335185212,
    35: 298865711,
    101: 1228482100,
    97: 1412297399,
    80: -1548790110,
    68: 335198970,
    85: -1944838731,
    20: 1769226743,
    6: 474192302,
    15: -412928195,
    23: 2093573039,
    16: -82936895,
    46: 299613891,
    8: 1842177126
}

for idx, row in df.iterrows():
    
    if idx % 1000 == 0 or idx == len(df):
        print(f"Processados: {idx} | Inseridos: {inserted_cont} | Não inseridos: {not_inserted_cont} | Concluído: {round((idx / len(df)) * 100, 2)}%")

    patient = exists(session, row["PACIENTEID"], "Id do Cliente", Contatos)
    if not patient:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do paciente vinculado não existe no banco de dados'
        not_inserted_data.append(row_dict)
        continue
    else:
        obs = verify_nan(row['DESCRICAO'])
        description = f"{patient.Nome} {obs}"
        patient_id = row['PACIENTEID']

    exists_row = session.query(Agenda).filter(getattr(Agenda, 'Id do Agendamento') == row["ID"]).first()
    if exists_row:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id já existe no banco de dados'
        not_inserted_data.append(row_dict)
        continue
    else:
        id_scheduling = row["ID"]
    
    dt_obj = verify_nan(row['DATA'])
    if not dt_obj:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Data inválida'
        not_inserted_data.append(row_dict)
        continue
    if isinstance(dt_obj, datetime):
        start_time = dt_obj.strftime('%Y-%m-%d %H:%M:%S')
        start = f'{start_time[:10]} {row['HORA']}'
        try:
            end_obj = datetime.strptime(start, '%Y-%m-%d %H:%M') + timedelta(minutes=15)
        except ValueError:
            not_inserted_cont += 1
            row_dict = row.to_dict()
            row_dict['Motivo'] = 'Hora inválida'
            not_inserted_data.append(row_dict)
            continue
        end = end_obj.strftime('%Y-%m-%d %H:%M')

    if is_valid_date(start, '%Y-%m-%d %H:%M'):
        start = start
    else:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Data de início inválida'
        not_inserted_data.append(row_dict)
        continue

    if is_valid_date(end, '%Y-%m-%d %H:%M'):
        end = end
    else:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Data de fim inválida'
        not_inserted_data.append(row_dict)
        continue

    user = verify_nan(row['USUARIOID'])
    if user in dict_user:
        user = dict_user[user]
    else:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Usuário inválido'
        not_inserted_data.append(row_dict)
        continue

    new_schedulling = Agenda(
        Descrição=description,
        Início=start,
        Final=end,
        Status=1,
    )

    setattr(new_schedulling, "Id do Agendamento", id_scheduling)
    setattr(new_schedulling, "Vinculado a", patient_id)
    setattr(new_schedulling, "Id do Usuário", user)
    
    log_data.append({
        "Id do Agendamento": id_scheduling,
        "Vinculado a": patient_id,
        "Id do Usuário": user,
        "Início": start,
        "Final": end,
        "Descrição": description,
        "Status" : 1
    })

    session.add(new_schedulling)

    inserted_cont+=1

    if inserted_cont % 1000 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos agendamentos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} agendamentos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_scheduling.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_scheduling.xlsx")
