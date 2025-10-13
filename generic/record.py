import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import urllib
from utils.utils import create_log, is_valid_date, exists
import json

def get_record(json_info):
    record = ''
    try:
        json_data = json.loads(json_info)

        for item in json_data:
            if item['nome'] in ['Gestante?', 'Tabagista?', 'Possui diabetes?', 'Possui hipertensão?', 'Utiliza marcapasso?',
                                'Possui alterações hormonais ou na tireóide?', 'Possui doença hepática?', 'Utiliza filtro solar diariamente?',
                                'Utiliza medicamentos contínuos?', 'Já fez cirurgia?', 'Realiza atividade física regular?']:
                record += f"{item['nome']}:<br>{'Sim' if item['conteudo'] == 1 else 'Não'}<br><br>"
            
            elif item['nome'] in ['Patologias cutâneas?']:
                patologies = ['Psoríase', 'Vitiligo', 'Lupus', 'Ros', 'Outro']
                try:
                    patology_id = int(item['conteudo']) - 1
                except ValueError:
                    patology_id = int(item['conteudo'][6:7]) - 1
                record += f'{item["nome"]}:<br>{patologies[patology_id]}<br><br>'
            else:
                if not item['nome'] == 'Tipo de hiperpigmentação periocular':
                    record += f'{item["nome"]}:<br>{item["conteudo"]}<br><br>'
            
    except json.JSONDecodeError:
        record = ''

    if record:
        record = record.replace('_x000D_','')

    return record

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
Contatos = getattr(Base.classes, "Contatos")

print("Sucesso! Inicializando migração de Históricos...")

todos_arquivos = glob.glob(f'{path_file}/planilha_historicos.xlsx')

df = pd.read_excel(todos_arquivos[0])
df = df.replace('None', '')

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

    id_patient = row['ID_PACIENTE']
    
    record = row['HISTORICO']
    if record == '':
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Histórico vazio'
        not_inserted_data.append(row_dict)
        continue

    date = row['DATA']

    classe = row['CLASSE']
    existing_class = exists(session, classe, "Classe", HistoricoClientes)
    if existing_class:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Histórico vazio'
        not_inserted_data.append(row_dict)
        continue

    new_record = HistoricoClientes(
        Histórico = record,
        Data = date,
        Classe = classe
    )

    setattr(new_record, "Id do Cliente", id_patient)
    setattr(new_record, "Id do Usuário", 0)

    log_data.append({
        "Id do Cliente": id_patient,
        "Data": date,
        "Histórico": record,
        "Classe": classe,
        "Id do Usuário": 0,
    })
    session.add(new_record)
    inserted_cont+=1

    if inserted_cont % 100 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos históricos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} históricos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_record.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_record.xlsx")