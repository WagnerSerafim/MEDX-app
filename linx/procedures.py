import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import exists, create_log, truncate_value
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

Procedimentos = getattr(Base.classes, "Procedimentos")

print("Sucesso! Inicializando migração de Procedimentos...")

extension_file = glob.glob(f'{path_file}/Procedimentos.xlsx')

df = pd.read_excel(extension_file[0])

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for _, row in df.iterrows():

    if row['ProcedimentoID'] in ['None', None, ''] or pd.isna(row['ProcedimentoID']):
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = f'Id {row['ProcedimentoID']} é um valor inválido ou vazio'
        not_inserted_data.append(row_dict)
        continue

    existing_procedure = exists(session, (row['ProcedimentoID'] + 1), 'Id do Procedimento', Procedimentos)
    if existing_procedure:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = f'Id {row['ProcedimentoID'] + 1} já existe no Banco de Dados'
        not_inserted_data.append(row_dict)
        continue
    else:
        id_procedure = row['ProcedimentoID'] + 1

    if row['Nome'] in ['None', None, ''] or pd.isna(row['Nome']):
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = f'O nome {row['Nome']} é um valor inválido ou vazio'
        not_inserted_data.append(row_dict)
        continue
    else:
        name_procedure = truncate_value(row['Nome'], 100)

    new_procedure = Procedimentos()

    setattr(new_procedure, "Id do Procedimento", id_procedure)
    setattr(new_procedure, 'Procedimento', name_procedure)
    setattr(new_procedure, 'Custo', 0)
    setattr(new_procedure, 'Preço Base', 0)
    setattr(new_procedure, 'Produto', 0)
    setattr(new_procedure, 'Sessões', 0)
    setattr(new_procedure, 'Comissao', 0)

    
    log_data.append({
        'Id do Procedimento': id_procedure,
        'Procedimento': name_procedure,
        'Custo': 0,
        'Preço Base': 0,
        'Produto': 0,
        'Sessões': 0,
        'Comissao': 0
    })

    session.add(new_procedure)

    inserted_cont += 1
    if inserted_cont % 1000 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos procedimentos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} procedimentos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_Procedimentos.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_Procedimentos.xlsx")
