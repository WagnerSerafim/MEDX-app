import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import create_log, exists

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

Convenios = getattr(Base.classes, "Convênios")

print("Sucesso! Inicializando migração de Convênios...")

todos_arquivos = glob.glob(f'{path_file}/CONVENIO.xml')

df = pd.read_xml(todos_arquivos[0], encoding='latin1')
df = df.replace('None', '')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for _, row in df.iterrows():
    
    if row['Num'] in [None, 'None', '', 'Num'] or pd.isna(row['Num']):
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Convênio inválido'
        not_inserted_data.append(row_dict)
        continue
    else:
        insurance_id = row['Num']

    existing_insurance = exists(session, insurance_id, "Id do Convênio", Convenios)
    if existing_insurance:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Histórico já existe no banco de dados'
        not_inserted_data.append(row_dict)
        continue
    
    insurance = row['Categoria']
    if insurance in [None, '', 'None'] or pd.isna(insurance):
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Nome do Convênio vazio ou inválido'
        not_inserted_data.append(row_dict)
        continue

    new_insurance = Convenios(
        Convênio = insurance
    )

    setattr(new_insurance, "Id do Convênio", insurance_id)

    log_data.append({
        "Id do Convênio": insurance_id,
        "Convênio": insurance
    })
    session.add(new_insurance)
    inserted_cont+=1

    if inserted_cont % 100 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos convênios foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} convênios não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_insurance_Convenio.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_insurance_Convenio.xlsx")