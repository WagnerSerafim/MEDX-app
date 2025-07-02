import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import create_log, is_valid_date

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

Contatos = getattr(Base.classes, "Contatos")

print("Sucesso! Inicializando atualização de Ids dos Contatos...")

todos_arquivos = glob.glob(f'{path_file}/dados.xlsx')

df = pd.read_excel(todos_arquivos[0], sheet_name='shosp_cadastro_paciente_michell')
df = df.replace('None', '')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont = 0
not_inserted_data = []
not_inserted_cont = 0

for _, row in df.iterrows():

    with session.no_autoflush:
        existing_id = session.query(Contatos).filter(
            Contatos.__table__.c['Id do Cliente'] == row['Pront.'],
            Contatos.Nome != row['Nome']
        ).first()

    if existing_id:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = f"ID {row['Pront.']} já está em uso por outro contato"
        not_inserted_data.append(row_dict)
        continue
 
    date = row['Dt. Nasc.']
    if is_valid_date(date, "%Y-%m-%d"):
        birthday = date
    else:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Data de nascimento está no formato errado'
        not_inserted_data.append(row_dict)
        continue

    with session.no_autoflush:
        patient = session.query(Contatos).filter(
            Contatos.Nome == row['Nome'],
            Contatos.Nascimento == birthday
        ).first()

    if not patient:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Esse nome + nascimento não existe nos Contatos do banco'
        not_inserted_data.append(row_dict)
        continue
    else:
        row_dict = row.to_dict()
        row_dict['Atualização'] = f'Atualizou do id {getattr(patient, 'Id do Cliente')} para {row['Pront.']}'
        setattr(patient, 'Id do Cliente', row['Pront.'])
        inserted_cont += 1
        session.add(patient)

    if inserted_cont % 500 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} ids de clientes foram atualizados com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} ids não foram atualizados, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_update_id_patients.xlsx")
create_log(not_inserted_data, log_folder, "log_not_update_id_patients.xlsx")
