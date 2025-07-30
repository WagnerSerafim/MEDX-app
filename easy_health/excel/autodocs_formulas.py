import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import exists, create_log

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")
father = input("Informe o ID do pai: ")

print("Conectando no Banco de Dados...")
DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine)

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Autodocs = getattr(Base.classes, "Autodocs")

print("Sucesso! Inicializando migração de Autodocs...")

todos_arquivos = glob.glob(f'{path_file}/dados.xlsx')

df = pd.read_excel(todos_arquivos[0], sheet_name='formulas')
df = df.replace('None', '')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for idx, row in df.iterrows():

    existing_autodoc = exists(session, row['CODIGO'], "Id do Texto", Autodocs)
    if existing_autodoc:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Receituário já existe'
        not_inserted_data.append(row_dict)
        continue
    else:
        id_text = row['CODIGO']

    if row['COMPOSICAO'] in ['', None] or pd.isna(row['COMPOSICAO']):
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Receituário vazio ou inválido'
        not_inserted_data.append(row_dict)
        continue
    else:
        text = row['COMPOSICAO']

    if row['NOME'] in ['', None] or pd.isna(row['NOME']):
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Título vazio ou inválido'
        not_inserted_data.append(row_dict)
        continue
    else:
        library = row['NOME']

    new_autodoc = Autodocs(
        Texto = text,
        Pai = father,
        Biblioteca = library
    )
    setattr(new_autodoc, "Id do Texto", id_text)
    
    log_data.append({
        "Id do Texto": id_text,
        "Biblioteca": library,
        "Pai": father,
        "Texto": text,
    })
    session.add(new_autodoc)
    inserted_cont+=1

    if inserted_cont % 100 == 0:
        session.commit()

    if (idx + 1) % 100 == 0 or (idx + 1) == len(df):
        print(f"Processados {idx + 1} de {len(df)} registros ({(idx + 1) / len(df) * 100:.2f}%)")

session.commit()

print(f"{inserted_cont} novos documentos de receituário foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} documentos de receituário não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_autodocs_formulas.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_autodocs_formulas.xlsx")
