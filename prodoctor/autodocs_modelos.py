import glob
import json
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log
from striprtf.striprtf import rtf_to_text


def get_text(row):
    """
    A partir da linha do dataframe, retorna o texto formatado.
    """
    try:
        text = rtf_to_text(row['texto'])
        text = text.replace('_x000D_', '')
    except:
        return ''
    
    return text


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

Autodocs = getattr(Base.classes, "Autodocs")

print("Sucesso! Inicializando migração de Autodocs...")

todos_arquivos = glob.glob(f'{path_file}/dados*.xlsx')

df = pd.read_excel(todos_arquivos[0], sheet_name='t_modeloslaudos')
df = df.replace('None', '')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0
cont_no_library = 1

for _, row in df.iterrows():

    existing_autodoc = exists(session, row['codigo'], "Id do Texto", Autodocs)
    if existing_autodoc:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Receituário já existe'
        not_inserted_data.append(row_dict)
        continue
    else:
        id_text = row['codigo']

    text = get_text(row)
    if text == "":
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Histórico vazio ou inválido'
        not_inserted_data.append(row_dict)
        continue
    
    father = 923707087

    if row["nome"] != "" and row["nome"] is not None :
        library = row["nome"]
    else:
        library = f"Receituário sem nome definido {cont_no_library}"
        cont_no_library += 1

    new_autodoc = Autodocs(
        Texto = text,
        Pai = father,
        Biblioteca = library
    )
    setattr(new_autodoc, "Id do Texto", id_text)
    
    log_data.append({
        "Id do Texto": id_text,
        "Pai": father,
        "Texto": text,
    })
    session.add(new_autodoc)
    inserted_cont+=1

    if inserted_cont % 10000 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos históricos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} históricos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_autodocs_modeloslaudos.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_autodocs_modeloslaudos.xlsx")
