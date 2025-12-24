import glob
import os
from sqlalchemy import MetaData, Table, create_engine, bindparam, UnicodeText
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
import urllib
from utils.utils import exists, create_log
from striprtf.striprtf import rtf_to_text

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")
father = input("Informe o ID do pai: ")

print("Conectando no Banco de Dados...")
DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

metadata = MetaData()
autodocs_tbl = Table("Autodocs", metadata, schema=f"schema_{sid}", autoload_with=engine)

Base = declarative_base()

class Autodocs(Base):
    __table__ = autodocs_tbl


SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

print("Sucesso! Inicializando migração de Autodocs...")

todos_arquivos = glob.glob(f'{path_file}/dados*.xlsx')

df = pd.read_excel(todos_arquivos[0], sheet_name='TEXTOSCOMPLEMENTARES')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for idx, row in df.iterrows():

    if idx % 500 == 0 or idx == len(df):
        print(f"Processados: {idx} | Inseridos: {inserted_cont} | Não inseridos: {not_inserted_cont} | Concluído: {round((idx / len(df)) * 100, 2)}%")

    if row['TextoComplementar'] in ['', None] or pd.isna(row['TextoComplementar']):
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Receituário vazio ou inválido'
        not_inserted_data.append(row_dict)
        continue
    else:
        text = rtf_to_text(row['TextoComplementar'])

    if row['NomeTextoComplementar'] in ['', None] or pd.isna(row['NomeTextoComplementar']):
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Título vazio ou inválido'
        not_inserted_data.append(row_dict)
        continue
    else:
        library = row['NomeTextoComplementar']

    new_autodoc = Autodocs(
        Pai = father,
        Biblioteca = library
    )
    setattr(new_autodoc, "Texto", bindparam(None, value=text, type_=UnicodeText()))
    
    log_data.append({
        "Biblioteca": library,
        "Pai": father,
        "Texto": text,
    })
    session.add(new_autodoc)
    inserted_cont+=1

    if inserted_cont % 500 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos documentos de receituário foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} documentos de receituário não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_autodocs_TEXTOSCOMPLEMENTARES.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_autodocs_TEXTOSCOMPLEMENTARES.xlsx")
