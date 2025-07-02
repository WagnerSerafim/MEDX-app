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

Estoque = getattr(Base.classes, "Estoque")

print("Sucesso! Inicializando migração de Estoque...")

extension_file = glob.glob(f'{path_file}/ProdutosApresentacoes.xlsx')

df = pd.read_excel(extension_file[0])

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for _, row in df.iterrows():

    if row['Ativo'] == 0:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = f'O produto não consta como ativo no backup {row['Ativo']}'
        not_inserted_data.append(row_dict)
        continue

    if row['ProdutoApresentacaoID'] in ['None', None, ''] or pd.isna(row['ProdutoApresentacaoID']):
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = f'Id {row['ProdutoApresentacaoID']} é um valor inválido ou vazio'
        not_inserted_data.append(row_dict)
        continue

    existing_product = exists(session, row['ProdutoApresentacaoID'], 'Id do Item', Estoque)
    if existing_product:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = f'Id {row['ProdutoApresentacaoID']} já existe no Banco de Dados'
        not_inserted_data.append(row_dict)
        continue
    else:
        id_product = row['ProdutoApresentacaoID']

    if row['Nome'] in ['None', None, ''] or pd.isna(row['Nome']):
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = f'O nome {row['Nome']} é um valor inválido ou vazio'
        not_inserted_data.append(row_dict)
        continue
    else:
        name_item = truncate_value(row['Nome'], 100)

    if row['CodigoBarra'] in ['None', None, ''] or pd.isna(row['CodigoBarra']):
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = f'O Código de Barra {row['CodigoBarra']} é um valor inválido ou vazio'
        not_inserted_data.append(row_dict)
        codebar = ''
    else:
        if len(row['CodigoBarra']) > 25:
            not_inserted_cont += 1
            row_dict = row.to_dict()
            row_dict['Motivo'] = f'O Código de Barra {row['CodigoBarra']} tem mais que 25 caracteres, não será inserido no banco'
            not_inserted_data.append(row_dict)
            codebar = ''
        else:
            codebar = row['CodigoBarra']

    new_product = Estoque()

    setattr(new_product, "Id do Item", id_product)
    setattr(new_product, "Código de Barras", codebar)
    setattr(new_product, 'Item', name_item)
    
    log_data.append({
        'Id do item': id_product,
        'Codigo_de_barras': codebar,
        'Item': name_item
    })

    session.add(new_product)

    inserted_cont += 1
    if inserted_cont % 1000 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos produtos foram inseridos ao estoque com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} produtos não foram inseridos ao estoque, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_ProdutosApresentacoes.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_ProdutosApresentacoes.xlsx")
