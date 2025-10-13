import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
import csv
from datetime import datetime
from utils.utils import is_valid_date, exists, create_log, verify_nan

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

print("Sucesso! Inicializando migração de Históricos...")

todos_arquivos = glob.glob(f'{path_file}/Estoque_14054.xlsx')

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

    if exists(session, row['Id do Item'], "Id do Item", Estoque):
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Item já existe'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    else:
        id_item = row['Id do Item']

    item = verify_nan(row['Item'])
    apresentation = verify_nan(row['Apresentação'])
    brand = verify_nan(row['Marca'])
    barcode = verify_nan(row['Código de Barras'])
    lote = verify_nan(row['Lote'])
    validade = verify_nan(row['Validade'])
    qtd_estoque = int(verify_nan(row['Qtd']))
    medium_custo = verify_nan(row['Custo Médio'])
    minimum_stock = verify_nan(row['Estoque Mínimo'])
    
    new_item = Estoque(
        Item=item,
        Apresentação=apresentation
    )
    setattr(new_item, "Id do Item", id_item)
    setattr(new_item, "Marca", brand)
    setattr(new_item, "Código de Barras", barcode)
    setattr(new_item, "Lote", lote)
    setattr(new_item, "Validade", validade)
    setattr(new_item, "Qtd", qtd_estoque)
    setattr(new_item, "Custo Médio", medium_custo)
    setattr(new_item, "Estoque Mínimo", minimum_stock)
    
    log_data.append({
        "Id do Item": id_item,
        "Item": item,
        "Apresentação": apresentation,
        "Marca": brand,
        "Código de Barras": barcode,
        "Lote": lote,
        "Validade": validade,
        "Qtd": qtd_estoque,
        "Custo Médio": medium_custo,
        "Estoque Mínimo": minimum_stock,
    })
    session.add(new_item)
    inserted_cont+=1

    if inserted_cont % 1000 == 0:
        session.commit()

    if (idx + 1) % 1000 == 0 or (idx + 1) == len(df):
        print(f"Processados {idx + 1} de {len(df)} registros ({(idx + 1) / len(df) * 100:.2f}%)")

session.commit()

print(f"{inserted_cont} novos itens foram inseridos com sucesso no Estoque!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} itens não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_stock_medicamentos.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_stock_medicamentos.xlsx")
