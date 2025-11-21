from datetime import datetime
import glob
import os
import re
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, truncate_value, verify_nan

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")

print("Conectando no Banco de Dados...")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

metadata = MetaData()
estoque_tbl = Table("Estoque", metadata, schema=f"schema_{sid}", autoload_with=engine)
movimentacao_tbl = Table("Estoque Movimentação", metadata, schema=f"schema_{sid}", autoload_with=engine)
itens_tbl = Table("Estoque Movimentação Itens", metadata, schema=f"schema_{sid}", autoload_with=engine)

Base = declarative_base()

class Estoque(Base):
    __table__ = estoque_tbl

class Movimentacao(Base):
    __table__ = movimentacao_tbl

class Itens(Base):
    __table__ = itens_tbl

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

print("Sucesso! Inicializando migração de Estoques...")

estoque_file = glob.glob(f'{path_file}/Estoque.json')
movimentacao_tbl = glob.glob(f'{path_file}/Estoque Movimentação.json')
itens_file = glob.glob(f'{path_file}/Estoque Movimentação Itens.json')

df_estoque = pd.read_json(estoque_file[0])
df_movimentacao = pd.read_json(movimentacao_tbl[0])
df_itens = pd.read_json(itens_file[0])

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

print("Iniciando migração de Estoques...")

for idx, row in df_estoque.iterrows():

    if idx % 1000 == 0 or idx == len(df_estoque):
        print(f"Processados: {idx} | Inseridos: {inserted_cont} | Não inseridos: {not_inserted_cont} | Concluído: {round((idx / len(df_estoque)) * 100, 2)}%")

    id_stock = verify_nan(row['Id do Item'])
    name_item = verify_nan(row['Item'])
    apresentation = verify_nan(row['Apresentação'])
    brand = verify_nan(row['Marca'])
    barcode = verify_nan(row['Código de Barras'])
    lote = verify_nan(row['Lote'])
    validity = verify_nan(row['Validade'])
    quantity = verify_nan(row['Qtd'])
    medium_cost = verify_nan(row['Custo Médio'])
    minimum_stock = verify_nan(row['Estoque Mínimo'])
    try:
        creationdate = datetime.strptime(row['CreationDate'], "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        creationdate = datetime.strptime(row['CreationDate'], "%Y-%m-%d %H:%M:%S")

    new_estoque = Estoque()
    setattr(new_estoque, "Id do Item", id_stock)
    setattr(new_estoque, "Item", name_item)
    setattr(new_estoque, "Apresentação", apresentation)
    setattr(new_estoque, "Marca", brand)
    setattr(new_estoque, "Código de Barras", barcode)
    setattr(new_estoque, "Lote", lote)
    setattr(new_estoque, "Validade", validity)
    setattr(new_estoque, "Qtd", quantity)
    setattr(new_estoque, "Custo Médio", medium_cost)
    setattr(new_estoque, "Estoque Mínimo", minimum_stock)
    setattr(new_estoque, "CreationDate", creationdate)

    log_data.append({
        "Id do Item": id_stock,
        "Item": name_item,
        "Apresentação": apresentation,
        "Marca": brand,
        "Código de Barras": barcode,
        "Lote": lote,
        "Validade": validity,
        "Qtd": quantity,
        "Custo Médio": medium_cost,
        "Estoque Mínimo": minimum_stock,
        "CreationDate": creationdate
    })

    session.add(new_estoque)
    inserted_cont+=1

    if inserted_cont % 1000 == 0:
        session.commit()
    
session.commit()

print(f"{inserted_cont} novos itens foram inseridos com sucesso no Estoque!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} itens não foram inseridos, verifique o log para mais detalhes.")

create_log(log_data, log_folder, "log_inserted_Estoque.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_Estoque.xlsx")
