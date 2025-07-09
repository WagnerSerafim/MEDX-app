import os
import pandas as pd
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import urllib

from utils.utils import create_log  # Se já existir no seu projeto

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém o arquivo autodocs_para_excluir: ")

print("Conectando no Banco de Dados...")
DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"
engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine)

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Autodocs = getattr(Base.classes, "Autodocs")

arquivo_excel = os.path.join(path_file, "autodocs_para_excluir.xlsx")
df = pd.read_excel(arquivo_excel)
ids_para_excluir = df['Id do Texto'].astype(str).tolist()

print(f"Excluindo {len(ids_para_excluir)} registros da tabela Autodocs...")

deletados = 0
nao_encontrados = 0
not_deleted_log = []

for id_texto in ids_para_excluir:
    try:
        id_int = int(id_texto)
    except Exception:
        id_int = None

    # Validação 1: não exclua ids entre 30 e 45 (inclusive)
    if id_int is not None and 30 <= id_int <= 45:
        not_deleted_log.append({
            "Id do Texto": id_texto,
            "Motivo": "Id protegido (entre 30 e 45)"
        })
        continue

    registro = session.query(Autodocs).filter(getattr(Autodocs, 'Id do Texto') == id_texto).first()
    if registro:
        session.delete(registro)
        deletados += 1
    else:
        nao_encontrados += 1
        not_deleted_log.append({
            "Id do Texto": id_texto,
            "Motivo": "Id não encontrado na tabela Autodocs"
        })

session.commit()
session.close()

print(f"{deletados} registros excluídos com sucesso!")
if nao_encontrados > 0:
    print(f"{nao_encontrados} Ids não encontrados na tabela Autodocs.")

if not_deleted_log:
    create_log(not_deleted_log, path_file, "log_autodocs_nao_excluidos.xlsx")
    print(f"Log de não excluídos salvo em: {os.path.join(path_file, 'log_autodocs_nao_excluidos.xlsx')}")