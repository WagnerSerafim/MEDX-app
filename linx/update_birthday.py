import os
import pandas as pd
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import urllib
from datetime import datetime

from utils.utils import create_log

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho onde ficará o log: ")

print("Conectando no Banco de Dados...")
DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"
engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine)

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Contatos = getattr(Base.classes, "Contatos")

# Busca apenas os contatos da sua query
registros = session.query(Contatos).filter(
    getattr(Contatos, 'Id do Cliente').between(0, 20000),
    getattr(Contatos, 'CreationDate') >= '2025-07-08 10:00:00.020'
).all()

atualizados = 0
log_data = []
not_updated_data = []

def inverter_data(data):
    if not data:
        return data
    try:
        # Aceita tanto string quanto datetime
        if isinstance(data, str):
            dt = datetime.strptime(data[:10], "%Y-%m-%d")
        else:
            dt = data
        # Inverte dia e mês
        return dt.replace(day=dt.month, month=dt.day).strftime("%Y-%m-%d")
    except Exception as e:
        return data

for contato in registros:
    nascimento = getattr(contato, 'Nascimento', None)
    nascimento_str = str(nascimento)[:10] if nascimento else None
    novo_nascimento = inverter_data(nascimento)
    if nascimento_str != novo_nascimento:
        log_data.append({
            "Id do Cliente": getattr(contato, "Id do Cliente", None),
            "Nome": getattr(contato, "Nome", None),
            "Nascimento_antes": nascimento_str,
            "Nascimento_corrigido": novo_nascimento
        })
        setattr(contato, 'Nascimento', novo_nascimento)
        atualizados += 1
    else:
        not_updated_data.append({
            "Id do Cliente": getattr(contato, "Id do Cliente", None),
            "Nome": getattr(contato, "Nome", None),
            "Nascimento": nascimento_str,
            "Motivo": "Nascimento já estava correto ou não pôde ser convertido"
        })

session.commit()
session.close()

create_log(log_data, path_file, "log_nascimento_corrigido.xlsx")
create_log(not_updated_data, path_file, "log_nascimento_nao_corrigido.xlsx")

print(f"{atualizados} datas de nascimento corrigidas com sucesso.")
print(f"Verifique os logs em {path_file}")