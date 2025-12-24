import os
import pandas as pd
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
import urllib
import re

from utils.utils import create_log

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho onde ficarão os logs: ")

print("Conectando no Banco de Dados...")
DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"
engine = create_engine(DATABASE_URL)

metadata = MetaData()
contatos_tbl = Table("Contatos", metadata, schema=f"schema_{sid}", autoload_with=engine)

Base = declarative_base()

class Contatos(Base):
    __table__ = contatos_tbl

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

print("Gerando backup da tabela Contatos...")
contatos_df = pd.read_sql(session.query(Contatos).statement, session.bind)
backup_path = os.path.join(path_file, "contatos_bkp.xlsx")
contatos_df.to_excel(backup_path, index=False)
print(f"Backup salvo em: {backup_path}")

def limpar_numero(valor):
    if valor is None:
        return None
    valor_str = str(valor)
    if valor_str.endswith('.0'):
        valor_str = valor_str[:-2]
    valor_str = valor_str.strip()
    return valor_str

def limpar_cpf(valor):
    if valor is None:
        return None
    valor_str = str(valor)
    if valor_str.endswith('.0'):
        valor_str = valor_str[:-2]
    valor_str = re.sub(r'\D', '', valor_str)
    if len(valor_str) < 11 and len(valor_str) > 0:
        valor_str = valor_str.zfill(11)
    return valor_str if valor_str else None

print("Atualizando campos em Contatos...")

registros = session.query(Contatos).all()
atualizados = 0
nao_atualizados = 0
log_data = []
not_updated_data = []

for contato in registros:
    alterado = False
    campos_alterados = {}
    valor_cpf = getattr(contato, 'CPF/CGC', None)
    novo_cpf = limpar_cpf(valor_cpf)
    if valor_cpf != novo_cpf:
        campos_alterados['CPF/CGC'] = {"anterior": valor_cpf, "novo": novo_cpf}
        setattr(contato, 'CPF/CGC', novo_cpf)
        alterado = True
    for campo in ['Cep Residencial', 'Telefone Residencial', 'Celular', 'RG']:
        valor = getattr(contato, campo, None)
        novo_valor = limpar_numero(valor)
        if valor != novo_valor:
            campos_alterados[campo] = {"anterior": valor, "novo": novo_valor}
            setattr(contato, campo, novo_valor)
            alterado = True
    if alterado:
        atualizados += 1
        log_data.append({
            "Id do Cliente": getattr(contato, "Id do Cliente", None),
            "Campos Alterados": campos_alterados
        })
    else:
        nao_atualizados += 1
        not_updated_data.append({
            "Id do Cliente": getattr(contato, "Id do Cliente", None),
            "Motivo": "Nenhum campo precisava ser alterado"
        })

session.commit()
session.close()

create_log(log_data, path_file, "log_update_patients.xlsx")
create_log(not_updated_data, path_file, "log_not_updated_patients.xlsx")

print(f"{atualizados} registros atualizados com sucesso!")
if nao_atualizados > 0:
    print(f"{nao_atualizados} registros não precisaram de atualização (veja log_not_updated_patients.xlsx).")