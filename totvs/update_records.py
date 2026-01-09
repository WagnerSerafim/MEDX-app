import html
import urllib
from sqlalchemy import create_engine, MetaData, Table, UnicodeText
from sqlalchemy.orm import sessionmaker, declarative_base

# 1. Configurações de Conexão (Seguindo seu padrão)
sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)
metadata = MetaData()
# Mapeando a tabela e o schema dinâmico
historico_tbl = Table("Histórico de Clientes", metadata, schema=f"schema_{sid}", autoload_with=engine)

Base = declarative_base()
class Historico(Base):
    __table__ = historico_tbl

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

print("Buscando registros que contêm entidades HTML (&...;)...")

# 2. Busca apenas os registros que possuem o caractere '&' no campo Histórico
# Isso evita processar a tabela inteira desnecessariamente
registros = session.query(Historico).filter(historico_tbl.c["Histórico"].like('%&%')).all()

print(f"Encontrados {len(registros)} registros para analisar.")

updated_count = 0

for reg in registros:
    texto_original = getattr(reg, "Histórico")
    
    if texto_original:
        # A mágica acontece aqui: html.unescape converte &Agrave; -> À, &nbsp; -> espaço, etc.
        texto_corrigido = html.unescape(texto_original)
        
        # Só atualiza se houve mudança real
        if texto_corrigido != texto_original:
            setattr(reg, "Histórico", texto_corrigido)
            updated_count += 1

    # Commit em lotes para performance e segurança
    if updated_count % 500 == 0 and updated_count > 0:
        session.commit()
        print(f"Progresso: {updated_count} registros corrigidos...")

session.commit()
print(f"Sucesso! Total de registros corrigidos: {updated_count}")
session.close()