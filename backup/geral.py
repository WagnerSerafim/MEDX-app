import os
import json
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base

sid = input("Informe o SoftwareID: ")
password = input("Informe a senha: ")
dbase = input("Informe o DATABASE: ")
backup_dir = input("Informe o diretório para salvar os backups: ").strip()

print("Iniciando a conexão com o banco de dados...")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)
Base = automap_base()
Base.prepare(autoload_with=engine)
SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

if not os.path.exists(backup_dir):
    os.makedirs(backup_dir)

inspector = inspect(engine)
tabelas = inspector.get_table_names()

registros_por_tabela = {}

for tabela in tabelas:
    print(f"Exportando tabela: {tabela}")
    try:
        result = session.execute(text(f"SELECT * FROM [{tabela}]"))
        rows = [dict(row._mapping) for row in result]
        result.close()
        json_path = os.path.join(backup_dir, f"{tabela}.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2, default=str)
        registros_por_tabela[tabela] = len(rows)
    except Exception as e:
        print(f"Erro ao exportar {tabela}: {e}")

print("\nResumo dos registros exportados:")
for tabela, qtd in registros_por_tabela.items():
    print(f"{tabela}: {qtd} registros")

print("Backup concluído!")