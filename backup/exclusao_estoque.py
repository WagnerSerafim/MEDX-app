import os
import json
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
import urllib

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
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

tabelas = [
    "Estoque",
    "Estoque Movimentação",
    "Estoque Movimentação Itens"
]

registros_por_tabela = {}
backup_ok = True

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
        # Confirma se o backup foi gerado corretamente
        if not os.path.exists(json_path) or os.path.getsize(json_path) == 0:
            print(f"Falha ao gerar backup da tabela {tabela}. Operação de exclusão abortada.")
            backup_ok = False
            break
    except Exception as e:
        print(f"Erro ao exportar {tabela}: {e}")
        backup_ok = False
        break

print("\nResumo dos registros exportados:")
for tabela, qtd in registros_por_tabela.items():
    print(f"{tabela}: {qtd} registros")

if backup_ok:
    print("\nBackup gerado com sucesso! Iniciando exclusão dos dados das tabelas...")
    for tabela in tabelas:
        try:
            session.execute(text(f"DELETE FROM [{tabela}]"))
            session.commit()
            print(f"Todos os registros da tabela {tabela} foram excluídos.")
        except Exception as e:
            print(f"Erro ao excluir registros da tabela {tabela}: {e}")
else:
    print("\nBackup não gerado corretamente. Nenhum dado foi excluído.")

session.close()