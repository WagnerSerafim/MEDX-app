from datetime import datetime
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, select
import urllib
from utils.utils import is_valid_date, exists, create_log, truncate_value

def log_denied(patient, motivo):
    row_dict = {col.name: getattr(patient, col.name) for col in ContatosSrc.__table__.columns}
    row_dict["Motivo"] = motivo
    row_dict["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_not_inserted.append(row_dict)

print("Por favor, informe os dados do banco de origem.\n")

sid_src = input("Informe o SoftwareID do banco de origem: ")
password_src = urllib.parse.quote_plus(input("Informe a senha do banco de origem: "))
dbase_src = input("Informe o DATABASE do banco de origem: ")

print("Agora informe os dados do banco de destino.\n")

sid_dest = input("Informe o SoftwareID do banco de destino: ")
password_dest = urllib.parse.quote_plus(input("Informe a senha do banco de destino: "))
dbase_dest = input("Informe o DATABASE do banco de destino: ")

path_file = input("Informe o caminho da pasta onde ficarão os logs: ")

print("Conectando aos Bancos de Dados...")

DATABASE_SRC = f"mssql+pyodbc://Medizin_{sid_src}:{password_src}@medxserver.database.windows.net:1433/{dbase_src}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"
DATABASE_URL = f"mssql+pyodbc://Medizin_{sid_dest}:{password_dest}@medxserver.database.windows.net:1433/{dbase_dest}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine_src = create_engine(DATABASE_SRC)
engine_dest = create_engine(DATABASE_URL)

BaseSrc = automap_base()
BaseSrc.prepare(autoload_with=engine_src)
ContatosSrc = getattr(BaseSrc.classes, "Contatos")

BaseDest = automap_base()
BaseDest.prepare(autoload_with=engine_dest)
ContatosDest = getattr(BaseDest.classes, "Contatos")

SessionSrc = sessionmaker(bind=engine_src)
SessionDest = sessionmaker(bind=engine_dest)

session_src = SessionSrc()
session_dest = SessionDest()

print("Sucesso! Inicializando migração de Contatos...")

log_inserted = []
log_not_inserted = []
valid_data = []

batch_size = 1000
last_id = -99999999999

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

print("Iniciando leitura e validação em blocos...")

while True:
    stmt = (
        select(ContatosSrc)
        .where(getattr(ContatosSrc, "Id do Cliente") > last_id)
        .order_by(getattr(ContatosSrc, "Id do Cliente"))
        .limit(batch_size)
    )
    patients = session_src.execute(stmt).scalars().all()

    if not patients:
        break

    for patient in patients:
        try:
            id_patient = getattr(patient, "Id do Cliente", None)
            last_id = max(last_id, id_patient)

            if exists(session_dest, id_patient, "Id do Cliente", ContatosDest):
                log_denied(patient, "Id do Cliente já existe no banco de destino")
                continue

            row_dict = {
            col.name: getattr(patient, col.name) for col in ContatosSrc.__table__.columns
            }

            valid_data.append(row_dict)

            row_log = row_dict
            row_log['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_inserted.append(row_log)

        except Exception as e:
            log_denied(patient, f"Erro inesperado: {str(e)}")

print("Inserindo dados no banco de destino...")

try:
    with session_dest.begin():
        for i in range(0, len(valid_data), 100):
            batch = valid_data[i:i+100]
            session_dest.bulk_insert_mappings(ContatosDest, batch)
    print(f"{len(valid_data)} contatos inseridos com sucesso!")
except Exception as e:
    print("Erro na inserção. Transação revertida.")
    print(str(e))

session_src.close()
session_dest.close()

print("Migração concluída. Criando logs...")

create_log(log_inserted, log_folder, "log_inserted_patients.xlsx")
create_log(log_not_inserted, log_folder, "log_not_inserted_patients.xlsx")

print("Logs criados com sucesso!\n")

print(f"Total de contatos inseridos: {len(log_inserted)}")
print(f"Total de contatos não inseridos: {len(log_not_inserted)}")

