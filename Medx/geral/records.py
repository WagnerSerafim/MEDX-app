from datetime import datetime
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, select
import urllib
from utils.utils import exists, create_log

log_inserted = []
log_not_inserted = []
batch_size = 500

def log_denied(patient, motivo):
    row_dict = {col.name: getattr(patient, col.name) for col in HistoricoSrc.__table__.columns}
    row_dict["Motivo"] = motivo
    row_dict["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_not_inserted.append(row_dict)

def generate_valid_data(session_src, session_dest, HistoricoSrc, HistoricoDest):
    last_id = -99999999999
    total_validated = 0

    while True:
        stmt = (
            select(HistoricoSrc)
            .where(getattr(HistoricoSrc, "Id do Histórico") > last_id)
            .order_by(getattr(HistoricoSrc, "Id do Histórico"))
            .limit(batch_size)
        )
        records = session_src.execute(stmt).scalars().all()

        if not records:
            break

        for record in records:
            try:
                id_record = getattr(record, "Id do Histórico", '')
                last_id = max(last_id, id_record)

                if exists(session_dest, id_record, "Id do Histórico", HistoricoDest):
                    log_denied(record, "Id do Histórico já existe no banco de destino")
                    continue

                record_data = {
                    'Id da Assinatura': getattr(record, "Id da Assinatura", ''),
                    'Id do Histórico': id_record,
                    'Id do Cliente': getattr(record, "Id do Cliente", ''),
                    'HIstórico': getattr(record, "Histórico", ''),
                    'Data': getattr(record, 'Data', ''),
                    'Id do Usuário': getattr(record, "Id do Usuário", ''),
                    'Classe': getattr(record, "Classe", ''),
                    'Palavraschave': getattr(record, "Palavraschave", ''),
                    'CreationDate': getattr(record, "CreationDate", '')
                }

                total_validated += 1
                print(f"Validados: {total_validated} registros...")

                yield record_data

            except Exception as e:
                log_denied(record, f"Erro inesperado: {str(e)}")

print("Por favor, informe os dados do banco de origem.\n")
sid_src = input("Informe o SoftwareID do banco de origem: ")
password_src = urllib.parse.quote_plus(input("Informe a senha do banco de origem: "))
dbase_src = input("Informe o DATABASE do banco de origem: ")

print("Agora informe os dados do banco de destino.\n")
sid_dest = input("Informe o SoftwareID do banco de destino: ")
password_dest = urllib.parse.quote_plus(input("Informe a senha do banco de destino: "))
dbase_dest = input("Informe o DATABASE do banco de destino: ")

path_file = input("Informe o caminho da pasta onde ficarão os logs: ")
if not os.path.exists(path_file):
    os.makedirs(path_file)

DATABASE_SRC = f"mssql+pyodbc://Medizin_{sid_src}:{password_src}@medxserver.database.windows.net:1433/{dbase_src}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"
DATABASE_URL = f"mssql+pyodbc://Medizin_{sid_dest}:{password_dest}@medxserver.database.windows.net:1433/{dbase_dest}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine_src = create_engine(DATABASE_SRC)
engine_dest = create_engine(DATABASE_URL)

BaseSrc = automap_base()
BaseSrc.prepare(autoload_with=engine_src)
HistoricoSrc = getattr(BaseSrc.classes, "Histórico de Clientes")

BaseDest = automap_base()
BaseDest.prepare(autoload_with=engine_dest)
HistoricoDest = getattr(BaseDest.classes, "Histórico de Clientes")

SessionSrc = sessionmaker(bind=engine_src)
SessionDest = sessionmaker(bind=engine_dest)
session_src = SessionSrc()

print("Sucesso! Inicializando migração de históricos...")

with SessionDest() as temp_session:
    try:
        buffer = []
        total = 0
        for record_data in generate_valid_data(session_src, temp_session, HistoricoSrc, HistoricoDest):
            buffer.append(record_data)

            if len(buffer) == batch_size:
                temp_session.bulk_insert_mappings(HistoricoDest, buffer)
                log_inserted.extend(buffer)
                total += len(buffer)
                print(f"Inseridos: {total} registros ({round((total / (total + len(log_not_inserted))) * 100, 2)}%)")
                buffer.clear()

        if buffer:
            temp_session.bulk_insert_mappings(HistoricoDest, buffer)
            log_inserted.extend(buffer)
            total += len(buffer)
            print(f"Inseridos: {total} registros ({round((total / (total + len(log_not_inserted))) * 100, 2)}%) - 100%")

        temp_session.commit()

    except Exception as e:
        temp_session.rollback()
        print("\nErro na inserção. Transação revertida.")
        print(f'ERROR: {str(e)}')

session_src.close()

print("Migração concluída. Criando logs...")
create_log(log_inserted, path_file, "log_inserted_records.xlsx")
create_log(log_not_inserted, path_file, "log_not_inserted_records.xlsx")
print("Logs criados com sucesso!\n")
print(f"Total de registros inseridos: {len(log_inserted)}")
print(f"Total de registros não inseridos: {len(log_not_inserted)}")
