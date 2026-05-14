import os
import asyncio
from sqlalchemy import MetaData, Table, insert, select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from datetime import datetime
import pandas as pd
import urllib
from utils.utils import create_log, is_valid_date, verify_nan
import glob

def get_record(row):
    tipo = verify_nan(row.get("TIPO"))
    if tipo is not None:
        tipo = str(tipo).strip()
        if not tipo:
            tipo = None

    texts = []
    for i in range(1, 192):
        value = verify_nan(row.get(f"TEXTO{i}"))
        if value is not None:
            value = str(value).strip()
            if value:
                texts.append(value)

    if not texts:
        return None

    parts = []
    if tipo is not None:
        parts.append(tipo)
    parts.extend(texts)

    record = "<br><br>".join(parts)
    return record

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")

DATABASE_URL = f"mssql+aioodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"
BATCH_SIZE = 1000

engine = create_async_engine(DATABASE_URL)

metadata = MetaData()


def _load_historico_table(sync_conn):
    return Table("Histórico de Clientes", metadata, schema=f"schema_{sid}", autoload_with=sync_conn)

SessionLocal = async_sessionmaker(bind=engine, expire_on_commit=False)

async def main():
    print("Sucesso! Inicializando migração de Históricos...")

    async with engine.begin() as conn:
        historico_tbl = await conn.run_sync(_load_historico_table)

    cadastro_file = glob.glob(f'{path_file}/ANAMNESE*.xlsx')
    df = pd.read_excel(cadastro_file[0], engine='openpyxl', dtype=str)

    log_folder = path_file
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)

    log_data = []
    inserted_cont = 0
    not_inserted_data = []
    not_inserted_cont = 0
    id_cont = 0

    pending_rows = []

    async with SessionLocal() as session:
        query_existing = select(historico_tbl.c["Id do Histórico"])
        existing_ids = {row[0] for row in (await session.execute(query_existing)).all()}

        for idx, row in df.iterrows():
            if idx % 1000 == 0 or idx == len(df):
                print(f"Processados: {idx} | Inseridos: {inserted_cont} | Não inseridos: {not_inserted_cont} | Concluído: {round((idx / len(df)) * 100, 2)}%")

            id_cont += 1

            record_id = id_cont
            if record_id is None:
                not_inserted_cont += 1
                row_dict = row.to_dict()
                row_dict['Motivo'] = 'Id do Histórico é vazio ou nulo'
                not_inserted_data.append(row_dict)
                continue

            if record_id in existing_ids:
                not_inserted_cont += 1
                row_dict = row.to_dict()
                row_dict['Motivo'] = 'Histórico já existe no banco de dados'
                not_inserted_data.append(row_dict)
                continue

            id_patient = verify_nan(row['PRONTUARIO'])
            if id_patient is None:
                not_inserted_cont += 1
                row_dict = row.to_dict()
                row_dict['Motivo'] = 'Id do Paciente é vazio ou nulo'
                not_inserted_data.append(row_dict)
                continue

            record = get_record(row)
            if record is None:
                not_inserted_cont += 1
                row_dict = row.to_dict()
                row_dict['Motivo'] = 'Histórico vazio'
                not_inserted_data.append(row_dict)
                continue

            try:
                date_obj = verify_nan(row["ATENDIMENTO"])
                if date_obj is None:
                    date = '1900-01-01'
                else:
                    date = datetime.strptime(date_obj, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
                    if not date or not is_valid_date(date, '%Y-%m-%d %H:%M:%S'):
                        date = '1900-01-01'
            except ValueError:
                date = '1900-01-01'

            new_row = {
                "Id do Histórico": record_id,
                "Id do Cliente": id_patient,
                "Data": date,
                "Histórico": record,
                "Id do Usuário": 0,
            }

            log_data.append(new_row)
            pending_rows.append(new_row)
            existing_ids.add(record_id)
            inserted_cont += 1

            if len(pending_rows) >= BATCH_SIZE:
                await session.execute(insert(historico_tbl), pending_rows)
                await session.commit()
                pending_rows.clear()

        if pending_rows:
            await session.execute(insert(historico_tbl), pending_rows)
            await session.commit()

    print(f"{inserted_cont} novos históricos foram inseridos com sucesso!")
    if not_inserted_cont > 0:
        print(f"{not_inserted_cont} históricos não foram inseridos, verifique o log para mais detalhes.")

    create_log(log_data, log_folder, "log_inserted_records_ANAMNESE.xlsx")
    create_log(not_inserted_data, log_folder, "log_not_inserted_records_ANAMNESE.xlsx")


if __name__ == "__main__":
    asyncio.run(main())