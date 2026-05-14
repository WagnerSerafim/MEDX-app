import glob
import os
import urllib
from datetime import datetime

import pandas as pd
from sqlalchemy import MetaData, Table, create_engine, insert, select
from sqlalchemy.pool import NullPool

from utils.utils import create_log, verify_nan


READ_CHUNK_SIZE = 2000
DB_INSERT_BATCH_SIZE = 500
EXISTING_ID_FETCH_CHUNK = 1000


sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")

print("Conectando no Banco de Dados...")
DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(
    DATABASE_URL,
    poolclass=NullPool,
    fast_executemany=True,
    use_insertmanyvalues=False,
    future=True,
)

metadata = MetaData()
agenda_tbl = Table("Agenda", metadata, autoload_with=engine)

print("Sucesso! Inicializando migração de Agendamentos...")

extension_file = glob.glob(f'{path_file}/Agenda_Marcos_Sanches*.csv')
if not extension_file:
    raise FileNotFoundError("Nenhum arquivo encontrado com o padrão 'schedules*.csv'.")

csv_file = extension_file[0]

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont = 0
not_inserted_data = []
not_inserted_cont = 0

procedures = {
    162726: "Primeira Consulta",
    162727: "Consulta",
    162728: "Retorno",
    162729: "Teleconsulta",
    168195: "Aplicação EV",
    168200: "Aplicação IM",
    172232: "Implante",
    181567: "consulta tricologia",
    193585: "ESTÉTICA"
    }

def chunked(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def parse_datetime_str(value):
    value = verify_nan(value)
    if value is None:
        return None
    try:
        dt = datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def parse_int(value):
    value = verify_nan(value)
    if value is None:
        return None

    raw = str(value).strip()
    if raw.endswith(".0"):
        raw = raw[:-2]

    if not raw:
        return None

    try:
        return int(raw)
    except ValueError:
        return None


def fetch_existing_schedule_ids(schedule_ids):
    existing = set()
    if not schedule_ids:
        return existing

    id_col = agenda_tbl.c["Id do Agendamento"]
    for id_batch in chunked(schedule_ids, EXISTING_ID_FETCH_CHUNK):
        with engine.connect() as conn:
            rows = conn.execute(select(id_col).where(id_col.in_(id_batch))).fetchall()
        existing.update(row[0] for row in rows if row[0] is not None)

    return existing


def resolve_description(patient_name, procedure_id, extra_description):
    patient_name = verify_nan(patient_name)
    if patient_name is None:
        base_description = "Sem informações"
    else:
        procedure_key = parse_int(procedure_id)
        procedure_name = procedures.get(procedure_key)
        if procedure_name:
            base_description = f"{patient_name} - {procedure_name}"
        else:
            base_description = str(patient_name)

    extra_description = verify_nan(extra_description)
    if extra_description is not None:
        return f"{base_description} - {str(extra_description).strip()}"

    return base_description


seen_ids_file = set()
insert_stmt = insert(agenda_tbl)

for df_chunk in pd.read_csv(
    csv_file,
    sep=',',
    encoding='utf-8',
    quotechar='"',
    chunksize=READ_CHUNK_SIZE,
):
    candidates = []

    for _, row in df_chunk.iterrows():
        row_dict = row.to_dict()

        id_scheduling = parse_int(row.get("id"))
        if id_scheduling is None:
            not_inserted_cont += 1
            row_dict['Motivo'] = 'Id do Agendamento inválido ou vazio'
            not_inserted_data.append(row_dict)
            continue

        if id_scheduling in seen_ids_file:
            not_inserted_cont += 1
            row_dict['Motivo'] = 'Id do Agendamento duplicado no arquivo'
            not_inserted_data.append(row_dict)
            continue

        seen_ids_file.add(id_scheduling)

        start_time = parse_datetime_str(row.get("start"))
        if start_time is None:
            not_inserted_cont += 1
            row_dict['Motivo'] = 'Data de Início vazia ou inválida'
            not_inserted_data.append(row_dict)
            continue

        end_time = parse_datetime_str(row.get("end"))
        if end_time is None:
            not_inserted_cont += 1
            row_dict['Motivo'] = 'Data de Fim vazia ou inválida'
            not_inserted_data.append(row_dict)
            continue

        id_patient = parse_int(row.get("patient_id"))
        if id_patient is None:
            not_inserted_cont += 1
            row_dict['Motivo'] = 'Id do paciente vazio'
            not_inserted_data.append(row_dict)
            continue

        description = resolve_description(
            row.get('title'),
            row.get('procedure_id'),
            row.get('description'),
        )
        user = 1

        payload = {
            "Id do Agendamento": id_scheduling,
            "Vinculado a": id_patient,
            "Id do Usuário": user,
            "Início": start_time,
            "Final": end_time,
            "Descrição": description,
            "Status": 1,
        }
        candidates.append((row_dict, payload))

    if not candidates:
        continue

    candidate_ids = [payload["Id do Agendamento"] for _, payload in candidates]
    existing_ids = fetch_existing_schedule_ids(candidate_ids)

    rows_to_insert = []
    for row_dict, payload in candidates:
        if payload["Id do Agendamento"] in existing_ids:
            not_inserted_cont += 1
            row_dict['Motivo'] = 'Id do Agendamento já existe'
            not_inserted_data.append(row_dict)
            continue
        rows_to_insert.append((row_dict, payload))

    for db_batch in chunked(rows_to_insert, DB_INSERT_BATCH_SIZE):
        batch_payload = [payload for _, payload in db_batch]
        try:
            with engine.begin() as conn:
                conn.execute(insert_stmt, batch_payload)

            inserted_cont += len(db_batch)
            for _, payload in db_batch:
                log_data.append(payload.copy())

        except Exception as batch_error:
            for row_dict, payload in db_batch:
                try:
                    with engine.begin() as conn:
                        conn.execute(insert_stmt, [payload])

                    inserted_cont += 1
                    log_data.append(payload.copy())
                except Exception as item_error:
                    not_inserted_cont += 1
                    row_dict['Motivo'] = f"Erro ao inserir: {item_error} | erro_lote: {batch_error}"
                    not_inserted_data.append(row_dict)

print(f"{inserted_cont} novos agendamentos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} agendamentos não foram inseridos, verifique o log para mais detalhes.")

engine.dispose()

create_log(log_data, log_folder, "log_inserted_schedules.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_schedules.xlsx")
