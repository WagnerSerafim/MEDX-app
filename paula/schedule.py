from datetime import datetime
from pathlib import Path
import csv
import json
import re
import sys
import time
import urllib.parse

import pandas as pd
from sqlalchemy import MetaData, Table, UnicodeText, bindparam, create_engine, select
from sqlalchemy.orm import sessionmaker


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.utils import clean_caracters  # noqa: E402


SOURCE_FILE_NAME = "medx_agenda.csv"
TARGET_TABLE = "Agenda"
BATCH_SIZE = 200
QUERY_CHUNK_SIZE = 900
THROTTLE_SECONDS = 0.5
EXECUTION_CONFIRMATION = "MIGRAR"
DEFAULT_USER_ID = 1
DEFAULT_STATUS = 1

REQUIRED_COLUMNS = {
    "IDENTIFICADOR",
    "IDENTIFICADOR_PACIENTE",
    "strDesc",
    "DATA_START",
    "DATA_END",
}


def now_text():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def clean_value(value):
    if value is None:
        return None
    if pd.isna(value):
        return None

    text = clean_caracters(str(value)).strip()
    if text.lower() in {"", "nan", "none", "null", "nul"}:
        return None

    return text or None


def clean_spaces(value):
    text = clean_value(value)
    if text is None:
        return None

    return re.sub(r"\s+", " ", text).strip() or None


def normalize_datetime(value):
    text = clean_value(value)
    if text:
        for date_format in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                parsed = datetime.strptime(text, date_format)
                if 1900 <= parsed.year <= 2100:
                    if date_format == "%Y-%m-%d %H:%M":
                        return f"{text}:00"
                    return text
            except ValueError:
                continue

    return None


def validate_columns(df):
    missing_columns = sorted(REQUIRED_COLUMNS - set(df.columns))
    if missing_columns:
        raise ValueError(f"Colunas obrigatórias ausentes: {missing_columns}")


def load_source_csv(path_file):
    csv.field_size_limit(20_000_000)
    source_path = Path(path_file) / SOURCE_FILE_NAME
    if not source_path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {source_path}")

    df = pd.read_csv(
        source_path,
        sep=";",
        engine="python",
        encoding="utf-8",
        dtype=str,
        keep_default_na=False,
    )
    validate_columns(df)
    return df


def connect_database():
    sid = input("Informe o SoftwareID: ").strip()
    password = urllib.parse.quote_plus(input("Informe a senha: "))
    dbase = input("Informe o DATABASE: ").strip()

    database_url = (
        f"mssql+pyodbc://Medizin_{sid}:{password}"
        f"@medxserver.database.windows.net:1433/{dbase}"
        "?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"
    )

    engine = create_engine(database_url, fast_executemany=False, pool_pre_ping=True)
    metadata = MetaData()
    agenda_tbl = Table(TARGET_TABLE, metadata, schema=f"schema_{sid}", autoload_with=engine)
    contatos_tbl = Table("Contatos", metadata, schema=f"schema_{sid}", autoload_with=engine)

    return engine, agenda_tbl, contatos_tbl


def iter_chunks(items, chunk_size):
    for start in range(0, len(items), chunk_size):
        yield items[start : start + chunk_size]


def fetch_existing_schedule_ids(session, agenda_tbl, schedule_ids):
    existing_ids = set()
    id_column = agenda_tbl.c["Id do Agendamento"]

    for id_chunk in iter_chunks(schedule_ids, QUERY_CHUNK_SIZE):
        result = session.execute(select(id_column).where(id_column.in_(id_chunk)))
        existing_ids.update(str(row[0]) for row in result if row[0] is not None)

    return existing_ids


def fetch_patient_names(session, contatos_tbl, patient_ids):
    patient_names = {}
    id_column = contatos_tbl.c["Id do Cliente"]
    name_column = contatos_tbl.c["Nome"]

    for id_chunk in iter_chunks(patient_ids, QUERY_CHUNK_SIZE):
        result = session.execute(select(id_column, name_column).where(id_column.in_(id_chunk)))
        for patient_id, patient_name in result:
            if patient_id is not None:
                patient_names[str(patient_id)] = clean_spaces(patient_name)

    return patient_names


def row_to_log(row, payload, reason=None):
    log_row = row.to_dict()
    log_row.update(payload)
    if reason:
        log_row["Motivo"] = reason
    log_row["Timestamp"] = now_text()
    return log_row


def write_json_log(log_data, log_folder, log_name):
    log_path = Path(log_folder) / log_name
    with open(log_path, "w", encoding="utf-8") as log_file:
        json.dump(log_data, log_file, ensure_ascii=False, default=str, indent=2)


def build_description(patient_name, source_description):
    description = clean_spaces(source_description)
    if description:
        return f"{patient_name} - {description}"

    return patient_name


def make_payload(row, patient_names):
    schedule_id = clean_spaces(row.get("IDENTIFICADOR"))
    patient_id = clean_spaces(row.get("IDENTIFICADOR_PACIENTE"))
    patient_name = patient_names.get(patient_id)

    payload = {
        "Id do Agendamento": schedule_id,
        "Vinculado a": patient_id,
        "Início": normalize_datetime(row.get("DATA_START")),
        "Final": normalize_datetime(row.get("DATA_END")),
        "Id do Usuário": DEFAULT_USER_ID,
        "Status": DEFAULT_STATUS,
    }

    if patient_name:
        payload["Descrição"] = build_description(patient_name, row.get("strDesc"))

    return {key: value for key, value in payload.items() if value is not None}


def prepare_rows(df, patient_names, existing_schedule_ids):
    payloads = []
    preview_log = []
    not_inserted_log = []
    seen_schedule_ids = set()
    invalid_dates_count = 0

    for _, row in df.iterrows():
        payload = make_payload(row, patient_names)
        schedule_id = clean_spaces(row.get("IDENTIFICADOR"))
        patient_id = payload.get("Vinculado a")

        if not payload.get("Início") or not payload.get("Final"):
            invalid_dates_count += 1

        if not schedule_id:
            not_inserted_log.append(row_to_log(row, payload, "Id do Agendamento vazio"))
            continue

        if not patient_id:
            not_inserted_log.append(row_to_log(row, payload, "Id do Cliente vazio"))
            continue

        if patient_id not in patient_names:
            not_inserted_log.append(row_to_log(row, payload, "Paciente não encontrado no destino"))
            continue

        if not patient_names.get(patient_id):
            not_inserted_log.append(row_to_log(row, payload, "Nome do paciente vazio no destino"))
            continue

        if not payload.get("Início"):
            not_inserted_log.append(row_to_log(row, payload, "Início vazio ou inválido"))
            continue

        if not payload.get("Final"):
            not_inserted_log.append(row_to_log(row, payload, "Final vazio ou inválido"))
            continue

        if payload["Final"] < payload["Início"]:
            not_inserted_log.append(row_to_log(row, payload, "Final menor que Início"))
            continue

        if not payload.get("Descrição"):
            not_inserted_log.append(row_to_log(row, payload, "Descrição vazia"))
            continue

        if schedule_id in seen_schedule_ids:
            not_inserted_log.append(row_to_log(row, payload, "Agendamento duplicado no CSV"))
            continue

        seen_schedule_ids.add(schedule_id)

        if schedule_id in existing_schedule_ids:
            not_inserted_log.append(row_to_log(row, payload, "Agendamento já existe no banco"))
            continue

        payloads.append(payload)
        preview_log.append(row_to_log(row, payload))

    return payloads, preview_log, not_inserted_log, invalid_dates_count


def build_insert_statement(agenda_tbl):
    return agenda_tbl.insert().values({
        "Id do Agendamento": bindparam("Id do Agendamento"),
        "Vinculado a": bindparam("Vinculado a"),
        "Início": bindparam("Início"),
        "Final": bindparam("Final"),
        "Id do Usuário": bindparam("Id do Usuário"),
        "Status": bindparam("Status"),
        "Descrição": bindparam("Descrição", type_=UnicodeText()),
    })


def insert_payloads(session, agenda_tbl, payloads):
    inserted_log = []
    not_inserted_log = []
    insert_statement = build_insert_statement(agenda_tbl)

    for batch_number, batch in enumerate(iter_chunks(payloads, BATCH_SIZE), start=1):
        try:
            session.execute(insert_statement, batch)
            session.commit()
            inserted_log.extend(batch)
            print(f"Lote {batch_number}: {len(batch)} agendamentos inseridos.")
        except Exception as batch_error:
            session.rollback()
            print(f"Lote {batch_number}: erro, tentando isolar linhas individualmente.")

            for payload in batch:
                try:
                    session.execute(insert_statement, [payload])
                    session.commit()
                    inserted_log.append(payload)
                except Exception as row_error:
                    session.rollback()
                    failed_row = dict(payload)
                    failed_row["Motivo"] = (
                        f"Erro no lote: {type(batch_error).__name__}: {batch_error}; "
                        f"Erro na linha: {type(row_error).__name__}: {row_error}"
                    )
                    failed_row["Timestamp"] = now_text()
                    not_inserted_log.append(failed_row)

        if THROTTLE_SECONDS:
            time.sleep(THROTTLE_SECONDS)

    return inserted_log, not_inserted_log


def main():
    print("=== Migração segura de agenda - Dra. Paula ===")
    path_file = input("Informe o caminho da pasta que contém o medx_agenda.csv: ").strip()
    log_folder = Path(path_file)
    df = load_source_csv(log_folder)

    print(f"CSV carregado: {len(df)} agendamentos.")
    print("Conectando no banco para refletir tabelas e buscar pacientes em massa...")
    engine, agenda_tbl, contatos_tbl = connect_database()
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()

    try:
        source_patient_ids = sorted({
            clean_spaces(value)
            for value in df["IDENTIFICADOR_PACIENTE"].tolist()
            if clean_spaces(value)
        })
        source_schedule_ids = sorted({
            clean_spaces(value)
            for value in df["IDENTIFICADOR"].tolist()
            if clean_spaces(value)
        })

        patient_names = fetch_patient_names(session, contatos_tbl, source_patient_ids)
        existing_schedule_ids = fetch_existing_schedule_ids(session, agenda_tbl, source_schedule_ids)
        payloads, preview_log, not_inserted_log, invalid_dates_count = prepare_rows(
            df,
            patient_names,
            existing_schedule_ids,
        )

        print("\n=== Pré-validação ===")
        print(f"Total lido: {len(df)}")
        print(f"Pacientes distintos no CSV: {len(source_patient_ids)}")
        print(f"Pacientes encontrados no destino: {len(patient_names)}")
        print(f"Agendamentos já existentes no banco: {len(existing_schedule_ids)}")
        print(f"Datas vazias/inválidas: {invalid_dates_count}")
        print(f"Prontos para inserção: {len(payloads)}")
        print(f"Não inseridos previstos: {len(not_inserted_log)}")

        write_json_log(preview_log, log_folder, "log_preview_schedule.json")
        if not_inserted_log:
            write_json_log(not_inserted_log, log_folder, "log_not_inserted_schedule.json")

        confirmation = input(
            f"\nDry-run concluído. Digite {EXECUTION_CONFIRMATION} para inserir no banco: "
        ).strip()

        if confirmation != EXECUTION_CONFIRMATION:
            print("Execução encerrada em modo dry-run. Nenhum dado foi inserido.")
            return

        print("\nIniciando inserção em lotes pequenos...")
        inserted_log, insert_errors_log = insert_payloads(session, agenda_tbl, payloads)
        not_inserted_log.extend(insert_errors_log)

        write_json_log(inserted_log, log_folder, "log_inserted_schedule.json")
        write_json_log(not_inserted_log, log_folder, "log_not_inserted_schedule.json")

        print("\n=== Resumo final ===")
        print(f"Inseridos: {len(inserted_log)}")
        print(f"Não inseridos: {len(not_inserted_log)}")
        print(f"Logs gravados em: {log_folder}")
    finally:
        session.close()
        engine.dispose()


if __name__ == "__main__":
    main()
