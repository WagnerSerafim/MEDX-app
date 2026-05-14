from datetime import datetime
from html import escape
from pathlib import Path
import csv
import re
import sys
import time
import urllib.parse

import pandas as pd
from sqlalchemy import MetaData, Table, UnicodeText, bindparam, create_engine, select
from sqlalchemy.orm import declarative_base, sessionmaker


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.utils import clean_caracters, create_log, is_valid_date  # noqa: E402


SOURCE_FILE_NAME = "medx_laudointerno.csv"
TARGET_TABLE = "Histórico de Clientes"
BATCH_SIZE = 200
QUERY_CHUNK_SIZE = 900
THROTTLE_SECONDS = 0.5
DEFAULT_DATETIME = "1900-01-01 00:00:00"
EXECUTION_CONFIRMATION = "MIGRAR"

REQUIRED_COLUMNS = {
    "identificador",
    "identificador_paciente",
    "strFuncionario",
    "data_referencia",
    "data_exame",
    "strNome",
    "strDesc",
    "dataCriacao",
    "dateLog",
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
    if text and is_valid_date(text, "%Y-%m-%d %H:%M:%S"):
        return text
    if text and is_valid_date(text, "%Y-%m-%d"):
        return f"{text} 00:00:00"

    return DEFAULT_DATETIME


def paragraphize_text(value):
    text = clean_value(value)
    if text is None:
        return ""

    text = text.replace("\r\n", "\n").replace("\r", "\n")
    blocks = [block.strip() for block in re.split(r"\n\s*\n+", text) if block.strip()]
    paragraphs = []

    for block in blocks:
        lines = [escape(line.strip()) for line in block.split("\n") if line.strip()]
        if lines:
            paragraphs.append(f"<p>{'<br>'.join(lines)}</p>")

    return "\n".join(paragraphs)


def build_history_html(row):
    title = clean_spaces(row.get("strNome"))
    professional = clean_spaces(row.get("strFuncionario"))
    exam_date = clean_spaces(row.get("data_exame"))
    body = paragraphize_text(row.get("strDesc"))

    parts = []
    if title:
        parts.append(f"<h3>{escape(title)}</h3>")

    parts.append("<p><strong>Laudo interno</strong></p>")

    meta = []
    if professional:
        meta.append(f"<strong>Profissional:</strong> {escape(professional)}")
    if exam_date:
        meta.append(f"<strong>Data do exame:</strong> {escape(exam_date)}")

    if meta:
        parts.append(f"<p>{'<br>'.join(meta)}</p>")

    if body:
        parts.append("<hr>")
        parts.append(body)

    return "\n".join(parts)


def validate_columns(df):
    missing_columns = sorted(REQUIRED_COLUMNS - set(df.columns))
    if missing_columns:
        raise ValueError(f"Colunas obrigatórias ausentes: {missing_columns}")


def load_source_csv(path_file):
    csv.field_size_limit(10_000_000)
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
    historico_tbl = Table(TARGET_TABLE, metadata, schema=f"schema_{sid}", autoload_with=engine)
    contatos_tbl = Table("Contatos", metadata, schema=f"schema_{sid}", autoload_with=engine)

    return engine, historico_tbl, contatos_tbl


def iter_chunks(items, chunk_size):
    for start in range(0, len(items), chunk_size):
        yield items[start : start + chunk_size]


def fetch_existing_patient_ids(session, contatos_tbl, patient_ids):
    existing_ids = set()
    id_column = contatos_tbl.c["Id do Cliente"]

    for id_chunk in iter_chunks(patient_ids, QUERY_CHUNK_SIZE):
        result = session.execute(select(id_column).where(id_column.in_(id_chunk)))
        existing_ids.update(str(row[0]) for row in result if row[0] is not None)

    return existing_ids


def fetch_existing_record_ids(session, historico_tbl, record_ids):
    existing_ids = set()
    id_column = historico_tbl.c["Id do Histórico"]

    for id_chunk in iter_chunks(record_ids, QUERY_CHUNK_SIZE):
        result = session.execute(select(id_column).where(id_column.in_(id_chunk)))
        existing_ids.update(str(row[0]) for row in result if row[0] is not None)

    return existing_ids


def row_to_log(row, payload, reason=None):
    log_row = row.to_dict()
    log_row.update(payload)
    if reason:
        log_row["Motivo"] = reason
    log_row["Timestamp"] = now_text()
    return log_row


def make_payload(row):
    source_id = clean_spaces(row.get("identificador"))
    patient_id = clean_spaces(row.get("identificador_paciente"))

    payload = {
        "Id do Histórico": source_id,
        "Id do Cliente": patient_id,
        "Id do Usuário": 0,
        "Data": normalize_datetime(row.get("dataCriacao")),
        "Histórico": build_history_html(row),
    }

    return {key: value for key, value in payload.items() if value is not None}


def prepare_rows(df, existing_patient_ids, existing_record_ids):
    payloads = []
    preview_log = []
    not_inserted_log = []
    seen_record_ids = set()
    invalid_dates_count = 0

    for _, row in df.iterrows():
        payload = make_payload(row)
        source_id = clean_spaces(row.get("identificador"))
        patient_id = payload.get("Id do Cliente")
        description = clean_value(row.get("strDesc"))

        if payload.get("Data") == DEFAULT_DATETIME:
            invalid_dates_count += 1

        if not source_id:
            not_inserted_log.append(row_to_log(row, payload, "Identificador do laudo vazio"))
            continue

        if not patient_id:
            not_inserted_log.append(row_to_log(row, payload, "Id do Cliente vazio"))
            continue

        if patient_id not in existing_patient_ids:
            not_inserted_log.append(row_to_log(row, payload, "Paciente não encontrado no destino"))
            continue

        if not description:
            not_inserted_log.append(row_to_log(row, payload, "Laudo interno sem descrição"))
            continue

        if source_id in seen_record_ids:
            not_inserted_log.append(row_to_log(row, payload, "Laudo duplicado no CSV"))
            continue

        seen_record_ids.add(source_id)

        if source_id in existing_record_ids:
            not_inserted_log.append(row_to_log(row, payload, "Laudo já existe no banco"))
            continue

        payloads.append(payload)
        preview_log.append(row_to_log(row, payload))

    return payloads, preview_log, not_inserted_log, invalid_dates_count


def build_insert_statement(historico_tbl):
    return historico_tbl.insert().values({
        "Id do Histórico": bindparam("Id do Histórico"),
        "Id do Cliente": bindparam("Id do Cliente"),
        "Id do Usuário": bindparam("Id do Usuário"),
        "Data": bindparam("Data"),
        "Histórico": bindparam("Histórico", type_=UnicodeText()),
    })


def insert_payloads(session, historico_tbl, payloads):
    inserted_log = []
    not_inserted_log = []
    insert_statement = build_insert_statement(historico_tbl)

    for batch_number, batch in enumerate(iter_chunks(payloads, BATCH_SIZE), start=1):
        try:
            session.execute(insert_statement, batch)
            session.commit()
            inserted_log.extend(batch)
            print(f"Lote {batch_number}: {len(batch)} laudos internos inseridos.")
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
    print("=== Migração segura de laudos internos - Dra. Paula ===")
    path_file = input("Informe o caminho da pasta que contém o medx_laudointerno.csv: ").strip()
    log_folder = Path(path_file)
    df = load_source_csv(log_folder)

    print(f"CSV carregado: {len(df)} laudos internos.")
    print("Conectando no banco para refletir tabelas e checar vínculos em massa...")
    engine, historico_tbl, contatos_tbl = connect_database()

    Base = declarative_base()

    class Historico(Base):
        __table__ = historico_tbl

    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()

    try:
        source_patient_ids = sorted({
            clean_spaces(value)
            for value in df["identificador_paciente"].tolist()
            if clean_spaces(value)
        })
        source_record_ids = sorted({
            clean_spaces(value)
            for value in df["identificador"].tolist()
            if clean_spaces(value)
        })

        existing_patient_ids = fetch_existing_patient_ids(session, contatos_tbl, source_patient_ids)
        existing_record_ids = fetch_existing_record_ids(session, historico_tbl, source_record_ids)
        payloads, preview_log, not_inserted_log, invalid_dates_count = prepare_rows(
            df,
            existing_patient_ids,
            existing_record_ids,
        )

        print("\n=== Pré-validação ===")
        print(f"Total lido: {len(df)}")
        print(f"Pacientes distintos no CSV: {len(source_patient_ids)}")
        print(f"Pacientes encontrados no destino: {len(existing_patient_ids)}")
        print(f"Laudos já existentes no banco: {len(existing_record_ids)}")
        print(f"Datas vazias/inválidas ajustadas para {DEFAULT_DATETIME}: {invalid_dates_count}")
        print(f"Prontos para inserção: {len(payloads)}")
        print(f"Não inseridos previstos: {len(not_inserted_log)}")

        create_log(preview_log, log_folder, "log_preview_records_laudointerno.xlsx")
        if not_inserted_log:
            create_log(not_inserted_log, log_folder, "log_not_inserted_records_laudointerno.xlsx")

        confirmation = input(
            f"\nDry-run concluído. Digite {EXECUTION_CONFIRMATION} para inserir no banco: "
        ).strip()

        if confirmation != EXECUTION_CONFIRMATION:
            print("Execução encerrada em modo dry-run. Nenhum dado foi inserido.")
            return

        print("\nIniciando inserção em lotes pequenos...")
        inserted_log, insert_errors_log = insert_payloads(session, historico_tbl, payloads)
        not_inserted_log.extend(insert_errors_log)

        create_log(inserted_log, log_folder, "log_inserted_records_laudointerno.xlsx")
        create_log(not_inserted_log, log_folder, "log_not_inserted_records_laudointerno.xlsx")

        print("\n=== Resumo final ===")
        print(f"Inseridos: {len(inserted_log)}")
        print(f"Não inseridos: {len(not_inserted_log)}")
        print(f"Logs gravados em: {log_folder}")
    finally:
        session.close()
        engine.dispose()


if __name__ == "__main__":
    main()
