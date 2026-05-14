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


SOURCE_FILE_NAME = "medx_pac_docs.csv"
TARGET_TABLE = "Autodocs"
PARENT_FOLDER_NAME = "Documentos Migração"
BATCH_SIZE = 100
THROTTLE_SECONDS = 0.5
EXECUTION_CONFIRMATION = "MIGRAR"

REQUIRED_COLUMNS = {
    "identificador",
    "strNome",
    "strDesc",
    "boolAtivo",
    "dataTSAlteracaoLog",
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
    autodocs_tbl = Table(TARGET_TABLE, metadata, schema=f"schema_{sid}", autoload_with=engine)

    return engine, autodocs_tbl


def iter_chunks(items, chunk_size):
    for start in range(0, len(items), chunk_size):
        yield items[start : start + chunk_size]


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


def find_parent_folder_id(session, autodocs_tbl):
    result = session.execute(
        select(autodocs_tbl.c["Id do Texto"])
        .where(autodocs_tbl.c["Pai"] == 0)
        .where(autodocs_tbl.c["Biblioteca"] == PARENT_FOLDER_NAME)
        .order_by(autodocs_tbl.c["Id do Texto"].desc())
    )
    row = result.fetchone()
    if row and row[0] is not None:
        return row[0]

    return None


def create_parent_folder(session, autodocs_tbl):
    statement = autodocs_tbl.insert().values({
        "Pai": bindparam("Pai"),
        "Biblioteca": bindparam("Biblioteca"),
    })
    session.execute(statement, [{"Pai": 0, "Biblioteca": PARENT_FOLDER_NAME}])
    session.commit()

    parent_id = find_parent_folder_id(session, autodocs_tbl)
    if parent_id is None:
        raise RuntimeError(f"Não foi possível obter o Id da pasta '{PARENT_FOLDER_NAME}'.")

    return parent_id


def fetch_existing_libraries_in_parent(session, autodocs_tbl, parent_id):
    if parent_id is None:
        return set()

    result = session.execute(
        select(autodocs_tbl.c["Biblioteca"])
        .where(autodocs_tbl.c["Pai"] == parent_id)
    )
    return {clean_spaces(row[0]) for row in result if clean_spaces(row[0])}


def make_payload(row, parent_id):
    payload = {
        "Pai": parent_id,
        "Biblioteca": clean_spaces(row.get("strNome")),
        "Texto": clean_value(row.get("strDesc")),
    }

    return {key: value for key, value in payload.items() if value is not None}


def prepare_rows(df, parent_id, existing_libraries):
    payloads = []
    preview_log = []
    not_inserted_log = []

    for _, row in df.iterrows():
        payload = make_payload(row, parent_id)
        library = payload.get("Biblioteca")
        text = payload.get("Texto")

        if not library:
            not_inserted_log.append(row_to_log(row, payload, "Nome do Autodoc vazio"))
            continue

        if not text:
            not_inserted_log.append(row_to_log(row, payload, "Texto do Autodoc vazio"))
            continue

        if library in existing_libraries:
            not_inserted_log.append(row_to_log(row, payload, "Autodoc já existe na pasta"))
            continue

        payloads.append(payload)
        preview_log.append(row_to_log(row, payload))

    return payloads, preview_log, not_inserted_log


def build_insert_statement(autodocs_tbl):
    return autodocs_tbl.insert().values({
        "Pai": bindparam("Pai"),
        "Biblioteca": bindparam("Biblioteca"),
        "Texto": bindparam("Texto", type_=UnicodeText()),
    })


def insert_payloads(session, autodocs_tbl, payloads):
    inserted_log = []
    not_inserted_log = []
    insert_statement = build_insert_statement(autodocs_tbl)

    for batch_number, batch in enumerate(iter_chunks(payloads, BATCH_SIZE), start=1):
        try:
            session.execute(insert_statement, batch)
            session.commit()
            inserted_log.extend(batch)
            print(f"Lote {batch_number}: {len(batch)} autodocs inseridos.")
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
    print("=== Migração segura de Autodocs Pac Docs - Dra. Paula ===")
    path_file = input("Informe o caminho da pasta que contém o medx_pac_docs.csv: ").strip()
    log_folder = Path(path_file)
    df = load_source_csv(log_folder)

    print(f"CSV carregado: {len(df)} autodocs.")
    print("Conectando no banco para refletir tabela e checar pasta em massa...")
    engine, autodocs_tbl = connect_database()
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()

    try:
        parent_id = find_parent_folder_id(session, autodocs_tbl)
        parent_status = "reutilizada" if parent_id is not None else "será criada na execução real"
        existing_libraries = fetch_existing_libraries_in_parent(session, autodocs_tbl, parent_id)

        preview_parent_id = parent_id if parent_id is not None else f"<nova pasta: {PARENT_FOLDER_NAME}>"
        payloads, preview_log, not_inserted_log = prepare_rows(
            df,
            preview_parent_id,
            existing_libraries,
        )

        print("\n=== Pré-validação ===")
        print(f"Total lido: {len(df)}")
        print(f"Pasta pai: {PARENT_FOLDER_NAME} ({parent_status})")
        print(f"Id da pasta pai atual: {parent_id}")
        print(f"Autodocs já existentes na pasta: {len(existing_libraries)}")
        print(f"Prontos para inserção: {len(payloads)}")
        print(f"Não inseridos previstos: {len(not_inserted_log)}")

        write_json_log(preview_log, log_folder, "log_preview_autodocs_pac_docs.json")
        if not_inserted_log:
            write_json_log(not_inserted_log, log_folder, "log_not_inserted_autodocs_pac_docs.json")

        confirmation = input(
            f"\nDry-run concluído. Digite {EXECUTION_CONFIRMATION} para inserir no banco: "
        ).strip()

        if confirmation != EXECUTION_CONFIRMATION:
            print("Execução encerrada em modo dry-run. Nenhum dado foi inserido.")
            return

        if parent_id is None:
            parent_id = create_parent_folder(session, autodocs_tbl)
            print(f"Pasta pai criada: {PARENT_FOLDER_NAME} | Id do Texto: {parent_id}")
            existing_libraries = set()
        else:
            print(f"Pasta pai reutilizada: {PARENT_FOLDER_NAME} | Id do Texto: {parent_id}")

        payloads, _, not_inserted_log = prepare_rows(df, parent_id, existing_libraries)

        print("\nIniciando inserção em lotes pequenos...")
        inserted_log, insert_errors_log = insert_payloads(session, autodocs_tbl, payloads)
        not_inserted_log.extend(insert_errors_log)

        write_json_log(inserted_log, log_folder, "log_inserted_autodocs_pac_docs.json")
        write_json_log(not_inserted_log, log_folder, "log_not_inserted_autodocs_pac_docs.json")

        print("\n=== Resumo final ===")
        print(f"Inseridos: {len(inserted_log)}")
        print(f"Não inseridos: {len(not_inserted_log)}")
        print(f"Logs gravados em: {log_folder}")
    finally:
        session.close()
        engine.dispose()


if __name__ == "__main__":
    main()
