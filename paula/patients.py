from datetime import datetime
from pathlib import Path
import csv
import re
import sys
import time
import urllib.parse

import pandas as pd
from sqlalchemy import MetaData, Table, create_engine, select
from sqlalchemy.orm import declarative_base, sessionmaker


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.utils import clean_caracters, create_log, is_valid_date, truncate_value  # noqa: E402


SOURCE_FILE_NAME = "medx_paciente.csv"
TARGET_TABLE = "Contatos"
BATCH_SIZE = 200
EXISTING_ID_CHUNK_SIZE = 900
THROTTLE_SECONDS = 0.5
DEFAULT_DATE = "1900-01-01"
EXECUTION_CONFIRMATION = "MIGRAR"

REQUIRED_COLUMNS = {
    "IDENTIFICADOR",
    "NOME",
    "SOBRENOME",
    "strNomeSocialApelido",
    "DATA_NASCIMENTO",
    "SEXO",
    "RUA_NUMERO",
    "BAIRRO",
    "CEP",
    "CIDADE",
    "UF",
    "TEL1_DDD",
    "TEL1_NUMERO",
    "TEL2_DDD",
    "TEL2_NUMERO",
    "EMAIL",
    "CPF",
    "RG",
    "PROFISSAO",
    "ANOTACOES",
    "ALERGIAS",
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

    text = re.sub(r"\s+", " ", text)
    return text or None


def only_digits(value):
    text = clean_value(value)
    if text is None:
        return None

    digits = re.sub(r"\D", "", text)
    return digits or None


def format_phone(ddd, number):
    phone_number = only_digits(number)
    if phone_number is None:
        return None

    phone_ddd = only_digits(ddd)
    if phone_ddd:
        return f"{phone_ddd}{phone_number}"

    return phone_number


def split_phones(row):
    tel1 = format_phone(row.get("TEL1_DDD"), row.get("TEL1_NUMERO"))
    tel2 = format_phone(row.get("TEL2_DDD"), row.get("TEL2_NUMERO"))

    if tel2:
        return tel2, tel1

    return tel1, None


def build_name(row):
    parts = [
        clean_value(row.get("NOME")),
        clean_value(row.get("SOBRENOME")),
    ]
    return " ".join(part for part in parts if part)


def build_observations(row):
    parts = []

    notes = clean_value(row.get("ANOTACOES"))
    if notes:
        parts.append(f"Anotações: {notes}")

    allergies = clean_value(row.get("ALERGIAS"))
    if allergies:
        parts.append(f"Alergias: {allergies}")

    return "\n".join(parts) if parts else None


def normalize_date(value):
    text = clean_value(value)
    if text and is_valid_date(text, "%Y-%m-%d"):
        return text

    return DEFAULT_DATE


def normalize_sex(value):
    text = clean_value(value)
    if text and text.lower().startswith("f"):
        return "F"

    return "M"


def get_column_max_length(table, column_name, default):
    column = table.c.get(column_name)
    if column is None:
        return default

    length = getattr(column.type, "length", None)
    if isinstance(length, int) and length > 0:
        return length

    return default


def make_payload(row, max_lengths):
    cellphone, residential_phone = split_phones(row)

    payload = {
        "Id do Cliente": clean_value(row.get("IDENTIFICADOR")),
        "Nome": truncate_value(build_name(row), max_lengths["Nome"]),
        "Nome Social": truncate_value(
            clean_value(row.get("strNomeSocialApelido")),
            max_lengths["Nome Social"],
        ),
        "Nascimento": normalize_date(row.get("DATA_NASCIMENTO")),
        "Sexo": normalize_sex(row.get("SEXO")),
        "Celular": truncate_value(cellphone, max_lengths["Celular"]),
        "Telefone Residencial": truncate_value(
            residential_phone,
            max_lengths["Telefone Residencial"],
        ),
        "Email": truncate_value(clean_value(row.get("EMAIL")), max_lengths["Email"]),
        "CPF/CGC": truncate_value(clean_value(row.get("CPF")), max_lengths["CPF/CGC"]),
        "RG": truncate_value(clean_value(row.get("RG")), max_lengths["RG"]),
        "Profissão": truncate_value(
            clean_value(row.get("PROFISSAO")),
            max_lengths["Profissão"],
        ),
        "Cep Residencial": truncate_value(
            only_digits(row.get("CEP")),
            max_lengths["Cep Residencial"],
        ),
        "Endereço Residencial": truncate_value(
            clean_value(row.get("RUA_NUMERO")),
            max_lengths["Endereço Residencial"],
        ),
        "Bairro Residencial": truncate_value(
            clean_value(row.get("BAIRRO")),
            max_lengths["Bairro Residencial"],
        ),
        "Cidade Residencial": truncate_value(
            clean_value(row.get("CIDADE")),
            max_lengths["Cidade Residencial"],
        ),
        "Estado Residencial": truncate_value(
            clean_value(row.get("UF")),
            max_lengths["Estado Residencial"],
        ),
        "Observações": build_observations(row),
    }

    return {key: value for key, value in payload.items() if value is not None}


def row_to_log(row, payload, reason=None):
    log_row = row.to_dict()
    log_row.update(payload)
    if reason:
        log_row["Motivo"] = reason
    log_row["Timestamp"] = now_text()
    return log_row


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
    contatos_tbl = Table(TARGET_TABLE, metadata, schema=f"schema_{sid}", autoload_with=engine)

    return engine, contatos_tbl


def iter_chunks(items, chunk_size):
    for start in range(0, len(items), chunk_size):
        yield items[start : start + chunk_size]


def fetch_existing_ids(session, contatos_tbl, source_ids):
    existing_ids = set()
    id_column = contatos_tbl.c["Id do Cliente"]

    for id_chunk in iter_chunks(source_ids, EXISTING_ID_CHUNK_SIZE):
        result = session.execute(select(id_column).where(id_column.in_(id_chunk)))
        existing_ids.update(str(row[0]) for row in result if row[0] is not None)

    return existing_ids


def build_max_lengths(contatos_tbl):
    return {
        "Nome": get_column_max_length(contatos_tbl, "Nome", 50),
        "Nome Social": get_column_max_length(contatos_tbl, "Nome Social", 50),
        "Celular": get_column_max_length(contatos_tbl, "Celular", 25),
        "Telefone Residencial": get_column_max_length(
            contatos_tbl,
            "Telefone Residencial",
            25,
        ),
        "Email": get_column_max_length(contatos_tbl, "Email", 100),
        "CPF/CGC": get_column_max_length(contatos_tbl, "CPF/CGC", 25),
        "RG": get_column_max_length(contatos_tbl, "RG", 25),
        "Profissão": get_column_max_length(contatos_tbl, "Profissão", 25),
        "Cep Residencial": get_column_max_length(contatos_tbl, "Cep Residencial", 10),
        "Endereço Residencial": get_column_max_length(
            contatos_tbl,
            "Endereço Residencial",
            50,
        ),
        "Bairro Residencial": get_column_max_length(
            contatos_tbl,
            "Bairro Residencial",
            25,
        ),
        "Cidade Residencial": get_column_max_length(
            contatos_tbl,
            "Cidade Residencial",
            25,
        ),
        "Estado Residencial": get_column_max_length(
            contatos_tbl,
            "Estado Residencial",
            2,
        ),
    }


def prepare_rows(df, existing_ids, max_lengths):
    payloads = []
    preview_log = []
    not_inserted_log = []
    seen_ids = set()
    invalid_dates_count = 0

    for _, row in df.iterrows():
        payload = make_payload(row, max_lengths)
        id_patient = payload.get("Id do Cliente")
        patient_name = payload.get("Nome")

        if not clean_value(row.get("DATA_NASCIMENTO")) or payload["Nascimento"] == DEFAULT_DATE:
            invalid_dates_count += 1

        if not id_patient:
            not_inserted_log.append(row_to_log(row, payload, "Id do Cliente vazio"))
            continue

        if id_patient in seen_ids:
            not_inserted_log.append(row_to_log(row, payload, "Id do Cliente duplicado no CSV"))
            continue

        seen_ids.add(id_patient)

        if id_patient in existing_ids:
            not_inserted_log.append(row_to_log(row, payload, "Id do Cliente já existe no banco"))
            continue

        if not patient_name:
            not_inserted_log.append(row_to_log(row, payload, "Nome do Paciente vazio"))
            continue

        payloads.append(payload)
        preview_log.append(row_to_log(row, payload))

    return payloads, preview_log, not_inserted_log, invalid_dates_count


def insert_payloads(session, contatos_class, payloads):
    inserted_log = []
    not_inserted_log = []

    for batch_number, batch in enumerate(iter_chunks(payloads, BATCH_SIZE), start=1):
        try:
            session.bulk_insert_mappings(contatos_class, batch)
            session.commit()
            inserted_log.extend(batch)
            print(f"Lote {batch_number}: {len(batch)} pacientes inseridos.")
        except Exception as batch_error:
            session.rollback()
            print(f"Lote {batch_number}: erro, tentando isolar linhas individualmente.")

            for payload in batch:
                try:
                    session.bulk_insert_mappings(contatos_class, [payload])
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
    print("=== Migração segura de pacientes - Dra. Paula ===")
    path_file = input("Informe o caminho da pasta que contém o medx_paciente.csv: ").strip()
    log_folder = Path(path_file)
    df = load_source_csv(log_folder)

    print(f"CSV carregado: {len(df)} pacientes.")
    print("Conectando no banco para refletir tabela e checar duplicidades em massa...")
    engine, contatos_tbl = connect_database()

    Base = declarative_base()

    class Contatos(Base):
        __table__ = contatos_tbl

    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()

    try:
        source_ids = [
            clean_value(value)
            for value in df["IDENTIFICADOR"].tolist()
            if clean_value(value)
        ]
        existing_ids = fetch_existing_ids(session, contatos_tbl, source_ids)
        max_lengths = build_max_lengths(contatos_tbl)
        payloads, preview_log, not_inserted_log, invalid_dates_count = prepare_rows(
            df,
            existing_ids,
            max_lengths,
        )

        print("\n=== Pré-validação ===")
        print(f"Total lido: {len(df)}")
        print(f"IDs já existentes no banco: {len(existing_ids)}")
        print(f"Datas vazias/inválidas ajustadas para {DEFAULT_DATE}: {invalid_dates_count}")
        print(f"Prontos para inserção: {len(payloads)}")
        print(f"Não inseridos previstos: {len(not_inserted_log)}")

        create_log(preview_log, log_folder, "log_preview_patients.xlsx")
        if not_inserted_log:
            create_log(not_inserted_log, log_folder, "log_not_inserted_patients.xlsx")

        confirmation = input(
            f"\nDry-run concluído. Digite {EXECUTION_CONFIRMATION} para inserir no banco: "
        ).strip()

        if confirmation != EXECUTION_CONFIRMATION:
            print("Execução encerrada em modo dry-run. Nenhum dado foi inserido.")
            return

        print("\nIniciando inserção em lotes pequenos...")
        inserted_log, insert_errors_log = insert_payloads(session, Contatos, payloads)
        not_inserted_log.extend(insert_errors_log)

        create_log(inserted_log, log_folder, "log_inserted_patients.xlsx")
        create_log(not_inserted_log, log_folder, "log_not_inserted_patients.xlsx")

        print("\n=== Resumo final ===")
        print(f"Inseridos: {len(inserted_log)}")
        print(f"Não inseridos: {len(not_inserted_log)}")
        print(f"Logs gravados em: {log_folder}")
    finally:
        session.close()
        engine.dispose()


if __name__ == "__main__":
    main()
