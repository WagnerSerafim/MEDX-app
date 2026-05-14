from __future__ import annotations

import csv
import glob
import json
import urllib.parse
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import MetaData, Table, UnicodeText, bindparam, create_engine, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool


# =========================
# CONFIGURACOES
# =========================
READ_CHUNK_SIZE = 5000
INSERT_SUBBATCH_SIZE = 1000
REFERENCE_QUERY_BATCH_SIZE = 1000
LOG_MAX_FIELD_LENGTH = 4000

HIST_CLIENT_COLUMN = "Id do Cliente"
HIST_DATE_COLUMN = "Data"
HIST_TEXT_COLUMN = "Histórico"
HIST_USER_COLUMN = "Id do Usuário"

CONTACT_ID_COLUMN = "Id do Cliente"
CONTACT_NAME_COLUMN = "Nome"


# =========================
# INPUTS
# =========================
sid = input("Informe o SoftwareID: ").strip()
password = urllib.parse.quote_plus(input("Informe a senha: ").strip())
dbase = input("Informe o DATABASE: ").strip()
path_file = input("Informe o caminho da pasta que contem os arquivos: ").strip()

print("Conectando no Banco de Dados...")

DATABASE_URL = (
    f"mssql+pyodbc://Medizin_{sid}:{password}"
    f"@medxserver.database.windows.net:1433/{dbase}"
    f"?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"
)

engine = create_engine(
    DATABASE_URL,
    future=True,
    poolclass=NullPool,
    fast_executemany=True,
)

metadata = MetaData()
historico_tbl = Table(
    "Histórico de Clientes",
    metadata,
    schema=f"schema_{sid}",
    autoload_with=engine,
)
contatos_tbl = Table(
    "Contatos",
    metadata,
    schema=f"schema_{sid}",
    autoload_with=engine,
)


# =========================
# HELPERS
# =========================
def append_jsonl(filepath: Path, record: dict) -> None:
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with filepath.open("a", encoding="utf-8") as file:
        file.write(json.dumps(record, ensure_ascii=False) + "\n")


def truncate_log_value(value, max_len: int = LOG_MAX_FIELD_LENGTH):
    if value is None:
        return None

    value = str(value)
    if len(value) <= max_len:
        return value

    return value[:max_len] + "... [TRUNCADO]"


def row_to_log_dict(row: dict, extra: dict | None = None) -> dict:
    base = {}
    for key, value in row.items():
        try:
            if pd.isna(value):
                base[key] = None
            else:
                base[key] = truncate_log_value(value)
        except Exception:
            base[key] = truncate_log_value(value)

    if extra:
        base.update(extra)

    return base


def chunk_list(items: list[tuple[dict, dict]], size: int):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def normalize_scalar(value) -> str | None:
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    value = str(value).strip()
    if value == "" or value.lower() in {"none", "nan", "null"}:
        return None

    if value.endswith(".0"):
        value = value[:-2]

    return value


def normalize_record_text(value) -> str | None:
    value = normalize_scalar(value)
    if value is None:
        return None

    value = value.replace("\r\n", "\n").replace("\r", "\n").strip()
    if not value:
        return None

    return value.replace("\n", "<br>")


def normalize_contact_name(value) -> str | None:
    value = normalize_scalar(value)
    if value is None:
        return None

    # Evita falha de match por espacos repetidos no nome.
    return " ".join(value.split())


def build_history_text(row: dict) -> str | None:
    sections = []

    anamnese_text = normalize_record_text(row.get("Anamnese"))
    if anamnese_text is not None:
        sections.append(f"Anamnese:<br>{anamnese_text}")

    receita_text = normalize_record_text(row.get("Receita Médica"))
    if receita_text is None:
        receita_text = normalize_record_text(row.get("Receita Medica"))
    if receita_text is not None:
        sections.append(f"Receita Médica:<br>{receita_text}")

    if not sections:
        return None

    return "<br><br>".join(sections)


def normalize_datetime(value) -> str:
    value = normalize_scalar(value)
    if value is None:
        return "1900-01-01 00:00:00"

    parse_formats = [
        "%b %d, %Y %I:%M %p",
        "%B %d, %Y %I:%M %p",
        "%b %d, %Y %I:%M:%S %p",
        "%B %d, %Y %I:%M:%S %p",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
    ]

    for parse_format in parse_formats:
        try:
            return datetime.strptime(value, parse_format).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue

    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return "1900-01-01 00:00:00"

    return parsed.to_pydatetime().strftime("%Y-%m-%d %H:%M:%S")


def fetch_client_map_by_name(name_values: list[str]) -> dict[str, str]:
    normalized_names = [normalize_contact_name(value) for value in name_values]
    normalized_names = [value for value in normalized_names if value is not None]

    if not normalized_names:
        return {}

    stmt = select(
        contatos_tbl.c[CONTACT_NAME_COLUMN],
        contatos_tbl.c[CONTACT_ID_COLUMN],
    ).where(
        contatos_tbl.c[CONTACT_NAME_COLUMN].in_(
            bindparam("names", expanding=True)
        )
    )

    result = {}

    unique_names = list(dict.fromkeys(normalized_names))

    with engine.connect() as conn:
        for start in range(0, len(unique_names), REFERENCE_QUERY_BATCH_SIZE):
            names_batch = unique_names[start:start + REFERENCE_QUERY_BATCH_SIZE]
            rows = conn.execute(stmt, {"names": names_batch}).fetchall()

            for row in rows:
                name = normalize_contact_name(row[0])
                client_id = normalize_scalar(row[1])

                if name is not None and client_id is not None:
                    result[name] = client_id

    return result


def build_base_candidate(row: dict) -> tuple[dict | None, dict | None]:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    record_text = build_history_text(row)
    if record_text is None:
        return None, {"Timestamp": timestamp, "Motivo": "Anamnese e Receita Médica vazias"}

    patient_name = normalize_contact_name(row.get("Paciente"))
    if patient_name is None:
        return None, {"Timestamp": timestamp, "Motivo": "Paciente vazio"}

    return {
        "record_text": record_text,
        "patient_name": patient_name,
        "date": normalize_datetime(row.get("Data Final")),
    }, None


def process_chunk(
    df_chunk: pd.DataFrame,
    inserted_log_file: Path,
    rejected_log_file: Path,
    error_log_file: Path,
) -> tuple[int, int]:
    inserted_count = 0
    rejected_count = 0
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    base_candidates: list[tuple[dict, dict]] = []
    patient_names_to_resolve: list[str] = []

    for _, row in df_chunk.iterrows():
        row_dict = row.to_dict()
        candidate, validation_error = build_base_candidate(row_dict)

        if validation_error:
            rejected_count += 1
            append_jsonl(rejected_log_file, row_to_log_dict(row_dict, validation_error))
            continue

        base_candidates.append((row_dict, candidate))
        patient_names_to_resolve.append(candidate["patient_name"])

    if not base_candidates:
        return inserted_count, rejected_count

    name_to_client = fetch_client_map_by_name(patient_names_to_resolve)
    to_insert: list[tuple[dict, dict]] = []

    for row_dict, candidate in base_candidates:
        patient_name = candidate["patient_name"]
        client_id = name_to_client.get(patient_name)

        if client_id is None:
            rejected_count += 1
            append_jsonl(
                rejected_log_file,
                row_to_log_dict(
                    row_dict,
                    {
                        "Timestamp": timestamp,
                        "Motivo": "Paciente nao encontrado em Contatos pela coluna [Nome]",
                        "Paciente normalizado": patient_name,
                    },
                ),
            )
            continue

        payload = {
            HIST_DATE_COLUMN: candidate["date"],
            HIST_TEXT_COLUMN: candidate["record_text"],
            HIST_CLIENT_COLUMN: client_id,
            HIST_USER_COLUMN: 0,
        }
        to_insert.append((row_dict, payload))

    if not to_insert:
        return inserted_count, rejected_count

    insert_stmt = historico_tbl.insert().values(
        {
            HIST_DATE_COLUMN: bindparam(HIST_DATE_COLUMN),
            HIST_TEXT_COLUMN: bindparam(HIST_TEXT_COLUMN, type_=UnicodeText()),
            HIST_CLIENT_COLUMN: bindparam(HIST_CLIENT_COLUMN),
            HIST_USER_COLUMN: bindparam(HIST_USER_COLUMN),
        }
    )

    for subbatch in chunk_list(to_insert, INSERT_SUBBATCH_SIZE):
        payload_list = [payload for _, payload in subbatch]

        try:
            with engine.begin() as conn:
                conn.execute(insert_stmt, payload_list)

            inserted_count += len(subbatch)

            for _, payload in subbatch:
                append_jsonl(
                    inserted_log_file,
                    {
                        "Timestamp": timestamp,
                        "Status": "Inserido",
                        **{
                            key: truncate_log_value(value)
                            for key, value in payload.items()
                        },
                    },
                )

        except (SQLAlchemyError, MemoryError) as batch_error:
            for row_dict, payload in subbatch:
                try:
                    with engine.begin() as conn:
                        conn.execute(insert_stmt, [payload])

                    inserted_count += 1
                    append_jsonl(
                        inserted_log_file,
                        {
                            "Timestamp": timestamp,
                            "Status": "Inserido",
                            "Modo": "fallback_individual",
                            **{
                                key: truncate_log_value(value)
                                for key, value in payload.items()
                            },
                        },
                    )

                except (SQLAlchemyError, MemoryError) as item_error:
                    rejected_count += 1
                    append_jsonl(
                        error_log_file,
                        row_to_log_dict(
                            row_dict,
                            {
                                "Timestamp": timestamp,
                                "Motivo": f"Erro ao inserir: {item_error} | erro_sublote: {batch_error}",
                            },
                        ),
                    )

    return inserted_count, rejected_count


def resolve_csv_file(base_path: str) -> str:
    patterns = [
        "Consultas Médicas.csv",
        "Consultas Medicas.csv",
        "*Consultas*Médicas*.csv",
        "*Consultas*Medicas*.csv",
    ]

    for pattern in patterns:
        matches = glob.glob(str(Path(base_path) / pattern))
        if matches:
            return matches[0]

    raise FileNotFoundError(
        f"Nenhum arquivo encontrado com o nome 'Consultas Médicas.csv' em: {base_path}"
    )


def build_csv_iter(csv_file: str):
    encodings_to_try = ["utf-8", "utf-8-sig", "cp1252", "latin1"]

    for encoding in encodings_to_try:
        try:
            csv_iter = pd.read_csv(
                csv_file,
                sep=",",
                encoding=encoding,
                quotechar='"',
                dtype=object,
                chunksize=READ_CHUNK_SIZE,
            )
            first_chunk = next(csv_iter)

            def iterator_with_first():
                yield first_chunk
                for chunk in csv_iter:
                    yield chunk

            return iterator_with_first(), encoding
        except StopIteration:
            return iter(()), encoding
        except UnicodeDecodeError:
            continue

    raise UnicodeDecodeError("csv", b"", 0, 1, "Nao foi possivel decodificar o CSV")


def main() -> None:
    print("Sucesso! Inicializando migracao de Historico de Consultas Medicas...")

    csv.field_size_limit(100000000)

    csv_file = resolve_csv_file(path_file)
    log_folder = Path(path_file).resolve()
    log_folder.mkdir(parents=True, exist_ok=True)

    inserted_log_file = log_folder / "log_inserted_records_consultas_medicas.jsonl"
    rejected_log_file = log_folder / "log_not_inserted_records_consultas_medicas.jsonl"
    error_log_file = log_folder / "log_insert_errors_records_consultas_medicas.jsonl"

    inserted_log_file.write_text("", encoding="utf-8")
    rejected_log_file.write_text("", encoding="utf-8")
    error_log_file.write_text("", encoding="utf-8")

    csv_iter, chosen_encoding = build_csv_iter(csv_file)
    print(f"Arquivo CSV: {csv_file}")
    print(f"Encoding escolhido: {chosen_encoding}")

    inserted_total = 0
    rejected_total = 0
    processed_total = 0
    chunk_index = 0

    for df_chunk in csv_iter:
        chunk_index += 1
        df_chunk = df_chunk.replace("None", "")
        current_chunk_size = len(df_chunk)

        print(
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
            f"Processando chunk {chunk_index} com {current_chunk_size} linhas..."
        )

        inserted_count, rejected_count = process_chunk(
            df_chunk,
            inserted_log_file,
            rejected_log_file,
            error_log_file,
        )

        inserted_total += inserted_count
        rejected_total += rejected_count
        processed_total += current_chunk_size

        print(
            f"Processados: {processed_total} | "
            f"Inseridos: {inserted_total} | "
            f"Nao inseridos: {rejected_total}"
        )

    print(f"{inserted_total} novos historicos foram inseridos com sucesso!")
    if rejected_total > 0:
        print(
            f"{rejected_total} historicos nao foram inseridos, "
            f"verifique os logs JSONL para mais detalhes."
        )

    print(f"Log inseridos: {inserted_log_file}")
    print(f"Log rejeitados: {rejected_log_file}")
    print(f"Log erros: {error_log_file}")

    engine.dispose()


if __name__ == "__main__":
    main()
