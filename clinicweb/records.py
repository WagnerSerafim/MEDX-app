from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path
import csv
import glob
import json
import urllib.parse

import pandas as pd
from sqlalchemy import MetaData, Table, UnicodeText, bindparam, create_engine, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool

from utils.utils import is_valid_date


# =========================
# CONFIGURAÇÕES
# =========================
READ_CHUNK_SIZE = 5000
INSERT_SUBBATCH_SIZE = 1000
THROTTLE_SECONDS = 3
LOG_MAX_FIELD_LENGTH = 4000


# =========================
# INPUTS
# =========================
sid = input("Informe o SoftwareID: ").strip()
password = urllib.parse.quote_plus(input("Informe a senha: ").strip())
dbase = input("Informe o DATABASE: ").strip()
path_file = input("Informe o caminho da pasta que contém os arquivos: ").strip()

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

ID_COLUMN = "Id do Histórico"


# =========================
# HELPERS
# =========================
def append_jsonl(filepath: Path, record: dict) -> None:
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with filepath.open("a", encoding="latin-1") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def truncate_log_value(value, max_len: int = LOG_MAX_FIELD_LENGTH):
    if value is None:
        return None
    value = str(value)
    if len(value) <= max_len:
        return value
    return value[:max_len] + "... [TRUNCADO]"


def row_to_log_dict(row_dict: dict, extra: dict | None = None) -> dict:
    base = {}
    for key, value in row_dict.items():
        if pd.isna(value):
            base[key] = None
        else:
            base[key] = truncate_log_value(value)

    if extra:
        base.update(extra)

    return base


def chunk_list(items: list[dict], size: int):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def build_csv_iterator_with_fallback(csv_file: str):
    encodings_to_try = ["utf-8", "utf-8-sig", "cp1252", "latin-1"]
    last_error = None

    for current_encoding in encodings_to_try:
        try:
            csv_iter = pd.read_csv(
                csv_file,
                sep=";",
                encoding=current_encoding,
                dtype=object,
                chunksize=READ_CHUNK_SIZE,
            )

            first_chunk = next(csv_iter, None)
            return current_encoding, first_chunk, csv_iter
        except UnicodeDecodeError as err:
            last_error = err

    if last_error:
        raise last_error

    raise ValueError("Não foi possível inicializar leitura do CSV com os encodings suportados.")


def iter_csv_chunks(first_chunk, csv_iter):
    if first_chunk is not None:
        yield first_chunk

    for chunk in csv_iter:
        yield chunk


def normalize_text(value):
    if value in [None, "", "None"] or pd.isna(value):
        return None
    return str(value)


def build_record_text(titulo, texto) -> str | None:
    texto_norm = normalize_text(texto)
    if not texto_norm:
        return None

    titulo_norm = normalize_text(titulo)
    if titulo_norm:
        return f"{titulo_norm}<br>{texto_norm}"

    return texto_norm


def normalize_datetime(date_value, hour_value) -> str:
    date_str = normalize_text(date_value)
    hour_str = normalize_text(hour_value)

    if not date_str or not hour_str:
        return "1900-01-01 00:00"

    raw = f"{date_str} {hour_str}"

    # tenta formatos mais comuns
    formats = [
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y %H:%M:%S",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(raw, fmt)
            formatted = dt.strftime("%Y-%m-%d %H:%M")
            if is_valid_date(formatted, "%Y-%m-%d %H:%M"):
                return formatted
        except Exception:
            pass

    return "1900-01-01 00:00"


def fetch_existing_ids(id_values: list) -> set:
    if not id_values:
        return set()

    stmt = select(historico_tbl.c[ID_COLUMN]).where(
        historico_tbl.c[ID_COLUMN].in_(bindparam("ids", expanding=True))
    )

    with engine.connect() as conn:
        rows = conn.execute(stmt, {"ids": id_values}).fetchall()

    return {row[0] for row in rows}


def build_insert_payload(row_dict: dict) -> tuple[dict | None, dict | None]:
    """
    Retorna:
      (payload_insert, log_erro)
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    id_historico = row_dict.get("IdHistoria")
    if id_historico in [None, "", "None"] or pd.isna(id_historico):
        return None, {"Timestamp": timestamp, "Motivo": "Id do Histórico vazio"}

    texto_final = build_record_text(
        row_dict.get("TituloHistoria"),
        row_dict.get("Texto"),
    )
    if not texto_final:
        return None, {"Timestamp": timestamp, "Motivo": "Histórico vazio ou inválido"}

    id_paciente = row_dict.get("idPaciente")
    if id_paciente in [None, "", "None"] or pd.isna(id_paciente):
        return None, {"Timestamp": timestamp, "Motivo": "Id do paciente vazio"}

    data_final = normalize_datetime(
        row_dict.get("Data"),
        row_dict.get("Hora"),
    )

    payload = {
        "Data": data_final,
        "Histórico": texto_final,
        "Id do Histórico": id_historico,
        "Id do Cliente": id_paciente,
        "Id do Usuário": 0,
    }

    return payload, None


def process_chunk(
    df_chunk: pd.DataFrame,
    inserted_log_file: Path,
    rejected_log_file: Path,
    error_log_file: Path,
) -> tuple[int, int]:
    inserted_count = 0
    rejected_count = 0
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    candidate_rows = []
    candidate_ids = []

    # 1) validações locais
    for _, row in df_chunk.iterrows():
        row_dict = row.to_dict()
        payload, validation_error = build_insert_payload(row_dict)

        if validation_error:
            rejected_count += 1
            append_jsonl(
                rejected_log_file,
                row_to_log_dict(row_dict, validation_error),
            )
            continue

        candidate_rows.append((row_dict, payload))
        candidate_ids.append(payload["Id do Histórico"])

    if not candidate_rows:
        return inserted_count, rejected_count

    # 2) remove duplicidade dentro do próprio chunk
    dedup_map = {}
    duplicate_ids = set()

    for row_dict, payload in candidate_rows:
        current_id = payload["Id do Histórico"]
        if current_id in dedup_map:
            duplicate_ids.add(current_id)
        dedup_map[current_id] = (row_dict, payload)

    kept_ids = set()
    deduped_items = []

    for row_dict, payload in candidate_rows:
        current_id = payload["Id do Histórico"]

        if current_id in duplicate_ids:
            if current_id not in kept_ids:
                kept_ids.add(current_id)
                rejected_count += 1
                append_jsonl(
                    rejected_log_file,
                    row_to_log_dict(
                        row_dict,
                        {
                            "Timestamp": timestamp,
                            "Motivo": "Id do Histórico duplicado dentro do próprio arquivo/chunk",
                        },
                    ),
                )
            continue

        deduped_items.append((row_dict, payload))

    if not deduped_items:
        return inserted_count, rejected_count

    # 3) consulta IDs já existentes em lote
    deduped_ids = [payload["Id do Histórico"] for _, payload in deduped_items]
    existing_ids = fetch_existing_ids(deduped_ids)

    to_insert = []

    for row_dict, payload in deduped_items:
        if payload["Id do Histórico"] in existing_ids:
            rejected_count += 1
            append_jsonl(
                rejected_log_file,
                row_to_log_dict(
                    row_dict,
                    {
                        "Timestamp": timestamp,
                        "Motivo": "Histórico já existe",
                    },
                ),
            )
        else:
            to_insert.append((row_dict, payload))

    if not to_insert:
        return inserted_count, rejected_count

    insert_stmt = historico_tbl.insert().values(
        {
            "Data": bindparam("Data"),
            "Histórico": bindparam("Histórico", type_=UnicodeText()),
            "Id do Histórico": bindparam("Id do Histórico"),
            "Id do Cliente": bindparam("Id do Cliente"),
            "Id do Usuário": bindparam("Id do Usuário"),
        }
    )

    # 4) insert em sublotes
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
                        **{k: truncate_log_value(v) for k, v in payload.items()},
                    },
                )

        except (SQLAlchemyError, MemoryError) as batch_error:
            # fallback individual
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
                            **{k: truncate_log_value(v) for k, v in payload.items()},
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


async def main():
    print("Sucesso! Inicializando migração de Histórico...")

    csv.field_size_limit(1000000)

    arquivos = glob.glob(f"{path_file}/prontuarios*.csv")
    if not arquivos:
        raise FileNotFoundError(
            f"Nenhum arquivo encontrado com o padrão 'prontuarios*.csv' em: {path_file}"
        )

    csv_file = arquivos[0]
    log_folder = Path(path_file).resolve()
    log_folder.mkdir(parents=True, exist_ok=True)

    inserted_log_file = log_folder / "log_inserted_record_prontuarios.jsonl"
    rejected_log_file = log_folder / "log_not_inserted_record_prontuarios.jsonl"
    error_log_file = log_folder / "log_insert_errors_record_prontuarios.jsonl"

    inserted_log_file.write_text("", encoding="utf-8")
    rejected_log_file.write_text("", encoding="utf-8")
    error_log_file.write_text("", encoding="utf-8")

    inserted_total = 0
    rejected_total = 0
    processed_total = 0
    chunk_index = 0

    selected_encoding, first_chunk, csv_iter = build_csv_iterator_with_fallback(csv_file)
    print(f"Encoding detectado para leitura do CSV: {selected_encoding}")

    for df_chunk in iter_csv_chunks(first_chunk, csv_iter):
        chunk_index += 1

        # substitui 'None' string por vazio, igual ao comportamento original
        df_chunk = df_chunk.replace("None", "")

        current_chunk_size = len(df_chunk)

        print(
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
            f"Processando chunk {chunk_index} com {current_chunk_size} linhas..."
        )

        inserted_count, rejected_count = await asyncio.to_thread(
            process_chunk,
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
            f"Não inseridos: {rejected_total}"
        )

        print(
            f"Aguardando {THROTTLE_SECONDS} segundos antes do próximo chunk "
            f"para aliviar o banco..."
        )
        await asyncio.sleep(THROTTLE_SECONDS)

    print(f"{inserted_total} novos históricos foram inseridos com sucesso!")
    if rejected_total > 0:
        print(
            f"{rejected_total} históricos não foram inseridos, "
            f"verifique os logs JSONL para mais detalhes."
        )

    print(f"Log inseridos: {inserted_log_file}")
    print(f"Log rejeitados: {rejected_log_file}")
    print(f"Log erros: {error_log_file}")

    engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())