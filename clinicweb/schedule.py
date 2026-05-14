from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from pathlib import Path
import csv
import glob
import json
import urllib.parse

import pandas as pd
from sqlalchemy import MetaData, Table, bindparam, create_engine, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool

from utils.utils import verify_nan


# =========================
# CONFIGURAÇÕES
# =========================
READ_CHUNK_SIZE = 5000
INSERT_SUBBATCH_SIZE = 1000
THROTTLE_SECONDS = 120
LOG_MAX_FIELD_LENGTH = 4000


# =========================
# INPUTS
# =========================
sid = input("Informe o SoftwareID: ").strip()
password = urllib.parse.quote_plus(input("Informe a senha: ").strip())
dbase = input("Informe o DATABASE: ").strip()
path_file = input("Informe o caminho da pasta que contém os arquivos: ").strip()

print("Conectando no Banco de dados...")

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

agenda_tbl = Table(
    "Agenda",
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

AGENDA_USER_COL = "Id do Usuário"
AGENDA_LINK_COL = "Vinculado a"
AGENDA_DESC_COL = "Descrição"
AGENDA_START_COL = "Início"
AGENDA_END_COL = "Final"
AGENDA_STATUS_COL = "Status"

CONTATOS_ID_COL = "Id do Cliente"
CONTATOS_NOME_COL = "Nome"


# =========================
# HELPERS
# =========================
def append_jsonl(filepath: Path, record: dict) -> None:
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with filepath.open("a", encoding="utf-8") as f:
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


def normalize_client_id(value):
    if value is None or pd.isna(value):
        return None

    value = str(value).strip()

    if value == "" or value.lower() == "none":
        return None

    if value.endswith(".0"):
        value = value[:-2]

    return value


def parse_date_time(date_value, hour_value) -> tuple[datetime | None, datetime | None]:
    date_str = verify_nan(date_value)
    hour_str = verify_nan(hour_value)

    if not date_str or not hour_str:
        return None, None

    date_formats = ["%Y-%m-%d", "%d/%m/%Y"]
    time_formats = ["%H:%M:%S", "%H:%M"]

    parsed_date = None
    parsed_time = None

    for fmt in date_formats:
        try:
            parsed_date = datetime.strptime(str(date_str), fmt)
            break
        except Exception:
            pass

    for fmt in time_formats:
        try:
            parsed_time = datetime.strptime(str(hour_str), fmt)
            break
        except Exception:
            pass

    if parsed_date is None or parsed_time is None:
        return None, None

    start_time = datetime.combine(parsed_date.date(), parsed_time.time())
    end_time = start_time + timedelta(minutes=30)
    return start_time, end_time


def map_user_id(user_id_raw):
    user_id = verify_nan(user_id_raw)

    if user_id is None:
        return None

    user_id_str = str(user_id).strip()

    if user_id_str == "55361796":
        return 1522426523
    elif user_id_str == "55361944":
        return -1290814303
    elif user_id_str == "55361969":
        return -1210566649

    return user_id


def fetch_contact_names_by_ids(id_values: list) -> dict:
    """
    Retorna dict: {Id do Cliente normalizado: Nome}
    """
    normalized_ids = [normalize_client_id(x) for x in id_values]
    normalized_ids = [x for x in normalized_ids if x is not None]

    if not normalized_ids:
        return {}

    stmt = select(
        contatos_tbl.c[CONTATOS_ID_COL],
        contatos_tbl.c[CONTATOS_NOME_COL],
    )

    with engine.connect() as conn:
        rows = conn.execute(stmt).fetchall()

    wanted = set(normalized_ids)
    result = {}

    for row in rows:
        db_id = normalize_client_id(row[0])
        if db_id in wanted:
            result[db_id] = row[1]

    return result


def build_candidate_base(row_dict: dict) -> tuple[dict | None, dict | None]:
    """
    Retorna:
      (candidate_base, log_erro)
    candidate_base ainda não tem a descrição final;
    ela será preenchida com o nome vindo da tabela Contatos.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    id_patient = normalize_client_id(verify_nan(row_dict.get("idPaciente")))
    if id_patient is None:
        return None, {"Timestamp": timestamp, "Motivo": "Id do paciente vazio ou inválido"}

    start_time, end_time = parse_date_time(
        row_dict.get("DataAgenda"),
        row_dict.get("HoraAgenda"),
    )
    if start_time is None or end_time is None:
        return None, {"Timestamp": timestamp, "Motivo": "DataAgenda ou HoraAgenda inválida"}

    user = map_user_id(row_dict.get("idProfissional"))
    if user is None:
        return None, {"Timestamp": timestamp, "Motivo": "Id do profissional vazio ou inválido"}

    candidate = {
        AGENDA_LINK_COL: id_patient,
        AGENDA_USER_COL: user,
        AGENDA_START_COL: start_time,
        AGENDA_END_COL: end_time,
        AGENDA_STATUS_COL: 1,
    }

    return candidate, None


def process_chunk(
    df_chunk: pd.DataFrame,
    inserted_log_file: Path,
    rejected_log_file: Path,
    error_log_file: Path,
) -> tuple[int, int]:
    inserted_count = 0
    rejected_count = 0
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    base_candidates = []
    contact_ids = []

    # 1) validações básicas em memória
    for _, row in df_chunk.iterrows():
        row_dict = row.to_dict()

        candidate, validation_error = build_candidate_base(row_dict)
        if validation_error:
            rejected_count += 1
            append_jsonl(
                rejected_log_file,
                row_to_log_dict(row_dict, validation_error),
            )
            continue

        base_candidates.append((row_dict, candidate))
        contact_ids.append(candidate[AGENDA_LINK_COL])

    if not base_candidates:
        return inserted_count, rejected_count

    # 2) busca nomes na tabela Contatos em lote
    unique_contact_ids = list(dict.fromkeys(contact_ids))
    contact_name_map = fetch_contact_names_by_ids(unique_contact_ids)

    to_insert = []

    for row_dict, candidate in base_candidates:
        contact_id = candidate[AGENDA_LINK_COL]
        contact_name = contact_name_map.get(contact_id)

        if contact_name is None or str(contact_name).strip() == "":
            rejected_count += 1
            append_jsonl(
                rejected_log_file,
                row_to_log_dict(
                    row_dict,
                    {
                        "Timestamp": timestamp,
                        "Motivo": "Contato não encontrado na tabela Contatos pelo Id do Cliente",
                        "IdPacienteNormalizado": contact_id,
                    },
                ),
            )
            continue

        payload = {
            AGENDA_DESC_COL: str(contact_name).strip(),
            AGENDA_START_COL: candidate[AGENDA_START_COL],
            AGENDA_END_COL: candidate[AGENDA_END_COL],
            AGENDA_STATUS_COL: candidate[AGENDA_STATUS_COL],
            AGENDA_LINK_COL: candidate[AGENDA_LINK_COL],
            AGENDA_USER_COL: candidate[AGENDA_USER_COL],
        }

        to_insert.append((row_dict, payload))

    if not to_insert:
        return inserted_count, rejected_count

    insert_stmt = agenda_tbl.insert().values(
        {
            AGENDA_DESC_COL: bindparam(AGENDA_DESC_COL),
            AGENDA_START_COL: bindparam(AGENDA_START_COL),
            AGENDA_END_COL: bindparam(AGENDA_END_COL),
            AGENDA_STATUS_COL: bindparam(AGENDA_STATUS_COL),
            AGENDA_LINK_COL: bindparam(AGENDA_LINK_COL),
            AGENDA_USER_COL: bindparam(AGENDA_USER_COL),
        }
    )

    # 3) insert em sublotes
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
    print("Sucesso! Inicializando migração de Agendamentos...")

    csv.field_size_limit(100000000)

    arquivos = glob.glob(f"{path_file}/agendamentos*.csv")
    if not arquivos:
        raise FileNotFoundError(
            f"Nenhum arquivo encontrado com o padrão 'agendamentos*.csv' em: {path_file}"
        )

    csv_file = arquivos[0]
    log_folder = Path(path_file).resolve()
    log_folder.mkdir(parents=True, exist_ok=True)

    inserted_log_file = log_folder / "log_inserted_agendamentos.jsonl"
    rejected_log_file = log_folder / "log_not_inserted_agendamentos.jsonl"
    error_log_file = log_folder / "log_insert_errors_agendamentos.jsonl"

    inserted_log_file.write_text("", encoding="utf-8")
    rejected_log_file.write_text("", encoding="utf-8")
    error_log_file.write_text("", encoding="utf-8")

    inserted_total = 0
    rejected_total = 0
    processed_total = 0
    chunk_index = 0

    csv_iter = pd.read_csv(
        csv_file,
        sep=";",
        dtype=object,
        encoding="latin1",
        quotechar='"',
        chunksize=READ_CHUNK_SIZE,
    )

    for df_chunk in csv_iter:
        chunk_index += 1
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

    print(f"{inserted_total} novos agendamentos foram inseridos com sucesso!")
    if rejected_total > 0:
        print(
            f"{rejected_total} agendamentos não foram inseridos, "
            f"verifique os logs JSONL para mais detalhes."
        )

    print(f"Log inseridos: {inserted_log_file}")
    print(f"Log rejeitados: {rejected_log_file}")
    print(f"Log erros: {error_log_file}")

    engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())