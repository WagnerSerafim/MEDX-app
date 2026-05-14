from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path
import glob
import json
import urllib.parse

import pandas as pd
from sqlalchemy import MetaData, Table, bindparam, create_engine, select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool


# =========================
# CONFIGURAÇÕES
# =========================
READ_CHUNK_SIZE = 5000
UPDATE_SUBBATCH_SIZE = 1000
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
contatos_tbl = Table(
    "Contatos",
    metadata,
    schema=f"schema_{sid}",
    autoload_with=engine,
)

REF_COL = "Referências"
BIRTH_COL = "Nascimento"
ID_CLIENT_COL = "Id do Cliente"


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


def chunk_list(items: list[dict], size: int):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def normalize_reference(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    value = str(value).strip()
    if value == "" or value.lower() == "none":
        return None

    if value.endswith(".0"):
        value = value[:-2]

    return value


def unix_ms_to_datetime_str(value) -> str | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    value = str(value).strip()
    if value == "" or value.lower() == "none":
        return None

    try:
        timestamp_ms = int(float(value))
        return datetime.fromtimestamp(timestamp_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def find_excel_file(folder: str) -> Path:
    matches = glob.glob(f"{folder}/dados*.xlsx")
    if not matches:
        raise FileNotFoundError(
            f"Nenhum arquivo encontrado com o padrão 'dados*.xlsx' em: {folder}"
        )
    return Path(matches[0]).resolve()


def load_excel_in_chunks(filepath: Path, chunk_size: int):
    df = pd.read_excel(filepath, sheet_name="pacientes", dtype=object)
    df = df.replace("None", "")

    for start in range(0, len(df), chunk_size):
        yield df.iloc[start:start + chunk_size].copy()


def fetch_existing_contacts_by_refs(ref_values: list[str]) -> dict[str, dict]:
    """
    Retorna:
      {
        referencia_normalizada: {
            "Id do Cliente": ...,
            "Nascimento": ...
        }
      }
    """
    normalized_refs = [normalize_reference(x) for x in ref_values]
    normalized_refs = [x for x in normalized_refs if x is not None]

    if not normalized_refs:
        return {}

    stmt = select(
        contatos_tbl.c[REF_COL],
        contatos_tbl.c[ID_CLIENT_COL],
        contatos_tbl.c[BIRTH_COL],
    )

    wanted = set(normalized_refs)
    result = {}

    with engine.connect() as conn:
        rows = conn.execute(stmt).fetchall()

    for row in rows:
        ref_db = normalize_reference(row[0])
        if ref_db in wanted:
            result[ref_db] = {
                ID_CLIENT_COL: row[1],
                BIRTH_COL: row[2],
            }

    return result


def process_chunk(
    df_chunk: pd.DataFrame,
    updated_log_file: Path,
    rejected_log_file: Path,
    error_log_file: Path,
) -> tuple[int, int]:
    updated_count = 0
    rejected_count = 0
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    candidates = []
    refs = []

    # 1) validações locais
    for _, row in df_chunk.iterrows():
        row_dict = row.to_dict()

        ref_value = normalize_reference(row_dict.get("ID"))
        if not ref_value:
            rejected_count += 1
            append_jsonl(
                rejected_log_file,
                row_to_log_dict(
                    row_dict,
                    {
                        "Timestamp": timestamp,
                        "Motivo": "ID vazio ou inválido para uso em Referências",
                    },
                ),
            )
            continue

        birthday = unix_ms_to_datetime_str(row_dict.get("NASCIMENTO"))
        if not birthday:
            rejected_count += 1
            append_jsonl(
                rejected_log_file,
                row_to_log_dict(
                    row_dict,
                    {
                        "Timestamp": timestamp,
                        "Motivo": "NASCIMENTO vazio ou inválido em Unix milissegundos",
                        "ReferênciaNormalizada": ref_value,
                    },
                ),
            )
            continue

        candidates.append((row_dict, ref_value, birthday))
        refs.append(ref_value)

    if not candidates:
        return updated_count, rejected_count

    # 2) busca contatos existentes por Referências
    ref_map = fetch_existing_contacts_by_refs(list(dict.fromkeys(refs)))

    to_update = []

    for row_dict, ref_value, new_birthday in candidates:
        contact = ref_map.get(ref_value)

        if not contact:
            rejected_count += 1
            append_jsonl(
                rejected_log_file,
                row_to_log_dict(
                    row_dict,
                    {
                        "Timestamp": timestamp,
                        "Motivo": "Paciente não encontrado na tabela Contatos pela coluna Referências",
                        "ReferênciaNormalizada": ref_value,
                    },
                ),
            )
            continue

        old_birthday = contact.get(BIRTH_COL)
        old_birthday_str = None if old_birthday is None else str(old_birthday)

        to_update.append(
            (
                row_dict,
                {
                    "ref_param": ref_value,
                    "birth_param": new_birthday,
                    "old_birth": old_birthday_str,
                    "client_id": contact.get(ID_CLIENT_COL),
                },
            )
        )

    if not to_update:
        return updated_count, rejected_count

    update_stmt = (
        update(contatos_tbl)
        .where(contatos_tbl.c[REF_COL] == bindparam("ref_param"))
        .values({BIRTH_COL: bindparam("birth_param")})
    )

    # 3) update em sublotes
    for subbatch in chunk_list(to_update, UPDATE_SUBBATCH_SIZE):
        payload_list = [
            {
                "ref_param": payload["ref_param"],
                "birth_param": payload["birth_param"],
            }
            for _, payload in subbatch
        ]

        try:
            with engine.begin() as conn:
                conn.execute(update_stmt, payload_list)

            updated_count += len(subbatch)

            for _, payload in subbatch:
                append_jsonl(
                    updated_log_file,
                    {
                        "Timestamp": timestamp,
                        "Status": "Atualizado",
                        "Referências": payload["ref_param"],
                        "Id do Cliente": truncate_log_value(payload["client_id"]),
                        "Nascimento_Antes": truncate_log_value(payload["old_birth"]),
                        "Nascimento_Depois": truncate_log_value(payload["birth_param"]),
                    },
                )

        except (SQLAlchemyError, MemoryError) as batch_error:
            for row_dict, payload in subbatch:
                try:
                    with engine.begin() as conn:
                        conn.execute(
                            update_stmt,
                            [{
                                "ref_param": payload["ref_param"],
                                "birth_param": payload["birth_param"],
                            }],
                        )

                    updated_count += 1

                    append_jsonl(
                        updated_log_file,
                        {
                            "Timestamp": timestamp,
                            "Status": "Atualizado",
                            "Modo": "fallback_individual",
                            "Referências": payload["ref_param"],
                            "Id do Cliente": truncate_log_value(payload["client_id"]),
                            "Nascimento_Antes": truncate_log_value(payload["old_birth"]),
                            "Nascimento_Depois": truncate_log_value(payload["birth_param"]),
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
                                "Motivo": f"Erro ao atualizar: {item_error} | erro_sublote: {batch_error}",
                                "Referências": payload["ref_param"],
                                "Nascimento_Tentado": payload["birth_param"],
                            },
                        ),
                    )

    return updated_count, rejected_count


async def main():
    print("Sucesso! Começando atualização de data de nascimento...")

    excel_file = find_excel_file(path_file)

    log_folder = Path(path_file).resolve()
    log_folder.mkdir(parents=True, exist_ok=True)

    updated_log_file = log_folder / "log_updated_birthdate_patients.jsonl"
    rejected_log_file = log_folder / "log_not_updated_birthdate_patients.jsonl"
    error_log_file = log_folder / "log_update_birthdate_errors_patients.jsonl"

    updated_log_file.write_text("", encoding="utf-8")
    rejected_log_file.write_text("", encoding="utf-8")
    error_log_file.write_text("", encoding="utf-8")

    updated_total = 0
    rejected_total = 0
    processed_total = 0
    chunk_index = 0

    for df_chunk in load_excel_in_chunks(excel_file, READ_CHUNK_SIZE):
        chunk_index += 1
        current_chunk_size = len(df_chunk)

        print(
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
            f"Processando chunk {chunk_index} com {current_chunk_size} linhas..."
        )

        updated_count, rejected_count = await asyncio.to_thread(
            process_chunk,
            df_chunk,
            updated_log_file,
            rejected_log_file,
            error_log_file,
        )

        updated_total += updated_count
        rejected_total += rejected_count
        processed_total += current_chunk_size

        print(
            f"Processados: {processed_total} | "
            f"Atualizados: {updated_total} | "
            f"Não atualizados: {rejected_total}"
        )

        print(
            f"Aguardando {THROTTLE_SECONDS} segundos antes do próximo chunk "
            f"para aliviar o banco..."
        )
        await asyncio.sleep(THROTTLE_SECONDS)

    print(f"{updated_total} pacientes tiveram a data de nascimento atualizada com sucesso!")
    if rejected_total > 0:
        print(
            f"{rejected_total} pacientes não foram atualizados, "
            f"verifique os logs JSONL para mais detalhes."
        )

    print(f"Log atualizados: {updated_log_file}")
    print(f"Log rejeitados: {rejected_log_file}")
    print(f"Log erros: {error_log_file}")

    engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())