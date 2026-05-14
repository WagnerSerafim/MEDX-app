from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path
import html
import json
import traceback
import urllib.parse

from sqlalchemy import MetaData, Table, create_engine, select, update, bindparam
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool


# =========================
# CONFIGURAÇÕES
# =========================
SELECT_BATCH_SIZE = 1000       # quantidade de registros lidos por página
UPDATE_BATCH_SIZE = 100        # quantidade de updates por sublote
THROTTLE_SECONDS = 5       # 2 minutos entre lotes
MAX_TEXT_LOG_LENGTH = 4000     # evita log gigante demais
INITIAL_LAST_ID = -9223372036854775808  # inclui IDs negativos e positivos na paginação


# =========================
# INPUTS
# =========================
sid = input("Informe o SoftwareID: ").strip()
password = urllib.parse.quote_plus(input("Informe a senha: ").strip())
dbase = input("Informe o DATABASE: ").strip()
log_folder = input("Informe a pasta onde deseja salvar os logs: ").strip()

print("Conectando no Banco de Dados...")

DATABASE_URL = (
    f"mssql+pyodbc://Medizin_{sid}:{password}"
    f"@medxserver.database.windows.net:1433/{dbase}"
    f"?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"
)

# Sem connection pooling
engine = create_engine(
    DATABASE_URL,
    poolclass=NullPool,
    future=True,
)

metadata = MetaData()

historico_tbl = Table(
    "Histórico de Clientes",
    metadata,
    schema=f"schema_{sid}",
    autoload_with=engine,
)

ID_COL = "Id do Histórico"
TEXT_COL = "Histórico"


# =========================
# HELPERS
# =========================
def append_jsonl(filepath: Path, record: dict) -> None:
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with filepath.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def chunk_list(items: list[dict], size: int):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def truncate_text(value: str | None, max_len: int = MAX_TEXT_LOG_LENGTH) -> str | None:
    if value is None:
        return None
    value = str(value)
    if len(value) <= max_len:
        return value
    return value[:max_len] + "... [TRUNCADO]"


def decode_html_entities(text: str | None) -> str | None:
    if text is None:
        return None

    text = str(text)
    decoded = html.unescape(text)

    # roda duas vezes porque às vezes vem duplamente escapado
    # exemplo: &amp;oacute; -> &oacute; -> ó
    if decoded != text:
        decoded2 = html.unescape(decoded)
        return decoded2

    return decoded


def has_html_entity(text: str | None) -> bool:
    if text is None:
        return False

    text = str(text)

    # heurística simples e barata:
    # procura por padrões comuns de entidades HTML
    return "&" in text and ";" in text


def fetch_batch(last_id: int, batch_size: int) -> list[dict]:
    id_col = historico_tbl.c[ID_COL]
    text_col = historico_tbl.c[TEXT_COL]

    stmt = (
        select(id_col, text_col)
        .where(id_col > last_id)
        .order_by(id_col.asc())
        .limit(batch_size)
    )

    with engine.connect() as conn:
        rows = conn.execute(stmt).fetchall()

    return [
        {
            "id": int(row[0]),
            "historico": None if row[1] is None else str(row[1]),
        }
        for row in rows
    ]


def process_records(records: list[dict], changed_log_file: Path, skipped_log_file: Path) -> tuple[list[dict], int]:
    updates = []
    skipped_count = 0
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for item in records:
        historico_id = item["id"]
        original_text = item["historico"]

        if not has_html_entity(original_text):
            skipped_count += 1
            continue

        normalized_text = decode_html_entities(original_text)

        if normalized_text == original_text:
            skipped_count += 1
            continue

        updates.append({
            "id_param": historico_id,
            "historico_param": normalized_text,
            "antes": original_text,
            "depois": normalized_text,
            "timestamp": timestamp,
        })

    # logamos só o que realmente será alterado
    for item in updates:
        append_jsonl(changed_log_file, {
            "Timestamp": item["timestamp"],
            "Id do Histórico": item["id_param"],
            "Antes": truncate_text(item["antes"]),
            "Depois": truncate_text(item["depois"]),
            "Status": "PendenteUpdate"
        })

    return updates, skipped_count


def apply_updates(update_items: list[dict], success_log_file: Path, error_log_file: Path) -> tuple[int, int]:
    if not update_items:
        return 0, 0

    stmt = (
        update(historico_tbl)
        .where(historico_tbl.c[ID_COL] == bindparam("id_param"))
        .values({TEXT_COL: bindparam("historico_param")})
    )

    updated_count = 0
    error_count = 0

    for update_chunk in chunk_list(update_items, UPDATE_BATCH_SIZE):
        try:
            payload = [
                {
                    "id_param": item["id_param"],
                    "historico_param": item["historico_param"],
                }
                for item in update_chunk
            ]

            with engine.begin() as conn:
                conn.execute(stmt, payload)

            updated_count += len(update_chunk)

            for item in update_chunk:
                append_jsonl(success_log_file, {
                    "Timestamp": item["timestamp"],
                    "Id do Histórico": item["id_param"],
                    "Antes": truncate_text(item["antes"]),
                    "Depois": truncate_text(item["depois"]),
                    "Status": "Atualizado"
                })

        except (SQLAlchemyError, MemoryError) as chunk_error:
            # fallback individual
            for item in update_chunk:
                try:
                    with engine.begin() as conn:
                        conn.execute(stmt, [{
                            "id_param": item["id_param"],
                            "historico_param": item["historico_param"],
                        }])

                    updated_count += 1

                    append_jsonl(success_log_file, {
                        "Timestamp": item["timestamp"],
                        "Id do Histórico": item["id_param"],
                        "Antes": truncate_text(item["antes"]),
                        "Depois": truncate_text(item["depois"]),
                        "Status": "Atualizado",
                        "Modo": "fallback_individual"
                    })

                except (SQLAlchemyError, MemoryError) as item_error:
                    error_count += 1
                    append_jsonl(error_log_file, {
                        "Timestamp": item["timestamp"],
                        "Id do Histórico": item["id_param"],
                        "Antes": truncate_text(item["antes"]),
                        "Depois": truncate_text(item["depois"]),
                        "Status": "Erro",
                        "Motivo": f"{str(item_error)} | erro_sublote: {str(chunk_error)}"
                    })

    return updated_count, error_count


async def main():
    try:
        log_dir = Path(log_folder).expanduser().resolve()
        log_dir.mkdir(parents=True, exist_ok=True)

        detected_log_file = log_dir / "log_detected_changes.jsonl"
        success_log_file = log_dir / "log_updated_records.jsonl"
        error_log_file = log_dir / "log_update_errors.jsonl"

        detected_log_file.write_text("", encoding="utf-8")
        success_log_file.write_text("", encoding="utf-8")
        error_log_file.write_text("", encoding="utf-8")

        print("Iniciando saneamento de entidades HTML no histórico...")

        last_id = INITIAL_LAST_ID
        total_lidos = 0
        total_detectados = 0
        total_atualizados = 0
        total_erros = 0
        total_ignorados = 0
        batch_index = 0

        while True:
            batch_index += 1

            records = await asyncio.to_thread(fetch_batch, last_id, SELECT_BATCH_SIZE)

            if not records:
                break

            total_lidos += len(records)
            last_id = records[-1]["id"]

            print(
                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                f"Lote {batch_index}: {len(records)} registros lidos "
                f"(último ID: {last_id})"
            )

            update_items, skipped_count = await asyncio.to_thread(
                process_records,
                records,
                detected_log_file,
                error_log_file,
            )

            total_detectados += len(update_items)
            total_ignorados += skipped_count

            updated_count, error_count = await asyncio.to_thread(
                apply_updates,
                update_items,
                success_log_file,
                error_log_file,
            )

            total_atualizados += updated_count
            total_erros += error_count

            print(
                f"Lote {batch_index} concluído | "
                f"Lidos: {total_lidos} | "
                f"Detectados para update: {total_detectados} | "
                f"Atualizados: {total_atualizados} | "
                f"Ignorados: {total_ignorados} | "
                f"Erros: {total_erros}"
            )

            print(
                f"Aguardando {THROTTLE_SECONDS} segundos "
                f"antes do próximo lote para aliviar o banco..."
            )
            await asyncio.sleep(THROTTLE_SECONDS)

        print("Processo finalizado com sucesso.")
        print(f"Total lidos: {total_lidos}")
        print(f"Total detectados para update: {total_detectados}")
        print(f"Total atualizados: {total_atualizados}")
        print(f"Total ignorados: {total_ignorados}")
        print(f"Total erros: {total_erros}")
        print(f"Log detecção: {detected_log_file}")
        print(f"Log atualizações: {success_log_file}")
        print(f"Log erros: {error_log_file}")

    except Exception as e:
        print("Erro durante o processo:")
        print(f"Tipo: {type(e).__name__}")
        print(f"Mensagem: {e!r}")
        print("Traceback:")
        print(traceback.format_exc())

    finally:
        engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())