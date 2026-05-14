from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path
import json
import traceback
import urllib.parse
from typing import Iterable

import pandas as pd
from sqlalchemy import (
    MetaData,
    Table,
    UnicodeText,
    bindparam,
    create_engine,
    select,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool


# =========================
# CONFIGURAÇÕES
# =========================
BATCH_SIZE = 5000
THROTTLE_SECONDS = 120  # 2 minutos entre lotes
DB_INSERT_BATCH_SIZE = 100  # sublote de insert para reduzir consumo de memória no executemany


# =========================
# INPUTS
# =========================
sid = input("Informe o SoftwareID: ").strip()
password = urllib.parse.quote_plus(input("Informe a senha: ").strip())
dbase = input("Informe o DATABASE: ").strip()
excel_folder = input("Informe o caminho da pasta EXCEL: ").strip()

print("Conectando no Banco de Dados...")

DATABASE_URL = (
    f"mssql+pyodbc://Medizin_{sid}:{password}"
    f"@medxserver.database.windows.net:1433/{dbase}"
    f"?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"
)

# Sem connection pooling
engine = create_engine(
    DATABASE_URL,
    fast_executemany=False,
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


# =========================
# HELPERS
# =========================
def find_excel_and_json(excel_dir_str: str) -> tuple[Path, Path, Path]:
    excel_dir = Path(excel_dir_str).expanduser().resolve()

    if not excel_dir.exists() or not excel_dir.is_dir():
        raise FileNotFoundError(f"Pasta EXCEL não encontrada: {excel_dir}")

    excel_files = sorted(excel_dir.glob("textFlip*.xlsx"))
    if not excel_files:
        raise FileNotFoundError(
            f"Nenhum arquivo no padrão 'textFlip*.xlsx' foi encontrado em: {excel_dir}"
        )

    excel_file = excel_files[0]
    base_dir = excel_dir.parent
    json_file = base_dir / "JSON" / "AletaTexto_CLINICA_batch_0.json"

    if not json_file.exists():
        raise FileNotFoundError(f"Arquivo JSON não encontrado em: {json_file}")

    return excel_file, json_file, base_dir


def load_json_file(json_path: Path) -> pd.DataFrame:
    raw = json_path.read_text(encoding="utf-8").strip()

    if not raw:
        raise ValueError(f"Arquivo JSON vazio: {json_path}")

    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return pd.DataFrame(parsed)
        if isinstance(parsed, dict):
            return pd.DataFrame([parsed])
        raise ValueError("Formato JSON inválido.")
    except json.JSONDecodeError:
        rows = []
        with json_path.open("r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError as e:
                    raise ValueError(
                        f"Erro ao ler JSONL na linha {line_num} de {json_path}: {e}"
                    ) from e
        return pd.DataFrame(rows)


def normalize_datetime(value) -> str:
    if pd.isna(value) or value in [None, "", "None"]:
        return "1900-01-01 00:00:00"

    try:
        dt = pd.to_datetime(value, errors="coerce")
        if pd.isna(dt):
            return "1900-01-01 00:00:00"
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "1900-01-01 00:00:00"


def append_jsonl(filepath: Path, record: dict) -> None:
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with filepath.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def chunk_dataframe(df: pd.DataFrame, size: int) -> Iterable[pd.DataFrame]:
    for start in range(0, len(df), size):
        yield df.iloc[start:start + size].copy()


def chunk_list(items: list[dict], size: int) -> Iterable[list[dict]]:
    for start in range(0, len(items), size):
        yield items[start:start + size]


def prepare_dataframes(excel_file: Path, json_file: Path) -> pd.DataFrame:
    print("Lendo arquivos...")

    df_excel = pd.read_excel(excel_file, dtype=object)
    df_json = load_json_file(json_file)

    required_excel_cols = ["Id", "FichaPacienteId", "DataInclusao"]
    required_json_cols = ["_id", "resumo1"]

    for col in required_excel_cols:
        if col not in df_excel.columns:
            raise ValueError(f"Coluna obrigatória não encontrada no Excel: {col}")

    for col in required_json_cols:
        if col not in df_json.columns:
            raise ValueError(f"Coluna obrigatória não encontrada no JSON: {col}")

    df_excel = df_excel[required_excel_cols].copy()
    df_json = df_json[required_json_cols].copy()

    df_excel["Id"] = pd.to_numeric(df_excel["Id"], errors="coerce")
    df_excel["FichaPacienteId"] = pd.to_numeric(df_excel["FichaPacienteId"], errors="coerce")
    df_json["_id"] = pd.to_numeric(df_json["_id"], errors="coerce")

    df_excel = df_excel.dropna(subset=["Id"])
    df_json = df_json.dropna(subset=["_id"])

    df_excel["Id"] = df_excel["Id"].astype("int64")
    df_json["_id"] = df_json["_id"].astype("int64")

    df_merged = df_excel.merge(
        df_json,
        left_on="Id",
        right_on="_id",
        how="left",
    )

    print(f"Total de linhas no Excel: {len(df_excel)}")
    print(f"Total de registros no JSON: {len(df_json)}")
    print(f"Total após relacionamento: {len(df_merged)}")

    return df_merged


def fetch_existing_ids(conn, historico_ids: list[int]) -> set[int]:
    if not historico_ids:
        return set()

    col_historico = historico_tbl.c["Id do Histórico"]

    stmt = (
        select(col_historico)
        .where(col_historico.in_(bindparam("ids", expanding=True)))
    )

    rows = conn.execute(stmt, {"ids": historico_ids}).fetchall()
    return {int(row[0]) for row in rows}


def process_batch(
    batch_df: pd.DataFrame,
    inserted_log_file: Path,
    not_inserted_log_file: Path,
) -> tuple[int, int]:
    inserted_count = 0
    not_inserted_count = 0

    timestamp_lote = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    valid_candidates = []
    candidate_ids = []

    # 1) validações em memória
    for _, row in batch_df.iterrows():
        historico_id = row["Id"]
        cliente_id = row["FichaPacienteId"]
        data = normalize_datetime(row["DataInclusao"])
        historico_texto = row["resumo1"]

        base_log = {
            "Id do Histórico": None if pd.isna(historico_id) else int(historico_id),
            "Id do Cliente": None if pd.isna(cliente_id) else int(cliente_id) if not pd.isna(cliente_id) else None,
            "Data": data,
            "Histórico": None if pd.isna(historico_texto) else str(historico_texto),
            "Id do Usuário": 0,
            "Timestamp": timestamp_lote,
        }

        if pd.isna(historico_id):
            not_inserted_count += 1
            append_jsonl(not_inserted_log_file, {
                **base_log,
                "Motivo": "Id do Histórico inválido no Excel",
            })
            continue

        if pd.isna(cliente_id):
            not_inserted_count += 1
            append_jsonl(not_inserted_log_file, {
                **base_log,
                "Motivo": "Id do Cliente vazio ou inválido",
            })
            continue

        if pd.isna(historico_texto) or str(historico_texto).strip() == "":
            not_inserted_count += 1
            append_jsonl(not_inserted_log_file, {
                **base_log,
                "Motivo": "Histórico não encontrado no JSON ou resumo1 vazio",
            })
            continue

        item = {
            "Id do Histórico": int(historico_id),
            "Id do Cliente": int(cliente_id),
            "Data": data,
            "Histórico": str(historico_texto),
            "Id do Usuário": 0,
            "Timestamp": timestamp_lote,
        }

        valid_candidates.append(item)
        candidate_ids.append(item["Id do Histórico"])

    if not valid_candidates:
        return inserted_count, not_inserted_count

    # 2) remove duplicidade dentro do próprio lote
    unique_candidates = {}
    for item in valid_candidates:
        unique_candidates[item["Id do Histórico"]] = item

    deduped_candidates = list(unique_candidates.values())
    deduped_ids = [item["Id do Histórico"] for item in deduped_candidates]

    # 3) consulta duplicidade em lote no banco
    try:
        with engine.connect() as conn:
            existing_ids = fetch_existing_ids(conn, deduped_ids)
    except SQLAlchemyError as e:
        for item in deduped_candidates:
            not_inserted_count += 1
            append_jsonl(not_inserted_log_file, {
                **item,
                "Motivo": f"Erro ao verificar existência do histórico em lote: {str(e)}",
            })
        return inserted_count, not_inserted_count

    to_insert = []
    for item in deduped_candidates:
        if item["Id do Histórico"] in existing_ids:
            not_inserted_count += 1
            append_jsonl(not_inserted_log_file, {
                **item,
                "Motivo": "Histórico já existe",
            })
        else:
            to_insert.append(item)

    if not to_insert:
        return inserted_count, not_inserted_count

    # 4) insert em sublotes para evitar MemoryError em executemany com textos longos
    insert_stmt = historico_tbl.insert().values(
        {
            "Data": bindparam("Data"),
            "Histórico": bindparam("Histórico", type_=UnicodeText()),
            "Id do Histórico": bindparam("Id do Histórico"),
            "Id do Cliente": bindparam("Id do Cliente"),
            "Id do Usuário": bindparam("Id do Usuário"),
        }
    )

    for insert_chunk in chunk_list(to_insert, DB_INSERT_BATCH_SIZE):
        try:
            with engine.begin() as conn:
                conn.execute(insert_stmt, insert_chunk)

            inserted_count += len(insert_chunk)

            for item in insert_chunk:
                append_jsonl(inserted_log_file, {
                    **item,
                    "Status": "Inserido",
                })

        except (SQLAlchemyError, MemoryError) as chunk_error:
            for item in insert_chunk:
                try:
                    with engine.begin() as conn:
                        conn.execute(insert_stmt, [item])

                    inserted_count += 1
                    append_jsonl(inserted_log_file, {
                        **item,
                        "Status": "Inserido",
                        "Modo": "fallback_individual",
                    })
                except (SQLAlchemyError, MemoryError) as item_error:
                    not_inserted_count += 1
                    append_jsonl(not_inserted_log_file, {
                        **item,
                        "Motivo": f"Erro ao inserir registro: {str(item_error)} | erro_sublote: {str(chunk_error)}",
                    })

    return inserted_count, not_inserted_count


async def main():
    try:
        excel_file, json_file, base_dir = find_excel_and_json(excel_folder)

        print("Sucesso! Arquivos encontrados:")
        print(f"Excel: {excel_file}")
        print(f"JSON:  {json_file}")

        log_dir = base_dir / "LOG"
        inserted_log_file = log_dir / "log_inserted_record.jsonl"
        not_inserted_log_file = log_dir / "log_not_inserted_record.jsonl"

        # limpa logs anteriores da execução atual
        inserted_log_file.parent.mkdir(parents=True, exist_ok=True)
        inserted_log_file.write_text("", encoding="utf-8")
        not_inserted_log_file.write_text("", encoding="utf-8")

        df_merged = prepare_dataframes(excel_file, json_file)

        total_inserted = 0
        total_not_inserted = 0
        total_rows = len(df_merged)

        print("Inicializando migração de Histórico...")

        batches = list(chunk_dataframe(df_merged, BATCH_SIZE))
        total_batches = len(batches)

        for batch_index, batch_df in enumerate(batches, start=1):
            print(
                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                f"Processando lote {batch_index}/{total_batches} "
                f"com {len(batch_df)} linhas..."
            )

            # roda o processamento do lote fora do event loop principal
            inserted, not_inserted = await asyncio.to_thread(
                process_batch,
                batch_df,
                inserted_log_file,
                not_inserted_log_file,
            )

            total_inserted += inserted
            total_not_inserted += not_inserted

            processed_rows = min(batch_index * BATCH_SIZE, total_rows)
            percent = round((processed_rows / total_rows) * 100, 2)

            print(
                f"Processados: {processed_rows}/{total_rows} | "
                f"Inseridos: {total_inserted} | "
                f"Não inseridos: {total_not_inserted} | "
                f"Concluído: {percent}%"
            )

            # throttling entre lotes
            if batch_index < total_batches:
                print(
                    f"Aguardando {THROTTLE_SECONDS} segundos "
                    f"antes do próximo lote para aliviar o banco..."
                )
                await asyncio.sleep(THROTTLE_SECONDS)

        print(f"{total_inserted} novos históricos foram inseridos com sucesso!")
        if total_not_inserted > 0:
            print(
                f"{total_not_inserted} históricos não foram inseridos, "
                f"verifique os logs JSONL para mais detalhes."
            )

        print(f"Log de inseridos: {inserted_log_file}")
        print(f"Log de não inseridos: {not_inserted_log_file}")

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