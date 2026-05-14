from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import hashlib
from pathlib import Path
import glob
import json
import os
import re
import urllib.parse
from typing import Iterable

import pandas as pd
import requests
from sqlalchemy import MetaData, Table, bindparam, create_engine, func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool


# =========================
# CONFIGURAÇÕES
# =========================
READ_CHUNK_SIZE = 1000
INSERT_SUBBATCH_SIZE = 200
THROTTLE_SECONDS = 1
LOG_MAX_FIELD_LENGTH = 4000
DOWNLOAD_TIMEOUT = 60
MAX_CLASS_LENGTH = 100

DOWNLOAD_ROOT = Path(r"D:\Medxdata\37239")

SOURCE_FILE_PATTERNS = [
    "patients_ehr.xlsx",
    "patients_ehr.xls",
    "patients_ehr.csv",
]


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

contatos_tbl = Table(
    "Contatos",
    metadata,
    schema=f"schema_{sid}",
    autoload_with=engine,
)

HIST_ID_COL = "Id do Histórico"
HIST_CLIENT_COL = "Id do Cliente"
HIST_DATE_COL = "Data"
HIST_TEXT_COL = "Histórico"
HIST_CLASS_COL = "Classe"
HIST_USER_COL = "Id do Usuário"

CONTATOS_ID_COL = "Id do Cliente"
CONTATOS_REF_COL = "Referências"

URL_COLUMNS = ["INF_PDFTXTPRESCRICAO", "INF_PDFTXT"]


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


def chunk_list(items: list, size: int):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def normalize_scalar(value):
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


def normalize_reference(value):
    return normalize_scalar(value)


def normalize_url(value):
    value = normalize_scalar(value)
    if not value:
        return None

    value = value.strip()

    if value.startswith("//"):
        value = "https:" + value
    elif value.startswith("www."):
        value = "https://" + value
    elif not re.match(r"^https?://", value, flags=re.IGNORECASE):
        return None

    return value


def unix_ms_to_datetime_str(value) -> str | None:
    value = normalize_scalar(value)
    if not value:
        return None

    try:
        ms = int(float(value))
        dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc).astimezone()
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def sanitize_filename(name: str) -> str:
    name = re.sub(r'[<>:"/\\|?*]+', "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name[:180] if name else "arquivo"


def filename_from_url(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    raw_name = os.path.basename(parsed.path) or "arquivo"
    decoded = urllib.parse.unquote(raw_name)
    safe = sanitize_filename(decoded)

    if "." not in safe:
        return safe + ".bin"
    return safe


def truncate_filename_keep_extension(filename: str, max_len: int) -> str:
    filename = sanitize_filename(filename)
    suffix = Path(filename).suffix
    stem = filename[:-len(suffix)] if suffix else filename

    if max_len <= 0:
        return "arquivo"

    if len(filename) <= max_len:
        return filename

    if not suffix:
        return filename[:max_len]

    min_stem_len = 1
    allowed_stem_len = max_len - len(suffix)
    if allowed_stem_len < min_stem_len:
        return filename[:max_len]

    return stem[:allowed_stem_len] + suffix


def limit_relative_path_length(folder_name: str, file_name: str, max_len: int = MAX_CLASS_LENGTH) -> Path:
    folder = sanitize_filename(folder_name or "sem_referencia")
    file_name = sanitize_filename(file_name or "arquivo.bin")

    relative = Path(folder) / file_name
    if len(str(relative)) <= max_len:
        return relative

    max_file_len = max_len - len(folder) - 1
    if max_file_len > 0:
        file_name = truncate_filename_keep_extension(file_name, max_file_len)
        relative = Path(folder) / file_name
        if len(str(relative)) <= max_len:
            return relative

    suffix = Path(file_name).suffix
    digest = hashlib.sha1(f"{folder}/{file_name}".encode("utf-8")).hexdigest()[:12]
    compact_name = f"arquivo_{digest}{suffix}"

    max_folder_len = max_len - len(compact_name) - 1
    if max_folder_len <= 0:
        max_folder_len = 1
        compact_name = truncate_filename_keep_extension(compact_name, max_len - max_folder_len - 1)

    folder = folder[:max_folder_len]
    relative = Path(folder) / compact_name
    if len(str(relative)) <= max_len:
        return relative

    # Fallback defensivo: preserva extensão e respeita o limite.
    safe_name = truncate_filename_keep_extension(compact_name, max_len - 2)
    return Path("x") / safe_name


def ensure_unique_relative_path(relative_path: Path, max_len: int = MAX_CLASS_LENGTH) -> Path:
    if not (DOWNLOAD_ROOT / relative_path).exists():
        return relative_path

    parent = relative_path.parent
    suffix = relative_path.suffix
    stem = relative_path.stem

    max_name_len = max_len - len(str(parent)) - 1
    if max_name_len <= 0:
        max_name_len = 1

    counter = 1
    while True:
        tag = f"_{counter}"
        allowed_stem = max_name_len - len(suffix) - len(tag)
        if allowed_stem < 1:
            allowed_stem = 1

        candidate_name = f"{stem[:allowed_stem]}{tag}{suffix}"
        candidate_relative = parent / candidate_name

        if len(str(candidate_relative)) <= max_len and not (DOWNLOAD_ROOT / candidate_relative).exists():
            return candidate_relative

        counter += 1


def download_file(url: str, folder_name: str) -> tuple[Path | None, str | None, str | None]:
    # Retorna (absolute_path, relative_class_path, error_message)
    try:
        initial_name = filename_from_url(url)
        relative_path = limit_relative_path_length(folder_name, initial_name, MAX_CLASS_LENGTH)
        relative_path = ensure_unique_relative_path(relative_path, MAX_CLASS_LENGTH)

        target_folder = DOWNLOAD_ROOT / relative_path.parent
        target_folder.mkdir(parents=True, exist_ok=True)

        target_path = DOWNLOAD_ROOT / relative_path

        with requests.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT) as response:
            response.raise_for_status()
            with target_path.open("wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 64):
                    if chunk:
                        f.write(chunk)

        return target_path, str(relative_path), None
    except Exception as e:
        return None, None, str(e)


def select_best_download_url(row_dict: dict) -> str | None:
    for col in URL_COLUMNS:
        url = normalize_url(row_dict.get(col))
        if url:
            return url
    return None


def find_source_file(folder: str) -> Path:
    base = Path(folder).expanduser().resolve()

    if not base.exists():
        raise FileNotFoundError(f"Pasta não encontrada: {base}")

    for pattern in SOURCE_FILE_PATTERNS:
        matches = list(base.glob(pattern))
        if matches:
            return matches[0]

    # fallback mais flexível
    for ext in ("*.xlsx", "*.xls", "*.csv"):
        matches = list(base.glob(ext))
        if matches:
            return matches[0]

    raise FileNotFoundError(
        f"Nenhum arquivo encontrado em {base}. Esperado algo como: {', '.join(SOURCE_FILE_PATTERNS)}"
    )


def load_file_in_chunks(filepath: Path, chunk_size: int) -> Iterable[pd.DataFrame]:
    suffix = filepath.suffix.lower()

    if suffix == ".csv":
        yield from pd.read_csv(
            filepath,
            sep=";",
            dtype=object,
            encoding="utf-8",
            chunksize=chunk_size,
        )
        return

    if suffix in (".xlsx", ".xls"):
        try:
            df = pd.read_excel(
                filepath,
                dtype=object,
                sheet_name="REGISTRO_ATENDIMENTO",
            )
        except ValueError as e:
            raise ValueError(
                f"A aba 'REGISTRO_ATENDIMENTO' não foi encontrada no arquivo {filepath.name}"
            ) from e

        for start in range(0, len(df), chunk_size):
            yield df.iloc[start:start + chunk_size].copy()
        return

    raise ValueError(f"Formato não suportado: {filepath.suffix}")


def fetch_reference_map(reference_values: list[str]) -> dict[str, str]:
    """
    Retorna:
      { referencia_normalizada: id_do_cliente_normalizado }
    """
    normalized_refs = [normalize_reference(x) for x in reference_values]
    normalized_refs = [x for x in normalized_refs if x is not None]

    if not normalized_refs:
        return {}

    stmt = select(
        contatos_tbl.c[CONTATOS_REF_COL],
        contatos_tbl.c[CONTATOS_ID_COL],
    )

    wanted = set(normalized_refs)
    result = {}

    with engine.connect() as conn:
        rows = conn.execute(stmt).fetchall()

    for row in rows:
        ref_db = normalize_reference(row[0])
        id_db = normalize_scalar(row[1])

        if ref_db in wanted and id_db is not None:
            result[ref_db] = id_db

    return result


def fetch_max_historico_id() -> int:
    stmt = select(func.max(historico_tbl.c[HIST_ID_COL]))
    with engine.connect() as conn:
        max_id = conn.execute(stmt).scalar()
    return int(max_id or 0)


def build_base_candidate(row_dict: dict) -> tuple[dict | None, dict | None]:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    created_date = unix_ms_to_datetime_str(row_dict.get("CREATED DATE"))
    if not created_date:
        return None, {"Timestamp": timestamp, "Motivo": "CREATED DATE inválido"}

    external_patient_ref = normalize_reference(row_dict.get("REL_PACIENTE_ID"))
    if not external_patient_ref:
        return None, {"Timestamp": timestamp, "Motivo": "REL_PACIENTE_ID vazio ou inválido"}

    url = select_best_download_url(row_dict)
    if not url:
        return None, {"Timestamp": timestamp, "Motivo": "Nenhuma URL válida encontrada em INF_PDFTXTPRESCRICAO/INF_PDFTXT"}

    return {
        "created_date": created_date,
        "external_patient_ref": external_patient_ref,
        "download_url": url,
    }, None


def process_chunk(
    df_chunk: pd.DataFrame,
    inserted_log_file: Path,
    rejected_log_file: Path,
    error_log_file: Path,
    start_historico_id: int,
) -> tuple[int, int, int]:
    """
    Retorna:
      inserted_count, rejected_count, last_used_historico_id
    """
    inserted_count = 0
    rejected_count = 0
    current_historico_id = start_historico_id
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    base_candidates = []
    refs_to_resolve = []

    # 1) validações locais
    for _, row in df_chunk.iterrows():
        row_dict = row.to_dict()
        candidate, validation_error = build_base_candidate(row_dict)

        if validation_error:
            rejected_count += 1
            append_jsonl(rejected_log_file, row_to_log_dict(row_dict, validation_error))
            continue

        base_candidates.append((row_dict, candidate))
        refs_to_resolve.append(candidate["external_patient_ref"])

    if not base_candidates:
        return inserted_count, rejected_count, current_historico_id

    # 2) resolve REL_PACIENTE_ID -> Contatos.[Id do Cliente]
    ref_to_client_map = fetch_reference_map(list(dict.fromkeys(refs_to_resolve)))

    to_insert = []

    # 3) download + montagem do payload
    for row_dict, candidate in base_candidates:
        external_ref = candidate["external_patient_ref"]
        client_id = ref_to_client_map.get(external_ref)

        if not client_id:
            rejected_count += 1
            append_jsonl(
                rejected_log_file,
                row_to_log_dict(
                    row_dict,
                    {
                        "Timestamp": timestamp,
                        "Motivo": "Paciente não encontrado na tabela Contatos pela coluna [Referências]",
                        "REL_PACIENTE_ID_NORMALIZADO": external_ref,
                    },
                ),
            )
            continue

        abs_path, class_path, download_error = download_file(candidate["download_url"], external_ref)
        if download_error or abs_path is None or class_path is None:
            rejected_count += 1
            append_jsonl(
                error_log_file,
                row_to_log_dict(
                    row_dict,
                    {
                        "Timestamp": timestamp,
                        "Motivo": f"Erro ao baixar arquivo: {download_error}",
                        "URL": candidate["download_url"],
                    },
                ),
            )
            continue

        current_historico_id += 1

        # Classe e caminho relativo do arquivo salvo devem ser idênticos.
        historico_name = abs_path.name

        payload = {
            HIST_ID_COL: current_historico_id,
            HIST_CLIENT_COL: client_id,
            HIST_DATE_COL: candidate["created_date"],
            HIST_TEXT_COL: historico_name,
            HIST_CLASS_COL: class_path,
            HIST_USER_COL: 0,
        }

        to_insert.append((row_dict, payload))

    if not to_insert:
        return inserted_count, rejected_count, current_historico_id

    insert_stmt = historico_tbl.insert().values(
        {
            HIST_ID_COL: bindparam(HIST_ID_COL),
            HIST_CLIENT_COL: bindparam(HIST_CLIENT_COL),
            HIST_DATE_COL: bindparam(HIST_DATE_COL),
            HIST_TEXT_COL: bindparam(HIST_TEXT_COL),
            HIST_CLASS_COL: bindparam(HIST_CLASS_COL),
            HIST_USER_COL: bindparam(HIST_USER_COL),
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
                                "PayloadTentado": truncate_log_value(payload),
                            },
                        ),
                    )

    return inserted_count, rejected_count, current_historico_id


async def main():
    print("Sucesso! Inicializando migração de Histórico...")

    DOWNLOAD_ROOT.mkdir(parents=True, exist_ok=True)

    source_file = find_source_file(path_file)

    log_folder = Path(path_file).resolve()
    log_folder.mkdir(parents=True, exist_ok=True)

    inserted_log_file = log_folder / "log_inserted_record_patients_ehr.jsonl"
    rejected_log_file = log_folder / "log_not_inserted_record_patients_ehr.jsonl"
    error_log_file = log_folder / "log_errors_record_patients_ehr.jsonl"

    inserted_log_file.write_text("", encoding="utf-8")
    rejected_log_file.write_text("", encoding="utf-8")
    error_log_file.write_text("", encoding="utf-8")

    inserted_total = 0
    rejected_total = 0
    processed_total = 0
    chunk_index = 0

    current_max_historico_id = fetch_max_historico_id()

    chunk_iter = load_file_in_chunks(source_file, READ_CHUNK_SIZE)

    for df_chunk in chunk_iter:
        chunk_index += 1
        current_chunk_size = len(df_chunk)

        print(
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
            f"Processando chunk {chunk_index} com {current_chunk_size} linhas..."
        )

        inserted_count, rejected_count, current_max_historico_id = await asyncio.to_thread(
            process_chunk,
            df_chunk,
            inserted_log_file,
            rejected_log_file,
            error_log_file,
            current_max_historico_id,
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

    print(f"Arquivo de origem: {source_file}")
    print(f"Pasta de downloads: {DOWNLOAD_ROOT}")
    print(f"Log inseridos: {inserted_log_file}")
    print(f"Log rejeitados: {rejected_log_file}")
    print(f"Log erros: {error_log_file}")

    engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())