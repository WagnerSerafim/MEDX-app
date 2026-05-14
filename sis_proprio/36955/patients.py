from __future__ import annotations

import glob
import json
import re
import urllib.parse
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import MetaData, Table, bindparam, create_engine, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool

from utils.utils import limpar_cpf, limpar_numero, truncate_value, verify_nan

# =========================
# CONFIGURACOES
# =========================
READ_CHUNK_SIZE = 5000
INSERT_SUBBATCH_SIZE = 1000
THROTTLE_SECONDS = 5
LOG_MAX_FIELD_LENGTH = 4000

UF_CODES = {
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS",
    "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC",
    "SP", "SE", "TO",
}

REFERENCE_COLUMN = "Referências"


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
contatos_tbl = Table("Contatos", metadata, schema=f"schema_{sid}", autoload_with=engine)


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


def chunk_list(items: list[tuple[dict, dict]], size: int):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def row_to_log_dict(row: dict, extra: dict | None = None) -> dict:
    base = {}
    for key, value in row.items():
        if pd.isna(value):
            base[key] = None
        else:
            base[key] = truncate_log_value(value)

    if extra:
        base.update(extra)

    return base


def normalize_birth_date(raw_value) -> str:
    birth_str = verify_nan(raw_value)
    if birth_str is None:
        return "1900-01-01"

    value = str(birth_str).strip()
    parse_formats = [
        "%b %d, %Y %I:%M %p",
        "%B %d, %Y %I:%M %p",
        "%Y-%m-%d",
        "%d/%m/%Y",
    ]

    for parse_format in parse_formats:
        try:
            return datetime.strptime(value, parse_format).strftime("%Y-%m-%d")
        except ValueError:
            continue

    return "1900-01-01"


def normalize_sex(raw_value) -> str:
    sex = str(verify_nan(raw_value) or "").strip().lower()
    if sex in {"f", "feminino", "female"}:
        return "F"
    return "M"


def normalize_cep(raw_value) -> str | None:
    raw = verify_nan(raw_value)
    if raw is None:
        return None
    digits = re.sub(r"\D", "", str(raw))
    return digits[:8] if digits else None


def parse_address(raw_value) -> dict:
    raw = verify_nan(raw_value)
    if raw is None:
        return {
            "address": None,
            "neighborhood": None,
            "city": None,
            "cep": None,
        }

    text = " ".join(str(raw).strip().split())
    text = re.sub(r",\s*(Brasil|Brazil)\s*$", "", text, flags=re.IGNORECASE)

    cep_match = re.search(r"\b\d{5}-?\d{3}\b", text)
    cep = normalize_cep(cep_match.group(0)) if cep_match else None
    if cep_match:
        text = (text[:cep_match.start()] + text[cep_match.end():]).strip(" ,")

    parts = [part.strip() for part in text.split(",") if part.strip()]
    if not parts:
        return {
            "address": None,
            "neighborhood": None,
            "city": None,
            "cep": cep,
        }

    street = parts[0]
    number = None
    neighborhood = None
    city = None

    if len(parts) >= 2:
        second = parts[1]
        if " - " in second:
            left, right = [item.strip() for item in second.split(" - ", 1)]
            if re.match(r"^\d+[A-Za-z]?$", left):
                number = left
                neighborhood = right or None
            else:
                if right.upper() in UF_CODES:
                    city = left
                else:
                    neighborhood = left or None
                    city = right or None
        elif re.match(r"^\d+[A-Za-z]?$", second):
            number = second
        else:
            neighborhood = second

    if len(parts) >= 3:
        third = parts[2]
        if " - " in third:
            city_part, state_part = [item.strip() for item in third.split(" - ", 1)]
            if city_part and city_part.upper() not in UF_CODES:
                city = city_part
            if city is None and state_part.upper() not in UF_CODES:
                city = state_part
        elif third.upper() not in UF_CODES:
            city = city or third

    # Enderecos do tipo "Rua X, 874 - Bom Despacho, MG" nao possuem bairro explicito.
    if city is None and number and neighborhood and len(parts) >= 3 and parts[2].upper() in UF_CODES:
        city = neighborhood
        neighborhood = None

    address = street
    if street and number:
        address = f"{street}, {number}"

    return {
        "address": address,
        "neighborhood": neighborhood,
        "city": city,
        "cep": cep,
    }


def fetch_existing_references(ref_values: list[str]) -> set[str]:
    if not ref_values:
        return set()

    stmt = select(contatos_tbl.c[REFERENCE_COLUMN]).where(
        contatos_tbl.c[REFERENCE_COLUMN].in_(bindparam("refs", expanding=True))
    )

    with engine.connect() as conn:
        rows = conn.execute(stmt, {"refs": ref_values}).fetchall()

    return {str(row[0]) for row in rows if row[0] is not None}


def build_insert_payload(row: dict) -> tuple[dict | None, dict | None]:
    reference = verify_nan(row.get("unique id"))
    if reference is None:
        return None, {"Motivo": "unique id (Referências) vazio"}

    name = verify_nan(row.get("Nome Completo"))
    if name is None:
        return None, {"Motivo": "Nome Completo vazio"}

    parsed_address = parse_address(row.get("Cidade"))

    payload = {
        "Nome": truncate_value(name, 50),
        "Nascimento": normalize_birth_date(row.get("Data de Nascimento")),
        "Sexo": normalize_sex(row.get("Sexo")),
        "Celular": truncate_value(limpar_numero(verify_nan(row.get("Telefone"))), 25),
        "Email": truncate_value(verify_nan(row.get("Email")), 100),
        "Referências": truncate_value(reference, 255),
        "CPF/CGC": truncate_value(limpar_cpf(verify_nan(row.get("CPF"))), 25),
        "RG": truncate_value(limpar_numero(verify_nan(row.get("RG"))), 25),
        "Endereço Residencial": truncate_value(parsed_address["address"], 50),
        "Bairro Residencial": truncate_value(parsed_address["neighborhood"], 25),
        "Cidade Residencial": truncate_value(parsed_address["city"], 25),
        "Cep Residencial": truncate_value(parsed_address["cep"], 10),
        "Mãe": truncate_value(verify_nan(row.get("Filiação")), 50),
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

    candidate_rows: list[tuple[dict, dict]] = []

    for _, row in df_chunk.iterrows():
        row_dict = row.to_dict()
        payload, validation_error = build_insert_payload(row_dict)
        if validation_error:
            rejected_count += 1
            append_jsonl(
                rejected_log_file,
                row_to_log_dict(row_dict, {"Timestamp": timestamp, **validation_error}),
            )
            continue
        candidate_rows.append((row_dict, payload))

    if not candidate_rows:
        return inserted_count, rejected_count

    dedup_map: dict[str, tuple[dict, dict]] = {}
    duplicated_inside_chunk: set[str] = set()

    for row_dict, payload in candidate_rows:
        ref = payload["Referências"]
        if ref in dedup_map:
            duplicated_inside_chunk.add(ref)
        dedup_map[ref] = (row_dict, payload)

    for duplicated_ref in duplicated_inside_chunk:
        rejected_count += 1
        row_dict, _ = dedup_map[duplicated_ref]
        append_jsonl(
            rejected_log_file,
            row_to_log_dict(
                row_dict,
                {
                    "Timestamp": timestamp,
                    "Motivo": "Referências duplicada dentro do arquivo/chunk",
                },
            ),
        )

    deduped_items: list[tuple[dict, dict]] = []
    seen_refs: set[str] = set()
    for row_dict, payload in candidate_rows:
        ref = payload["Referências"]
        if ref in duplicated_inside_chunk:
            if ref in seen_refs:
                continue
            seen_refs.add(ref)
            continue
        if ref not in seen_refs:
            deduped_items.append((row_dict, payload))
            seen_refs.add(ref)

    if not deduped_items:
        return inserted_count, rejected_count

    deduped_refs = [payload["Referências"] for _, payload in deduped_items]
    existing_refs = fetch_existing_references(deduped_refs)

    to_insert: list[tuple[dict, dict]] = []
    for row_dict, payload in deduped_items:
        if payload["Referências"] in existing_refs:
            rejected_count += 1
            append_jsonl(
                rejected_log_file,
                row_to_log_dict(
                    row_dict,
                    {
                        "Timestamp": timestamp,
                        "Motivo": "Referências ja existe",
                    },
                ),
            )
        else:
            to_insert.append((row_dict, payload))

    if not to_insert:
        return inserted_count, rejected_count

    insert_stmt = contatos_tbl.insert().values(
        {
            "Nome": bindparam("Nome"),
            "Nascimento": bindparam("Nascimento"),
            "Sexo": bindparam("Sexo"),
            "Celular": bindparam("Celular"),
            "Email": bindparam("Email"),
            "Referências": bindparam("Referências"),
            "CPF/CGC": bindparam("CPF/CGC"),
            "RG": bindparam("RG"),
            "Endereço Residencial": bindparam("Endereço Residencial"),
            "Bairro Residencial": bindparam("Bairro Residencial"),
            "Cidade Residencial": bindparam("Cidade Residencial"),
            "Cep Residencial": bindparam("Cep Residencial"),
            "Mãe": bindparam("Mãe"),
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
                        **{key: truncate_log_value(value) for key, value in payload.items()},
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
                            **{key: truncate_log_value(value) for key, value in payload.items()},
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
    exact_match = glob.glob(f"{base_path}/Cadastro de Pacientes.csv")
    if exact_match:
        return exact_match[0]

    fallback_match = glob.glob(f"{base_path}/*Cadastro*Pacientes*.csv")
    if fallback_match:
        return fallback_match[0]

    raise FileNotFoundError(
        f"Nenhum arquivo encontrado com o nome 'Cadastro de Pacientes.csv' em: {base_path}"
    )


def build_csv_iter(csv_file: str):
    encodings_to_try = ["utf-8", "utf-8-sig", "latin1"]

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
    print("Sucesso! Inicializando migracao de Contatos...")

    csv_file = resolve_csv_file(path_file)
    log_folder = Path(path_file).resolve()
    log_folder.mkdir(parents=True, exist_ok=True)

    inserted_log_file = log_folder / "log_inserted_patients_cadastro_pacientes.jsonl"
    rejected_log_file = log_folder / "log_not_inserted_patients_cadastro_pacientes.jsonl"
    error_log_file = log_folder / "log_insert_errors_patients_cadastro_pacientes.jsonl"

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

        print(
            f"Aguardando {THROTTLE_SECONDS} segundos antes do proximo chunk "
            f"para aliviar o banco..."
        )
        if THROTTLE_SECONDS > 0:
            import time

            time.sleep(THROTTLE_SECONDS)

    print(f"{inserted_total} novos contatos foram inseridos com sucesso!")
    if rejected_total > 0:
        print(
            f"{rejected_total} contatos nao foram inseridos, "
            f"verifique os logs JSONL para mais detalhes."
        )

    print(f"Log inseridos: {inserted_log_file}")
    print(f"Log rejeitados: {rejected_log_file}")
    print(f"Log erros: {error_log_file}")

    engine.dispose()


if __name__ == "__main__":
    main()