from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path
import glob
import json
import urllib.parse

import pandas as pd
from sqlalchemy import MetaData, Table, bindparam, create_engine, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool

from utils.utils import (
    is_valid_date,
    limpar_cpf,
    limpar_numero,
    truncate_value,
    verify_nan,
)

# =========================
# CONFIGURAÇÕES
# =========================
READ_CHUNK_SIZE = 5000         # linhas lidas por chunk do CSV
INSERT_SUBBATCH_SIZE = 1000    # tamanho do sublote de insert
THROTTLE_SECONDS = 5         # 2 minutos entre chunks
LOG_MAX_FIELD_LENGTH = 4000    # evita logs gigantes

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
contatos_tbl = Table("Contatos", metadata, schema=f"schema_{sid}", autoload_with=engine)

ID_COLUMN = "Id do Cliente"


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


def chunk_list(items: list[dict], size: int):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def normalize_birthday(raw_value) -> str:
    birthday_str = verify_nan(raw_value)
    if birthday_str is None:
        return "1900-01-01"

    try:
        birthday_obj = datetime.strptime(str(birthday_str), "%d/%m/%Y")
        birthday = birthday_obj.strftime("%Y-%m-%d")
        if not is_valid_date(birthday, "%Y-%m-%d"):
            return "1900-01-01"
        return birthday
    except Exception:
        pass

    try:
        birthday_obj = datetime.strptime(str(birthday_str), "%Y-%m-%d")
        birthday = birthday_obj.strftime("%Y-%m-%d")
        if not is_valid_date(birthday, "%Y-%m-%d"):
            return "1900-01-01"
        return birthday
    except Exception:
        return "1900-01-01"


def normalize_sex(raw_value) -> str:
    return "F" if raw_value == "F" else "M"


def build_address(row) -> str | None:
    address = verify_nan(row.get("logradouro"))
    if not address:
        return None

    num = limpar_numero(verify_nan(row.get("numero")))
    if num:
        return f"{address} {num}"

    return address


def fetch_existing_ids(id_values: list) -> set:
    if not id_values:
        return set()

    stmt = select(contatos_tbl.c[ID_COLUMN]).where(
        contatos_tbl.c[ID_COLUMN].in_(bindparam("ids", expanding=True))
    )

    with engine.connect() as conn:
        rows = conn.execute(stmt, {"ids": id_values}).fetchall()

    return {row[0] for row in rows}


def build_insert_payload(row) -> tuple[dict | None, dict | None]:
    """
    Retorna:
      (payload_insert, log_erro)
    Um dos dois será None.
    """
    id_patient = row.get("idPaciente")
    if id_patient in [None, "", "NULL", "None"] or pd.isna(id_patient):
        return None, {"Motivo": "Id do Cliente vazio"}

    name = row.get("nome")
    if name in [None, "", "None"] or pd.isna(name):
        return None, {"Motivo": "Nome do Paciente vazio"}

    birthday = normalize_birthday(row.get("dataNascimento"))
    sex = normalize_sex(row.get("sexo"))
    email = verify_nan(row.get("email"))
    cpf = limpar_cpf(verify_nan(row.get("cpf")))
    rg = limpar_numero(verify_nan(row.get("rg")))
    telephone = None
    cellphone = limpar_numero(verify_nan(row.get("telCelular")))
    cep = limpar_numero(verify_nan(row.get("cep")))
    complement = verify_nan(row.get("complemento"))
    neighbourhood = verify_nan(row.get("bairro"))
    city = verify_nan(row.get("cidade"))
    state = verify_nan(row.get("uf"))
    occupation = verify_nan(row.get("profissao"))
    mother = None
    father = None
    address = build_address(row)

    payload = {
        "Nome": truncate_value(name, 50),
        "Nascimento": birthday,
        "Sexo": sex,
        "Celular": truncate_value(cellphone, 25),
        "Email": truncate_value(email, 100),
        "Id do Cliente": id_patient,
        "CPF/CGC": truncate_value(cpf, 25),
        "Cep Residencial": truncate_value(cep, 10),
        "Endereço Residencial": truncate_value(address, 50),
        "Endereço Comercial": truncate_value(complement, 50),
        "Bairro Residencial": truncate_value(neighbourhood, 25),
        "Cidade Residencial": truncate_value(city, 25),
        "Estado Residencial": truncate_value(state, 2),
        "Telefone Residencial": truncate_value(telephone, 25),
        "Profissão": truncate_value(occupation, 25),
        "Pai": truncate_value(father, 50),
        "Mãe": truncate_value(mother, 50),
        "RG": truncate_value(rg, 25),
    }

    return payload, None


def row_to_log_dict(row, extra: dict | None = None) -> dict:
    base = {}
    for key, value in dict(row).items():
        if pd.isna(value):
            base[key] = None
        else:
            base[key] = truncate_log_value(value)

    if extra:
        base.update(extra)

    return base


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
                row_to_log_dict(row_dict, {"Timestamp": timestamp, **validation_error}),
            )
            continue

        candidate_rows.append((row_dict, payload))
        candidate_ids.append(payload["Id do Cliente"])

    if not candidate_rows:
        return inserted_count, rejected_count

    # 2) remove duplicidade dentro do próprio chunk
    dedup_map = {}
    duplicated_inside_chunk = set()

    for row_dict, payload in candidate_rows:
        row_id = payload["Id do Cliente"]
        if row_id in dedup_map:
            duplicated_inside_chunk.add(row_id)
        dedup_map[row_id] = (row_dict, payload)

    for dup_id in duplicated_inside_chunk:
        rejected_count += 1
        row_dict, _ = dedup_map[dup_id]
        append_jsonl(
            rejected_log_file,
            row_to_log_dict(
                row_dict,
                {
                    "Timestamp": timestamp,
                    "Motivo": "Id do Cliente duplicado dentro do próprio arquivo/chunk",
                },
            ),
        )

    deduped_items = []
    seen = set()
    for row_dict, payload in candidate_rows:
        row_id = payload["Id do Cliente"]
        if row_id in duplicated_inside_chunk:
            if row_id in seen:
                continue
            seen.add(row_id)
            continue

        if row_id not in seen:
            deduped_items.append((row_dict, payload))
            seen.add(row_id)

    if not deduped_items:
        return inserted_count, rejected_count

    # 3) consulta existência em lote
    deduped_ids = [payload["Id do Cliente"] for _, payload in deduped_items]
    existing_ids = fetch_existing_ids(deduped_ids)

    to_insert = []

    for row_dict, payload in deduped_items:
        if payload["Id do Cliente"] in existing_ids:
            rejected_count += 1
            append_jsonl(
                rejected_log_file,
                row_to_log_dict(
                    row_dict,
                    {
                        "Timestamp": timestamp,
                        "Motivo": "Id do Cliente já existe",
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
            "Id do Cliente": bindparam("Id do Cliente"),
            "CPF/CGC": bindparam("CPF/CGC"),
            "Cep Residencial": bindparam("Cep Residencial"),
            "Endereço Residencial": bindparam("Endereço Residencial"),
            "Endereço Comercial": bindparam("Endereço Comercial"),
            "Bairro Residencial": bindparam("Bairro Residencial"),
            "Cidade Residencial": bindparam("Cidade Residencial"),
            "Estado Residencial": bindparam("Estado Residencial"),
            "Telefone Residencial": bindparam("Telefone Residencial"),
            "Profissão": bindparam("Profissão"),
            "Pai": bindparam("Pai"),
            "Mãe": bindparam("Mãe"),
            "RG": bindparam("RG"),
        }
    )

    # 4) insert em sublotes
    for subbatch in chunk_list(to_insert, INSERT_SUBBATCH_SIZE):
        payload_list = [payload for _, payload in subbatch]

        try:
            with engine.begin() as conn:
                conn.execute(insert_stmt, payload_list)

            inserted_count += len(subbatch)

            for row_dict, payload in subbatch:
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
    print("Sucesso! Inicializando migração de Contatos...")

    extension_file = glob.glob(f"{path_file}/pacientes*.csv")
    if not extension_file:
        raise FileNotFoundError(
            f"Nenhum arquivo encontrado com o padrão 'pacientes*.csv' em: {path_file}"
        )

    csv_file = extension_file[0]
    log_folder = Path(path_file).resolve()
    log_folder.mkdir(parents=True, exist_ok=True)

    inserted_log_file = log_folder / "log_inserted_patients_pacientes.jsonl"
    rejected_log_file = log_folder / "log_not_inserted_patients_pacientes.jsonl"
    error_log_file = log_folder / "log_insert_errors_patients_pacientes.jsonl"

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
        encoding="latin1",
        quotechar='"',
        dtype=object,
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

    print(f"{inserted_total} novos contatos foram inseridos com sucesso!")
    if rejected_total > 0:
        print(
            f"{rejected_total} contatos não foram inseridos, "
            f"verifique os logs JSONL para mais detalhes."
        )

    print(f"Log inseridos: {inserted_log_file}")
    print(f"Log rejeitados: {rejected_log_file}")
    print(f"Log erros: {error_log_file}")

    engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())