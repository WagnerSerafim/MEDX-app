from __future__ import annotations

import csv
import glob
import html
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
INSERT_SUBBATCH_SIZE = 1000
REFERENCE_QUERY_BATCH_SIZE = 1000
LOG_MAX_FIELD_LENGTH = 4000

HIST_CLIENT_COLUMN = "Id do Cliente"
HIST_DATE_COLUMN = "Data"
HIST_TEXT_COLUMN = "Histórico"
HIST_USER_COLUMN = "Id do Usuário"

CONTACT_ID_COLUMN = "Id do Cliente"
CONTACT_NAME_COLUMN = "Nome"
CONTACT_NAME_MAX_LENGTH = 50


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


def normalize_text(value) -> str | None:
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

    return " ".join(value.split())[:CONTACT_NAME_MAX_LENGTH]


def html_text(value: str) -> str:
    return html.escape(value).replace("\n", "<br>")


def normalize_datetime(value) -> str | None:
    value = normalize_scalar(value)
    if value is None:
        return None

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
        return None

    return parsed.to_pydatetime().strftime("%Y-%m-%d %H:%M:%S")


def datetime_sort_value(value: str | None) -> datetime:
    if value is None:
        return datetime.max

    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return datetime.max


def time_sort_value(value) -> tuple[int, int]:
    value = normalize_scalar(value)
    if value is None:
        return (99, 99)

    for parse_format in ("%H:%M", "%H:%M:%S"):
        try:
            parsed = datetime.strptime(value, parse_format)
            return (parsed.hour, parsed.minute)
        except ValueError:
            continue

    return (99, 99)


def add_line(parts: list[str], value: str | None) -> None:
    if value is None:
        return

    parts.append(html_text(value))


def build_meal_title(row: dict) -> str | None:
    meal_type = normalize_text(row.get("Tipo Refeição"))
    meal_time = normalize_text(row.get("Horário"))

    if meal_type and meal_time:
        return f"{meal_type} - {meal_time}"
    if meal_type:
        return meal_type
    if meal_time:
        return meal_time

    return None


def build_history_text(group_df: pd.DataFrame) -> str | None:
    first_row = group_df.iloc[0].to_dict()

    cardapio = normalize_text(first_row.get("Nome Cardápio"))
    patient_name = normalize_text(first_row.get("Paciente"))

    creation_dates = []
    for value in group_df["Creation Date"].tolist():
        normalized_date = normalize_datetime(value)
        if normalized_date is not None:
            creation_dates.append(normalized_date)

    modified_dates = []
    for value in group_df["Modified Date"].tolist():
        normalized_date = normalize_datetime(value)
        if normalized_date is not None:
            modified_dates.append(normalized_date)

    created_at = min(creation_dates, key=datetime_sort_value) if creation_dates else None
    modified_at = max(modified_dates, key=datetime_sort_value) if modified_dates else None

    parts: list[str] = []
    add_line(parts, cardapio)
    add_line(parts, patient_name)
    add_line(parts, created_at)
    add_line(parts, modified_at)

    meal_parts: list[str] = []
    meal_rows = [meal.to_dict() for _, meal in group_df.iterrows()]
    meal_rows.sort(
        key=lambda row: (
            time_sort_value(row.get("Horário")),
            normalize_scalar(row.get("ID")) or "",
        )
    )

    for row in meal_rows:
        meal_lines: list[str] = []

        title = build_meal_title(row)
        if title is not None:
            meal_lines.append(html_text(title))

        add_line(meal_lines, normalize_text(row.get("Observação")))
        add_line(meal_lines, normalize_text(row.get("Sub")))
        add_line(meal_lines, normalize_text(row.get("Observações Sub")))

        if meal_lines:
            meal_parts.append("<br>".join(meal_lines))

    if not meal_parts:
        return None

    if meal_parts:
        if parts:
            parts.append("")
        parts.append("<br><br>".join(meal_parts))

    history_text = "<br>".join(parts).strip()
    return history_text or None


def build_group_date(group_df: pd.DataFrame) -> str:
    creation_dates = []
    for value in group_df["Creation Date"].tolist():
        normalized_date = normalize_datetime(value)
        if normalized_date is not None:
            creation_dates.append(normalized_date)

    if not creation_dates:
        return "1900-01-01 00:00:00"

    return min(creation_dates, key=datetime_sort_value)


def fetch_client_ids_by_name(name_values: list[str]) -> dict[str, list[str]]:
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

    result: dict[str, list[str]] = {}
    unique_names = list(dict.fromkeys(normalized_names))

    with engine.connect() as conn:
        for start in range(0, len(unique_names), REFERENCE_QUERY_BATCH_SIZE):
            names_batch = unique_names[start:start + REFERENCE_QUERY_BATCH_SIZE]
            rows = conn.execute(stmt, {"names": names_batch}).fetchall()

            for row in rows:
                name = normalize_contact_name(row[0])
                client_id = normalize_scalar(row[1])

                if name is None or client_id is None:
                    continue

                result.setdefault(name, []).append(client_id)

    return result


def resolve_csv_file(base_path: str) -> str:
    patterns = [
        "Refeições.csv",
        "Refeicoes.csv",
        "*Refeições*.csv",
        "*Refeicoes*.csv",
    ]

    for pattern in patterns:
        matches = glob.glob(str(Path(base_path) / pattern))
        if matches:
            return matches[0]

    raise FileNotFoundError(
        f"Nenhum arquivo encontrado com o nome 'Refeições.csv' em: {base_path}"
    )


def read_csv_file(csv_file: str) -> tuple[pd.DataFrame, str]:
    encodings_to_try = ["utf-8", "utf-8-sig", "cp1252", "latin1"]

    for encoding in encodings_to_try:
        try:
            df = pd.read_csv(
                csv_file,
                sep=",",
                encoding=encoding,
                quotechar='"',
                dtype=object,
            )
            return df.replace("None", ""), encoding
        except UnicodeDecodeError:
            continue

    raise UnicodeDecodeError("csv", b"", 0, 1, "Nao foi possivel decodificar o CSV")


def build_group_log_row(group_df: pd.DataFrame) -> dict:
    first_row = group_df.iloc[0].to_dict()
    return {
        "Paciente": normalize_text(first_row.get("Paciente")),
        "Nome Cardápio": normalize_text(first_row.get("Nome Cardápio")),
        "Quantidade de refeições": len(group_df),
        "Menor Creation Date": build_group_date(group_df),
    }


def build_payloads(
    df: pd.DataFrame,
    rejected_log_file: Path,
) -> list[tuple[dict, dict]]:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    required_columns = ["Paciente", "Nome Cardápio", "Creation Date", "Modified Date", "Horário"]
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        raise ValueError(f"Colunas obrigatorias ausentes: {', '.join(missing_columns)}")

    group_columns = ["Paciente", "Nome Cardápio"]
    groups = list(df.groupby(group_columns, dropna=False, sort=False))

    patient_names = [
        normalize_contact_name(group_key[0])
        for group_key, _ in groups
        if normalize_contact_name(group_key[0]) is not None
    ]
    name_to_client_ids = fetch_client_ids_by_name(patient_names)

    to_insert: list[tuple[dict, dict]] = []
    for (patient_name_raw, cardapio_raw), group_df in groups:
        patient_name = normalize_text(patient_name_raw)
        cardapio = normalize_text(cardapio_raw)
        log_row = build_group_log_row(group_df)

        if patient_name is None:
            append_jsonl(
                rejected_log_file,
                row_to_log_dict(
                    log_row,
                    {
                        "Timestamp": timestamp,
                        "Motivo": "Paciente vazio no grupo de refeicoes",
                    },
                ),
            )
            continue

        if cardapio is None:
            append_jsonl(
                rejected_log_file,
                row_to_log_dict(
                    log_row,
                    {
                        "Timestamp": timestamp,
                        "Motivo": "Nome Cardápio vazio no grupo de refeicoes",
                    },
                ),
            )
            continue

        patient_contact_name = normalize_contact_name(patient_name)
        client_ids = name_to_client_ids.get(patient_contact_name, [])
        if not client_ids:
            append_jsonl(
                rejected_log_file,
                row_to_log_dict(
                    log_row,
                    {
                        "Timestamp": timestamp,
                        "Motivo": "Paciente nao encontrado em Contatos pela coluna [Nome]",
                        "Paciente normalizado": patient_name,
                        "Nome buscado em Contatos": patient_contact_name,
                    },
                ),
            )
            continue

        if len(client_ids) > 1:
            append_jsonl(
                rejected_log_file,
                row_to_log_dict(
                    log_row,
                    {
                        "Timestamp": timestamp,
                        "Motivo": "Mais de um contato encontrado com o mesmo [Nome]",
                        "Paciente normalizado": patient_name,
                        "Nome buscado em Contatos": patient_contact_name,
                        "Ids encontrados": ", ".join(client_ids),
                    },
                ),
            )
            continue

        history_text = build_history_text(group_df)
        if history_text is None:
            append_jsonl(
                rejected_log_file,
                row_to_log_dict(
                    log_row,
                    {
                        "Timestamp": timestamp,
                        "Motivo": "Historico vazio apos montagem do grupo",
                    },
                ),
            )
            continue

        payload = {
            HIST_DATE_COLUMN: build_group_date(group_df),
            HIST_TEXT_COLUMN: history_text,
            HIST_CLIENT_COLUMN: client_ids[0],
            HIST_USER_COLUMN: 0,
        }
        to_insert.append((log_row, payload))

    return to_insert


def insert_payloads(
    to_insert: list[tuple[dict, dict]],
    inserted_log_file: Path,
    error_log_file: Path,
) -> tuple[int, int]:
    inserted_count = 0
    error_count = 0
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if not to_insert:
        return inserted_count, error_count

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
            for log_row, payload in subbatch:
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
                    error_count += 1
                    append_jsonl(
                        error_log_file,
                        row_to_log_dict(
                            log_row,
                            {
                                "Timestamp": timestamp,
                                "Motivo": f"Erro ao inserir: {item_error} | erro_sublote: {batch_error}",
                            },
                        ),
                    )

    return inserted_count, error_count


def main() -> None:
    print("Sucesso! Inicializando migracao de Historico de Refeicoes...")

    csv.field_size_limit(100000000)

    csv_file = resolve_csv_file(path_file)
    log_folder = Path(path_file).resolve()
    log_folder.mkdir(parents=True, exist_ok=True)

    inserted_log_file = log_folder / "log_inserted_records_refeicoes.jsonl"
    rejected_log_file = log_folder / "log_not_inserted_records_refeicoes.jsonl"
    error_log_file = log_folder / "log_insert_errors_records_refeicoes.jsonl"

    inserted_log_file.write_text("", encoding="utf-8")
    rejected_log_file.write_text("", encoding="utf-8")
    error_log_file.write_text("", encoding="utf-8")

    df, chosen_encoding = read_csv_file(csv_file)
    print(f"Arquivo CSV: {csv_file}")
    print(f"Encoding escolhido: {chosen_encoding}")
    print(f"Linhas lidas: {len(df)}")

    to_insert = build_payloads(df, rejected_log_file)
    inserted_total, error_total = insert_payloads(
        to_insert,
        inserted_log_file,
        error_log_file,
    )

    with rejected_log_file.open("r", encoding="utf-8") as file:
        rejected_total = sum(1 for _ in file)
    not_inserted_total = rejected_total + error_total

    print(f"{inserted_total} novos historicos de refeicoes foram inseridos com sucesso!")
    if not_inserted_total > 0:
        print(
            f"{not_inserted_total} historicos de refeicoes nao foram inseridos, "
            f"verifique os logs JSONL para mais detalhes."
        )

    print(f"Log inseridos: {inserted_log_file}")
    print(f"Log rejeitados: {rejected_log_file}")
    print(f"Log erros: {error_log_file}")

    engine.dispose()


if __name__ == "__main__":
    main()
