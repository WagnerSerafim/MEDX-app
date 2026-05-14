import glob
import math
import os
import urllib
from datetime import datetime

import pandas as pd
from sqlalchemy import MetaData, Table, create_engine, insert, select
from sqlalchemy.pool import NullPool

from utils.utils import create_log


BATCH_SIZE = 500
EXISTING_ID_FETCH_CHUNK = 1000


def clean_value(value):
    if pd.isna(value) or value in [None, "None"]:
        return ""
    return str(value).strip()


def truncate_value(value, max_len):
    value = clean_value(value)
    return value[:max_len] if len(value) > max_len else value


def parse_birthdate(value):
    value = clean_value(value)
    if not value:
        return "1900-01-01"

    accepted_formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d/%m/%Y %H:%M:%S",
    ]

    for fmt in accepted_formats:
        try:
            dt = datetime.strptime(value, fmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass

    return "1900-01-01"


def normalize_sex(value):
    value = clean_value(value).lower()
    if value in {"f", "feminino"}:
        return "F"
    return "M"


def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]


def get_existing_ids(engine, contatos_table, ids_to_check):
    """
    Busca IDs existentes no banco em lotes, para evitar 1 SELECT por linha.
    """
    existing_ids = set()
    if not ids_to_check:
        return existing_ids

    id_column = contatos_table.c["Id do Cliente"]

    for batch_ids in chunked(ids_to_check, EXISTING_ID_FETCH_CHUNK):
        with engine.connect() as conn:
            stmt = select(id_column).where(id_column.in_(batch_ids))
            rows = conn.execute(stmt).fetchall()
            existing_ids.update(row[0] for row in rows if row[0] is not None)

    return existing_ids


def build_patient_dict(row, has_id, has_reference):
    name = truncate_value(row.get("NOME", ""), 50)
    if not name:
        return None, "Nome vazio"

    address = ""
    endereco = clean_value(row.get("ENDERECO", ""))
    numero = clean_value(row.get("NUMERO", ""))

    if endereco and numero:
        address = f"{endereco} {numero}"
    elif endereco:
        address = endereco

    patient = {
        "Nome": name,
        "Nascimento": parse_birthdate(row.get("NASCIMENTO", "")),
        "Sexo": normalize_sex(row.get("SEXO", "")),
        "Celular": truncate_value(row.get("CELULAR", ""), 25),
        "Email": truncate_value(row.get("EMAIL", ""), 100),
        "CPF/CGC": truncate_value(row.get("CPF", ""), 25),
        "Cep Residencial": truncate_value(row.get("CEP", ""), 10),
        "Endereço Residencial": truncate_value(address, 50),
        "Endereço Comercial": truncate_value(row.get("COMPLEMENTO", ""), 50),
        "Bairro Residencial": truncate_value(row.get("BAIRRO", ""), 25),
        "Cidade Residencial": truncate_value(row.get("CIDADE", ""), 25),
        "Telefone Residencial": truncate_value(row.get("TELEFONE", ""), 25),
        "Profissão": truncate_value(row.get("PROFISSAO", ""), 25),
        "Pai": truncate_value(row.get("PAI", ""), 50),
        "Mãe": truncate_value(row.get("MAE", ""), 50),
        "RG": truncate_value(row.get("RG", ""), 25),
        "Observações": clean_value(row.get("OBSERVACOES", "")),
    }

    if has_id:
        patient["Id do Cliente"] = int(clean_value(row["ID"]))

    if has_reference:
        reference = clean_value(row.get("REFERENCIAS", ""))
        if reference:
            patient["Referências"] = reference

    return patient, None


def main():
    sid = input("Informe o SoftwareID: ").strip()
    password = urllib.parse.quote_plus(input("Informe a senha: ").strip())
    dbase = input("Informe o DATABASE: ").strip()
    path_file = input("Informe o caminho da pasta que contém os arquivos: ").strip()

    print("Conectando no banco de dados...")

    database_url = (
        f"mssql+pyodbc://Medizin_{sid}:{password}"
        f"@medxserver.database.windows.net:1433/{dbase}"
        f"?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"
    )

    try:
        engine = create_engine(
            database_url,
            poolclass=NullPool,          # não mantém conexão presa
            fast_executemany=True,       # acelera inserção em lote no pyodbc
            use_insertmanyvalues=False,  # favorece executemany no SQL Server
            future=True,
        )

        metadata = MetaData()
        contatos = Table("Contatos", metadata, autoload_with=engine)
        contatos_columns = set(contatos.c.keys())

    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return

    print("Conexão bem-sucedida. Lendo planilha...")

    excel_files = glob.glob(os.path.join(path_file, "dados*.xlsx"))
    if not excel_files:
        print("Nenhum arquivo 'dados*.xlsx' foi encontrado.")
        return

    df = pd.read_excel(
        excel_files[0],
        sheet_name="pacientes",
        dtype=str
    ).fillna("")

    # Normaliza nomes das colunas uma vez só
    df.columns = [str(col).strip().upper() for col in df.columns]

    has_id = "ID" in df.columns
    has_db_id_column = "Id do Cliente" in contatos_columns
    has_reference = "REFERENCIAS" in df.columns and "Referências" in contatos_columns

    if has_id and not has_db_id_column:
        print("A planilha tem coluna ID, mas a tabela Contatos não possui 'Id do Cliente'.")
        print("Não é possível inserir mantendo o ID de origem nesse banco.")
        return

    if "REFERENCIAS" in df.columns and "Referências" not in contatos_columns:
        print("Aviso: coluna 'Referências' não existe na tabela Contatos deste banco e será ignorada.")

    inserted_log = []
    not_inserted_log = []

    # Pré-validação local dos IDs
    ids_from_file = []
    duplicated_ids_in_file = set()

    if has_id:
        seen = set()
        for raw_id in df["ID"].tolist():
            raw_id = clean_value(raw_id)

            if not raw_id:
                continue

            if not raw_id.isdigit():
                continue

            numeric_id = int(raw_id)
            if numeric_id in seen:
                duplicated_ids_in_file.add(numeric_id)
            else:
                seen.add(numeric_id)
                ids_from_file.append(numeric_id)

        print("Consultando IDs existentes no banco em lote...")
        existing_ids = get_existing_ids(engine, contatos, ids_from_file)
    else:
        existing_ids = set()

    rows_to_insert = []

    for row in df.to_dict(orient="records"):
        if has_id:
            raw_id = clean_value(row.get("ID", ""))

            if not raw_id:
                row["Motivo"] = "Id do Cliente vazio"
                not_inserted_log.append(row)
                continue

            if not raw_id.isdigit():
                row["Motivo"] = "Id do Cliente não é numérico"
                not_inserted_log.append(row)
                continue

            numeric_id = int(raw_id)

            if numeric_id in duplicated_ids_in_file:
                row["Motivo"] = "Id do Cliente duplicado no arquivo"
                not_inserted_log.append(row)
                continue

            if numeric_id in existing_ids:
                row["Motivo"] = "Id do Cliente já existe no banco de dados"
                not_inserted_log.append(row)
                continue

        patient_dict, reason = build_patient_dict(row, has_id, has_reference)
        if reason:
            row["Motivo"] = reason
            not_inserted_log.append(row)
            continue

        # Evita erro de "coluna inválida" quando o schema do cliente é diferente.
        patient_dict = {k: v for k, v in patient_dict.items() if k in contatos_columns}
        rows_to_insert.append(patient_dict)

    total_to_insert = len(rows_to_insert)
    inserted_count = 0

    print(f"Preparados {total_to_insert} registros para inserção.")

    # Inserção em lotes com conexões curtas
    for batch in chunked(rows_to_insert, BATCH_SIZE):
        try:
            with engine.begin() as conn:
                conn.execute(insert(contatos), batch)
            inserted_count += len(batch)
            inserted_log.extend(item.copy() for item in batch)
            print(f"Inseridos {inserted_count}/{total_to_insert}")
        except Exception as e:
            # Se um batch falhar, tenta inserção item a item para isolar erro real.
            for item in batch:
                try:
                    with engine.begin() as conn:
                        conn.execute(insert(contatos), [item])
                    inserted_count += 1
                    inserted_log.append(item.copy())
                except Exception as item_error:
                    fail_item = item.copy()
                    fail_item["Motivo"] = f"Erro ao inserir: {item_error} | erro_lote: {e}"
                    not_inserted_log.append(fail_item)

            print(f"Lote com falha, fallback individual concluído. Inseridos {inserted_count}/{total_to_insert}")

    print(f"{inserted_count} novos contatos foram inseridos com sucesso!")

    if not_inserted_log:
        print(f"{len(not_inserted_log)} contatos não foram inseridos, verifique o log.")

    os.makedirs(path_file, exist_ok=True)
    create_log(inserted_log, path_file, "log_inserted_patients.xlsx")
    create_log(not_inserted_log, path_file, "log_not_inserted_patients.xlsx")

    engine.dispose()
    print("Processo finalizado.")


if __name__ == "__main__":
    main()