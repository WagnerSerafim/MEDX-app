from datetime import datetime
from html import escape
from pathlib import Path
import csv
import json
import re
import sys
import time
import urllib.parse

import pandas as pd
from sqlalchemy import MetaData, Table, UnicodeText, bindparam, create_engine, select
from sqlalchemy.orm import sessionmaker


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.utils import clean_caracters  # noqa: E402


SOURCE_FILE_NAME = "medx_prontoario_geral.csv"
TARGET_TABLE = "Histórico de Clientes"
BATCH_SIZE = 200
QUERY_CHUNK_SIZE = 900
THROTTLE_SECONDS = 0.5
DEFAULT_DATETIME = "1900-01-01 00:00:00"
EXECUTION_CONFIRMATION = "MIGRAR"

REQUIRED_COLUMNS = {
    "IDENTIFICADOR",
    "IDENTIFICADOR_PACIENTE",
    "strFuncionario",
    "DATA_REFERENCIA",
}

TEXT_SECTIONS = [
    ("História", "HISTORIA"),
    ("Exame oftalmológico", "EXAME_OFTALMOLOGICO"),
    ("Hipóteses diagnósticas", "HIPOTESIS_DIAGNOSTICAS"),
    ("Conduta", "CONDUTA"),
    ("Prescrição / medicamentos", "medicamento_prescricao"),
]

GROUP_SECTIONS = [
    (
        "Olho direito",
        [
            ("Mikra", "MIKRA_OD"),
            ("Visus", "VISUS_OD"),
            ("Visus intermediário", "VISUS_OD_INT"),
            ("Visão subjetiva", "VISAO_SUBJETIVA_OD"),
            ("Medicamento injetado", "MEDICAMENTO_INJETADA_OD"),
            ("Achados angiográficos", "ACHADOS_ANGIOGRAFICOS_OD"),
            ("Tonometria", "tonometriaOD"),
            ("Pigment density mácula", "decMaculaPigmentDensity_OD"),
            ("Escavação papilar OCT", "decEscavacaoPapilarOCT_OD"),
            ("Espessura tumoral", "espessura_tumoral_OD"),
        ],
    ),
    (
        "Olho esquerdo",
        [
            ("Mikra", "MIKRA_OS"),
            ("Visus", "VISUS_OS"),
            ("Visus intermediário", "VISUS_OS_INT"),
            ("Visão subjetiva", "VISAO_SUBJETIVA_OS"),
            ("Medicamento injetado", "MEDICAMENTO_INJETADA_OS"),
            ("Achados angiográficos", "ACHADOS_ANGIOGRAFICOS_OS"),
            ("Tonometria", "tonometriaOS"),
            ("Pigment density mácula", "decMaculaPigmentDensity_OS"),
            ("Escavação papilar OCT", "decEscavacaoPapilarOCT_OS"),
            ("Espessura tumoral", "espessura_tumoral_OS"),
        ],
    ),
    (
        "Óculos / refração",
        [
            ("Esférico OD", "oculosEsfOD"),
            ("Esférico OS", "oculosEsfOS"),
            ("Cilíndrico OD", "oculosCilOD"),
            ("Cilíndrico OS", "oculosCilOS"),
            ("Eixo OD", "oculosEixOD"),
            ("Eixo OS", "oculosEixOS"),
            ("Adição", "oculosAdicao"),
            ("Descrição", "oculosStrDesc"),
        ],
    ),
    (
        "Plástica ocular",
        [
            ("Dermatocálase", "plastica_dermatocalase"),
            ("Alterações pálpebras inferiores", "plastica_alteracoes_palpebras_inferiores"),
            ("Frouxidão pálpebras inferiores", "plastica_frouxidao_palpebras_inferiores"),
            ("Motilidade ocular", "plastica_motilidade_ocular"),
            ("Distância margem-reflexo 1", "plastica_distancia_margem_reflexo_1"),
            ("Distância margem-reflexo 2", "plastica_distancia_margem_reflexo_2"),
            ("Teste de Bell", "plastica_teste_de_bell"),
            ("Força músculo orbicular", "plastica_forca_musculo_orbicular"),
            (
                "Função músculo levantador da pálpebra superior",
                "plastica_funcao_musculo_levantador_palpebra_superior",
            ),
            ("Supercílio", "plastica_supercilio"),
            ("Ptose palpebral", "plastica_ptose_palpebral"),
            ("Fototipo de pele", "plastica_fototipo_de_pele"),
            ("Manchas na pele", "plastica_manchas_na_pele"),
            ("Biomicroscopia", "plastica_biomicroscopia"),
            ("História pregressa", "plastica_historia_pregressa"),
            ("Rugas visíveis", "plastica_rugas_visiveis"),
            ("Tratamentos prévios", "plastica_tratamentos_previos"),
        ],
    ),
    (
        "Estrabismo",
        [
            ("Característica do desvio ocular", "estrabismo_caracteristica_desvio_ocular"),
            ("Desvio ocular", "estrabismo_desvio_ocular"),
            ("Krimsky", "estrabismo_krimsky"),
            ("Cover test", "estrabismo_cover_test"),
            ("Versões", "estrabismo_versoes"),
            ("Oclusão ocular", "estrabismo_oclusao_ocular"),
        ],
    ),
]


def now_text():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def clean_value(value):
    if value is None:
        return None
    if pd.isna(value):
        return None

    text = clean_caracters(str(value)).strip()
    if text.lower() in {"", "nan", "none", "null", "nul"}:
        return None

    return text or None


def clean_spaces(value):
    text = clean_value(value)
    if text is None:
        return None

    return re.sub(r"\s+", " ", text).strip() or None


def normalize_datetime(value):
    text = clean_value(value)
    if text:
        for date_format in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(text, date_format)
                if 1900 <= parsed.year <= 2100:
                    if date_format == "%Y-%m-%d":
                        return f"{text} 00:00:00"
                    return text
            except ValueError:
                continue

    return DEFAULT_DATETIME


def paragraphize_text(value):
    text = clean_value(value)
    if text is None:
        return ""

    text = text.replace("\r\n", "\n").replace("\r", "\n")
    blocks = [block.strip() for block in re.split(r"\n\s*\n+", text) if block.strip()]
    paragraphs = []

    for block in blocks:
        lines = [escape(line.strip()) for line in block.split("\n") if line.strip()]
        if lines:
            paragraphs.append(f"<p>{'<br>'.join(lines)}</p>")

    return "\n".join(paragraphs)


def build_field_paragraph(label, value):
    text = paragraphize_text(value)
    if not text:
        return ""

    return f"<h4>{escape(label)}</h4>\n{text}"


def build_group_section(title, fields, row):
    items = []
    for label, column in fields:
        text = clean_value(row.get(column))
        if not text:
            continue

        item = paragraphize_text(text)
        if item:
            items.append(f"<p><strong>{escape(label)}:</strong></p>\n{item}")

    if not items:
        return ""

    return f"<h4>{escape(title)}</h4>\n" + "\n".join(items)


def build_history_html(row):
    professional = clean_spaces(row.get("strFuncionario"))
    parts = ["<h3>Prontuário geral</h3>"]

    if professional:
        parts.append(f"<p><strong>Profissional:</strong> {escape(professional)}</p>")

    parts.append("<hr>")

    for title, column in TEXT_SECTIONS:
        section = build_field_paragraph(title, row.get(column))
        if section:
            parts.append(section)

    for title, fields in GROUP_SECTIONS:
        section = build_group_section(title, fields, row)
        if section:
            parts.append(section)

    return "\n".join(parts)


def has_history_content(row):
    for _, column in TEXT_SECTIONS:
        if clean_value(row.get(column)):
            return True

    for _, fields in GROUP_SECTIONS:
        for _, column in fields:
            if clean_value(row.get(column)):
                return True

    return False


def validate_columns(df):
    columns_to_check = set(REQUIRED_COLUMNS)
    columns_to_check.update(column for _, column in TEXT_SECTIONS)
    for _, fields in GROUP_SECTIONS:
        columns_to_check.update(column for _, column in fields)

    missing_columns = sorted(columns_to_check - set(df.columns))
    if missing_columns:
        raise ValueError(f"Colunas obrigatórias ausentes: {missing_columns}")


def load_source_csv(path_file):
    csv.field_size_limit(20_000_000)
    source_path = Path(path_file) / SOURCE_FILE_NAME
    if not source_path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {source_path}")

    df = pd.read_csv(
        source_path,
        sep=";",
        engine="python",
        encoding="utf-8",
        dtype=str,
        keep_default_na=False,
    )
    validate_columns(df)
    return df


def connect_database():
    sid = input("Informe o SoftwareID: ").strip()
    password = urllib.parse.quote_plus(input("Informe a senha: "))
    dbase = input("Informe o DATABASE: ").strip()

    database_url = (
        f"mssql+pyodbc://Medizin_{sid}:{password}"
        f"@medxserver.database.windows.net:1433/{dbase}"
        "?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"
    )

    engine = create_engine(database_url, fast_executemany=False, pool_pre_ping=True)
    metadata = MetaData()
    historico_tbl = Table(TARGET_TABLE, metadata, schema=f"schema_{sid}", autoload_with=engine)
    contatos_tbl = Table("Contatos", metadata, schema=f"schema_{sid}", autoload_with=engine)

    return engine, historico_tbl, contatos_tbl


def iter_chunks(items, chunk_size):
    for start in range(0, len(items), chunk_size):
        yield items[start : start + chunk_size]


def fetch_existing_patient_ids(session, contatos_tbl, patient_ids):
    existing_ids = set()
    id_column = contatos_tbl.c["Id do Cliente"]

    for id_chunk in iter_chunks(patient_ids, QUERY_CHUNK_SIZE):
        result = session.execute(select(id_column).where(id_column.in_(id_chunk)))
        existing_ids.update(str(row[0]) for row in result if row[0] is not None)

    return existing_ids


def fetch_existing_record_ids(session, historico_tbl, record_ids):
    existing_ids = set()
    id_column = historico_tbl.c["Id do Histórico"]

    for id_chunk in iter_chunks(record_ids, QUERY_CHUNK_SIZE):
        result = session.execute(select(id_column).where(id_column.in_(id_chunk)))
        existing_ids.update(str(row[0]) for row in result if row[0] is not None)

    return existing_ids


def row_to_log(row, payload, reason=None):
    log_row = row.to_dict()
    log_row.update(payload)
    if reason:
        log_row["Motivo"] = reason
    log_row["Timestamp"] = now_text()
    return log_row


def write_json_log(log_data, log_folder, log_name):
    log_path = Path(log_folder) / log_name
    with open(log_path, "w", encoding="utf-8") as log_file:
        json.dump(log_data, log_file, ensure_ascii=False, default=str, indent=2)


def make_payload(row):
    source_id = clean_spaces(row.get("IDENTIFICADOR"))
    patient_id = clean_spaces(row.get("IDENTIFICADOR_PACIENTE"))

    payload = {
        "Id do Histórico": source_id,
        "Id do Cliente": patient_id,
        "Id do Usuário": 0,
        "Data": normalize_datetime(row.get("DATA_REFERENCIA")),
        "Histórico": build_history_html(row),
    }

    return {key: value for key, value in payload.items() if value is not None}


def prepare_rows(df, existing_patient_ids, existing_record_ids):
    payloads = []
    preview_log = []
    not_inserted_log = []
    seen_record_ids = set()
    invalid_dates_count = 0

    for _, row in df.iterrows():
        payload = make_payload(row)
        source_id = clean_spaces(row.get("IDENTIFICADOR"))
        patient_id = payload.get("Id do Cliente")

        if payload.get("Data") == DEFAULT_DATETIME:
            invalid_dates_count += 1

        if not source_id:
            not_inserted_log.append(row_to_log(row, payload, "Id do Histórico vazio"))
            continue

        if not patient_id:
            not_inserted_log.append(row_to_log(row, payload, "Id do Cliente vazio"))
            continue

        if patient_id not in existing_patient_ids:
            not_inserted_log.append(row_to_log(row, payload, "Paciente não encontrado no destino"))
            continue

        if not has_history_content(row):
            not_inserted_log.append(row_to_log(row, payload, "Prontuário sem conteúdo clínico"))
            continue

        if source_id in seen_record_ids:
            not_inserted_log.append(row_to_log(row, payload, "Prontuário duplicado no CSV"))
            continue

        seen_record_ids.add(source_id)

        if source_id in existing_record_ids:
            not_inserted_log.append(row_to_log(row, payload, "Prontuário já existe no banco"))
            continue

        payloads.append(payload)
        preview_log.append(row_to_log(row, payload))

    return payloads, preview_log, not_inserted_log, invalid_dates_count


def build_insert_statement(historico_tbl):
    return historico_tbl.insert().values({
        "Id do Histórico": bindparam("Id do Histórico"),
        "Id do Cliente": bindparam("Id do Cliente"),
        "Id do Usuário": bindparam("Id do Usuário"),
        "Data": bindparam("Data"),
        "Histórico": bindparam("Histórico", type_=UnicodeText()),
    })


def insert_payloads(session, historico_tbl, payloads):
    inserted_log = []
    not_inserted_log = []
    insert_statement = build_insert_statement(historico_tbl)

    for batch_number, batch in enumerate(iter_chunks(payloads, BATCH_SIZE), start=1):
        try:
            session.execute(insert_statement, batch)
            session.commit()
            inserted_log.extend(batch)
            print(f"Lote {batch_number}: {len(batch)} prontuários gerais inseridos.")
        except Exception as batch_error:
            session.rollback()
            print(f"Lote {batch_number}: erro, tentando isolar linhas individualmente.")

            for payload in batch:
                try:
                    session.execute(insert_statement, [payload])
                    session.commit()
                    inserted_log.append(payload)
                except Exception as row_error:
                    session.rollback()
                    failed_row = dict(payload)
                    failed_row["Motivo"] = (
                        f"Erro no lote: {type(batch_error).__name__}: {batch_error}; "
                        f"Erro na linha: {type(row_error).__name__}: {row_error}"
                    )
                    failed_row["Timestamp"] = now_text()
                    not_inserted_log.append(failed_row)

        if THROTTLE_SECONDS:
            time.sleep(THROTTLE_SECONDS)

    return inserted_log, not_inserted_log


def main():
    print("=== Migração segura de prontuário geral - Dra. Paula ===")
    path_file = input("Informe o caminho da pasta que contém o medx_prontoario_geral.csv: ").strip()
    log_folder = Path(path_file)
    df = load_source_csv(log_folder)

    print(f"CSV carregado: {len(df)} prontuários gerais.")
    print("Conectando no banco para refletir tabelas e checar vínculos em massa...")
    engine, historico_tbl, contatos_tbl = connect_database()
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()

    try:
        source_patient_ids = sorted({
            clean_spaces(value)
            for value in df["IDENTIFICADOR_PACIENTE"].tolist()
            if clean_spaces(value)
        })
        source_record_ids = sorted({
            clean_spaces(value)
            for value in df["IDENTIFICADOR"].tolist()
            if clean_spaces(value)
        })

        existing_patient_ids = fetch_existing_patient_ids(session, contatos_tbl, source_patient_ids)
        existing_record_ids = fetch_existing_record_ids(session, historico_tbl, source_record_ids)
        payloads, preview_log, not_inserted_log, invalid_dates_count = prepare_rows(
            df,
            existing_patient_ids,
            existing_record_ids,
        )

        print("\n=== Pré-validação ===")
        print(f"Total lido: {len(df)}")
        print(f"Pacientes distintos no CSV: {len(source_patient_ids)}")
        print(f"Pacientes encontrados no destino: {len(existing_patient_ids)}")
        print(f"Prontuários já existentes no banco: {len(existing_record_ids)}")
        print(f"Datas vazias/inválidas ajustadas para {DEFAULT_DATETIME}: {invalid_dates_count}")
        print(f"Prontos para inserção: {len(payloads)}")
        print(f"Não inseridos previstos: {len(not_inserted_log)}")

        write_json_log(preview_log, log_folder, "log_preview_records_prontuario_geral.json")
        if not_inserted_log:
            write_json_log(
                not_inserted_log,
                log_folder,
                "log_not_inserted_records_prontuario_geral.json",
            )

        confirmation = input(
            f"\nDry-run concluído. Digite {EXECUTION_CONFIRMATION} para inserir no banco: "
        ).strip()

        if confirmation != EXECUTION_CONFIRMATION:
            print("Execução encerrada em modo dry-run. Nenhum dado foi inserido.")
            return

        print("\nIniciando inserção em lotes pequenos...")
        inserted_log, insert_errors_log = insert_payloads(session, historico_tbl, payloads)
        not_inserted_log.extend(insert_errors_log)

        write_json_log(inserted_log, log_folder, "log_inserted_records_prontuario_geral.json")
        write_json_log(
            not_inserted_log,
            log_folder,
            "log_not_inserted_records_prontuario_geral.json",
        )

        print("\n=== Resumo final ===")
        print(f"Inseridos: {len(inserted_log)}")
        print(f"Não inseridos: {len(not_inserted_log)}")
        print(f"Logs gravados em: {log_folder}")
    finally:
        session.close()
        engine.dispose()


if __name__ == "__main__":
    main()
