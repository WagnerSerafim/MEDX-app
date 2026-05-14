from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd
from striprtf.striprtf import rtf_to_text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.utils import is_valid_date, verify_nan


BATCH_SIZE = 5000
DEFAULT_USER_ID = 0
DEFAULT_DATE = "1900-01-01 00:00:00"
ENCODINGS_TO_TRY = ("utf-8", "utf-8-sig", "cp1252", "latin1")


@dataclass(frozen=True)
class RecordSource:
    file_name: str
    source_name: str
    text_column: str
    date_column: str
    patient_id_column: str
    source_id_column: str


RECORD_SOURCES = (
    RecordSource(
        file_name="ANAM_PAC.CSV",
        source_name="ANAM_PAC",
        text_column="Texto_Anamnese",
        date_column="Anam_Date",
        patient_id_column="ID_Pac",
        source_id_column="ID_Anam",
    ),
    RecordSource(
        file_name="CONS_PAC.CSV",
        source_name="CONS_PAC",
        text_column="Text",
        date_column="Date_Cons",
        patient_id_column="ID_Pac",
        source_id_column="ID_Cons",
    ),
    RecordSource(
        file_name="TEX_PAC.CSV",
        source_name="TEX_PAC",
        text_column="TextodoPaciente",
        date_column="Date",
        patient_id_column="ID_Pac",
        source_id_column="ID_Text",
    ),
)


def write_json_array(file_path: Path, payload: list[dict]) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)


def normalize_scalar(value):
    value = verify_nan(value)
    if value is None:
        return None

    value = str(value).strip()
    if value.endswith(".0"):
        value = value[:-2]

    return value or None


def normalize_id(value):
    normalized = normalize_scalar(value)
    if normalized is None:
        return None

    if normalized.lstrip("-").isdigit():
        try:
            return int(normalized)
        except ValueError:
            return normalized

    return normalized


def normalize_datetime(value) -> tuple[str, bool]:
    normalized = normalize_scalar(value)
    if normalized is None:
        return DEFAULT_DATE, True

    if is_valid_date(normalized, "%Y-%m-%d %H:%M:%S"):
        return normalized, False

    candidate_formats = (
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y",
    )

    for fmt in candidate_formats:
        try:
            parsed = datetime.strptime(normalized, fmt)
            if fmt in ("%Y-%m-%d", "%d/%m/%Y"):
                parsed = parsed.replace(hour=0, minute=0, second=0)
            return parsed.strftime("%Y-%m-%d %H:%M:%S"), False
        except ValueError:
            continue

    try:
        parsed = pd.to_datetime(normalized)
        if pd.isna(parsed):
            return DEFAULT_DATE, True
        return parsed.strftime("%Y-%m-%d %H:%M:%S"), False
    except Exception:
        return DEFAULT_DATE, True


def normalize_record_text(value):
    normalized = verify_nan(value)
    if normalized is None:
        return None

    try:
        text = rtf_to_text(str(normalized))
    except Exception:
        return None

    text = text.replace("_x000D_", "<br>").strip()
    return text or None


def build_issue_entry(source_record_id: str, errors=None, warnings=None, others=None):
    errors = errors or []
    warnings = warnings or []
    others = others or []
    return {
        "sourceRecordId": source_record_id,
        "errors": errors,
        "warnings": warnings,
        "others": others,
        "totalIssues": len(errors) + len(warnings) + len(others),
    }


def read_source_csv(file_path: Path) -> tuple[pd.DataFrame, str]:
    last_error = None

    for encoding in ENCODINGS_TO_TRY:
        try:
            df = pd.read_csv(
                file_path,
                sep="\t",
                quotechar='"',
                engine="python",
                dtype=object,
                encoding=encoding,
            )
            df = df.replace("None", "")
            return df, encoding
        except UnicodeDecodeError as exc:
            last_error = exc

    if last_error:
        raise last_error

    raise ValueError(f"Não foi possível ler o arquivo {file_path.name}.")


def build_source_record_id(source: RecordSource, row_dict: dict, row_number: int) -> str:
    source_id = normalize_scalar(row_dict.get(source.source_id_column))
    if source_id:
        return f"{source.source_name}:{source_id}"
    return f"{source.source_name}:row-{row_number}"


def validate_row(source: RecordSource, row_dict: dict, row_number: int):
    source_record_id = build_source_record_id(source, row_dict, row_number)
    errors = []
    warnings = []

    patient_id = normalize_id(row_dict.get(source.patient_id_column))
    if patient_id is None:
        errors.append(
            {
                "code": "RECORD_PATIENT_ID_MISSING",
                "message": "Id do paciente vazio ou inválido.",
                "field": source.patient_id_column,
            }
        )

    source_id = normalize_scalar(row_dict.get(source.source_id_column))
    if source_id is None:
        warnings.append(
            {
                "code": "RECORD_SOURCE_ID_MISSING",
                "message": "Id de origem ausente. O rastreamento do registro usará o número da linha.",
                "field": source.source_id_column,
            }
        )

    text = normalize_record_text(row_dict.get(source.text_column))
    if text is None:
        errors.append(
            {
                "code": "RECORD_TEXT_EMPTY",
                "message": "Histórico vazio, inválido ou com falha na conversão RTF.",
                "field": source.text_column,
            }
        )

    normalized_date, date_defaulted = normalize_datetime(row_dict.get(source.date_column))
    if date_defaulted:
        warnings.append(
            {
                "code": "RECORD_DATE_DEFAULTED",
                "message": f"Data inválida ou ausente. Foi aplicado o valor padrão {DEFAULT_DATE}.",
                "field": source.date_column,
            }
        )

    if errors:
        return None, build_issue_entry(source_record_id, errors=errors, warnings=warnings)

    payload = {
        "Id_do_Historico": None,
        "Id_do_Cliente": patient_id,
        "Historico": text,
        "Data": normalized_date,
        "Id_do_Usuario": DEFAULT_USER_ID,
    }

    if warnings:
        return payload, build_issue_entry(source_record_id, warnings=warnings)

    return payload, None


def flush_batch(
    batch_number: int,
    records_payload: list[dict],
    issues_payload: list[dict],
    data_dir: Path,
    errors_dir: Path,
) -> int:
    if not records_payload and not issues_payload:
        return batch_number

    batch_number += 1

    if records_payload:
        write_json_array(data_dir / f"records_batch{batch_number}.json", records_payload)

    if issues_payload:
        write_json_array(errors_dir / f"issues-records_batch{batch_number}.json", issues_payload)

    return batch_number


def main():
    path_file = (
        input("Informe o caminho da pasta que contém os arquivos CSV: ")
        .strip()
        .strip('"')
        .lstrip("\ufeff")
    )
    base_dir = Path(path_file).expanduser().resolve()

    if not base_dir.exists():
        raise FileNotFoundError(f"Pasta não encontrada: {base_dir}")

    logs_dir = base_dir / "LOGS"
    data_dir = logs_dir / "DATA"
    errors_dir = logs_dir / "ERRORS"
    data_dir.mkdir(parents=True, exist_ok=True)
    errors_dir.mkdir(parents=True, exist_ok=True)

    batch_records = []
    batch_issues = []
    batch_number = 0

    total_read = 0
    total_valid = 0
    total_issues = 0

    print("Iniciando validação de históricos do HiDoctor...")

    processed_sources = 0

    for source in RECORD_SOURCES:
        source_path = base_dir / source.file_name
        if not source_path.exists():
            print(f"Arquivo não encontrado, pulando: {source.file_name}")
            continue

        processed_sources += 1
        df, encoding = read_source_csv(source_path)
        print(
            f"Lendo {source.file_name} com encoding {encoding} | "
            f"linhas encontradas: {len(df)}"
        )

        for row_number, (_, row) in enumerate(df.iterrows(), start=1):
            total_read += 1

            payload, issue = validate_row(source, row.to_dict(), row_number)

            if payload is not None:
                batch_records.append(payload)
                total_valid += 1

            if issue is not None:
                batch_issues.append(issue)
                total_issues += 1

            if len(batch_records) >= BATCH_SIZE:
                batch_number = flush_batch(
                    batch_number=batch_number,
                    records_payload=batch_records,
                    issues_payload=batch_issues,
                    data_dir=data_dir,
                    errors_dir=errors_dir,
                )
                print(
                    f"Lote {batch_number} gravado | "
                    f"válidos acumulados: {total_valid} | "
                    f"issues acumuladas: {total_issues}"
                )
                batch_records = []
                batch_issues = []

    if processed_sources == 0:
        raise FileNotFoundError(
            f"Nenhum dos arquivos esperados foi encontrado em: {base_dir}"
        )

    batch_number = flush_batch(
        batch_number=batch_number,
        records_payload=batch_records,
        issues_payload=batch_issues,
        data_dir=data_dir,
        errors_dir=errors_dir,
    )

    print("Validação concluída!")
    print(f"Arquivos de origem processados: {processed_sources}")
    print(f"Registros lidos: {total_read}")
    print(f"Registros válidos: {total_valid}")
    print(f"Issues registradas: {total_issues}")
    print(f"Lotes gerados: {batch_number}")
    print(f"Saída de dados: {data_dir}")
    print(f"Saída de issues: {errors_dir}")
    print("O campo Id_do_Historico foi exportado como null e será ignorado pela entidade records.")


if __name__ == "__main__":
    main()
