import os
from datetime import datetime

from sqlalchemy import MetaData, Table, insert, text
from sqlalchemy.orm import sessionmaker

from medsystem.core.db import build_source_engine, build_target_engine
from medsystem.core.logs import write_jsonl_log
from utils.utils import is_valid_date, limpar_numero, truncate_value, verify_nan


BATCH_SIZE = 1000


def _extract_date_part(raw_value):
    value = verify_nan(raw_value)
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.date()

    text_value = str(value).strip()
    if not text_value:
        return None

    date_text = text_value.split(" ")[0]
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(date_text, fmt).date()
        except ValueError:
            continue
    return None


def _extract_time_part(raw_value):
    value = verify_nan(raw_value)
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.time().replace(microsecond=0)

    text_value = str(value).strip()
    if not text_value:
        return None

    if " " in text_value:
        text_value = text_value.split(" ")[-1]
    text_value = text_value.split(".")[0]

    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(text_value, fmt).time()
        except ValueError:
            continue
    return None


def _compose_datetime(date_raw, time_raw):
    date_part = _extract_date_part(date_raw)
    time_part = _extract_time_part(time_raw)

    if date_part is None or time_part is None:
        return None

    composed = datetime.combine(date_part, time_part).strftime("%Y-%m-%d %H:%M:%S")
    if not is_valid_date(composed, "%Y-%m-%d %H:%M:%S"):
        return None
    return composed


def _format_error_message(error):
    details = []
    message = str(error).strip()
    if message:
        details.append(message)

    original = getattr(error, "orig", None)
    if original is not None:
        original_message = str(original).strip()
        if original_message and original_message not in details:
            details.append(original_message)

        original_args = getattr(original, "args", None)
        if original_args:
            args_text = " | ".join(str(item) for item in original_args if str(item).strip())
            if args_text and args_text not in details:
                details.append(args_text)

    if details:
        return " | ".join(details)
    return repr(error)


def _flush_batch(session, agenda_tbl, payload, existing_schedule_ids, inserted_log, not_inserted_log):
    inserted_count = 0
    not_inserted_count = 0

    if not payload:
        return inserted_count, not_inserted_count

    try:
        records_to_insert = [item[1] for item in payload]
        session.execute(insert(agenda_tbl), records_to_insert)
        session.commit()
        inserted_count += len(records_to_insert)
        inserted_log.extend(records_to_insert)
        for committed_id, _ in payload:
            existing_schedule_ids.add(committed_id)
        payload.clear()
        return inserted_count, not_inserted_count
    except Exception as batch_error:
        session.rollback()
        batch_error_message = _format_error_message(batch_error)

    for schedule_key, record in payload:
        try:
            session.execute(insert(agenda_tbl), [record])
            session.commit()
            inserted_count += 1
            inserted_log.append(record)
            existing_schedule_ids.add(schedule_key)
        except Exception as row_error:
            session.rollback()
            not_inserted_count += 1
            row_error_message = _format_error_message(row_error)
            not_inserted_log.append(
                {
                    **record,
                    "Motivo": (
                        f"Falha no commit do lote: {batch_error_message} | "
                        f"Falha no registro: {row_error_message}"
                    ),
                }
            )

    payload.clear()
    return inserted_count, not_inserted_count


def main():
    source_engine, source_database = build_source_engine()
    target_engine, sid, dbase = build_target_engine()

    log_folder = input("Informe a pasta para salvar os logs: ").strip()
    os.makedirs(log_folder, exist_ok=True)

    print("Conectando e preparando metadados de destino...")
    metadata = MetaData()
    agenda_tbl = Table("Agenda", metadata, schema=f"schema_{sid}", autoload_with=target_engine)

    SessionLocal = sessionmaker(bind=target_engine, future=True)

    inserted_log = []
    not_inserted_log = []

    inserted_count = 0
    not_inserted_count = 0
    total_processed = 0

    with SessionLocal() as session:
        existing_schedule_ids_result = session.execute(text(f"SELECT [Id do Agendamento] FROM [schema_{sid}].[Agenda]"))
        existing_schedule_ids = {str(row[0]) for row in existing_schedule_ids_result if row[0] is not None}

        contacts_result = session.execute(text(f"SELECT [Id do Cliente], [Nome] FROM [schema_{sid}].[Contatos]"))
        patient_name_by_id = {str(row[0]): row[1] for row in contacts_result if row[0] is not None}

        print(f"Agendamentos existentes carregados: {len(existing_schedule_ids)}")
        print(f"Pacientes em memória para descrição: {len(patient_name_by_id)}")

        source_query = text(
            """
            SELECT
                [Código] AS CODIGO,
                [Código do Cliente] AS CODIGO_CLIENTE,
                [Código do Usuário] AS CODIGO_USUARIO,
                [Data] AS DATA_INICIO,
                [Hora] AS HORA_INICIO,
                [DataFinal] AS DATA_FINAL,
                [HoraFinal] AS HORA_FINAL
            FROM [dbo].[SWConsultas]
            ORDER BY [Código]
            """
        )

        payload = []

        with source_engine.connect().execution_options(stream_results=True) as source_conn:
            rows = source_conn.execute(source_query).mappings()

            for row in rows:
                total_processed += 1

                try:
                    id_schedule = limpar_numero(verify_nan(row["CODIGO"]))
                    if id_schedule is None:
                        not_inserted_count += 1
                        not_inserted_log.append({**dict(row), "Motivo": "Id do Agendamento vazio"})
                        continue

                    schedule_key = str(id_schedule)
                    if schedule_key in existing_schedule_ids:
                        not_inserted_count += 1
                        not_inserted_log.append({**dict(row), "Motivo": "Id do Agendamento já existe"})
                        continue

                    id_patient = limpar_numero(verify_nan(row["CODIGO_CLIENTE"]))
                    if id_patient is None:
                        not_inserted_count += 1
                        not_inserted_log.append({**dict(row), "Motivo": "Id do Cliente vazio"})
                        continue

                    patient_key = str(id_patient)
                    name_patient = patient_name_by_id.get(patient_key)
                    if not name_patient:
                        not_inserted_count += 1
                        not_inserted_log.append({**dict(row), "Motivo": "Paciente não encontrado na tabela Contatos"})
                        continue

                    id_user = limpar_numero(verify_nan(row["CODIGO_USUARIO"]))
                    if id_user is None:
                        not_inserted_count += 1
                        not_inserted_log.append({**dict(row), "Motivo": "Id do Usuário vazio"})
                        continue

                    if str(id_user) == "1":
                        id_user = 10

                    start_time = _compose_datetime(row["DATA_INICIO"], row["HORA_INICIO"])
                    if start_time is None:
                        not_inserted_count += 1
                        not_inserted_log.append({**dict(row), "Motivo": "Data/Hora inicial inválida"})
                        continue

                    end_time = _compose_datetime(row["DATA_FINAL"], row["HORA_FINAL"])
                    if end_time is None:
                        end_time = start_time

                    rec = {
                        "Id do Agendamento": id_schedule,
                        "Vinculado a": id_patient,
                        "Id do Usuário": id_user,
                        "Descrição": truncate_value(name_patient, 255),
                        "Início": start_time,
                        "Final": end_time,
                        "Status": 1,
                    }

                    payload.append((schedule_key, rec))

                    if len(payload) >= BATCH_SIZE:
                        inserted_delta, not_inserted_delta = _flush_batch(
                            session,
                            agenda_tbl,
                            payload,
                            existing_schedule_ids,
                            inserted_log,
                            not_inserted_log,
                        )
                        inserted_count += inserted_delta
                        not_inserted_count += not_inserted_delta

                    if total_processed % 1000 == 0:
                        print(
                            f"Processados: {total_processed} | "
                            f"Inseridos: {inserted_count} | "
                            f"Não inseridos: {not_inserted_count}"
                        )

                except Exception as e:
                    not_inserted_count += 1
                    not_inserted_log.append({**dict(row), "Motivo": _format_error_message(e)})

        if payload:
            inserted_delta, not_inserted_delta = _flush_batch(
                session,
                agenda_tbl,
                payload,
                existing_schedule_ids,
                inserted_log,
                not_inserted_log,
            )
            inserted_count += inserted_delta
            not_inserted_count += not_inserted_delta

    inserted_log_path = write_jsonl_log(inserted_log, log_folder, f"log_inserted_schedule_medsystem_{dbase}.jsonl")
    not_inserted_log_path = write_jsonl_log(
        not_inserted_log,
        log_folder,
        f"log_not_inserted_schedule_medsystem_{dbase}.jsonl",
    )

    print("\nMigração finalizada!")
    print(f"Origem: {source_database}")
    print(f"Total processados: {total_processed}")
    print(f"Inseridos: {inserted_count}")
    print(f"Não inseridos: {not_inserted_count}")
    print(f"Log de inseridos: {inserted_log_path}")
    print(f"Log de não inseridos: {not_inserted_log_path}")


if __name__ == "__main__":
    main()
