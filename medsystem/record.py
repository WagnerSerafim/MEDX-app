import os
from datetime import datetime

from sqlalchemy import MetaData, Table, insert, text
from sqlalchemy.orm import sessionmaker

from medsystem.core.db import build_source_engine, build_target_engine
from medsystem.core.logs import write_jsonl_log
from utils.utils import truncate_value, verify_nan


BATCH_SIZE = 1000


def _normalize_record_date(raw_value):
    value = verify_nan(raw_value)
    if value is None:
        return "1900-01-01"

    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")

    text_value = str(value).strip()
    if not text_value:
        return "1900-01-01"

    for fmt in ("%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(text_value, fmt)
            if fmt in ("%Y/%m/%d", "%Y-%m-%d"):
                return parsed.strftime("%Y-%m-%d")
            return parsed.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue

    return "1900-01-01"


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


def _flush_batch(session, historico_tbl, payload, existing_history_ids, inserted_log, not_inserted_log):
    inserted_count = 0
    not_inserted_count = 0

    if not payload:
        return inserted_count, not_inserted_count

    try:
        records_to_insert = [item[1] for item in payload]
        session.execute(insert(historico_tbl), records_to_insert)
        session.commit()
        inserted_count += len(records_to_insert)
        inserted_log.extend(records_to_insert)
        for committed_id, _ in payload:
            existing_history_ids.add(committed_id)
        payload.clear()
        return inserted_count, not_inserted_count
    except Exception as batch_error:
        session.rollback()
        batch_error_message = _format_error_message(batch_error)

    for history_key, record in payload:
        try:
            session.execute(insert(historico_tbl), [record])
            session.commit()
            inserted_count += 1
            inserted_log.append(record)
            existing_history_ids.add(history_key)
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
    historico_tbl = Table("Histórico de Clientes", metadata, schema=f"schema_{sid}", autoload_with=target_engine)

    history_columns = {column.name for column in historico_tbl.columns}

    SessionLocal = sessionmaker(bind=target_engine, future=True)

    inserted_log = []
    not_inserted_log = []

    inserted_count = 0
    not_inserted_count = 0
    total_processed = 0

    with SessionLocal() as session:
        existing_history_ids_result = session.execute(
            text(f"SELECT [Id do Histórico] FROM [schema_{sid}].[Histórico de Clientes]")
        )
        existing_history_ids = {str(row[0]) for row in existing_history_ids_result if row[0] is not None}

        agenda_result = session.execute(text(f"SELECT [Id do Agendamento], [Vinculado a] FROM [schema_{sid}].[Agenda]"))
        client_by_schedule_id = {
            str(row[0]): row[1]
            for row in agenda_result
            if row[0] is not None and row[1] is not None
        }

        print(f"Históricos existentes carregados: {len(existing_history_ids)}")
        print(f"Agendamentos em memória para vínculo paciente: {len(client_by_schedule_id)}")

        source_query = text(
            """
            SELECT
                [Código] AS CODIGO,
                [Texto] AS TEXTO,
                [Descrição] AS DESCRICAO,
                [Código da Consulta] AS CODIGO_CONSULTA
            FROM [dbo].[SWConsim]
            ORDER BY [Código]
            """
        )

        payload = []

        with source_engine.connect().execution_options(stream_results=True) as source_conn:
            rows = source_conn.execute(source_query).mappings()

            for row in rows:
                total_processed += 1

                try:
                    id_history = verify_nan(row["CODIGO"])
                    if id_history is None:
                        not_inserted_count += 1
                        not_inserted_log.append({**dict(row), "Motivo": "Id do Histórico vazio"})
                        continue

                    history_key = str(id_history).strip()
                    if history_key in existing_history_ids:
                        not_inserted_count += 1
                        not_inserted_log.append({**dict(row), "Motivo": "Id do Histórico já existe"})
                        continue

                    history_text = verify_nan(row["TEXTO"])
                    if history_text is None or str(history_text).strip() == "":
                        not_inserted_count += 1
                        not_inserted_log.append({**dict(row), "Motivo": "Histórico vazio"})
                        continue

                    id_schedule = verify_nan(row["CODIGO_CONSULTA"])
                    if id_schedule is None:
                        not_inserted_count += 1
                        not_inserted_log.append({**dict(row), "Motivo": "Id do Agendamento vazio"})
                        continue

                    schedule_key = str(id_schedule).strip()
                    id_client = client_by_schedule_id.get(schedule_key)
                    if id_client is None:
                        not_inserted_count += 1
                        not_inserted_log.append({**dict(row), "Motivo": "Agendamento não encontrado na Agenda"})
                        continue

                    history_date = _normalize_record_date(row["DESCRICAO"])

                    rec = {
                        "Id do Histórico": id_history,
                        "Id do Cliente": id_client,
                        "Histórico": truncate_value(str(history_text), 100000),
                        "Data": history_date,
                        "Id do Usuário": 0,
                    }

                    if "Id do Agendamento" in history_columns:
                        rec["Id do Agendamento"] = id_schedule

                    payload.append((history_key, rec))

                    if len(payload) >= BATCH_SIZE:
                        inserted_delta, not_inserted_delta = _flush_batch(
                            session,
                            historico_tbl,
                            payload,
                            existing_history_ids,
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

                except Exception as error:
                    session.rollback()
                    not_inserted_count += 1
                    not_inserted_log.append({**dict(row), "Motivo": _format_error_message(error)})

        if payload:
            inserted_delta, not_inserted_delta = _flush_batch(
                session,
                historico_tbl,
                payload,
                existing_history_ids,
                inserted_log,
                not_inserted_log,
            )
            inserted_count += inserted_delta
            not_inserted_count += not_inserted_delta

    inserted_log_path = write_jsonl_log(inserted_log, log_folder, f"log_inserted_record_medsystem_{dbase}.jsonl")
    not_inserted_log_path = write_jsonl_log(
        not_inserted_log,
        log_folder,
        f"log_not_inserted_record_medsystem_{dbase}.jsonl",
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
