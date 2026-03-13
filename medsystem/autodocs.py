import os
from datetime import datetime

from sqlalchemy import MetaData, Table, insert, text
from sqlalchemy.orm import sessionmaker

from medsystem.core.db import build_source_engine, build_target_engine
from medsystem.core.logs import write_jsonl_log
from utils.utils import truncate_value, verify_nan


BATCH_SIZE = 1000


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


def _flush_batch(session, autodocs_tbl, payload, inserted_log, not_inserted_log):
    inserted_count = 0
    not_inserted_count = 0

    if not payload:
        return inserted_count, not_inserted_count

    try:
        session.execute(insert(autodocs_tbl), payload)
        session.commit()
        inserted_count += len(payload)
        inserted_log.extend(payload)
        payload.clear()
        return inserted_count, not_inserted_count
    except Exception as batch_error:
        session.rollback()
        batch_error_message = _format_error_message(batch_error)

    for record in payload:
        try:
            session.execute(insert(autodocs_tbl), [record])
            session.commit()
            inserted_count += 1
            inserted_log.append(record)
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
    autodocs_tbl = Table("Autodocs", metadata, schema=f"schema_{sid}", autoload_with=target_engine)

    SessionLocal = sessionmaker(bind=target_engine, future=True)

    inserted_log = []
    not_inserted_log = []
    payload = []

    inserted_count = 0
    not_inserted_count = 0
    total_processed = 0

    parent_name = f"Textos Migração MEDSYSTEM {datetime.now().strftime('%d/%m/%Y')}"

    with SessionLocal() as session:
        try:
            parent_record = {
                "Pai": 0,
                "Biblioteca": truncate_value(parent_name, 100),
            }
            session.execute(insert(autodocs_tbl), [parent_record])
            session.commit()
        except Exception as error:
            session.rollback()
            raise RuntimeError(f"Falha ao criar registro pai no Autodocs: {_format_error_message(error)}") from error

        parent_id_result = session.execute(
            text(
                f"""
                SELECT TOP 1 [Id do Texto]
                FROM [schema_{sid}].[Autodocs]
                WHERE [Pai] = 0 AND [Biblioteca] = :parent_name
                ORDER BY [Id do Texto] DESC
                """
            ),
            {"parent_name": truncate_value(parent_name, 100)},
        )
        parent_id_row = parent_id_result.fetchone()
        if not parent_id_row or parent_id_row[0] is None:
            raise RuntimeError("Não foi possível obter o Id do Texto do registro pai criado em Autodocs.")

        parent_id = parent_id_row[0]
        print(f"Registro pai criado no Autodocs. Id do Texto: {parent_id}")

        source_query = text(
            """
            SELECT
                [Nome] AS NOME,
                [Texto] AS TEXTO
            FROM [dbo].[SWTextos]
            """
        )

        with source_engine.connect().execution_options(stream_results=True) as source_conn:
            rows = source_conn.execute(source_query).mappings()

            for row in rows:
                total_processed += 1

                try:
                    name = verify_nan(row["NOME"])
                    if name is None or str(name).strip() == "":
                        not_inserted_count += 1
                        not_inserted_log.append({**dict(row), "Motivo": "Nome vazio"})
                        continue

                    text_value = verify_nan(row["TEXTO"])
                    if text_value is None or str(text_value).strip() == "":
                        not_inserted_count += 1
                        not_inserted_log.append({**dict(row), "Motivo": "Texto vazio"})
                        continue

                    record = {
                        "Biblioteca": truncate_value(str(name).strip(), 100),
                        "Texto": str(text_value),
                        "Pai": parent_id,
                    }

                    payload.append(record)

                    if len(payload) >= BATCH_SIZE:
                        inserted_delta, not_inserted_delta = _flush_batch(
                            session,
                            autodocs_tbl,
                            payload,
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
                autodocs_tbl,
                payload,
                inserted_log,
                not_inserted_log,
            )
            inserted_count += inserted_delta
            not_inserted_count += not_inserted_delta

    inserted_log_path = write_jsonl_log(inserted_log, log_folder, f"log_inserted_autodocs_medsystem_{dbase}.jsonl")
    not_inserted_log_path = write_jsonl_log(
        not_inserted_log,
        log_folder,
        f"log_not_inserted_autodocs_medsystem_{dbase}.jsonl",
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
