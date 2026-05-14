import os
from datetime import datetime

from sqlalchemy import MetaData, Table, insert, text
from sqlalchemy.orm import sessionmaker

from medsystem.core.db import build_source_engine, build_target_engine
from medsystem.core.logs import write_jsonl_log
from utils.utils import is_valid_date, truncate_value, verify_nan


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
		existing_history_ids = {str(row[0]).strip() for row in existing_history_ids_result if row[0] is not None}
		next_history_id = -1

		print(f"Históricos existentes carregados: {len(existing_history_ids)}")

		source_query = text(
			"""
			SELECT
				i.[Código da Consulta] AS CODIGO_CONSULTA,
				i.[Código do Item] AS CODIGO_ITEM,
				i.[Código] AS CODIGO,
				i.[Ident] AS IDENT,
				i.[Dados] AS DADOS,
				i.[CRC] AS CRC,
				i.[UniqueCod] AS UNIQUECOD,
				c.[Código do Cliente] AS CODIGO_CLIENTE,
				c.[Data] AS DATA_CONSULTA,
				c.[Hora] AS HORA_CONSULTA,
				c.[Código do Usuário] AS CODIGO_USUARIO
			FROM [dbo].[SWItens] i
			LEFT JOIN [dbo].[SWConsultas] c
				ON c.[Código] = i.[Código da Consulta]
			ORDER BY i.[Código da Consulta], i.[Código do Item], i.[UniqueCod]
			"""
		)

		payload = []

		with source_engine.connect().execution_options(stream_results=True) as source_conn:
			rows = source_conn.execute(source_query).mappings()

			for row in rows:
				total_processed += 1

				try:
					history_text = verify_nan(row["DADOS"])
					if history_text is None or str(history_text).strip() == "":
						continue

					id_client = verify_nan(row["CODIGO_CLIENTE"])
					if id_client is None:
						not_inserted_count += 1
						not_inserted_log.append({
							**dict(row),
							"Motivo": "Consulta não encontrada em SWConsultas ou Id do Cliente vazio",
						})
						continue

					id_client = str(id_client).strip()
					if not id_client:
						not_inserted_count += 1
						not_inserted_log.append({**dict(row), "Motivo": "Id do Cliente inválido"})
						continue

					history_date = _compose_datetime(row["DATA_CONSULTA"], row["HORA_CONSULTA"])
					if history_date is None:
						not_inserted_count += 1
						not_inserted_log.append({**dict(row), "Motivo": "Data/Hora da consulta inválida"})
						continue

					while str(next_history_id) in existing_history_ids:
						next_history_id -= 1

					id_history = next_history_id
					history_key = str(id_history)
					next_history_id -= 1

					rec = {
						"Id do Histórico": id_history,
						"Id do Cliente": id_client,
						"Histórico": truncate_value(str(history_text), 100000),
						"Data": history_date,
						"Id do Usuário": 0,
					}

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

	inserted_log_path = write_jsonl_log(inserted_log, log_folder, f"log_inserted_record_historico_medsystem_{dbase}.jsonl")
	not_inserted_log_path = write_jsonl_log(
		not_inserted_log,
		log_folder,
		f"log_not_inserted_record_historico_medsystem_{dbase}.jsonl",
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
