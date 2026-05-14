import os
import re
from datetime import datetime

import pandas as pd
import urllib
from sqlalchemy import MetaData, Table, create_engine, insert, text
from sqlalchemy.orm import sessionmaker

from utils.utils import create_log, limpar_numero, truncate_value, verify_nan


PATIENTS_CLINICA_PATH = (
	r"D:\Migracoes\36739_TOTVS\TransferNow-20260310pp16wFmY\Dados"
	r"\pmed-extract-pacient-data\Excel\patients_CLINICA.xlsx"
)
BATCH_SIZE = 1000


def _find_column_name(df, candidates):
	column_by_normalized = {str(column).strip().lower(): column for column in df.columns}
	for candidate in candidates:
		found = column_by_normalized.get(candidate.strip().lower())
		if found is not None:
			return found
	return None


def _normalize_key(value):
	clean_value = limpar_numero(verify_nan(value))
	if clean_value is None:
		return None
	text = str(clean_value).strip()
	if not text:
		return None
	return text


def _normalize_text_id(value):
	clean_value = verify_nan(value)
	if clean_value is None:
		return None
	text = str(clean_value).strip()
	if not text:
		return None
	if text.endswith(".0"):
		text = text[:-2].strip()
	return text if text else None


def _build_lookup_variants(value):
	base = _normalize_text_id(value)
	if base is None:
		return []

	variants = [base]
	digits_only = re.sub(r"\D", "", base)
	if digits_only and digits_only not in variants:
		variants.append(digits_only)

	if digits_only:
		without_leading_zeros = digits_only.lstrip("0")
		if without_leading_zeros and without_leading_zeros not in variants:
			variants.append(without_leading_zeros)

	base_without_leading_zeros = base.lstrip("0")
	if base_without_leading_zeros and base_without_leading_zeros not in variants:
		variants.append(base_without_leading_zeros)

	return variants


def _normalize_datetime(value):
	clean_value = verify_nan(value)
	if clean_value is None:
		return "1900-01-01"

	try:
		parsed = pd.to_datetime(clean_value, errors="coerce")
		if pd.isna(parsed):
			return "1900-01-01"
		return parsed.strftime("%Y-%m-%d %H:%M:%S")
	except Exception:
		return "1900-01-01"


def _truncate_by_column(record, table):
	normalized = {}
	for key, value in record.items():
		column = table.columns.get(key)
		max_length = getattr(getattr(column, "type", None), "length", None) if column is not None else None
		if isinstance(max_length, int) and max_length > 0:
			normalized[key] = truncate_value(value, max_length)
		else:
			normalized[key] = value
	return normalized


def _resolve_ficha_by_arquivo_id(df_arquivo, df_arquivo_paciente):
	ficha_col = _find_column_name(df_arquivo_paciente, ["FichaPacienteId", "FICHAPACIENTEID"])
	if ficha_col is None:
		return {}, "A planilha 'Arquivo_Paciente' não possui a coluna 'FichaPacienteId'."

	arquivo_id_col = _find_column_name(df_arquivo, ["id", "Id", "ID"])
	if arquivo_id_col is None:
		return {}, "A planilha 'Arquivo' não possui coluna de id (id/Id/ID)."

	arquivo_ids = {
		key
		for key in (_normalize_key(value) for value in df_arquivo[arquivo_id_col])
		if key is not None
	}

	candidate_cols = []
	for column in df_arquivo_paciente.columns:
		normalized = str(column).strip().lower()
		if normalized in {"id", "arquivoid", "arquivo_id", "idarquivo", "arquivoidfk", "arquivofkid"}:
			candidate_cols.append(column)

	best_mapping = None
	best_col = None
	best_overlap = -1

	for key_col in candidate_cols:
		mapping = {}
		for _, row in df_arquivo_paciente.iterrows():
			key = _normalize_key(row.get(key_col))
			ficha = _normalize_key(row.get(ficha_col))
			if key is not None:
				mapping[key] = ficha

		overlap = len(set(mapping.keys()) & arquivo_ids)
		if overlap > best_overlap:
			best_overlap = overlap
			best_col = key_col
			best_mapping = mapping

	if best_mapping and best_overlap > 0:
		return best_mapping, None, best_col, best_overlap

	if len(df_arquivo_paciente) == len(df_arquivo):
		mapping = {}
		for idx, row in df_arquivo.iterrows():
			arquivo_id = _normalize_key(row.get(arquivo_id_col))
			ficha = _normalize_key(df_arquivo_paciente.iloc[idx].get(ficha_col))
			if arquivo_id is not None:
				mapping[str(arquivo_id)] = ficha
		return mapping, None, "ordem_das_linhas", len(mapping)

	return {}, (
		"Não foi possível relacionar 'Arquivo' com 'Arquivo_Paciente'. "
		"Inclua coluna de vínculo (id/ArquivoId) em 'Arquivo_Paciente', "
		"ou mantenha a mesma ordem/quantidade de linhas entre as abas."
	), None, 0


def main():
	sid = input("Informe o SoftwareID: ").strip()
	password = urllib.parse.quote_plus(input("Informe a senha: "))
	dbase = input("Informe o DATABASE: ").strip()
	arquivo_referencia_path = input("Informe o caminho do Arquivo_Referencia.xlsx: ").strip()

	if not os.path.isfile(arquivo_referencia_path):
		raise FileNotFoundError(f"Arquivo não encontrado: {arquivo_referencia_path}")

	if not os.path.isfile(PATIENTS_CLINICA_PATH):
		raise FileNotFoundError(f"Arquivo de pacientes não encontrado: {PATIENTS_CLINICA_PATH}")

	log_folder = os.path.dirname(arquivo_referencia_path) or os.getcwd()
	os.makedirs(log_folder, exist_ok=True)

	print("Conectando no Banco de Dados...")
	database_url = (
		f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}"
		"?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"
	)
	engine = create_engine(database_url, future=True)

	metadata = MetaData()
	historico_tbl = Table("Histórico de Clientes", metadata, schema=f"schema_{sid}", autoload_with=engine)

	session_local = sessionmaker(bind=engine, future=True)

	print("Lendo planilhas do Arquivo_Referencia...")
	df_arquivo = pd.read_excel(arquivo_referencia_path, sheet_name="Arquivo")
	df_arquivo_paciente = pd.read_excel(arquivo_referencia_path, sheet_name="Arquivo_Paciente")

	arquivo_id_col = _find_column_name(df_arquivo, ["id", "Id", "ID"])
	required_columns = ["CloudBlobDirectory", "CloudBlobBlock", "Nome", "DataInclusao"]
	missing_columns = [column for column in required_columns if column not in df_arquivo.columns]
	if missing_columns:
		raise ValueError(f"Colunas ausentes na aba 'Arquivo': {missing_columns}")
	if arquivo_id_col is None:
		raise ValueError("A aba 'Arquivo' não possui a coluna 'id' (id/Id/ID).")

	ficha_by_arquivo_id, join_error, join_key_col, join_overlap = _resolve_ficha_by_arquivo_id(df_arquivo, df_arquivo_paciente)
	if join_error:
		raise ValueError(join_error)

	print(
		f"Vínculo Arquivo x Arquivo_Paciente resolvido por: {join_key_col} | "
		f"ids cruzados: {join_overlap}"
	)

	print("Lendo base de pacientes TOTVS...")
	df_patients = pd.read_excel(PATIENTS_CLINICA_PATH)
	if "FICHAPACIENTEID" not in df_patients.columns or "PACIENTEID" not in df_patients.columns:
		raise ValueError("A planilha patients_CLINICA.xlsx precisa das colunas 'FICHAPACIENTEID' e 'PACIENTEID'.")

	pacienteid_by_ficha = {}
	for _, row in df_patients.iterrows():
		ficha = _normalize_text_id(row.get("FICHAPACIENTEID"))
		pacienteid = _normalize_text_id(row.get("PACIENTEID"))
		if ficha is not None and pacienteid is not None:
			pacienteid_by_ficha[ficha] = pacienteid

	inserted_log = []
	not_inserted_log = []
	payload = []

	inserted_count = 0
	not_inserted_count = 0

	with session_local() as session:
		existing_history_ids_result = session.execute(
			text(f"SELECT [Id do Histórico] FROM [schema_{sid}].[Histórico de Clientes]")
		)
		existing_history_ids = {str(row[0]) for row in existing_history_ids_result if row[0] is not None}

		contatos_result = session.execute(text(f"SELECT [Id do Cliente], [Referências] FROM [schema_{sid}].[Contatos]"))
		id_cliente_by_referencia = {}
		referencias_raw_samples = []
		for row in contatos_result:
			id_cliente = _normalize_text_id(row[0])
			referencia = row[1]
			if id_cliente is None:
				continue

			ref_variants = _build_lookup_variants(referencia)
			if not ref_variants:
				continue

			if len(referencias_raw_samples) < 10:
				referencias_raw_samples.append(str(referencia))

			for ref_key in ref_variants:
				if ref_key not in id_cliente_by_referencia:
					id_cliente_by_referencia[ref_key] = id_cliente

		print(f"Exemplos de Referências do banco (raw): {referencias_raw_samples}")
		referencias_norm_samples = list(id_cliente_by_referencia.keys())[:10]
		print(f"Exemplos de Referências normalizadas (mapa): {referencias_norm_samples}")

		print(
			f"Mapeamentos carregados | Históricos: {len(existing_history_ids)} | "
			f"Pacientes TOTVS: {len(pacienteid_by_ficha)} | Referências Contatos: {len(id_cliente_by_referencia)}"
		)

		debug_missing_referencia = []

		for idx, row in df_arquivo.iterrows():
			if idx % 1000 == 0 or idx == len(df_arquivo):
				print(
					f"Processados: {idx} | Inseridos: {inserted_count} | "
					f"Não inseridos: {not_inserted_count} | "
					f"Concluído: {round((idx / len(df_arquivo)) * 100, 2) if len(df_arquivo) else 100}%"
				)

			arquivo_id = _normalize_key(row.get(arquivo_id_col))
			if arquivo_id is None:
				not_inserted_count += 1
				not_inserted_log.append({**row.to_dict(), "Motivo": "Id do Histórico vazio"})
				continue

			history_key = str(arquivo_id)
			if history_key in existing_history_ids:
				not_inserted_count += 1
				not_inserted_log.append({**row.to_dict(), "Motivo": "Id do Histórico já existe"})
				continue

			cloud_dir = verify_nan(row.get("CloudBlobDirectory"))
			cloud_block = verify_nan(row.get("CloudBlobBlock"))
			if cloud_dir is None or cloud_block is None:
				not_inserted_count += 1
				not_inserted_log.append({**row.to_dict(), "Motivo": "CloudBlobDirectory/CloudBlobBlock vazio"})
				continue

			nome = verify_nan(row.get("Nome"))
			if nome is None:
				not_inserted_count += 1
				not_inserted_log.append({**row.to_dict(), "Motivo": "Nome vazio"})
				continue

			if history_key not in ficha_by_arquivo_id:
				not_inserted_count += 1
				not_inserted_log.append(
					{
						**row.to_dict(),
						"IdBuscaArquivo": history_key,
						"Motivo": "Id não encontrado no vínculo com a aba Arquivo_Paciente",
					}
				)
				continue

			ficha_paciente_id = ficha_by_arquivo_id.get(history_key)
			if ficha_paciente_id is None:
				not_inserted_count += 1
				not_inserted_log.append(
					{
						**row.to_dict(),
						"IdBuscaArquivo": history_key,
						"Motivo": "FichaPacienteId vazio no vínculo da aba Arquivo_Paciente",
					}
				)
				continue

			pacienteid = pacienteid_by_ficha.get(_normalize_text_id(ficha_paciente_id))
			if pacienteid is None:
				not_inserted_count += 1
				not_inserted_log.append(
					{
						**row.to_dict(),
						"FichaPacienteId": ficha_paciente_id,
						"Motivo": "PACIENTEID não encontrado no patients_CLINICA.xlsx",
					}
				)
				continue

			pacienteid_variants = _build_lookup_variants(pacienteid)
			id_cliente = None
			matched_key = None
			for ref_key in pacienteid_variants:
				candidate = id_cliente_by_referencia.get(ref_key)
				if candidate is not None:
					id_cliente = candidate
					matched_key = ref_key
					break

			if id_cliente is None:
				for ref_key in pacienteid_variants:
					sql_match = session.execute(
						text(
							f"""
							SELECT TOP 1 [Id do Cliente], [Referências]
							FROM [schema_{sid}].[Contatos]
							WHERE LTRIM(RTRIM(CONVERT(NVARCHAR(255), [Referências]))) = :ref
							"""
						),
						{"ref": ref_key},
					).first()

					if sql_match is not None:
						id_cliente = _normalize_text_id(sql_match[0])
						matched_key = ref_key
						id_cliente_by_referencia[ref_key] = id_cliente
						break

			if id_cliente is None:
				not_inserted_count += 1
				if len(debug_missing_referencia) < 20:
					debug_missing_referencia.append(
						{
							"ArquivoId": history_key,
							"FichaPacienteId": ficha_paciente_id,
							"PACIENTEID_raw": str(pacienteid),
							"PACIENTEID_variants": " | ".join(pacienteid_variants),
						}
					)
				not_inserted_log.append(
					{
						**row.to_dict(),
						"FichaPacienteId": ficha_paciente_id,
						"PACIENTEID": pacienteid,
						"PACIENTEID_variants": " | ".join(pacienteid_variants),
						"Motivo": "Paciente não encontrado na tabela Contatos por Referências",
					}
				)
				continue

			record = {
				"Classe": f"{cloud_dir}/{cloud_block}",
				"Histórico": str(nome),
				"Data": _normalize_datetime(row.get("DataInclusao")),
				"Id do Histórico": arquivo_id,
				"Id do Cliente": id_cliente,
				"Id do Usuário": 0,
			}

			record = _truncate_by_column(record, historico_tbl)
			payload.append((history_key, record))

			if len(payload) >= BATCH_SIZE:
				try:
					records_to_insert = [item[1] for item in payload]
					session.execute(insert(historico_tbl), records_to_insert)
					session.commit()

					inserted_count += len(records_to_insert)
					inserted_log.extend(records_to_insert)
					for committed_id, _ in payload:
						existing_history_ids.add(committed_id)
				except Exception as error:
					session.rollback()
					for _, failed in payload:
						not_inserted_count += 1
						not_inserted_log.append({**failed, "Motivo": f"Falha no commit do lote: {error}"})
				finally:
					payload.clear()

		if payload:
			try:
				records_to_insert = [item[1] for item in payload]
				session.execute(insert(historico_tbl), records_to_insert)
				session.commit()

				inserted_count += len(records_to_insert)
				inserted_log.extend(records_to_insert)
				for committed_id, _ in payload:
					existing_history_ids.add(committed_id)
			except Exception as error:
				session.rollback()
				for _, failed in payload:
					not_inserted_count += 1
					not_inserted_log.append({**failed, "Motivo": f"Falha no commit do lote final: {error}"})
			finally:
				payload.clear()

	create_log(inserted_log, log_folder, "log_inserted_indexes_excel.xlsx")
	create_log(not_inserted_log, log_folder, "log_not_inserted_indexes_excel.xlsx")

	if debug_missing_referencia:
		create_log(debug_missing_referencia, log_folder, "log_debug_missing_referencias.xlsx")

	print("\nMigração finalizada!")
	print(f"Total lidos: {len(df_arquivo)}")
	print(f"Inseridos: {inserted_count}")
	print(f"Não inseridos: {not_inserted_count}")
	print(f"Log de inseridos: {os.path.join(log_folder, 'log_inserted_indexes_excel.xlsx')}")
	print(f"Log de não inseridos: {os.path.join(log_folder, 'log_not_inserted_indexes_excel.xlsx')}")
	if debug_missing_referencia:
		print(f"Log de debug de referências: {os.path.join(log_folder, 'log_debug_missing_referencias.xlsx')}")


if __name__ == "__main__":
	main()
