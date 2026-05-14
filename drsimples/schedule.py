from datetime import date, datetime, timedelta
import glob
import json
import os
import urllib

from sqlalchemy import MetaData, Table, create_engine, select
from sqlalchemy.orm import sessionmaker

from utils.utils import verify_nan


sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")

print("Conectando no Banco de dados...")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"
engine = create_engine(DATABASE_URL)
metadata = MetaData()
agenda_tbl = Table("Agenda", metadata, schema=f"schema_{sid}", autoload_with=engine)


SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

print("Sucesso! Inicializando migração de Agendamentos...")


def _json_serializer(value):
	if isinstance(value, (datetime, date)):
		return value.isoformat()
	return str(value)


def write_jsonl_log(records, log_folder, filename):
	os.makedirs(log_folder, exist_ok=True)
	file_path = os.path.join(log_folder, filename)

	with open(file_path, "w", encoding="utf-8") as file:
		for record in records:
			file.write(json.dumps(record, ensure_ascii=False, default=_json_serializer))
			file.write("\n")

	return file_path


def append_jsonl_record(file_handle, record):
	file_handle.write(json.dumps(record, ensure_ascii=False, default=_json_serializer))
	file_handle.write("\n")


def flush_pending_inserts(db_session, table, rows, chunk_size):
	if not rows:
		return

	for start in range(0, len(rows), chunk_size):
		chunk = rows[start:start + chunk_size]
		db_session.execute(table.insert(), chunk)
	db_session.commit()


def load_json_records(file_path, root_key=None):
	with open(file_path, "r", encoding="utf-8") as f:
		data = json.load(f)

	if isinstance(data, list):
		return data

	if isinstance(data, dict):
		if root_key and isinstance(data.get(root_key), list):
			return data[root_key]

		for value in data.values():
			if isinstance(value, list):
				return value

	return []


def get_first_value(row, keys):
	for key in keys:
		value = verify_nan(row.get(key))
		if value is not None:
			return value
	return None


def parse_schedule_datetime(date_value, time_value):
	if date_value is None or time_value is None:
		return None

	date_text = str(date_value).strip()
	time_text = str(time_value).strip()

	if not date_text or not time_text:
		return None

	datetime_text = f"{date_text} {time_text}"

	try:
		return datetime.strptime(datetime_text, "%Y-%m-%d %H:%M")
	except ValueError:
		return None


schedule_file = (
	glob.glob(f"{path_file}/*CONSULTAS_AGENDADAS*.json")
	or glob.glob(f"{path_file}/agendamentos*.json")
	or glob.glob(f"{path_file}/AGENDAMENTO*.json")
	or glob.glob(f"{path_file}/agendamento*.json")
)

if not schedule_file:
	raise FileNotFoundError("Arquivo de agendamentos JSON não encontrado no caminho informado")

schedule_data = load_json_records(schedule_file[0], root_key="AGENDAMENTOS")

log_folder = path_file
inserted_cont = 0
not_inserted_cont = 0

total_rows = len(schedule_data)
DB_CHUNK_SIZE = 200
PENDING_BUFFER_SIZE = 1000
INSERT_STATUS = 1

existing_schedule_ids = {
	int(value)
	for value in session.execute(select(agenda_tbl.c["Id do Agendamento"])).scalars()
	if value is not None
}

pending_inserts = []
inserted_log_path = os.path.join(log_folder, "log_inserted_agendamento.jsonl")
not_inserted_log_path = os.path.join(log_folder, "log_not_inserted_agendamento.jsonl")

os.makedirs(log_folder, exist_ok=True)

inserted_log_file = open(inserted_log_path, "w", encoding="utf-8")
not_inserted_log_file = open(not_inserted_log_path, "w", encoding="utf-8")

try:
	for idx, row in enumerate(schedule_data):
		if not isinstance(row, dict):
			not_inserted_cont += 1
			append_jsonl_record(not_inserted_log_file, {
				"row": row,
				"Motivo": "Registro inválido (não é objeto JSON)",
				"Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
			})
			continue

		if idx % 1000 == 0 or idx == total_rows:
			concluido = round((idx / total_rows) * 100, 2) if total_rows else 100
			print(
				f"Processados: {idx} | Inseridos: {inserted_cont} | Não inseridos: {not_inserted_cont} | Concluído: {concluido}%"
			)

		id_scheduling = get_first_value(row, ["CD_CONSULTA_AGENDADA", "CD_AGENDA", "ID_AGENDAMENTO", "id"])
		if id_scheduling is None:
			not_inserted_cont += 1
			row_dict = row.copy()
			row_dict["Motivo"] = "Id do Agendamento vazio"
			row_dict["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
			append_jsonl_record(not_inserted_log_file, row_dict)
			continue

		try:
			id_scheduling = int(id_scheduling)
		except (ValueError, TypeError):
			not_inserted_cont += 1
			row_dict = row.copy()
			row_dict["Motivo"] = "Id do Agendamento inválido"
			row_dict["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
			append_jsonl_record(not_inserted_log_file, row_dict)
			continue

		if id_scheduling in existing_schedule_ids:
			not_inserted_cont += 1
			row_dict = row.copy()
			row_dict["Motivo"] = "Id do Agendamento já existe no banco"
			row_dict["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
			append_jsonl_record(not_inserted_log_file, row_dict)
			continue

		patient_name = get_first_value(row, ["NM_PACIENTE", "NOME_PACIENTE", "PACIENTE"])
		if patient_name is None:
			not_inserted_cont += 1
			row_dict = row.copy()
			row_dict["Motivo"] = "NM_PACIENTE vazio"
			row_dict["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
			append_jsonl_record(not_inserted_log_file, row_dict)
			continue

		date_value = get_first_value(row, ["DT_CONSULTA_AGENDADA", "DT_AGENDAMENTO", "DATA_CONSULTA", "DATA"])
		time_value = get_first_value(row, ["HR_CONSULTA_AGENDADA", "HR_AGENDAMENTO", "HORA_CONSULTA", "HORA"])

		start_datetime = parse_schedule_datetime(date_value, time_value)
		if start_datetime is None:
			not_inserted_cont += 1
			row_dict = row.copy()
			row_dict["Motivo"] = "Data/Hora inválida. Formato esperado: YYYY-MM-DD HH:MM"
			row_dict["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
			append_jsonl_record(not_inserted_log_file, row_dict)
			continue

		end_datetime = start_datetime + timedelta(minutes=30)

		start_time = start_datetime.strftime("%Y-%m-%d %H:%M:%S")
		end_time = end_datetime.strftime("%Y-%m-%d %H:%M:%S")

		append_jsonl_record(
			inserted_log_file,
			{
				"Id do Agendamento": id_scheduling,
				"Id do Usuário": 1,
				"Vinculado a": None,
				"Início": start_time,
				"Final": end_time,
				"Descrição": str(patient_name),
				"Status": INSERT_STATUS,
				"Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
			},
		)

		pending_inserts.append(
			{
				"Id do Agendamento": id_scheduling,
				"Id do Usuário": 1,
				"Descrição": str(patient_name),
				"Início": start_time,
				"Final": end_time,
				"Status": INSERT_STATUS,
			}
		)

		existing_schedule_ids.add(id_scheduling)
		inserted_cont += 1

		if len(pending_inserts) >= PENDING_BUFFER_SIZE:
			flush_pending_inserts(session, agenda_tbl, pending_inserts, DB_CHUNK_SIZE)
			pending_inserts.clear()

	if pending_inserts:
		flush_pending_inserts(session, agenda_tbl, pending_inserts, DB_CHUNK_SIZE)
finally:
	inserted_log_file.close()
	not_inserted_log_file.close()

print(f"{inserted_cont} novos agendamentos foram inseridos com sucesso!")
if not_inserted_cont > 0:
	print(f"{not_inserted_cont} agendamentos não foram inseridos, verifique o log para mais detalhes.")

session.close()

print(f"Log inseridos: {inserted_log_path}")
print(f"Log não inseridos: {not_inserted_log_path}")
