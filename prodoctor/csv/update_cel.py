from datetime import datetime
import csv
import glob
import os
import urllib

import pandas as pd
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from utils.utils import create_log, verify_nan


def limpar_numero(valor):
	if valor is None:
		return None
	valor_str = str(valor)
	if valor_str.endswith('.0'):
		valor_str = valor_str[:-2]
	valor_str = valor_str.strip()
	return valor_str if valor_str else None


def get_csv_path(path_file):
	cadastro_file = glob.glob(f"{path_file}/t_pacientes.csv")
	if not cadastro_file:
		raise FileNotFoundError(
			"Arquivo t_pacientes.csv nao encontrado na pasta informada."
		)
	return cadastro_file[0]


sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contem o t_pacientes.csv: ")

print("Conectando no Banco de Dados...")

DATABASE_URL = (
	f"mssql+pyodbc://Medizin_{sid}:{password}"
	f"@medxserver.database.windows.net:1433/{dbase}"
	"?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"
)

engine = create_engine(DATABASE_URL)

metadata = MetaData()
contatos_tbl = Table("Contatos", metadata, schema=f"schema_{sid}", autoload_with=engine)

Base = declarative_base()


class Contatos(Base):
	__table__ = contatos_tbl


SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

print("Sucesso! Inicializando correcao de Celular/Telefone Residencial...")

csv.field_size_limit(1000000)
csv_path = get_csv_path(path_file)
df = pd.read_csv(csv_path, sep=",", engine="python", quotechar='"')

if not os.path.exists(path_file):
	os.makedirs(path_file)

existing_columns = set(contatos_tbl.columns.keys())
has_tel_residencial = "Telefone Residencial" in existing_columns
has_tel_generico = "Telefone" in existing_columns

if not has_tel_residencial and not has_tel_generico:
	raise ValueError(
		"Tabela Contatos nao possui coluna 'Telefone Residencial' nem 'Telefone'."
	)

updated_data = []
not_updated_data = []
updated_cont = 0
not_updated_cont = 0

for idx, row in df.iterrows():
	if idx % 1000 == 0 or idx == len(df):
		print(
			f"Processados: {idx} | Atualizados: {updated_cont} | "
			f"Nao atualizados: {not_updated_cont} | "
			f"Concluido: {round((idx / len(df)) * 100, 2)}%"
		)

	id_patient = verify_nan(row.get("codigo"))
	if id_patient is None:
		not_updated_cont += 1
		row_dict = row.to_dict()
		row_dict["Motivo"] = "Id do Cliente vazio"
		row_dict["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
		not_updated_data.append(row_dict)
		continue

	telefone_2 = limpar_numero(verify_nan(row.get("telefone_2")))
	telefone_1 = limpar_numero(verify_nan(row.get("telefone_1")))

	if not telefone_2:
		not_updated_cont += 1
		row_dict = row.to_dict()
		row_dict["Motivo"] = "telefone_2 vazio"
		row_dict["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
		not_updated_data.append(row_dict)
		continue

	contato = (
		session.query(Contatos)
		.filter(getattr(Contatos, "Id do Cliente") == id_patient)
		.first()
	)

	if not contato:
		not_updated_cont += 1
		row_dict = row.to_dict()
		row_dict["Motivo"] = "Contato nao encontrado no banco"
		row_dict["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
		not_updated_data.append(row_dict)
		continue

	setattr(contato, "Celular", telefone_2)
	if has_tel_residencial:
		setattr(contato, "Telefone Residencial", telefone_1)
	if has_tel_generico:
		setattr(contato, "Telefone", telefone_1)

	updated_data.append(
		{
			"Id do Cliente": id_patient,
			"Nome": verify_nan(row.get("nome")),
			"Celular (novo)": telefone_2,
			"Telefone Residencial (novo)": telefone_1,
			"Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
		}
	)
	updated_cont += 1

	if updated_cont % 500 == 0:
		session.commit()

session.commit()
session.close()

create_log(updated_data, path_file, "log_updated_celular_pacientes.xlsx")
create_log(not_updated_data, path_file, "log_not_updated_celular_pacientes.xlsx")

print(f"{updated_cont} contatos atualizados com sucesso!")
if not_updated_cont > 0:
	print(
		f"{not_updated_cont} contatos nao foram atualizados. "
		"Verifique o log de nao atualizados."
	)
