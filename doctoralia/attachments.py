import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import create_log

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
base_path = input("Informe o caminho da pasta que contém os arquivos: ")

print("Conectando no Banco de Dados...")
DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"
engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine)

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

HistoricoClientes = getattr(Base.classes, "Histórico de Clientes")

patientfiles_dirs = [d for d in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, d)) and d.startswith("PatientFiles_")]
if not patientfiles_dirs:
    print("Nenhum diretório PatientFiles_xxx encontrado no caminho informado.")
    exit(1)
root_folder = os.path.join(base_path, patientfiles_dirs[0])
print(f"Usando diretório de anexos: {root_folder}")

log_folder = root_folder
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
not_inserted_data = []
inserted_cont = 0
not_inserted_cont = 0
id_record = -1

for pasta_id in os.listdir(root_folder):
    pasta_path = os.path.join(root_folder, pasta_id)
    if not os.path.isdir(pasta_path):
        continue
    id_patient = pasta_id
    for nome_arquivo in os.listdir(pasta_path):
        arquivo_path = os.path.join(pasta_path, nome_arquivo)
        if not os.path.isfile(arquivo_path):
            continue
        record = os.path.splitext(nome_arquivo)[0]
        date = '1900-01-01 00:00'
        classe = f"{pasta_id}/{nome_arquivo}"

        try:
            new_record = HistoricoClientes(
                Histórico=record,
                Data=date
            )
            setattr(new_record, "Id do Histórico", id_record)
            setattr(new_record, "Id do Cliente", id_patient)
            setattr(new_record, "Id do Usuário", 0)
            setattr(new_record, "Classe", classe)

            session.add(new_record)
            log_data.append({
                "Id do Histórico": id_record,
                "Id do Cliente": id_patient,
                "Data": date,
                "Histórico": record,
                "Classe": classe,
                "Id do Usuário": 0,
            })
            inserted_cont += 1
        except Exception as e:
            not_inserted_cont += 1
            not_inserted_data.append({
                "Id do Histórico": id_record,
                "Id do Cliente": id_patient,
                "Data": date,
                "Histórico": record,
                "Classe": classe,
                "Erro": str(e)
            })
        id_record -= 1
        if inserted_cont % 1000 == 0:
            session.commit()

session.commit()
session.close()

create_log(log_data, log_folder, "log_inserted_attachments.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_attachments.xlsx")

print(f"{inserted_cont} anexos inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} anexos não foram inseridos, verifique o log para mais detalhes.")
