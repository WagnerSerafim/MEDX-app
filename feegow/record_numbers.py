import json
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import urllib
from striprtf.striprtf import rtf_to_text
from utils.utils import is_valid_date

# sid = input("Informe o SoftwareID: ")
# password = urllib.parse.quote_plus(input("Informe a senha: "))
# dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")

file = 0
while True:
    file += 1
    try:
        excel_file = os.path.join(path_file, f"_{file}.xlsx")
        df = pd.read_excel(excel_file)
        print(f"Encontramos! {excel_file}")
    except FileNotFoundError:
        print(f"Não foi possível encontrar o arquivo _{file}.xlsx")
        break

print(f'Passa aqui')
# print("Conectando no Banco de Dados...")
# DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

# engine = create_engine(DATABASE_URL)

# Base = automap_base()
# Base.prepare(autoload_with=engine)

# SessionLocal = sessionmaker(bind=engine)
# session = SessionLocal()

# HistoricoClientes = getattr(Base.classes, "Histórico de Clientes")
# Contatos = getattr(Base.classes, "Contatos")

# print("Sucesso! Inicializando migração de consulta MySmartClinic...")

# log_folder = os.path.dirname(excel_file)

# df = pd.read_excel(excel_file, sheet_name="consulta")
# df = df.fillna(value="")

# if not os.path.exists(log_folder):
#     os.makedirs(log_folder)

# log_data = []
# cont=0
# cont_commit = 0
# for index, row in df.iterrows():

#     existing_patient = session.query(Contatos).filter(getattr(Contatos, "Referências")==row["id_paciente"]).first()
#     if existing_patient:
#         id_patient = getattr(existing_patient, "Id do Cliente")
#     else:
#         print(f"Paciente com ID {row['id_paciente']} não encontrado.")
#         continue

#     record = get_record(row)
#     if record == "":
#         continue
    
#     date = row['data_criacao'] 
    
#     new_record = HistoricoClientes(
#         Histórico=record,
#         Data=date
#     )
#     # setattr(new_record, "Id do Histórico", (0-row["ID_Anam"]))
#     setattr(new_record, "Id do Cliente", id_patient)
#     setattr(new_record, "Id do Usuário", 0)
    
#     log_data.append({
#         # "Id do Histórico": (0-row["ID_Anam"]),
#         "Id do Cliente": id_patient,
#         "Data": date,
#         "Histórico": record,
#         "Id do Usuário": 0,
#     })
#     cont+=1
#     session.add(new_record)

#     if cont % 1000 == 0:
#         session.commit()
#         print(f"{cont} históricos commitados com sucesso!")

# session.commit()

# print(f"{cont} novos históricos foram inseridos com sucesso!")

# session.close()

# log_df = pd.DataFrame(log_data)
# log_file_path = os.path.join(log_folder, "log_record_consulta.xlsx")
# log_df.to_excel(log_file_path, index=False)
