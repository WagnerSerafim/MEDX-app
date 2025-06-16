import json
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import urllib
from utils.utils import create_log, exists

def replace_null_with_empty_string(data):
    if isinstance(data, dict): 
        return {key: replace_null_with_empty_string(value) for key, value in data.items()}
    elif isinstance(data, list):  
        return [replace_null_with_empty_string(item) for item in data]
    elif data is None:  
        return ""
    else:
        return data    

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe a pasta do arquivo JSON: ").strip()

print("Conectando no Banco de Dados...")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"
engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine) 

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Contatos = getattr(Base.classes, "Contatos")

print("Sucesso! Inicializando atualização de fornecedores...")

log_folder = path_file
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

json_path = os.path.join(path_file, 'fornecedores.json')
with open(json_path, 'r', encoding='utf-8') as file:
    json_data = json.load(file)
    json_data = replace_null_with_empty_string(json_data)

upgraded_log = []
not_upgraded_log = []
updated_count = 0
not_updated_count = 0

for entry in json_data:
    patient_id = entry.get("PACIENcodigo")
    if not patient_id:
        entry['Motivo'] = 'PACIENcodigo vazio'
        not_upgraded_log.append(entry)
        not_updated_count += 1
        continue

    existing_patient = session.query(Contatos).filter(getattr(Contatos, "Id do Cliente") == patient_id).first()
    if existing_patient:
        try:
            setattr(existing_patient, "Tipo", 2)
            session.add(existing_patient)
            upgraded_log.append({"Id do Cliente": patient_id, "Nome":entry['PACIENnome'], "Status": "Atualizado para Tipo=2"})
            updated_count += 1
        except Exception as e:
            entry['Motivo'] = f'Erro ao atualizar: {str(e)}'
            not_upgraded_log.append(entry)
            not_updated_count += 1
    else:
        entry['Motivo'] = 'Paciente não encontrado no banco'
        not_upgraded_log.append(entry)
        not_updated_count += 1

    if updated_count % 100 == 0:
        session.commit()

session.commit()
session.close()

print(f"{updated_count} pacientes atualizados com sucesso!")
if not_updated_count > 0:
    print(f"{not_updated_count} pacientes não foram atualizados, verifique o log para mais detalhes.")

create_log(upgraded_log, log_folder, "log_upgraded_fornecedores.xlsx")
create_log(not_upgraded_log, log_folder, "log_not_upgraded_fornecedores.xlsx")
