import csv
from datetime import datetime
import glob
import json
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, truncate_value

def get_record(json):
    """Extraindo o histórico do JSON"""
    try:
        if json.get('id'):
            id_record = json['id']
        else:
            return None, None, "Campo id não encontrado no JSON"

        record = ""
        
        if json.get('medicamentos'):
            record += f"Prescrição(ões) Memed:<br><br>"
            if isinstance(json['medicamentos'], list):
                for medicamento in json['medicamentos']:
                    if isinstance(medicamento, dict):
                        record += f"Medicamento: {medicamento.get('nome', '')} <br>"
                        if medicamento.get('tipo') is not None or medicamento.get('tiop') != "":
                            record += f"Tipo: {medicamento.get('tipo', '')} <br>"
                        
                        if medicamento.get('tarja') is not None or medicamento.get('tarja') != "":
                            record += f"Tarja: {medicamento.get('tarja', '')} <br>"
                        
                        if medicamento.get('descricao') is not None or medicamento.get('descricao') != "":
                            record += f"Descrição: {medicamento.get('descricao', '')} <br>"
                        
                        if medicamento.get('sanitized_posology') is not None or medicamento.get('sanitized_posology') != "":
                            record += f"Posologia: {medicamento.get('sanitized_posology', '')} <br>"

                        if medicamento.get('quantidade') is not None or medicamento.get('quantidade') != "":    
                            record += f"Quantidade: {medicamento.get('quantidade', '')} <br>"
                        
                        if medicamento.get('fabricante') is not None or medicamento.get('fabricante') != "":
                            record += f"Fabricante: {medicamento.get('fabricante', '')} <br>"
                        
                        record += "<br>"

    except Exception as e:
        return None, None, f"Erro ao processar o JSON {json}: {e}"
    
    return record, id_record, None


sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho do arquivo que contém os dados: ")

print("Conectando no Banco de Dados...")
DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine)

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

HistoricoClientes = getattr(Base.classes, "Histórico de Clientes")
Contatos = getattr(Base.classes, "Contatos")

print("Sucesso! Inicializando migração de Histórico...")

log_folder = path_file
csv_file = glob.glob(f'{path_file}/prescricoes_memed.csv')
csv.field_size_limit(10**6)

with open(csv_file[0], "r", encoding="utf-8") as f:
    lines = f.readlines()

data = [line.strip().split(";", maxsplit=5) for line in lines]  
df = pd.DataFrame(data, columns=["Cod Paciente", "Nome Paciente", "Cod Medico", "Nome Medico", "Data", "Json"])

# 
# df = pd.read_csv(csv_file[0], sep=";", quotechar="'", engine='python')
# df = df.fillna(value="")

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for index, row in df.iterrows():

    if not pd.isna(row["Json"]) and isinstance(row["Json"], str):
        try:
            json_str = row["Json"].replace("'","")
            json_data = json.loads(json_str)
            record, id_record, error_message = get_record(json_data)

            if record is None or record == "":
                not_inserted_cont += 1
                row_dict = row.to_dict()
                row_dict['Motivo'] = error_message
                not_inserted_data.append(row_dict)
                continue
        
        except json.JSONDecodeError:
            not_inserted_cont += 1
            row_dict = row.to_dict()
            row_dict['Motivo'] = 'Erro ao decodificar JSON'
            not_inserted_data.append(row_dict)
            continue
    else:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Campo Json vazio ou inválido'
        continue

    exists_row = exists(session, id_record, "Id do Histórico", HistoricoClientes)
    if exists_row:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id já existe no banco de dados'
        not_inserted_data.append(row_dict)
        continue
    if is_valid_date(row['Data'], '%d-%m-%Y %H:%M:%S'):
        date = datetime.strptime(row['Data'].replace("/", "-"), "%d-%m-%Y %H:%M:%S")
    else:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Data ou Hora inválida'
        not_inserted_data.append(row_dict)
        continue
    
    if row['Cod Paciente'] == "" or row['Cod Paciente'] == None or row['Cod Paciente'] == 'None':
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do paciente vazio'
        not_inserted_data.append(row_dict)
        continue
    else:
        id_patient = row["Cod Paciente"]

    new_record = HistoricoClientes(
        Histórico=record,
        Data=date,
    )

    setattr(new_record, "Id do Cliente", id_patient)
    setattr(new_record, "Id do Histórico", id_record)
    setattr(new_record, "Id do Usuário", 0)
    
    log_data.append({
        "Id do Histórico": id_record,
        "Id do Cliente": id_patient,
        "Data": date,
        "Histórico": record,
        "Id do Usuário": 0,
        })

    session.add(new_record)
    inserted_cont+=1

    if inserted_cont % 10000 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos históricos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} históricos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_record_prescricoes_memed.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_record_prescricoes_memed.xlsx")