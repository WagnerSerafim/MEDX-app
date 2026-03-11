from datetime import datetime
import glob
import json
import os
import re
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, truncate_value, verify_nan, limpar_cpf, limpar_numero
import csv
from striprtf.striprtf import rtf_to_text
    
sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")

print("Conectando no Banco de Dados...")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

metadata = MetaData()
contatos_tbl = Table("Contatos", metadata, schema=f"schema_{sid}", autoload_with=engine)

Base = declarative_base()

class Contatos(Base):
    __table__ = contatos_tbl

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

print("Sucesso! Inicializando migração de Contatos...")

cadastro_file = glob.glob(f'{path_file}/PACIENTES*.json')
extra_file = glob.glob(f'{path_file}/CIDADES*.json')

if not cadastro_file:
    raise FileNotFoundError("Arquivo PACIENTES*.json não encontrado no caminho informado")

if not extra_file:
    raise FileNotFoundError("Arquivo CIDADES*.json não encontrado no caminho informado")


def load_json_records(file_path, root_key=None):
    with open(file_path, 'r', encoding='utf-8') as f:
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

patients_data = load_json_records(cadastro_file[0], root_key="PACIENTES")
cities_data = load_json_records(extra_file[0], root_key="CIDADES")

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont = 0
not_inserted_data = []
not_inserted_cont = 0

cities = {}
for city in cities_data:
    if not isinstance(city, dict):
        continue
    id_city = verify_nan(city.get("CD_CIDADE"))
    if id_city is not None:
        cities[id_city] = {
            "cidade": verify_nan(city.get("NM_CIDADE")),
            "estado": verify_nan(city.get("NM_UF"))
        }

total_rows = len(patients_data)

for idx, row in enumerate(patients_data):
    if idx % 1000 == 0 or idx == total_rows:
        concluido = round((idx / total_rows) * 100, 2) if total_rows else 100
        print(f"Processados: {idx} | Inseridos: {inserted_cont} | Não inseridos: {not_inserted_cont} | Concluído: {concluido}%")

    id_patient = verify_nan(row.get("CD_PACIENTE"))
    if id_patient == None:
        not_inserted_cont +=1
        row_dict = row.copy() if isinstance(row, dict) else {"row": row}
        row_dict['Motivo'] = 'Id do Cliente vazio'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    try:
        id_patient = int(id_patient)
    except ValueError:
        not_inserted_cont +=1
        row_dict = row.copy() if isinstance(row, dict) else {"row": row}
        row_dict['Motivo'] = 'Id do Cliente inválido'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    name = verify_nan(row.get("NM_PACIENTE"))
    if name == None:
        not_inserted_cont +=1
        row_dict = row.copy() if isinstance(row, dict) else {"row": row}
        row_dict['Motivo'] = 'Nome do Paciente vazio'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    existing_record = exists(session, id_patient, "Id do Cliente", Contatos)
    if existing_record:
        not_inserted_cont +=1
        row_dict = row.copy() if isinstance(row, dict) else {"row": row}
        row_dict['Motivo'] = 'Id do Cliente já existe no Banco de Dados'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    email = verify_nan(row.get("DS_EMAIL"))
     
    try:
        birthday_obj = verify_nan(row.get("DT_NASCIMENTO"))
        if birthday_obj == None:
            birthday = '1900-01-01'
        else:
            birthday = datetime.strptime(birthday_obj, '%Y-%m-%d').strftime('%Y-%m-%d')
            if not birthday or not is_valid_date(birthday, '%Y-%m-%d'):
                birthday = '1900-01-01'
    except ValueError:
        birthday = '1900-01-01'

    sex = verify_nan(row.get("FL_SEXO"))
    mother = verify_nan(row.get('NM_MAE'))
    father = verify_nan(row.get('NM_PAI'))
    rg = limpar_numero(verify_nan(row.get('NR_IDENTIDADE')))
    cpf = limpar_cpf(verify_nan(row.get('NR_CPF')))
    conjuge = None
    observations = None
    cellphone = limpar_numero(verify_nan(row.get('NR_CELULAR')))
    phone = verify_nan(row.get('NR_FAX'))
    occupation = verify_nan(row.get('NM_PROFISSAO'))
    cep = verify_nan(row.get('NR_CEP'))
    address = verify_nan(row.get('DS_ENDERECO'))
    complement = verify_nan(row.get('DS_COMPLEMENTO'))
    neighborhood = verify_nan(row.get('DS_BAIRRO'))
    city_info = cities.get(verify_nan(row.get('CD_CIDADE')), {})
    city = city_info.get('cidade', None)
    state = city_info.get('estado', None)

    new_patient = Contatos(
        Nome=truncate_value(name, 50),
        Nascimento=birthday,
        Sexo=sex,
        Email=truncate_value(email, 100),
    )

    setattr(new_patient, "Id da Assinatura", id_patient)
    setattr(new_patient, "Id do Cliente", id_patient)
    setattr(new_patient, "CPF/CGC", truncate_value(cpf, 25))
    setattr(new_patient, "Pai", truncate_value(father, 50))
    setattr(new_patient, "Mãe", truncate_value(mother, 50))
    setattr(new_patient, "RG", truncate_value(rg, 25))
    setattr(new_patient, "Cônjugue", truncate_value(conjuge, 50))
    setattr(new_patient, "Observações", observations)
    setattr(new_patient, "Celular", truncate_value(cellphone, 20))
    setattr(new_patient, "Telefone", truncate_value(phone, 20))
    setattr(new_patient, "Cep Residencial", cep)
    setattr(new_patient, "Endereço Residencial", truncate_value(address, 50))
    setattr(new_patient, "Endereço Comercial", truncate_value(complement, 50))
    setattr(new_patient, "Bairro Residencial", truncate_value(neighborhood, 25))
    setattr(new_patient, "Cidade Residencial", truncate_value(city, 25))
    setattr(new_patient, "Estado Residencial", truncate_value(state, 2))
    setattr(new_patient, "Profissão", truncate_value(occupation, 25))

    
    log_data.append({
        "Id do Cliente": id_patient,
        "Nome": name,
        "Nascimento": birthday,
        "Sexo": sex,
        "CPF/CGC": cpf,
        "Pai": father,
        "Mãe": mother,
        "Email": email,
        "Cônjugue": conjuge,
        "RG": rg,
        "Observações": observations,
        "Celular": cellphone,
        "Telefone": phone,
        "Cep Residencial": cep,
        "Endereço Residencial": address,
        "Endereço Comercial": complement,
        "Bairro Residencial": neighborhood,
        "Cidade Residencial": city,
        "Estado Residencial": state,
        "Profissão": occupation,
        "TimeStamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

    session.add(new_patient)

    inserted_cont+=1
    if inserted_cont % 1000 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos contatos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} contatos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_PACIENTE.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_PACIENTE.xlsx")
