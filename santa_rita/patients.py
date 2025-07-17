from datetime import datetime
import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, truncate_value
import csv

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")

print("Conectando no Banco de Dados...")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine)

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Contatos = getattr(Base.classes, "Contatos")

print("Sucesso! Inicializando migração de Contatos...")

csv.field_size_limit(1000000)
extension_file = glob.glob(f'{path_file}/pacientes.csv')
address_file = glob.glob(f'{path_file}/endereco.csv')
phone_file = glob.glob(f'{path_file}/telefone.csv')

df = pd.read_csv(extension_file[0], sep=',')
df_address = pd.read_csv(address_file[0], sep=',')
df_phone = pd.read_csv(phone_file[0], sep=',')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

address_lookup = {row['Id']: [row['Cidade'], row['Bairro'], row['Rua'], row['Numero']] for _,row in df_address.iterrows() if pd.notna(row['Id'])}
phone_lookup = {row['Id']: row['Numero'] for _,row in df_phone.iterrows() if pd.notna(row['Id'])}

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for idx, row in df.iterrows():

    if row["Id"] in [None, '', 'None'] or pd.isna(row["Id"]):
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Cliente vazio'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    else:
        id_patient = int(row["Id"])

    existing_record = exists(session, id_patient, "Id do Cliente", Contatos)
    if existing_record:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Cliente já existe'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    
    if row['Nome'] in [None, '', 'None'] or pd.isna(row['Nome']):
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Nome do Paciente vazio'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    else:
        name = row['Nome']


    if is_valid_date(str(row["DataNascimento"][:10]), "%Y-%m-%d"):
        birthday = str(row["DataNascimento"][:10])
    else:
        birthday = '1900-01-01'

    locations = address_lookup.get(row['EnderecoId'])
    print(f'Location {locations}')
    if locations is not None:
        if locations[2] not in [None, 'NULL', '']:
            address = f"{locations[2]} {locations[3]}" if locations[3] not in [None, 'NULL', ''] else locations[2]
        else:
            address = ''
        neighbourhood = locations[1] if locations[1] not in [None, 'NULL', ''] else ''
        city = locations[0] if locations[0] not in [None, 'NULL', ''] else ''
    else:
        address = ''
        neighbourhood = ''
        city = ''

    sex = 'M'
    email = ''
    cpf = row['Cpf'] if 'Cpf' in row and row['Cpf'] not in [None, '', 'NULL'] else ''
    telephone = ''
    cellphone = phone_lookup.get(row['TelefoneId'], '')

    if cellphone in [None, '', 'NULL']:
        cellphone = ''

    cep = ''
    complement = ''
    mother = ''
    state = ''
    observation = ''
    marital_status = ''
    occupation = ''
    rg = ''
    father = ''

    new_patient = Contatos(
        Nome=truncate_value(name, 50),
        Nascimento=birthday,
        Sexo=sex,
        Celular=truncate_value(cellphone, 25),
        Email=truncate_value(email, 100),
    )

    setattr(new_patient, "Id do Cliente", id_patient)
    setattr(new_patient, "CPF/CGC", truncate_value(cpf, 25))
    setattr(new_patient, "Cep Residencial", truncate_value(cep, 10))
    setattr(new_patient, "Endereço Residencial", truncate_value(address, 50))
    setattr(new_patient, "Endereço Comercial", truncate_value(complement, 50))
    setattr(new_patient, "Bairro Residencial", truncate_value(neighbourhood, 25))
    setattr(new_patient, "Cidade Residencial", truncate_value(city, 25))
    setattr(new_patient, "Estado Residencial", truncate_value(state, 2))
    setattr(new_patient, "Telefone Residencial", truncate_value(telephone, 25))
    setattr(new_patient, "Profissão", truncate_value(occupation, 25))
    setattr(new_patient, "Pai", truncate_value(father, 50))
    setattr(new_patient, "Mãe", truncate_value(mother, 50))
    setattr(new_patient, "RG", truncate_value(rg, 25))
    setattr(new_patient, "Observações", observation)
    setattr(new_patient, "Estado Civil", truncate_value(marital_status, 25))

    
    log_data.append({
        "Id do Cliente": id_patient,
        "Nome": name,
        "Nascimento": birthday,
        "Sexo": sex,
        "CPF/CGC": cpf,
        "RG" : rg,
        "Profissão": occupation,
        "Pai": father,
        "Mãe": mother,
        "Telefone Residencial": telephone,
        "Celular": cellphone,
        "Email": email,
        "Cep Residencial": cep,
        "Endereço Residencial": address,
        "Endereço Comercial": truncate_value(complement, 50),
        "Bairro Residencial": truncate_value(neighbourhood, 25),
        "Cidade Residencial": city,
        "Estado Residencial": state,
        "Observações": observation,
        "Estado Civil": marital_status,
        "TimeStamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

    session.add(new_patient)

    inserted_cont+=1
    if inserted_cont % 1000 == 0:
        session.commit()

    if (idx + 1) % 1000 == 0 or (idx + 1) == len(df):
        print(f"Processados {idx + 1} de {len(df)} registros ({(idx + 1) / len(df) * 100:.2f}%)")

session.commit()

print(f"{inserted_cont} novos contatos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} contatos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_patients.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_patients.xlsx")
