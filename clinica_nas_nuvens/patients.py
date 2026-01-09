from datetime import datetime
import glob
import os
import re
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import DataError, IntegrityError
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, limpar_cpf, limpar_numero, truncate_value, verify_nan
import csv

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

csv.field_size_limit(1000000)
extension_file = glob.glob(f'{path_file}/PESSOA.csv')
address_file = glob.glob(f'{path_file}/ENDERECO.csv')
contact_file = glob.glob(f'{path_file}/CONTATO.csv')
city_file = glob.glob(f'{path_file}/CIDADE.csv')
patient_file = glob.glob(f'{path_file}/PACIENTE.csv')

df = pd.read_csv(extension_file[0], sep=',', engine='python', encoding='utf-8', quotechar='"')

df_address = pd.read_csv(address_file[0], sep=',', engine='python', encoding='utf-8', quotechar='"')
address_lookup = {}
for idx, row in df_address.iterrows():
    address_lookup[row['codigo']] = {
        'bairro': verify_nan(row['bairro']),
        'cep': verify_nan(row['cep']),
        'id_cidade': verify_nan(row['codcidade']),
        'endereco': verify_nan(row['rua']),
        'numero': verify_nan(row['numero']),
        'complemento': verify_nan(row['complemento'])
    }

df_city = pd.read_csv(city_file[0], sep=',', engine='python', encoding='utf-8', quotechar='"')
city_lookup = {}
for idx, row in df_city.iterrows():
    city_lookup[row['codigo']] = verify_nan(row['nome'])

df_contact = pd.read_csv(contact_file[0], sep=',', engine='python', encoding='utf-8', quotechar='"')
contact_lookup = {}
for idx, row in df_contact.iterrows():
    contact_lookup[row['codigo']] = {
        'telefone': verify_nan(row['telefoneResidencial']),
        'celular': verify_nan(row['telefoneCelular'])
    }

df_patient = pd.read_csv(patient_file[0], sep=',', engine='python', encoding='utf-8', quotechar='"')
patient_lookup = {}
for idx, row in df_patient.iterrows():
    patient_lookup[row['codpessoa_fk']] = {
        'id_patient': verify_nan(row['codpessoa'])
    }

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for idx, row in df.iterrows():

    if idx % 1000 == 0 or idx == len(df):
        print(f"Processados: {idx} | Inseridos: {inserted_cont} | Não inseridos: {not_inserted_cont} | Concluído: {round((idx / len(df)) * 100, 2)}%")

    id_patient = verify_nan(row["codigo"])
    if id_patient is None:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Cliente vazio'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    id_patient_infos = patient_lookup.get(id_patient, {})
    if not id_patient_infos:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Cliente não encontrado em PACIENTE'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    id_patient = id_patient_infos.get('id_patient')


    existing_record = exists(session, id_patient, "Id do Cliente", Contatos)
    if existing_record:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Cliente já existe'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue
    
    name = verify_nan(row['nomecivilcompleto'])
    if name is None:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Nome do Paciente vazio'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    birthday_str = verify_nan(row["dataNascimento"])
    if birthday_str is None:
        birthday = '1900-01-01'
    else:
        birthday_obj = datetime.strptime(birthday_str, "%Y-%m-%d")
        birthday = birthday_obj.strftime("%Y-%m-%d")

    if row['sexo'] == 'F':
        sex = 'F'
    else:
        sex = 'M'

    id_endereco = verify_nan(row['codendereco'])
    if id_endereco is None:
        address = None
        cep = None
        neighbourhood = None
        city = None
        complement = None
    else:
        address_info = address_lookup.get(id_endereco, {})
        neighbourhood = address_info.get('bairro')
        cep = limpar_numero(address_info.get('cep'))
        city_id = address_info.get('id_cidade')
        address = address_info.get('endereco')
        if address_info.get('numero'):
            address = f"{address} {address_info.get('numero')}"
        complement = address_info.get('complemento')
        city = city_lookup.get(city_id)
    
    id_contact = verify_nan(row['codcontato'])
    if id_contact is None:
        telephone = None
        cellphone = None
    else:
        contact_info = contact_lookup.get(id_contact, {})
        telephone = limpar_numero(contact_info.get('telefone'))
        cellphone = limpar_numero(contact_info.get('celular'))

    email = verify_nan(row['email'])
    cpf = limpar_cpf(verify_nan(row['cpfOuCnpj']))
    observation = None
    occupation = verify_nan(row['profissao'])
    rg = limpar_numero(verify_nan(row['numrg']))
    mother = None
    father = None
    marital_status = None

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
        "Observações": observation,
        "Estado Civil": marital_status,
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

create_log(log_data, log_folder, "log_inserted_patients_PESSOA.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_patients_PESSOA.xlsx")
