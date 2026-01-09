from datetime import datetime
import glob
import os
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
import urllib
from utils.utils import clean_string, is_valid_date, exists, create_log, limpar_cpf, limpar_numero, truncate_value, verify_nan

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


extension_file = glob.glob(f'{path_file}/patients*.xlsx')

df = pd.read_excel(extension_file[0])

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont = 0
not_inserted_data = []
not_inserted_cont = 0

for idx, row in df.iterrows():
    
    if idx % 1000 == 0 or idx == len(df):
        print(f"Processados: {idx} | Inseridos: {inserted_cont} | Não inseridos: {not_inserted_cont} | Concluído: {round((idx / len(df)) * 100, 2)}%")

    id_patient = limpar_numero(verify_nan(row['FICHAPACIENTEID']))
    if id_patient is None:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Cliente vazio'
        not_inserted_data.append(row_dict)
        continue

    ref = limpar_numero(verify_nan(row['PACIENTEID']))

    existing_record = exists(session, id_patient, "Id do Cliente", Contatos)
    if existing_record:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Cliente já existe'
        not_inserted_data.append(row_dict)
        continue

    name = verify_nan(row['NOME'])
    if name is None:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Nome do Paciente vazio'
        not_inserted_data.append(row_dict)
        continue
    clean_name = clean_string(name)

    birthday_str = verify_nan(row['DATANASCIMENTO'])
    if birthday_str is None:
        birthday = '1900-01-01'
    else:
        try:
            birthday_obj = datetime.strptime(birthday_str, "%d/%m/%Y")
            birthday = birthday_obj.strftime("%Y-%m-%d")
            if not is_valid_date(birthday, "%Y-%m-%d"):
                birthday = '1900-01-01'
        except:
            birthday_obj = datetime.strptime(birthday_str, "%Y-%m-%d")
            birthday = birthday_obj.strftime("%Y-%m-%d")
            if not is_valid_date(birthday, "%Y-%m-%d"):
                birthday = '1900-01-01'

    if row['SEXO'] == 'Feminino':
        sex = 'F'
    else:
        sex = 'M'
    
    telephone = verify_nan(row['TELEFONE'])
    if telephone:
        telephone_ddd = verify_nan(row['TELEFONEDDD'])
        telephone = f"({telephone_ddd}) {telephone}" if telephone_ddd else telephone
    
    cellphone = verify_nan(row['CELULAR'])
    if cellphone:
        cellphone_ddd = verify_nan(row['CELULARDDD'])
        cellphone = f"({cellphone_ddd}) {cellphone}" if cellphone_ddd else cellphone

    email = verify_nan(row['EMAIL'])
    cpf = limpar_cpf(verify_nan(row['CPFCNPJ']))
    rg = limpar_numero(verify_nan(row['RG']))
    cep = limpar_numero(verify_nan(row['CEP']))
    complement = verify_nan(row['COMPLEMENTO'])
    neighbourhood = verify_nan(row['BAIRRO'])
    city = verify_nan(row['CIDADE'])
    state = verify_nan(row['ESTADO'])
    occupation = verify_nan(row['PROFISSAO'])
    mother = verify_nan(row['NOMEMAE'])
    father = verify_nan(row['NOMEPAI'])
    observation = verify_nan(row['OBSERVACAO'])

    address = verify_nan(row['ENDERECO'])
    if address:
        num = limpar_numero(verify_nan(row['NUMERO']))
        if num:
            address = f"{address} {num}"

    new_patient = Contatos(
        Nome=truncate_value(clean_name, 50),
        Nascimento=birthday,
        Sexo=sex,
        Celular=truncate_value(cellphone, 25),
        Email=truncate_value(email, 100),
    )

    setattr(new_patient, "Id do Cliente", id_patient)
    setattr(new_patient, "CPF/CGC", truncate_value(cpf, 25))
    setattr(new_patient, "Observação", truncate_value(observation, 255))
    setattr(new_patient, "Referências", ref)
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

create_log(log_data, log_folder, "log_inserted_patients_pacientes.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_patients_pacientes.xlsx")
