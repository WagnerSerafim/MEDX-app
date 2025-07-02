import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, truncate_value
import csv
from datetime import datetime

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

print("Sucesso! Inicializando a leitura do arquivo...")

extension_file = glob.glob(f'{path_file}/pacientes.csv')

csv.field_size_limit(1000000000)
df = pd.read_csv(extension_file[0], sep=';', engine='python')

# Remove aspas simples de todos os valores string do DataFrame
df = df.applymap(lambda x: x.replace("'", "") if isinstance(x, str) else x)

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

print("Sucesso! Iniciando a migração de pacientes...")

for _, row in df.iterrows():

    # Verifica se Cod Paciente é um número
    try:
        id_patient = int(row['Cod Paciente'])
    except (ValueError, TypeError):
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Cod Paciente não é um número válido'
        not_inserted_data.append(row_dict)
        continue

    existing_record = exists(session, id_patient, "Id do Cliente", Contatos)
    if existing_record:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Cliente já existe no banco de dados'
        not_inserted_data.append(row_dict)
        continue

    if row["Cod Paciente"] in [None, '', 'None'] or pd.isna(row["Cod Paciente"]):
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Cliente vazio'
        not_inserted_data.append(row_dict)
        continue
    else:
        id_patient = row["Cod Paciente"]
    
    if row['Nome Paciente'] in [None, '', 'None'] or pd.isna(row['Nome Paciente']):
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Nome do Paciente vazio'
        not_inserted_data.append(row_dict)
        continue
    else:
        name = row['Nome Paciente']

    if is_valid_date(row["Nascimento"], "%d-%m-%Y"):
        dt_datetime = datetime.strptime(row['Nascimento'], "%d/%m/%Y")
        birthday = dt_datetime.strftime("%Y-%m-%d")
    else:
        birthday = '01/01/1900'

    if row['Sexo'] == 'F':
        sex = 'F'
    else:
        sex = 'M'

    if row['Endereco'] in [None, '', 'None'] or pd.isna(row['Endereco']):
        address = ''
    else:
        if row['Numero'] not in [None, '', 'None'] or pd.isna(row['Numero']):
            number = str(row['Numero'])
            address = f"{row['Endereco']} {number}"
        else:
            address = row['Endereco']

    if row['Complemento'] in [None, '', 'None'] or pd.isna(row['Complemento']):
        complement = ''
    else:
        complement = row['Complemento']

    if row['Bairro'] in [None, '', 'None'] or pd.isna(row['Bairro']):
        neighbourhood = ''
    else:
        neighbourhood = row['Bairro']
    
    if row['Cidade'] in [None, '', 'None'] or pd.isna(row['Cidade']):
        city = ''
    else:
        city = row['Cidade']

    if row['UF'] in [None, '', 'None'] or pd.isna(row['UF']):
        state = ''
    else:
        state = row['UF']

    if row['CEP'] in [None, '', 'None'] or pd.isna(row['CEP']):
        cep = ''
    else:
        cep = row['CEP']

    if row['E-mail'] in [None, '', 'None'] or pd.isna(row['E-mail']):
        email = ''
    else:
        email = row['E-mail']

    if row['Telefone'] in [None, '', 'None'] or pd.isna(row['Telefone']):
        telephone = ''
    else:
        telephone = row['Telefone']

    if row['Celular'] in [None, '', 'None'] or pd.isna(row['Celular']):
        cellphone = ''
    else:
        cellphone = row['Celular']

    if row['CPF'] in [None, '', 'None'] or pd.isna(row['CPF']):
        cpf = ''
    else:
        cpf = row['CPF']
    
    if row['RG'] in [None, '', 'None'] or pd.isna(row['RG']):
        rg = ''
    else:
        rg = row['RG']

    if row['Ocupacao'] in [None, '', 'None'] or pd.isna(row['Ocupacao']):
        occupation = ''
    else:
        occupation = row['Ocupacao']

    if row['Observacao'] in [None, '', 'None'] or pd.isna(row['Observacao']):
        observation = ''
    else:
        observation = row['Observacao']

    if row['Pai'] in [None, '', 'None'] or pd.isna(row['Pai']):
        father = ''
    else:
        father = row['Pai']
    
    if row['Mae'] in [None, '', 'None'] or pd.isna(row['Mae']):
        mother = ''
    else:
        mother = row['Mae']
       
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
        'Estado Residencial': state,
        "Observações": observation
    })

    session.add(new_patient)

    inserted_cont+=1
    if inserted_cont % 100 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos contatos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} contatos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_patients_pacientes.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_patients_pacientes.xlsx")
