import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, truncate_value
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

print("Sucesso! Inicializando migração de Contatos...")

extension_file = glob.glob(f'{path_file}/Pacientes.xlsx')
cities_file = glob.glob(f'{path_file}/Cidades.xlsx')
neighbourhoods_file = glob.glob(f'{path_file}/Bairros.xlsx')

df = pd.read_excel(extension_file[0])
df_cities = pd.read_excel(cities_file[0])
df_neighbourhoods = pd.read_excel(neighbourhoods_file[0])

city_lookup = {row["CidadeID"]: row["NomeCidade"] for _, row in df_cities.iterrows()}
neighbourhood_lookup = {row["BairroID"]: row["Nome"] for _, row in df_neighbourhoods.iterrows()}

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

for _, row in df.iterrows():

    if row["PacienteID"] == None or row["PacienteID"] == '' or row["PacienteID"] == 'None':
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Cliente vazio'
        not_inserted_data.append(row_dict)
        continue
    else:
        id_patient = row["PacienteID"]
    
    existing_record = exists(session, row['PacienteID'], "Id do Cliente", Contatos)
    if existing_record:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Cliente já existe'
        not_inserted_data.append(row_dict)
        continue

    
    if row['Nome'] == None or row['Nome'] == '' or row['Nome'] == 'None':
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Nome do Paciente vazio'
        not_inserted_data.append(row_dict)
        continue
    else:
        name = row['Nome']


    try:
        date_str = row['DataNascimento'][:10].strip()
        date = datetime.strptime(date_str, '%m/%d/%Y')
        date = date.strftime('%Y/%m/%d')
        if is_valid_date(date, "%Y-%m-%d"):
            birthday = date
        else:
            raise Exception
    except Exception as e:
        birthday = '01/01/1900'
        continue

    sex = row['Sexo']

    city = city_lookup.get(row["CidadeID"], None)
    neighbourhood = neighbourhood_lookup.get(row["BairroID"], None)

    email = row['Email']
    cpf = row['CPF']
    rg = row['RG']
    telephone = row['Telefone']
    cellphone = row['Celular']
    cep = row['CEP']
    complement = row['Complemento']
    state = ''
    occupation = row['Profissao']
    mother = row['NomeMae']
    father = row['NomePai']

    observation = ''
    
    address = f"{row['Endereco']} {row['NumeroEndereco']}"

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
        "RG": rg,
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

    inserted_cont += 1
    if inserted_cont % 10000 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos contatos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} contatos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_cadPaciente.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_cadPaciente.xlsx")
