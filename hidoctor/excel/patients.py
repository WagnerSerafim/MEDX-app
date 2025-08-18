import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import urllib
from striprtf.striprtf import rtf_to_text
from utils.utils import is_valid_date, exists, create_log, verify_nan, truncate_value

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase= input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")

print("Conectando no Banco de Dados...\n")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine)

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Contatos = getattr(Base.classes, "Contatos")

print("Sucesso! Inicializando migração de Contatos...")

extension_file = glob.glob(f'{path_file}/dados*.xlsx')

df = pd.read_excel(extension_file[0], sheet_name='PACIENTES_ARQUIVO')

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

    patient_id = verify_nan(row['ID_Pac'])
    if not patient_id:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'ID_Pac vazio'
        row_dict['TimeStamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        not_inserted_data.append(row_dict)
        continue

    existing_patient = exists(session, patient_id, "Id do Cliente", Contatos)
    if existing_patient:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Cliente já existe'
        row_dict['TimeStamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        not_inserted_data.append(row_dict)
        continue

    name = verify_nan(row['Nome_Paciente'])
    if not name:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Nome do Paciente vazio'
        row_dict['TimeStamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        not_inserted_data.append(row_dict)
        continue

    if is_valid_date(row['Nascimento'], '%Y-%m-%d %H:%M:%S'):
        birthday = row['Nascimento']
    else:
        birthday = '1900-01-01'
        
    sex = row['SexoPaciente']
    if sex != 'F':
        sex = 'M'

    address = verify_nan(row['LogradouroPaciente'])
    
    observation = verify_nan(row['PacObservacoes'])
    if observation:
        observation = rtf_to_text(row['PacObservacoes'])

    neighborhood = verify_nan(row['BairroPaciente'])
    cpf = verify_nan(row['CPFPaciente'])
    cellphone = verify_nan(row['PacTelefones'])
    email = verify_nan(row['EMail'])
    cep = verify_nan(row['CEPPaciente'])
    city = verify_nan(row['CidadePaciente'])
    state = verify_nan(row['UFPaciente'])
    occupation = verify_nan(row['ProfissaoPaciente'])

    new_patient = Contatos(
        Nome=truncate_value(name, 50),
        Nascimento=birthday,
        Sexo=sex,
        Celular=truncate_value(cellphone, 25),
        Email=truncate_value(email, 100),
    )

    setattr(new_patient, "Id do Cliente", patient_id)
    setattr(new_patient, "CPF/CGC", truncate_value(cpf, 25))
    setattr(new_patient, "Cep Residencial", truncate_value(cep, 10))
    setattr(new_patient, "Endereço Residencial", truncate_value(address, 50))
    setattr(new_patient, "Bairro Residencial", truncate_value(neighborhood, 25))
    setattr(new_patient, "Cidade Residencial", truncate_value(city, 25))
    setattr(new_patient, "Observações", observation)

    
    log_data.append({
        "Id do Cliente": patient_id,
        "Nome": truncate_value(row["Nome_Paciente"], 50),
        "Nascimento": birthday,
        "Sexo": sex,
        "CPF/CGC": cpf,
        "Celular": row["PacTelefones"],
        "Email": row["EMail"],
        "Cep Residencial": cep,
        "Endereço Residencial": address,
        "Bairro Residencial": truncate_value(neighborhood, 25),
        "Cidade Residencial": city,
        "Observações": observation,
        "TimeStamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
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

create_log(log_data, log_folder, "log_inserted_patients_PACIENTES_ARQUIVO.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_patients_PACIENTES_ARQUIVO.xlsx")