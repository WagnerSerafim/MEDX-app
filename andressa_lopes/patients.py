from datetime import datetime
import glob
import os
import re
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, truncate_value, verify_nan
    
def limpar_numero(valor):
    if valor is None:
        return None
    valor_str = str(valor)
    if valor_str.endswith('.0'):
        valor_str = valor_str[:-2]
    valor_str = valor_str.strip()
    return valor_str

def limpar_cpf(valor):
    if valor is None:
        return None
    valor_str = str(valor)
    if valor_str.endswith('.0'):
        valor_str = valor_str[:-2]
    valor_str = re.sub(r'\D', '', valor_str)
    if len(valor_str) < 11 and len(valor_str) > 0:
        valor_str = valor_str.zfill(11)
    return valor_str if valor_str else None

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

cadastro_file = glob.glob(f'{path_file}/pacientes.json')
responsaveis_file = glob.glob(f'{path_file}/respons.json')

with open(cadastro_file[0], 'r', encoding='latin1') as f:
    df = pd.read_json(f)

with open(responsaveis_file[0], 'r', encoding='latin1') as f:
    df_responsaveis = pd.read_json(f)

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont = 0 
not_inserted_data = []
not_inserted_cont = 0

responsaveis = {}
for _,row in df_responsaveis.iterrows():
    responsaveis[int(row['RESCOD'])] = {
        'mae': verify_nan(row['RESMAE']),
        'pai': verify_nan(row['RESPAI']),
        'endereco': verify_nan(row['RESEND']),
        'bairro': verify_nan(row['RESBAI']),
        'cidade': verify_nan(row['RESCID']),
        'celular': verify_nan(row['RESCEL']),
        'telefone': verify_nan(row['RESFON']),
    }

for idx, row in df.iterrows():

    if idx % 1000 == 0 or idx == len(df):
        print(f"Processados: {idx} | Inseridos: {inserted_cont} | Não inseridos: {not_inserted_cont} | Concluído: {round((idx / len(df)) * 100, 2)}%")

    id_resp = verify_nan(row["PACRES"])
    if id_resp == None:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do responsável vazio'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    id_child = verify_nan(row["PACSEQ"])
    if id_child == None:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do filho vazio'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    id_patient = f"{id_resp}-{id_child}"
    
    name = verify_nan(row["PACNOM"])
    if name == None:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Nome do Paciente vazio'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    existing_record = exists(session, id_patient, "Referências", Contatos)
    if existing_record:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Cliente já existe no Banco de Dados'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    email = None

    try:
        birthday_obj = verify_nan(row["PACNAS"])
        if birthday_obj == None:
            birthday = '1900-01-01'
        else:
            birthday = datetime.strptime(birthday_obj, '%Y-%m-%d').strftime('%Y-%m-%d')
            if not birthday or not is_valid_date(birthday, '%Y-%m-%d'):
                birthday = '1900-01-01'
    except ValueError:
        birthday = '1900-01-01'

    sex = verify_nan(row['PACSEX'])
    sex = 'F' if sex == 'FEMININO' else 'M'

    pac_infos = responsaveis.get(id_patient, None)
    if pac_infos:
        mother = pac_infos.get('mae', None)
        father = pac_infos.get('pai', None)
        address = pac_infos.get('endereco', None)
        neighborhood = pac_infos.get('bairro', None)
        city = pac_infos.get('cidade', None)
        cellphone = pac_infos.get('celular', None)
        phone = pac_infos.get('telefone', None)
    else:
        mother = None
        father = None
        address = None
        neighborhood = None
        city = None
        cellphone = None
        phone = None
    
    rg = None
    cpf = None
    conjuge = None
    observations = verify_nan(row['PACOBS'])
    occupation = None
    cep = None
    complement = None
    state = None

    new_patient = Contatos(
        Nome=truncate_value(name, 50),
        Nascimento=birthday,
        Sexo=sex,
        Email=truncate_value(email, 100),
    )

    setattr(new_patient, "Referências", id_patient)
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

create_log(log_data, log_folder, "log_inserted_pacientes.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_pacientes.xlsx")
