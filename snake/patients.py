import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import *
from datetime import datetime

def validate_sqlserver_date(date_str):
    try:
        if pd.isna(date_str) or date_str in ['', None]:
            return None
        dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        # SQL Server aceita datas a partir de 1753-01-01
        if dt.year < 1900:
            return None
        return date_str
    except Exception:
        return None

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")

print("Conectando no Banco de dados...")

try:
    DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

    engine = create_engine(DATABASE_URL)

    Base = automap_base()
    Base.prepare(autoload_with=engine)

    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()

    Contatos = Base.classes.Contatos

except Exception as e:
    print(f"Erro ao conectar ao banco de dados: {e}")
    exit()

print("Sucesso! Começando migração de pacientes...")

excel_file = glob.glob(f'{path_file}/pacientes*.xlsx')
df = pd.read_excel(excel_file[0])
df = df.replace('None', '')

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

    id_patient = verify_nan(row['IdPaciente'])
    if row['IdPaciente'] in [None, '', 'None']:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Cliente vazio'
        not_inserted_data.append(row_dict)
        continue

    existing_patient = exists(session, id_patient, "Id do Cliente", Contatos)
    if existing_patient:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Cliente já existe no Banco de Dados'
        not_inserted_data.append(row_dict)
        continue

    birthday = parse_us_datetime_to_sql(row['DataNascimento'])
    birthday = validate_sqlserver_date(birthday)
    if birthday is None:
        birthday = '1900-01-01 00:00:00'

    if row['Sexo'] == 'F':
        sex = "F"
    else:
        sex = "M"

    address = verify_nan(row["Logradouro"])
    number = verify_nan(row["Numero"])
    if number not in [None, '', 'None']:
        address = f"{address} {number}"

    if row['Nome'] in [None, '', 'None'] or pd.isna(row['Nome']):
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Nome vazio'
        not_inserted_data.append(row_dict)
        continue
    else:
        name = truncate_value(row["Nome"], 50)
    
        
    rg = truncate_value(clean_value(str(row["RG"])), 25)
    if rg not in [None, '', 'None']:
        rg = str(rg).replace('.0', '').zfill(8)
    else:
        rg = ''

    cpf = truncate_value(verify_nan(row['CpfCnpj']), 10)
    if cpf not in [None, '', 'None']:
        cpf = str(cpf).replace('.0', '').zfill(8)
    else:
        cpf = ''

    cellphone = truncate_value(clean_value(row["Telefone1"]), 25)
    if cellphone not in [None, '', 'None']:
        cellphone = str(cellphone).replace('.0', '').zfill(8)
    else:
        cellphone = ''

    email = truncate_value(clean_value(row["Email"]), 100)

    cep = truncate_value(verify_nan(row['Cep']), 10)
    if cep not in [None, '', 'None']:
        cep = str(cep).replace('.0', '').zfill(8)
    else:
        cep = ''

    complement = truncate_value(clean_value(row["Complemento"]), 50)

    neighbourhood = truncate_value(clean_value(row["Bairro"]), 25)

    city = truncate_value(clean_value(row["Cidade"]), 25)

    father = truncate_value(clean_value(row["Pai"]), 50)

    observation = clean_value(row["Observacoes"])

    address = truncate_value(clean_value(address), 50)

    new_patient = Contatos(
        Nome=name,
        Nascimento=birthday,
        Sexo=sex,
        Celular=cellphone,
        Email=email,
    )

    setattr(new_patient, "Id do Cliente", id_patient)
    setattr(new_patient, "CPF/CGC", cpf)
    setattr(new_patient, "Cep Residencial", cep)
    setattr(new_patient, "Endereço Residencial", address)
    setattr(new_patient, "Endereço Comercial", complement)
    setattr(new_patient, "Bairro Residencial", neighbourhood)
    setattr(new_patient, "Cidade Residencial", city)
    setattr(new_patient, "Pai", father)
    setattr(new_patient, "RG", rg)
    setattr(new_patient, "Observações", observation)
    
    log_data.append({
        "Id do Cliente": id_patient,
        "Nome": name,
        "Nascimento": birthday,
        "Sexo": sex,
        "CPF/CGC": cpf,
        "RG" : rg,
        "Pai": father,
        "Celular": cellphone,
        "Email": email,
        "Cep Residencial": cep,
        "Endereço Residencial": address,
        "Endereço Comercial": complement,
        "Bairro Residencial": neighbourhood,
        "Cidade Residencial": city,
        "Observações": observation
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
