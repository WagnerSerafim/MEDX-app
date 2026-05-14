from datetime import datetime
import glob
import os
import sys
import urllib

from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from utils.utils import (
    is_valid_date,
    exists,
    create_log,
    truncate_value,
    verify_nan,
    limpar_numero,
    limpar_cpf,
)


def parse_birthday(value):
    value = verify_nan(value)
    if value is None:
        return '1900-01-01'
    raw = str(value).strip()
    for fmt in ('%d/%m/%Y %H:%M', '%d/%m/%Y %H:%M:%S', '%d/%m/%Y'):
        try:
            parsed = datetime.strptime(raw, fmt).strftime('%Y-%m-%d')
            if is_valid_date(parsed, '%Y-%m-%d') and not parsed.startswith('1900-01-01'):
                return parsed
            return parsed if is_valid_date(parsed, '%Y-%m-%d') else '1900-01-01'
        except ValueError:
            continue
    return '1900-01-01'


def parse_sex(value):
    value = verify_nan(value)
    if value is None:
        return 'M'
    value = str(value).strip().upper()
    if value.startswith('F'):
        return 'F'
    return 'M'


sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")

print("Conectando no Banco de Dados...")

DATABASE_URL = (
    f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/"
    f"{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"
)

engine = create_engine(DATABASE_URL)

metadata = MetaData()
contatos_tbl = Table("Contatos", metadata, schema=f"schema_{sid}", autoload_with=engine)

Base = declarative_base()


class Contatos(Base):
    __table__ = contatos_tbl


SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

print("Sucesso! Inicializando migração de Contatos...")

cadastro_file = glob.glob(f'{path_file}/Pacientes_exporta_dados*.csv')
if not cadastro_file:
    raise FileNotFoundError(f"Nenhum arquivo Pacientes_exporta_dados*.csv encontrado em {path_file}")

try:
    df = pd.read_csv(cadastro_file[0], engine='python', dtype=str, sep=';', quotechar='"', encoding='utf-8')
except UnicodeDecodeError:
    df = pd.read_csv(cadastro_file[0], engine='python', dtype=str, sep=';', quotechar='"', encoding='latin1')

log_folder = path_file
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont = 0
not_inserted_data = []
not_inserted_cont = 0

total = len(df)

for idx, row in df.iterrows():

    if idx % 1000 == 0 or idx == total:
        pct = round((idx / total) * 100, 2) if total else 0
        print(f"Processados: {idx} | Inseridos: {inserted_cont} | Não inseridos: {not_inserted_cont} | Concluído: {pct}%")

    id_patient = verify_nan(row["Paciente_Codigo"])
    if id_patient is None:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Cliente vazio'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    name = verify_nan(row["paciente_nome"])
    if name is None:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Nome do Paciente vazio'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    inativo = verify_nan(row.get("inativo"))
    if inativo is not None and str(inativo).strip().lower() == 'true':
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Paciente inativo'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    existing_record = exists(session, id_patient, "Referências", Contatos)
    if existing_record:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Cliente já existe no Banco de Dados'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    email = verify_nan(row["paciente_email"])
    birthday = parse_birthday(row["paciente_nascimento"])
    sex = parse_sex(row['paciente_sexo'])

    rg = limpar_numero(verify_nan(row['paciente_rg']))
    cpf = limpar_cpf(verify_nan(row['paciente_cpf']))
    cellphone = limpar_numero(verify_nan(row['paciente_celular']))
    phone = limpar_numero(verify_nan(row['telefone']))
    occupation = verify_nan(row['paciente_profissao'])
    observations = verify_nan(row['observacoes'])
    conjuge = verify_nan(row['conjuge_nome'])

    cep = limpar_numero(verify_nan(row['cep']))
    address = verify_nan(row['endereco'])
    number = verify_nan(row['Numero'])
    if address and number:
        address = f"{address} {number}"
    complement = verify_nan(row['complemento'])
    neighborhood = verify_nan(row['bairro'])
    city = verify_nan(row['cidade'])
    state = verify_nan(row['estado'])

    mother = None
    father = None

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
        "Referências": id_patient,
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
        "TimeStamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })

    session.add(new_patient)

    inserted_cont += 1
    if inserted_cont % 1000 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos contatos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} contatos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_pacientes.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_pacientes.xlsx")
