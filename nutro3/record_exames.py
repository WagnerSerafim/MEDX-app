import csv
import glob
import os
from sqlalchemy import MetaData, Table, create_engine, bindparam, UnicodeText
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
import urllib
from utils.utils import is_valid_date, create_log, verify_nan, exists

def get_record(exame_id, exames, resultado):
    """
    A partir da linha do dataframe, retorna o histórico formatado.
    """
    record = ''
    try:
        exames_info = exames[exame_id]
        descricao = exames_info['descricao']
        unidade = exames_info['unidade']

        record = f"{descricao}: {resultado} {unidade}"
    except:
        return ''
    
    return record


sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")

print("Conectando no Banco de Dados...")
DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

metadata = MetaData()
historico_tbl = Table("Histórico de Clientes", metadata, schema=f"schema_{sid}", autoload_with=engine)
contatos_tbl = Table("Contatos", metadata, schema=f"schema_{sid}", autoload_with=engine)

Base = declarative_base()

class Historico(Base):
    __table__ = historico_tbl

class Contatos(Base):
    __table__ = contatos_tbl

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

print("Sucesso! Inicializando migração de Históricos...")

csv.field_size_limit(100000000000)
todos_arquivos = glob.glob(os.path.join(path_file, 'CONSULTA_EXAME_*.csv'))
exame = glob.glob(os.path.join(path_file, 'EXAME_[0-9]*.csv'))
paciente_consultas = glob.glob(os.path.join(path_file, 'PACIENTE_CONSULTAS_*.csv'))
consultas = glob.glob(os.path.join(path_file, 'CONSULTA_[0-9]*.csv'))

df = pd.read_csv(todos_arquivos[0], dtype=str, sep=',', encoding='utf-8', quotechar='"')
df_paciente = pd.read_csv(paciente_consultas[0], dtype=str, sep=',', encoding='utf-8', quotechar='"')
df_exame = pd.read_csv(exame[0], dtype=str, sep=',', encoding='utf-8', quotechar='"')
df_consultas = pd.read_csv(consultas[0], dtype=str, sep=',', encoding='utf-8', quotechar='"')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

pacientes = {}
for _,row in df_paciente.iterrows():
    pacientes[row['CONSULTA']] = row['PACIENTE']

exames = {}
for _,row in df_exame.iterrows():
    exames[row['ID']] = {
        'descricao': row['DESCRICAO'],
        'unidade': row['UNIDADE']
    }

consultas = {}
for _,row in df_consultas.iterrows():
    consultas[row['ID']] = row['DATA']

for idx, row in df.iterrows():

    if idx % 1000 == 0 or idx == len(df):
        print(f"Processados: {idx} | Inseridos: {inserted_cont} | Não inseridos: {not_inserted_cont} | Concluído: {round((idx / len(df)) * 100, 2)}%")

    consulta_id = row['CONSULTA']

    resultado = verify_nan(row['RESULTADO'])
    if resultado == "0,00":
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Resultado igual a 0,00'
        not_inserted_data.append(row_dict)
        continue

    id_patient = pacientes.get(consulta_id, None)
    if id_patient == None:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id da consulta não existe no lookup de pacientes'
        not_inserted_data.append(row_dict)
        continue

    patient = exists(session, id_patient, 'Referências', Contatos)
    if not patient:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Paciente não encontrado no banco de dados'
        not_inserted_data.append(row_dict)
        continue
    else:
        id_patient = getattr(patient, "Id do Cliente")

    record = get_record(row['EXAME'], exames, row['RESULTADO'])
    if record == "":
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Histórico vazio ou inválido'
        not_inserted_data.append(row_dict)
        continue

    date = consultas.get(consulta_id, None)
    if not is_valid_date(date, '%Y-%m-%d'):
        date = '01/01/1900 00:00'

    new_record = Historico(
        Data=date
    )
    # setattr(new_record, "Id do Histórico", (row['id']))
    setattr(new_record, "Id do Cliente", id_patient)
    setattr(new_record, "Id do Usuário", 0)
    setattr(new_record, "Histórico", bindparam(None, value=record, type_=UnicodeText()))
    
    log_data.append({
        # "Id do Histórico": (row['id']),
        "Id do Cliente": id_patient,
        "Data": date,
        "Histórico": record,
        "Id do Usuário": 0,
    })
    session.add(new_record)
    inserted_cont+=1

    if inserted_cont % 1000 == 0:
        session.commit()


session.commit()

print("Migração concluída! Gerando logs...")
print(f"{inserted_cont} novos históricos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} históricos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_record_exames.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_record_exames.xlsx")
