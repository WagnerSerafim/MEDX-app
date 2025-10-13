import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import *

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

excel_file = glob.glob(f'{path_file}/dados*.xlsx')
df = pd.read_excel(excel_file[0], sheet_name = 'pacientes_semicol')
df = df.replace('None', '')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0
existId = True

for idx, row in df.iterrows():

    if idx % 1000 == 0 or idx == len(df):
        print(f"Processados: {idx} | Inseridos: {inserted_cont} | Não inseridos: {not_inserted_cont} | Concluído: {round((idx / len(df)) * 100, 2)}%")

    id_patient = verify_nan(row['ID'])
    # Considere ID válido apenas se não for vazio, não for None, não for 'None', não for NaN e não for 0
    if id_patient in [None, '', 'None'] or pd.isna(id_patient) or str(id_patient).strip() == '0':
        existId = False
        id_patient = ''
    else:
        id_patient = int(float(id_patient))  # Garante conversão mesmo se vier como float em string
        existId = True
        existing_patient = exists(session, id_patient, "Id do Cliente", Contatos)
        if existing_patient:
            not_inserted_cont +=1
            row_dict = row.to_dict()
            row_dict['Motivo'] = 'Id do Cliente já existe no Banco de Dados'
            not_inserted_data.append(row_dict)
            continue

    print(f'ID do paciente sendo processado: {id_patient}; Existia: {existId}')

    if row['NOME'] in [None, '', 'None'] or pd.isna(row['NOME']):
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Nome vazio'
        not_inserted_data.append(row_dict)
        continue
    else:
        name = truncate_value(clean_value(row["NOME"]), 50)

    cellphone = truncate_value(clean_value(row["CELULAR"]), 25)
    if cellphone not in [None, '', 'None']:
        cellphone = str(cellphone).replace('.0', '').zfill(8)
    else:
        cellphone = ''

    new_patient = Contatos(
        Nome=name,
        Nascimento='1900-01-01',
        Sexo='M',
        Celular=cellphone,
    )

    if existId:
        setattr(new_patient, "Id do Cliente", id_patient)
        setattr(new_patient, "Id da Assinatura", id_patient)
        log_data.append({"Id do Cliente": id_patient})
        log_data.append({"Id da Assinatura": id_patient})

    
    log_data.append({
        "Nome": name,
        "Nascimento": '1900-01-01',
        "Sexo": 'M',
        "Celular": cellphone
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

create_log(log_data, log_folder, "log_inserted_patients.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_patients.xlsx")
