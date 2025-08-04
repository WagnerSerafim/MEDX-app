import glob
import os
import pandas as pd
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import urllib
import re

from utils.utils import create_log, exists


def verify_nan(patient,value):
    if getattr(patient, value) == 'nan':
        setattr(patient, value, '')


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

extension_file = glob.glob(f'{path_file}/dados.xlsx')

df = pd.read_excel(extension_file[0], sheet_name='pacientes')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
updated_cont=0
not_updated_data = []
not_inserted_cont = 0

for idx, row in df.iterrows():
    updated = False
    campos_alterados = {}

    patient = exists(session, row['CODIGO'], 'Id do Cliente', Contatos)
    if not patient:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Cliente não encontrado'
        not_updated_data.append(row_dict)
        continue

    # CPF/CGC
    patient_cpf = getattr(patient, 'CPF/CGC')
    if patient_cpf in ['nan', '', 'NULL', None]:
        campos_alterados['CPF/CGC'] = {'anterior': patient_cpf, 'novo': ''}
        setattr(patient, 'CPF/CGC', '')
        updated = True
    else:
        if row['CPF'] not in ['', 'nan', 'NULL', None] and not pd.isna(row['CPF']):
            campos_alterados['CPF/CGC'] = {'anterior': patient_cpf, 'novo': str(row['CPF'])}
            setattr(patient, 'CPF/CGC', str(row['CPF']))
            updated = True

    # Outros campos
    for campo in ['Bairro Residencial', 'Endereço Residencial', 'Cidade Residencial', 'Cep Residencial', 'Email', 'RG']:
        valor_antigo = getattr(patient, campo)
        if valor_antigo == 'nan':
            campos_alterados[campo] = {'anterior': valor_antigo, 'novo': ''}
            setattr(patient, campo, '')
            updated = True
        # Atualiza RG se vier novo valor
        if campo == 'RG' and row['RG'] not in ['', 'nan', 'NULL', None] and not pd.isna(row['RG']):
            campos_alterados[campo] = {'anterior': valor_antigo, 'novo': str(row['RG'])}
            setattr(patient, campo, str(row['RG']))
            updated = True

    if updated:
        updated_cont += 1
        log_data.append({
            "Id do Cliente": getattr(patient, "Id do Cliente", None),
            "Campos Alterados": campos_alterados
        })
    else:
        not_updated_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Nenhum campo precisava ser alterado'
        not_updated_data.append(row_dict)

session.commit()
session.close()

create_log(log_data, path_file, "log_update_patients.xlsx")
create_log(not_updated_data, path_file, "log_not_updated_patients.xlsx")