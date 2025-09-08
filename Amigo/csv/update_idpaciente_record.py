from datetime import datetime
import glob
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
import csv
from utils.utils import exists, create_log

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine)

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

HistoricoClientes = getattr(Base.classes, "Histórico de Clientes")
Contatos = getattr(Base.classes, "Contatos")

print("Sucesso! Inicializando migração de Históricos...")

csv.field_size_limit(10000000000000)
todos_arquivos = glob.glob(f'{path_file}/records.csv')
patients_file = glob.glob(f'{path_file}/patients.csv')

df = pd.read_csv(todos_arquivos[0], sep=',')
df = df.replace('None', '')

df_patients = pd.read_csv(patients_file[0], sep=',')

lookup_patients = {
    row['id']: row['name']
    for _,row in df_patients.iterrows()
}

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

    id_record = row['id']
    record = exists(session, id_record, "Id do Histórico", HistoricoClientes)
    if not record:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do Histórico não encontrado'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    id_patient = getattr(record, 'Id do Cliente')
    patient = exists(session, id_patient, "Id do Cliente", Contatos)
    if not patient:
        patient_name = lookup_patients.get(id_patient)
        if not patient_name:
            not_inserted_cont +=1
            row_dict = row.to_dict()
            row_dict['Motivo'] = 'Esse ID não foi encontrado nem no banco e nem do arquivo'
            row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            not_inserted_data.append(row_dict)
            continue

        patient = exists(session, patient_name, 'Nome', Contatos)
        if not patient:
            not_inserted_cont +=1
            row_dict = row.to_dict()
            row_dict['Motivo'] = 'Nome do paciente não encontrado no banco de dados'
            row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            not_inserted_data.append(row_dict)
            continue
        if patient_name == patient.Nome:
            setattr(record, 'Id do Cliente', getattr(patient, 'Id do Cliente'))
            session.commit()
            inserted_cont += 1
            log_data.append({
                'id': id_record,
                'Id antigo': id_patient,
                'Id novo': getattr(patient, 'Id do Cliente'),
                'Motivo': 'Id do paciente atualizado com sucesso',
                'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
        else:
            not_inserted_cont +=1
            row_dict = row.to_dict()
            row_dict['Motivo'] = 'Nome do paciente não confere com o nome do banco'
            row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            not_inserted_data.append(row_dict)
            continue

session.close()
create_log(log_folder, 'update_idpaciente_record', log_data, not_inserted_data)
create_log(log_folder, 'update_idpaciente_record_not_inserted', not_inserted_data)