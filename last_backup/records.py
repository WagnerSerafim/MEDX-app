import glob
import os
from sqlalchemy import MetaData, Table, Text, create_engine, bindparam, UnicodeText
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
import urllib
import csv
from datetime import datetime
from utils.utils import is_valid_date, exists, create_log, truncate_value, verify_nan

def get_record(row):
    record = ''
    id_consulta = verify_nan(row['ID'])
    
    record += f"""
    IDADE: {verify_nan(row['IDADE'])}<br>
    PESO: {verify_nan(row['PESO'])}<br>
    ESTATURA: {verify_nan(row['ESTATURA'])}<br>
    IMC: {verify_nan(row['IMC'])}<br>
    PESO ALVO: {verify_nan(row['PESO_ALVO'])}<br>
    """

    pront = verify_nan(row['PRONTUARIO'])
    if pront:
        record += f"<br>{pront}<br>"

    activity = get_activies(id_consulta, activities_map, activity_map)
    if activity:
        record += f"<br>ATIVIDADES:<br>{activity}<br>"

    return record

def get_activies(id_consulta, activities_map, activity_map):
    activities = activities_map.get(id_consulta, [])
    activities_str = ''

    for act_id in activities:
        act_info = activity_map.get(act_id, '')
        if act_info:
            activities_str += f"{act_info}<br>"
    
    return activities_str

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

print("Sucesso! Inicializando migração de históricos...")

consulta_file = glob.glob(f'{path_file}/CONSULTA*.csv')
plano_file = glob.glob(f'{path_file}/PLANO*.csv')
atividades_file = glob.glob(f'{path_file}/ATIVIDADES*.csv')
exame_file = glob.glob(f'{path_file}/EXAME*.csv')
impresso_file = glob.glob(f'{path_file}/IMPRESSO*.csv')
pedido_file = glob.glob(f'{path_file}/PEDIDO_EXAME*.csv')
consulta_atividades_file = glob.glob(f'{path_file}/CONSULTA_ATIVIDADE*.csv')
consulta_exame_file = glob.glob(f'{path_file}/CONSULTA_EXAME*.csv')
consulta_plano_file = glob.glob(f'{path_file}/CONSULTA_PLANO*.csv')
consulta_impresso_file = glob.glob(f'{path_file}/CONSULTA_IMPRESSO*.csv')
consulta_pedidoexame_file = glob.glob(f'{path_file}/CONSULTA_PEDIDOEXAME*.csv')
pac_consultas_file = glob.glob(f'{path_file}/PACIENTE_CONSULTAS*.csv')

csv.field_size_limit(1000000)
df_consulta = pd.read_csv(consulta_file[0], sep=',')
df_pac_consultas = pd.read_csv(pac_consultas_file[0], sep=',')
df_atividades = pd.read_csv(atividades_file[0], sep=',')
df_consulta_atividades = pd.read_csv(consulta_atividades_file[0], sep=',')

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

patients_id_map = {row['CONSULTA']: row['PACIENTE'] for _, row in df_pac_consultas.iterrows()}
activity_map = {}

for _,row in df_atividades.iterrows():
    activity_map[row['ID']] = f"""Dia de Semana: {verify_nan(row['DIA_SEMANA'])}<br>
    Atividade: {verify_nan(row['ATIVIDADE'])}<br>"""

activities_map = {}

for _,row in df_consulta_atividades.iterrows():
    atividade = row['ATIVIDADE']
    consulta = row['CONSULTA']

    if consulta not in activities_map:
        activities_map[consulta] = [atividade]
    else:
        activities_map[consulta].append(atividade)

for idx, row in df_consulta.iterrows():

    if idx % 100 == 0 or idx == len(df_consulta):
        print(f"Processados: {idx} | Inseridos: {inserted_cont} | Não inseridos: {not_inserted_cont} | Concluído: {round((idx / len(df_consulta)) * 100, 2)}%")

    id_patient_str = patients_id_map.get(row['ID'])
    patient = exists(session, id_patient_str, 'Referências', Contatos)
    if patient:
        id_patient = getattr(patient, "Id do Cliente")
    else:
        not_inserted_cont +=1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Paciente não encontrado'
        row_dict['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        not_inserted_data.append(row_dict)
        continue

    date = verify_nan(row['DATA'])
    if not date or not is_valid_date(date, "%Y-%m-%d"):
        date = '1900-01-01 00:00:00'

    print(f"DATA TIPO: {type(date)} | PACIENTE ID: {id_patient} ")
    
    record = get_record(row)

    
    new_record = Historico(
        Histórico = Text(record),
        Data=date
    )
    # setattr(new_record, "Id do Histórico", id_record)
    setattr(new_record, "Id do Cliente", id_patient)
    setattr(new_record, "Histórico", bindparam(None, value=record, type_=UnicodeText()))
    setattr(new_record, "Id do Usuário", 0)
    
    log_data.append({
        # "Id do Histórico": id_record,
        "Id do Cliente": id_patient,
        "Data": date,
        "Histórico": record,
        "Id do Usuário": 0,
    })
    session.add(new_record)
    inserted_cont+=1

    session.commit()

session.commit()

print(f"{inserted_cont} novos históricos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} históricos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_record.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_record.xlsx")
