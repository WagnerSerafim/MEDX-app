import csv
import glob
import os
from sqlalchemy import MetaData, Table, create_engine, bindparam, UnicodeText
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
import urllib
from utils.utils import is_valid_date, create_log, verify_nan, exists
from striprtf.striprtf import rtf_to_text
import re

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")

print("Conectando no Banco de Dados...")
DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(
    DATABASE_URL,
    future=True,
    implicit_returning=False
)

metadata = MetaData()
historico_tbl = Table("Histórico de Clientes", metadata, schema=f"schema_{sid}", autoload_with=engine)

Base = declarative_base()

class Historico(Base):
    __table__ = historico_tbl

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

print("Sucesso! Inicializando migração de Históricos...")

csv.field_size_limit(100000000000)
todos_arquivos = glob.glob(os.path.join(path_file, 't_pacientesevolucoes.csv'))

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
inserted_cont = 0
not_inserted_data = []
not_inserted_cont = 0
idx_global = 0
LAST_HIST_ID = 232497

# lê em chunks para evitar carregar 300mil registros de uma vez em memória
CHUNK_SIZE = 500  # processa 5k registros por vez antes de fazer commit
BATCH_SIZE = 10   # faz commit a cada 100 registros inseridos

for chunk_df in pd.read_csv(
    todos_arquivos[0], dtype=str, sep=',',
    encoding='utf-8', quotechar='"', chunksize=CHUNK_SIZE
):
    batch_inserted = 0

    with session.no_autoflush:
        for idx, row in chunk_df.iterrows():
            idx_global += 1

            # 👉 Primeiro, pega o Id do Histórico
            id_record = verify_nan(row['id'])

            # Se o id for vazio, continua tratando como antes
            if id_record == "":
                not_inserted_cont += 1
                row_dict = row.to_dict()
                row_dict['Motivo'] = 'Id do Histórico é vazio ou nulo'
                not_inserted_data.append(row_dict)
                continue

            # ✅ PULAR tudo que já foi inserido (<= 89293)
            try:
                if int(id_record) <= LAST_HIST_ID:
                    # Apenas pula a linha e não tenta inserir de novo
                    continue
            except ValueError:
                # Se não conseguir converter para int, trata como erro normal
                not_inserted_cont += 1
                row_dict = row.to_dict()
                row_dict['Motivo'] = 'Id do Histórico inválido (não numérico)'
                not_inserted_data.append(row_dict)
                continue

            if idx_global % 5000 == 0:
                print(f"Processados: {idx_global} | Inseridos: {inserted_cont} | Não inseridos: {not_inserted_cont}")

            # A PARTIR DAQUI segue o fluxo que você já tinha,
            # mas SEM redefinir id_record:
            # existing_record, id_patient, record, date, etc.
            existing_record = exists(session, id_record, "Id do Histórico", Historico)
            if existing_record:
                not_inserted_cont += 1
                row_dict = row.to_dict()
                row_dict['Motivo'] = 'Histórico já existe'
                not_inserted_data.append(row_dict)
                continue

            id_patient = verify_nan(row['paciente'])
            if id_patient is None:
                not_inserted_cont += 1
                row_dict = row.to_dict()
                row_dict['Motivo'] = 'Id da consulta não existe no lookup de pacientes'
                not_inserted_data.append(row_dict)
                continue

            record = verify_nan(row['texto'])
            if record is None:
                not_inserted_cont += 1
                row_dict = row.to_dict()
                row_dict['Motivo'] = 'Histórico vazio ou inválido'
                not_inserted_data.append(row_dict)
                continue

            if is_valid_date(row['data'], '%Y-%m-%d %H:%M:%S'):
                date = row['data']
            else:
                date = '01/01/1900 00:00'

            new_record = Historico(
                Data=date
            )
            setattr(new_record, "Id do Histórico", id_record)
            setattr(new_record, "Id do Cliente", id_patient)
            setattr(new_record, "Id do Usuário", 0)

            # ❌ NÃO usar bindparam aqui
            setattr(new_record, "Histórico", bindparam(None, value=record, type_=UnicodeText()))
            # ✅ Usar o valor direto
            # setattr(new_record, "Histórico", record)

            log_data.append({
                "Id do Histórico": id_record,
                "Id do Cliente": id_patient,
                "Data": date,
                "Histórico": record,
                "Id do Usuário": 0,
            })

            session.add(new_record)
            inserted_cont += 1
            batch_inserted += 1

            if batch_inserted % BATCH_SIZE == 0:
                try:
                    session.commit()
                    print(f"  [Batch commit] {batch_inserted} registros inseridos neste lote")
                    batch_inserted = 0
                except Exception as e:
                    print(f"Erro ao fazer commit: {e}")
                    session.rollback()
                    raise
    
    # commit final do chunk
    try:
        session.commit()
        print(f"[Chunk concluído] Total inseridos: {inserted_cont} | Total não inseridos: {not_inserted_cont}")
    except Exception as e:
        print(f"Erro ao fazer commit final do chunk: {e}")
        session.rollback()
        raise

print("Migração concluída! Gerando logs...")
print(f"{inserted_cont} novos históricos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} históricos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_record_t_pacientesimpressos_texto_1.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_record_t_pacientesimpressos_texto_1.xlsx")
