import os
import json
import traceback
from sqlalchemy import create_engine, insert, quoted_name, text, MetaData, Table
from sqlalchemy.orm import sessionmaker, declarative_base
import urllib
from utils.utils import exists, is_valid_date, truncate_value, verify_nan, create_log  # add excel logger
import gc

# JSONL logging removed; we'll use Excel logs via create_log()

# ---------- conexões ----------
PG_URL = "postgresql+psycopg://postgres:Er07021972?@localhost:5432/36460_Ariana_Favila"
engine_pg = create_engine(PG_URL)

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
log_folder = input("Informe o caminho da pasta para salvar os logs: ")

print("Conectando no Banco de Dados...")

MYSQL_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"
engine = create_engine(MYSQL_URL, fast_executemany=False, pool_pre_ping=True, pool_recycle=1800)

metadata = MetaData()
ctt_tbl = Table(
    quoted_name("Contatos", True),
    metadata,
    schema=f"Schema_{sid}",
    autoload_with=engine
)

Base = declarative_base()
class Contatos(Base):
    __table__ = ctt_tbl

SessionLocal = sessionmaker(bind=engine, autoflush=False, future=True)
session = SessionLocal()

# os.makedirs(log_folder, exist_ok=True)
# nao_inseridos_file = f"not_inserted_patients{dbase}.jsonl"
# resumo_file = f"summary_patients{dbase}.json"

BATCH_SIZE = 100
total = 0
lote = 1
inserted_cont = 0
not_inserted_cont = 0

# DICA: para testes, pode limitar ou cortar texto muito grande:
sql = text("""
    SELECT *
    FROM public.t_pacientes
    WHERE codigo < 300
""")

with engine_pg.connect().execution_options(stream_results=True) as conn_pg:
    # .mappings() -> rows como dict, bom p/ não criar DataFrame
    result = conn_pg.execute(sql).mappings()

    payload = []
    not_inserted_batch = []  # logs do lote atual (não acumular tudo)
    not_inserted_all = []  # acumulador global para garantir escrita do log final
    log_data = []
    os.makedirs(log_folder, exist_ok=True)

    for idx, row in enumerate(result, start=1):
        try:
            id_patient = verify_nan(row['codigo'])
            if not id_patient:
                entry = {**row, "Motivo": "ID do paciente vazio"}
                not_inserted_batch.append(entry)
                not_inserted_all.append(entry)
                not_inserted_cont += 1
                continue

            patient = exists(session, id_patient, "Id do Cliente", Contatos)

            name = verify_nan(row['nome'])
            if not name:
                entry = {**row, "Motivo": "Nome do paciente vazio"}
                not_inserted_batch.append(entry)
                not_inserted_all.append(entry)
                not_inserted_cont += 1
                continue

            birthday = verify_nan(row['datanascimento'])
            if not is_valid_date(birthday, '%Y-%m-%d'):
                birthday = '1900-01-01'

            address = verify_nan(row['r_logradouro'])
            if address:
                address = f'{address} {verify_nan(row["r_numero"])}'.strip()

            complement = verify_nan(row['r_complemento'])
            neighborhood = verify_nan(row['r_bairro'])
            cep = verify_nan(row['r_cep'])
            cellphone = verify_nan(row['telefone_1'])
            email = verify_nan(row['correioeletronico'])
            occupation = verify_nan(row['profissao'])
            sex = verify_nan(row['sexo'])
            if sex == 1:
                sex = 'F'
            else:
                sex = 'M'
            rg = verify_nan(row['identidade'])
            cpf = verify_nan(row['cpf'])
            obs = f'{verify_nan(row['observacoes'])} {verify_nan(row["pendencias"])}'.strip()
            mother = verify_nan(row['mae_nome'])
            father = verify_nan(row['pai_nome'])

            rec = {
                'Id do Cliente': id_patient,
                'Nome': truncate_value(name, 50),
                'Nascimento': birthday,
                'Endereço Residencial': truncate_value(address, 50),
                'Endereço Comercial': truncate_value(complement, 50),
                'Bairro Residencial': truncate_value(neighborhood, 25),
                'Cep Residencial': truncate_value(cep, 10),
                'Celular': truncate_value(cellphone, 25),
                'Email': truncate_value(email, 100),
                'Profissão': truncate_value(occupation, 25),
                'Sexo': sex,
                'RG': truncate_value(rg, 25),
                'CPF/CGC': truncate_value(cpf, 25),
                'Observações': obs,
                'Mãe': truncate_value(mother, 50),
                'Pai': truncate_value(father, 50),
            }
            payload.append(rec)
            log_data.append(rec)

            # mini-lote
            if len(payload) >= BATCH_SIZE:
                try:
                    print(f"Inserindo mini-lote {lote} com {len(payload)} registros...")

                except Exception as e:
                    session.rollback()
                    print("Erro ao inserir mini-lote:", traceback.format_exc())
                    for p in payload:
                        entry = {**p, "Motivo": f"Falha no commit do mini-lote: {e}"}
                        not_inserted_batch.append(entry)
                        not_inserted_all.append(entry)
                        not_inserted_cont += 1

                # if not_inserted_batch:
                #     append_log_jsonl(log_folder, nao_inseridos_file, not_inserted_batch)
                #     not_inserted_batch.clear()

                payload.clear()
                lote += 1
                gc.collect()

            total += 1

        except Exception as e:
            entry = {**dict(row), "Motivo": str(e)}
            not_inserted_batch.append(entry)
            not_inserted_all.append(entry)
            not_inserted_cont += 1

    if payload:
        try:
            session.execute(insert(Contatos.__table__), payload)
            session.commit()
            inserted_cont += len(payload)

        except Exception as e:
            session.rollback()
            print("Erro ao inserir mini-lote final:", traceback.format_exc())
            for p in payload:
                entry = {**p, "Motivo": f"Falha no commit do mini-lote final: {e}"}
                not_inserted_batch.append(entry)
                not_inserted_all.append(entry)
                not_inserted_cont += 1
        payload.clear()
        gc.collect()

    if not_inserted_batch:
        create_log(not_inserted_batch, log_folder, f"log_not_inserted_patients_{dbase}.xlsx")
        print(f"Logs gravados em: {log_folder}")

print(f"Concluído. Total lidas: {total} | Inseridos: {inserted_cont} | Não inseridos: {not_inserted_cont}")
# write Excel logs (inserted and not-inserted)
create_log(log_data, log_folder, f"log_inserted_patients_{dbase}.xlsx")

if not_inserted_batch:
    create_log(not_inserted_batch, log_folder, f"log_not_inserted_patients_{dbase}.xlsx")
    print(f"Logs gravados em: {log_folder}")
