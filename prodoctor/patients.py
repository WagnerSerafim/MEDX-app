import os
import json
import traceback
from sqlalchemy import create_engine, insert, quoted_name, text, MetaData, Table
from sqlalchemy.orm import sessionmaker, declarative_base
import urllib
from utils.utils import exists, is_valid_date, verify_nan  # removi create_log/not_inserted_log aqui
import gc

def append_log_jsonl(folder: str, filename: str, records):
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, filename)
    if isinstance(records, dict):
        records = [records]
    with open(path, "a", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False))
            f.write("\n")
    return path

def write_summary_json(folder: str, filename: str, payload: dict):
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return path

# ---------- conexões ----------
PG_URL = "postgresql+psycopg://postgres:ER07021972@localhost:5432/climes_35412"
engine_pg = create_engine(PG_URL)

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
# log_folder = input("Informe o caminho da pasta para salvar os logs: ")

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
    WHERE codigo > 39876
""")

with engine_pg.connect().execution_options(stream_results=True) as conn_pg:
    # .mappings() -> rows como dict, bom p/ não criar DataFrame
    result = conn_pg.execute(sql).mappings()

    payload = []
    not_inserted_batch = []  # logs do lote atual (não acumular tudo)

    for idx, row in enumerate(result, start=1):
        try:
            id_patient = verify_nan(row['codigo'])
            if not id_patient:
                not_inserted_batch.append({**row, "Motivo": "ID do paciente vazio"})
                not_inserted_cont += 1
                continue

            patient = exists(session, id_patient, "Id do Cliente", Contatos)

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

            payload.append({
                'Id do Cliente': id_patient,
                'Nascimento': birthday,
                'Endereço Residencial': address,
                'Endereço Comercial': complement,
                'Bairro Residencial': neighborhood,
                'Cep Residencial': cep,
                'Celular': cellphone,
                'Email': email,
                'Profissão': occupation,
                'Sexo': sex,
                'RG': rg,
                'CPF/CGC': cpf,
                'Observações': obs,
                'Mãe': mother,
                'Pai': father,
            })

            # mini-lote
            if len(payload) >= BATCH_SIZE:
                try:
                    print(f"Inserindo mini-lote {lote} com {len(payload)} registros...")

                except Exception as e:
                    session.rollback()
                    print("Erro ao inserir mini-lote:", traceback.format_exc())
                    for p in payload:
                        not_inserted_batch.append({**p, "Motivo": f"Falha no commit do mini-lote: {e}"})
                        not_inserted_cont += 1

                # if not_inserted_batch:
                #     append_log_jsonl(log_folder, nao_inseridos_file, not_inserted_batch)
                #     not_inserted_batch.clear()

                payload.clear()
                lote += 1
                gc.collect()

            total += 1

        except Exception as e:
            not_inserted_batch.append({**dict(row), "Motivo": str(e)})
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
                not_inserted_batch.append({**p, "Motivo": f"Falha no commit do mini-lote final: {e}"})
                not_inserted_cont += 1
        payload.clear()
        gc.collect()

    # if not_inserted_batch:
    #     append_log_jsonl(log_folder, nao_inseridos_file, not_inserted_batch)
    #     not_inserted_batch.clear()

print(f"Concluído. Total lidas: {total} | Inseridos: {inserted_cont} | Não inseridos: {not_inserted_cont}")

# # resumo leve (não explode memória)
# write_summary_json(log_folder, resumo_file, {
#     "database_destino": dbase,
#     "total_linhas_lidas": total,
#     "total_inseridos": inserted_cont,
#     "total_nao_inseridos": not_inserted_cont,
#     "arquivo_nao_inseridos_jsonl": nao_inseridos_file
# })
# print(f"Logs em: {os.path.join(log_folder, nao_inseridos_file)} e {os.path.join(log_folder, resumo_file)}")
