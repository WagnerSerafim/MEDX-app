import glob
import os
import re
import html
import csv
import urllib
from datetime import datetime

import pandas as pd
from sqlalchemy import MetaData, Table, create_engine, bindparam, UnicodeText, insert
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.pool import NullPool

from utils.utils import is_valid_date, create_log, verify_nan


def select_answer(num):
    if num == 1:
        return "Sim"
    elif num == 2:
        return "Não"
    elif num == 3:
        return "Talvez"
    return None


def normalize_text(value):
    if value is None:
        return ""

    value = str(value)
    value = html.unescape(value)
    value = value.replace("&nbsp;", " ")

    # tags que devem virar quebra de linha
    value = re.sub(r"<br\s*/?>", "\n", value, flags=re.IGNORECASE)
    value = re.sub(r"</p\s*>", "\n", value, flags=re.IGNORECASE)
    value = re.sub(r"<p[^>]*>", "", value, flags=re.IGNORECASE)
    value = re.sub(r"</div\s*>", "\n", value, flags=re.IGNORECASE)
    value = re.sub(r"<div[^>]*>", "", value, flags=re.IGNORECASE)
    value = re.sub(r"</li\s*>", "\n", value, flags=re.IGNORECASE)
    value = re.sub(r"<li[^>]*>", "- ", value, flags=re.IGNORECASE)

    # remove outras tags
    value = re.sub(r"<[^>]+>", "", value)

    # normaliza finais de linha
    value = value.replace("\r\n", "\n").replace("\r", "\n")

    # remove espaços no começo/fim de cada linha, preservando linhas
    value = "\n".join(line.strip() for line in value.split("\n"))

    # remove linhas vazias repetidas
    value = re.sub(r"\n{3,}", "\n\n", value)

    return value.strip()


def is_empty_answer(answer):
    if answer is None:
        return True

    answer_str = str(answer).strip()
    if not answer_str:
        return True

    invalid_values = {
        "<p></p>",
        "<p><br></p>",
        "None",
        "nan",
    }

    if answer_str in invalid_values:
        return True

    if '"select":[]' in answer_str:
        return True

    normalized = normalize_text(answer_str)
    return normalized == ""


def parse_special_answer(question, answer):
    """
    Trata perguntas específicas em que a resposta vem em formato numérico/estruturado.
    """
    if question in ['FUMA', 'BEBE COM FREQUENCIA?', 'ESTÁ EM USO, JÁ USOU OU DESEJA USAR HORMONIOS?']:
        try:
            match = re.search(r'(\d+)', str(answer))
            if match:
                return select_answer(int(match.group(1))) or normalize_text(answer)
        except Exception:
            pass

    return normalize_text(answer)


def build_group_history(group_df):
    """
    Monta um único texto de histórico para toda a anamnese.
    Deduplica perguntas/respostas repetidas dentro do mesmo grupo.
    """
    sections = []
    seen = set()

    for _, row in group_df.iterrows():
        question = verify_nan(row.get('Pergunta'))
        answer = verify_nan(row.get('Resposta'))

        if not question:
            continue

        if is_empty_answer(answer):
            continue

        question = normalize_text(question).upper().strip()
        answer = parse_special_answer(question, answer)

        if not answer:
            continue

        dedup_key = (question.lower(), answer.lower())
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        section = f"{question}<br>{answer}"
        sections.append(section)

    return "<br><br>" + ("<br><br>".join(sections).strip())


def get_group_date(group_df):
    """
    Usa a menor data válida do grupo.
    """
    valid_dates = []

    for _, row in group_df.iterrows():
        raw_date = row.get('Data de criação')
        if is_valid_date(raw_date, "%Y-%m-%d %H:%M:%S"):
            valid_dates.append(datetime.strptime(raw_date, "%Y-%m-%d %H:%M:%S"))

    if valid_dates:
        return min(valid_dates).strftime("%Y-%m-%d %H:%M:%S")

    return "1900-01-01 00:00:00"


sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")

print("Conectando no Banco de dados...")

DATABASE_URL = (
    f"mssql+pyodbc://Medizin_{sid}:{password}"
    f"@medxserver.database.windows.net:1433/{dbase}"
    f"?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"
)

engine = create_engine(
    DATABASE_URL,
    poolclass=NullPool,     # evita pooling
    future=True
)

metadata = MetaData()
historico_tbl = Table(
    "Histórico de Clientes",
    metadata,
    schema=f"schema_{sid}",
    autoload_with=engine
)

Base = declarative_base()

class Historico(Base):
    __table__ = historico_tbl

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
session = SessionLocal()

print("Sucesso! Inicializando migração de Históricos...")

csv.field_size_limit(10000000)
todos_arquivos = glob.glob(f"{path_file}/Anamneses*respostas.csv")

if not todos_arquivos:
    raise FileNotFoundError("Nenhum arquivo 'Anamneses*respostas.csv' foi encontrado na pasta informada.")

df = pd.read_csv(
    todos_arquivos[0],
    sep=';',
    engine='python',
    quotechar='"',
    encoding='latin1',
    on_bad_lines='skip'
)

log_folder = path_file
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
not_inserted_data = []

inserted_cont = 0
not_inserted_cont = 0

df = df[df['Tipo de pergunta'] == "Calculadora gestacional"].copy()

df['Código da anamnese'] = df['Código da anamnese'].apply(
    lambda x: int(x) if pd.notna(x) else None
)

df['Código do paciente'] = df['Código do paciente'].apply(
    lambda x: int(x) if pd.notna(x) else None
)

df['Pergunta'] = df['Pergunta'].apply(verify_nan)
df['Resposta'] = df['Resposta'].apply(verify_nan)

grouped = df.groupby(['Código da anamnese', 'Código do paciente'], dropna=True, sort=False)

batch_rows = []
batch_size = 200  # poucos inserts e commits

for idx, ((cod_anam, id_patient), group_df) in enumerate(grouped, start=1):
    if idx % 100 == 0:
        print(
            f"Grupos processados: {idx} | "
            f"Inseridos: {inserted_cont} | "
            f"Não inseridos: {not_inserted_cont}"
        )

    if cod_anam is None or str(cod_anam).strip() == "":
        not_inserted_cont += 1
        not_inserted_data.append({
            "Código da anamnese": cod_anam,
            "Código do paciente": id_patient,
            "Motivo": "Código da anamnese vazio ou nulo",
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })
        continue

    if id_patient is None or str(id_patient).strip() == "":
        not_inserted_cont += 1
        not_inserted_data.append({
            "Código da anamnese": cod_anam,
            "Código do paciente": id_patient,
            "Motivo": "Id do Paciente vazio",
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })
        continue

    record = build_group_history(group_df)

    if not record:
        not_inserted_cont += 1
        not_inserted_data.append({
            "Código da anamnese": cod_anam,
            "Código do paciente": id_patient,
            "Motivo": "Grupo sem respostas válidas",
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })
        continue

    date = get_group_date(group_df)

    id_patient = int(id_patient)

    batch_rows.append({
        "Data": date,
        "Histórico": record,
        "Id do Cliente": id_patient,
        "Id do Usuário": 0,
    })

    log_data.append({
        "Código da anamnese": cod_anam,
        "Id do Cliente": id_patient,
        "Data": date,
        "Histórico": record,
        "Id do Usuário": 0,
        "Quantidade de linhas agrupadas": len(group_df),
    })

    inserted_cont += 1

    if len(batch_rows) >= batch_size:
        session.execute(insert(historico_tbl), batch_rows)
        session.commit()
        batch_rows = []

if batch_rows:
    session.execute(insert(historico_tbl), batch_rows)
    session.commit()

session.close()

print(f"{inserted_cont} novos históricos consolidados foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} grupos não foram inseridos, verifique o log para mais detalhes.")

create_log(log_data, log_folder, "log_inserted_records_Anamnese_Consolidado.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_records_Anamnese_Consolidado.xlsx")