from datetime import datetime
import glob
import os
import re
import zlib
from urllib.parse import quote_plus, urlparse

import pandas as pd
import requests
from sqlalchemy import MetaData, Table, create_engine, bindparam, UnicodeText
from sqlalchemy.orm import declarative_base, sessionmaker


# --------------------------
# Helpers
# --------------------------
def verify_nan(value):
    if value is None:
        return None
    try:
        # pandas NaN
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, str) and value.strip() == "":
        return None
    return value


def create_log(data_list, folder, filename):
    if not os.path.exists(folder):
        os.makedirs(folder)
    df_log = pd.DataFrame(data_list)
    out_path = os.path.join(folder, filename)
    df_log.to_excel(out_path, index=False)


def exists(session, value, column_name, model_cls):
    # Procura por registro existente: SELECT 1 WHERE <column_name> = value
    col = getattr(model_cls, column_name)
    return session.query(model_cls).filter(col == value).first() is not None


def filename_from_url(url: str) -> str:
    # pega a última parte do path (sem querystring)
    parsed = urlparse(url)
    name = os.path.basename(parsed.path)
    return name.strip()


def safe_download(url: str, out_path: str, timeout=(10, 120)) -> tuple[bool, str]:
    """
    Retorna (ok, motivo_ou_msg).
    Baixa em streaming e grava no disco.
    """
    try:
        with requests.get(url, stream=True, timeout=timeout) as r:
            if r.status_code != 200:
                return False, f"HTTP {r.status_code}"
            os.makedirs(os.path.dirname(out_path), exist_ok=True)

            # grava em arquivo temporário e depois renomeia (evita arquivo corrompido)
            tmp_path = out_path + ".part"
            with open(tmp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
            os.replace(tmp_path, out_path)
        return True, "OK"
    except Exception as e:
        return False, str(e)


# --------------------------
# Inputs
# --------------------------
sid = input("Informe o SoftwareID: ").strip()
password = quote_plus(input("Informe a senha: ").strip())
dbase = input("Informe o DATABASE: ").strip()
path_file = input("Informe o caminho da pasta que contém o dados.xlsx: ").strip()

print("Conectando no Banco de Dados...")
DATABASE_URL = (
    f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}"
    f"?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"
)

engine = create_engine(DATABASE_URL)

metadata = MetaData()
historico_tbl = Table("Histórico de Clientes", metadata, schema=f"schema_{sid}", autoload_with=engine)

Base = declarative_base()


class Historico(Base):
    __table__ = historico_tbl


SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

print("Sucesso! Inicializando processamento de anexos...")

# --------------------------
# Carrega Excel
# --------------------------
# tenta encontrar dados.xlsx (ou algo começando com dados)
xlsx_candidates = glob.glob(os.path.join(path_file, "dados*.xlsx"))
if not xlsx_candidates:
    raise FileNotFoundError(f"Não achei nenhum arquivo dados*.xlsx em: {path_file}")

xlsx_path = xlsx_candidates[0]

df_anexos = pd.read_excel(xlsx_path, sheet_name="anexos_consulta")
df_anexos = df_anexos.replace("None", "")

# Opcional: se existir o sheet atendimento, cria o mapa CODIGOATENDIMENTO -> [CODIGOPACIENTE, DATAINICIAL]
schedules = {}
try:
    df_schedule = pd.read_excel(xlsx_path, sheet_name="atendimento").replace("None", "")
    # tenta mapear usando nomes iguais ao seu exemplo
    if "CODIGOATENDIMENTO" in df_schedule.columns:
        # tenta achar colunas mais prováveis
        patient_col = "CODIGOPACIENTE" if "CODIGOPACIENTE" in df_schedule.columns else None
        date_col = "DATAINICIAL" if "DATAINICIAL" in df_schedule.columns else None

        if patient_col and date_col:
            for _, r in df_schedule.iterrows():
                schedules[r["CODIGOATENDIMENTO"]] = [r[patient_col], r[date_col]]
except Exception:
    # sem atendimento, segue sem schedules
    pass

# --------------------------
# Pasta de download
# --------------------------
download_dir = os.path.join(path_file, "anexos")
os.makedirs(download_dir, exist_ok=True)

# --------------------------
# Logs/contadores
# --------------------------
log_inserted = []
log_not_inserted = []
inserted_cont = 0
not_inserted_cont = 0

# --------------------------
# Loop principal
# --------------------------
if "LINK" not in df_anexos.columns:
    raise KeyError("A planilha 'anexos_consulta' não tem a coluna 'LINK'.")

for idx, row in df_anexos.iterrows():
    link = verify_nan(row.get("LINK"))

    if link is None:
        not_inserted_cont += 1
        rdict = row.to_dict()
        rdict["Motivo"] = "LINK vazio ou inválido"
        log_not_inserted.append(rdict)
        continue

    # nome do arquivo (última parte do link)
    file_name = filename_from_url(str(link))
    if not file_name:
        not_inserted_cont += 1
        rdict = row.to_dict()
        rdict["Motivo"] = "Não foi possível extrair o nome do arquivo do LINK"
        log_not_inserted.append(rdict)
        continue

    # record e classe
    record = file_name
    classe = file_name

    if len(classe) > 100:
        not_inserted_cont += 1
        rdict = row.to_dict()
        rdict["Motivo"] = f"Classe > 100 caracteres (len={len(classe)})"
        rdict["Arquivo"] = file_name
        log_not_inserted.append(rdict)
        continue

    # baixa o arquivo
    out_path = os.path.join(download_dir, file_name)
    if not os.path.exists(out_path):
        ok, msg = safe_download(str(link), out_path)
        if not ok:
            not_inserted_cont += 1
            rdict = row.to_dict()
            rdict["Motivo"] = f"Falha ao baixar arquivo: {msg}"
            rdict["Arquivo"] = file_name
            log_not_inserted.append(rdict)
            continue

    # --------------------------
    # Define Id do Cliente e Data
    # --------------------------
    id_patient = None
    date = None

    # 1) Se já vier na própria planilha
    for cand in ["Id do Cliente", "ID_CLIENTE", "CODIGOPACIENTE", "CODIGOPACIENTE "]:
        v = verify_nan(row.get(cand))
        if v is not None:
            id_patient = v
            break

    for cand in ["Data", "DATA", "DATAINICIAL", "DATA_INICIAL"]:
        v = verify_nan(row.get(cand))
        if v is not None:
            # se for datetime, formata
            if hasattr(v, "strftime"):
                date = v.strftime("%Y-%m-%d")
            else:
                # tenta converter para datetime
                try:
                    date = pd.to_datetime(v).strftime("%Y-%m-%d")
                except Exception:
                    date = "1900-01-01"
            break

    # 2) Se houver CODIGOATENDIMENTO e schedules (sheet atendimento)
    if id_patient is None or date is None:
        codigo_atendimento = verify_nan(row.get("CODIGOATENDIMENTO"))
        if codigo_atendimento is not None and codigo_atendimento in schedules:
            sched = schedules[codigo_atendimento]
            # sched = [paciente, data]
            if id_patient is None:
                id_patient = verify_nan(sched[0])
            if date is None:
                d = verify_nan(sched[1])
                if d is None:
                    date = "1900-01-01"
                elif hasattr(d, "strftime"):
                    date = d.strftime("%Y-%m-%d")
                else:
                    try:
                        date = pd.to_datetime(d).strftime("%Y-%m-%d")
                    except Exception:
                        date = "1900-01-01"

    if id_patient is None:
        not_inserted_cont += 1
        rdict = row.to_dict()
        rdict["Motivo"] = "Id do Cliente não encontrado (nem na planilha, nem via atendimento)"
        rdict["Arquivo"] = file_name
        log_not_inserted.append(rdict)
        continue

    if date is None:
        date = "1900-01-01"

    # --------------------------
    # Define Id do Histórico (determinístico)
    # --------------------------
    # Se existir alguma coluna de id, usa; senão usa hash do link (reprodutível)
    id_record = verify_nan(row.get("CODIGO")) or verify_nan(row.get("ID")) or verify_nan(row.get("Id do Histórico"))
    if id_record is None:
        id_record = zlib.crc32(str(link).encode("utf-8")) & 0xFFFFFFFF

    # Não inserir se já existir
    try:
        if exists(session, id_record, "Id do Histórico", Historico):
            not_inserted_cont += 1
            rdict = row.to_dict()
            rdict["Motivo"] = "Histórico já existe no banco de dados"
            rdict["Arquivo"] = file_name
            log_not_inserted.append(rdict)
            continue
    except Exception as e:
        # Se der erro por tipo/coluna, loga e pula
        not_inserted_cont += 1
        rdict = row.to_dict()
        rdict["Motivo"] = f"Erro ao verificar existência (exists): {e}"
        rdict["Arquivo"] = file_name
        log_not_inserted.append(rdict)
        continue

    # --------------------------
    # Insere
    # --------------------------
    try:
        new_record = Historico(Data=date)
        setattr(new_record, "Id do Histórico", id_record)
        setattr(new_record, "Id do Cliente", id_patient)
        setattr(new_record, "Id do Usuário", 0)

        # record (nome do arquivo) no campo "Histórico"
        setattr(new_record, "Histórico", bindparam(None, value=record, type_=UnicodeText()))

        # classe (nome do arquivo) no campo "Classe"
        # (sem UnicodeText normalmente, mas pode colocar se quiser)
        setattr(new_record, "Classe", classe)

        session.add(new_record)
        inserted_cont += 1

        log_inserted.append(
            {
                "Id do Histórico": id_record,
                "Id do Cliente": id_patient,
                "Data": date,
                "Histórico": record,
                "Classe": classe,
                "Arquivo": file_name,
                "LINK": link,
                "Id do Usuário": 0,
            }
        )

        if inserted_cont % 500 == 0:
            session.commit()

    except Exception as e:
        session.rollback()
        not_inserted_cont += 1
        rdict = row.to_dict()
        rdict["Motivo"] = f"Erro ao inserir no banco: {e}"
        rdict["Arquivo"] = file_name
        log_not_inserted.append(rdict)

    if (idx + 1) % 500 == 0 or (idx + 1) == len(df_anexos):
        print(f"Processados {idx + 1} de {len(df_anexos)} ({(idx + 1) / len(df_anexos) * 100:.2f}%)")

# Commit final
session.commit()
session.close()

print("\nConcluído!")
print(f"{inserted_cont} registros inseridos com sucesso.")
print(f"{not_inserted_cont} registros NÃO inseridos (ver logs).")

# Logs
create_log(log_inserted, path_file, "log_inserted_anexos.xlsx")
create_log(log_not_inserted, path_file, "log_not_inserted_anexos.xlsx")

print(f"\nArquivos baixados em: {download_dir}")
print(f"Logs gerados em: {path_file}")
