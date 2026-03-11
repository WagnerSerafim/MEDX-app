from datetime import datetime
import glob
import json
import os
from sqlalchemy import MetaData, Table, create_engine, bindparam, UnicodeText
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, verify_nan

# Config
sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos JSON: ")
mode = input("Modo de execução - 'batch' (commit por 1000) ou 'atomic' (commit único no final) [batch/atomic]: ").strip().lower()
if mode not in ("batch", "atomic"):
    mode = "batch"

print("Conectando no Banco de Dados...")
DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"
engine = create_engine(DATABASE_URL)

metadata = MetaData()
historico_tbl = Table("Histórico de Clientes", metadata, schema=f"schema_{sid}", autoload_with=engine)
Base = declarative_base()

class Historico(Base):
    __table__ = historico_tbl

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

# Logging JSONL
log_folder = path_file
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

execution_start = datetime.now()
log_entries = []

def log_jsonl_entry(event_type, message, details=None):
    entry = {
        "timestamp": datetime.now().isoformat(),
        "event_type": event_type,
        "message": message,
    }
    if details is not None:
        entry["details"] = details
    log_entries.append(entry)

def save_jsonl_log(name_prefix="migracao_anexos"):
    log_path = os.path.join(log_folder, f"{name_prefix}_{execution_start.strftime('%Y%m%d_%H%M%S')}.jsonl")
    with open(log_path, 'w', encoding='utf-8') as f:
        for entry in log_entries:
            f.write(json.dumps(entry, ensure_ascii=False) + '\n')
    return log_path


def normalize_text(s):
    """Remove prefix D:\\Medxdata\\ and attempt to fix common mojibake (latin1<->utf-8)."""
    if s is None:
        return s
    if not isinstance(s, str):
        s = str(s)

    # remove prefix variants
    for prefix in (r"D:\\Medxdata\\", r"D:/Medxdata/", r"D:\\Medxdata/", r"D:/Medxdata\\"):
        if s.startswith(prefix):
            s = s[len(prefix):]
            break

    # if filename/path still contains the drive with single backslash pattern
    if s.startswith('D:\\Medxdata\\'):
        s = s[len('D:\\Medxdata\\'):]
    if s.startswith('D:\\'):
        # remove only D:\ prefix if present
        s = s[3:]

    # fix common mojibake where UTF-8 was decoded as latin1
    if 'Ã' in s or 'Â' in s or '\ufffd' in s:
        try:
            fixed = s.encode('latin1').decode('utf-8')
            s = fixed
        except Exception:
            pass

    return s

log_jsonl_entry("START", "Início da execução", {"mode": mode})

# Find JSON file(s) like patients.py
# The user may provide either a path to a JSON file or a directory containing JSON files.
if os.path.isfile(path_file) and path_file.lower().endswith('.json'):
    json_file = path_file
else:
    json_files = glob.glob(os.path.join(path_file, "*.json"))
    if not json_files:
        log_jsonl_entry("ERROR", "Nenhum arquivo JSON encontrado no diretório", {"path": path_file})
        print(f"Nenhum arquivo JSON encontrado em {path_file}")
        save_jsonl_log()
        session.close()
        raise FileNotFoundError(f"Nenhum arquivo JSON encontrado em {path_file}")

    # Prefer files named anexos_backup*.json if present
    candidates = [p for p in json_files if os.path.basename(p).lower().startswith('anexos_backup')]
    if candidates:
        json_file = sorted(candidates)[-1]
    else:
        json_file = sorted(json_files)[0]

log_jsonl_entry("INPUT", "Arquivo JSON selecionado", {"arquivo": json_file})
print(f"Lendo {json_file} ...")

# Read JSON into DataFrame (preserve encoding)
try:
    with open(json_file, 'r', encoding='latin1') as f:
        data = json.load(f)
    df = pd.DataFrame(data)
except Exception:
    # last resort
    df = pd.read_json(json_file, encoding='latin1')

log_jsonl_entry("INPUT", "JSON carregado em DataFrame", {"linhas": len(df)})

# Counters
inserted_count = 0
skipped_duplicate = 0
skipped_invalid = 0
skipped_errors = 0
not_inserted_data = []

batch_size = 1000
batch_objects = []
batch_index = 0
# start Ids for new históricos (negative range to avoid conflicts)
id_record = -100000

# Atomic mode: open outer transaction
outer_tx = None
if mode == 'atomic':
    outer_tx = session.begin()
    log_jsonl_entry("TRANSACTION", "Atomic transaction started")

print(f"Processando {len(df)} registros (modo={mode})...")
log_jsonl_entry("PROCESS", "Iniciando processamento de registros", {"total": len(df)})

for idx, row in df.iterrows():
    if idx % 500 == 0:
        # progress log every 500
        print(f"Processados: {idx}/{len(df)} | Inseridos: {inserted_count} | Duplicados: {skipped_duplicate} | Inválidos: {skipped_invalid} | Erros: {skipped_errors}")
        log_jsonl_entry("PROGRESS", "Progresso", {"idx": idx, "inseridos": inserted_count, "duplicados": skipped_duplicate})

    # Validate required fields
    id_cliente = verify_nan(row.get('Id do Cliente'))
    raw_classe = verify_nan(row.get('Classe'))
    raw_historico = verify_nan(row.get('Histórico'))

    # normalize path and encoding issues
    classe = normalize_text(raw_classe) if raw_classe is not None else raw_classe
    historico = normalize_text(raw_historico) if raw_historico is not None else raw_historico
    date_str = verify_nan(row.get('Data'))

    if id_cliente in [None, '', 'None'] or classe in [None, '', 'None']:
        skipped_invalid += 1
        rec = row.to_dict()
        rec['motivo'] = 'Id do Cliente ou Classe ausente'
        not_inserted_data.append(rec)
        log_jsonl_entry("SKIP", "Registro inválido, campos ausentes", {"linha": idx, "motivo": "Id do Cliente ou Classe ausente"})
        continue

    # Validate date
    if date_str in ['', None]:
        date = '1900-01-01'
    else:
        try:
            if isinstance(date_str, str):
                # Accept 'YYYY-mm-dd HH:MM:SS' or 'YYYY-mm-dd'
                if len(date_str) == 10:
                    date = datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y-%m-%d')
                else:
                    date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
            else:
                date = pd.to_datetime(date_str).strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            date = '1900-01-01'

    # Check duplicate by Classe field (same logic as images.py)
    try:
        # check against normalized class
        if exists(session, classe, 'Classe', Historico):
            skipped_duplicate += 1
            rec = row.to_dict()
            rec['motivo'] = 'Classe já existe'
            not_inserted_data.append(rec)
            log_jsonl_entry("SKIP", "Duplicado detectado (Classe)", {"linha": idx, "classe": classe, "classe_original": raw_classe})
            continue
    except Exception as e:
        # DB check error -> treat as error
        skipped_errors += 1
        rec = row.to_dict()
        rec['motivo'] = f'Erro verificando duplicado: {str(e)}'
        not_inserted_data.append(rec)
        log_jsonl_entry("ERROR", "Erro ao verificar duplicado", {"linha": idx, "erro": str(e)})
        continue

    # Create Historico object
    try:
        new_obj = Historico(
            Histórico=historico if historico is not None else classe,
            Data=date,
            Classe=classe
        )
        # assign a unique negative Id do Histórico to avoid collisions
        setattr(new_obj, "Id do Histórico", id_record)
        id_record -= 1
        setattr(new_obj, "Id do Cliente", id_cliente)
        setattr(new_obj, "Id do Usuário", 0)

        # Add to batch
        batch_objects.append((idx, new_obj, row.to_dict()))

        # Commit per batch in batch mode, or keep accumulating in atomic
        if mode == 'batch' and len(batch_objects) >= batch_size:
            batch_index += 1
            try:
                for _idx, obj, sketch in batch_objects:
                    session.add(obj)
                session.commit()
                inserted_count += len(batch_objects)
                log_jsonl_entry("BATCH_COMMIT", "Commit de batch bem-sucedido", {"batch_index": batch_index, "batch_size": len(batch_objects)})
                batch_objects = []
            except Exception as e:
                # rollback this batch
                session.rollback()
                skipped_errors += len(batch_objects)
                log_jsonl_entry("BATCH_ROLLBACK", "Rollback do batch devido a erro", {"batch_index": batch_index, "erro": str(e)})
                # record each failed row
                for _idx, _obj, sketch in batch_objects:
                    sketch['motivo'] = f'Erro ao inserir no batch: {str(e)}'
                    not_inserted_data.append(sketch)
                batch_objects = []
                continue

    except Exception as e:
        skipped_errors += 1
        rec = row.to_dict()
        rec['motivo'] = f'Erro ao criar objeto Historico: {str(e)}'
        not_inserted_data.append(rec)
        log_jsonl_entry("ERROR", "Erro ao construir objeto para inserção", {"linha": idx, "erro": str(e)})
        continue

# End loop: flush remaining
if mode == 'batch' and batch_objects:
    batch_index += 1
    try:
        for _idx, obj, sketch in batch_objects:
            session.add(obj)
        session.commit()
        inserted_count += len(batch_objects)
        log_jsonl_entry("BATCH_COMMIT", "Commit final de batch bem-sucedido", {"batch_index": batch_index, "batch_size": len(batch_objects)})
        batch_objects = []
    except Exception as e:
        session.rollback()
        skipped_errors += len(batch_objects)
        log_jsonl_entry("BATCH_ROLLBACK", "Rollback do batch final devido a erro", {"batch_index": batch_index, "erro": str(e)})
        for _idx, _obj, sketch in batch_objects:
            sketch['motivo'] = f'Erro ao inserir no batch final: {str(e)}'
            not_inserted_data.append(sketch)
        batch_objects = []

# Atomic mode: commit or rollback
if mode == 'atomic':
    try:
        # add all accumulated objects to the session then commit
        for _idx, obj, sketch in batch_objects:
            session.add(obj)
        session.commit()
        inserted_count = len(batch_objects)
        log_jsonl_entry("TRANSACTION", "Atomic commit bem-sucedido", {"inseridos": inserted_count, "batches_total_objects": len(batch_objects)})
    except Exception as e:
        session.rollback()
        log_jsonl_entry("TRANSACTION_ROLLBACK", "Rollback atômico devido a erro", {"erro": str(e)})
        # mark all as not inserted
        skipped_errors = len(df)
        not_inserted_data = [row.to_dict() for _, row in df.iterrows()]

# Final summary
session.close()

summary = {
    "timestamp_inicio": execution_start.isoformat(),
    "timestamp_fim": datetime.now().isoformat(),
    "total_registros": len(df),
    "total_inseridos": inserted_count,
    "total_duplicados": skipped_duplicate,
    "total_invalidos": skipped_invalid,
    "total_erros": skipped_errors,
    "batch_size": batch_size,
    "mode": mode
}

log_jsonl_entry("SUMMARY", "Resumo final da execução", summary)
log_path = save_jsonl_log("migracao_anexos")

# Also keep compatibility with old Excel log function
if not_inserted_data:
    try:
        create_log(not_inserted_data, log_folder, "log_not_inserted_anexos.xlsx")
    except Exception:
        pass

print(f"Execução finalizada. Inseridos: {inserted_count}, Duplicados: {skipped_duplicate}, Inválidos: {skipped_invalid}, Erros: {skipped_errors}")
print(f"Log JSONL salvo em: {log_path}")
