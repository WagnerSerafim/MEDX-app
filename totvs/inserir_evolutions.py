from datetime import datetime
import glob
import json
import os
from sqlalchemy import MetaData, Table, create_engine, bindparam, UnicodeText, select
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
import urllib
from utils.utils import is_valid_date, exists, create_log, verify_nan

# ===== CONFIGURAÇÕES INICIAIS =====
sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")

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

# ===== INICIALIZAÇÃO DE VARIÁVEIS =====
log_folder = path_file
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

execution_start = datetime.now()
log_entries = []

def log_jsonl_entry(event_type, message, details=None):
    """Adiciona uma entrada ao log JSONL"""
    entry = {
        "timestamp": datetime.now().isoformat(),
        "event_type": event_type,
        "message": message,
    }
    if details:
        entry["details"] = details
    log_entries.append(entry)
    return entry

def save_jsonl_log():
    """Salva o log em formato JSONL"""
    log_path = os.path.join(log_folder, f"migracao_evolutions_{execution_start.strftime('%Y%m%d_%H%M%S')}.jsonl")
    with open(log_path, 'w', encoding='utf-8') as f:
        for entry in log_entries:
            f.write(json.dumps(entry, ensure_ascii=False) + '\n')
    return log_path

# ===== CARREGAMENTO DO BACKUP EXISTENTE =====
print("Carregando dados de backup existentes...")
log_jsonl_entry("BACKUP", "Procurando arquivo de backup existente")

backup_df = None
backup_files = glob.glob(f'{path_file}/backup_historico_*.xlsx')

if backup_files:
    # Usar o arquivo de backup mais recente
    backup_file = sorted(backup_files)[-1]
    try:
        backup_df = pd.read_excel(backup_file)
        log_jsonl_entry("BACKUP", "Arquivo de backup existente carregado com sucesso", {
            "caminho_arquivo": backup_file,
            "total_registros": len(backup_df)
        })
        print(f"Backup existente carregado: {len(backup_df)} registros")
    except Exception as e:
        log_jsonl_entry("AVISO", "Erro ao ler arquivo de backup, buscando dados do banco de dados", {"erro": str(e)})
        print(f"Aviso: Erro ao ler backup ({e}), buscando do banco...")
        backup_df = None
else:
    log_jsonl_entry("AVISO", "Nenhum arquivo de backup encontrado, buscando dados do banco de dados")
    print("Nenhum arquivo de backup encontrado, buscando dados do banco...")

# Se não houver backup de arquivo, buscar do banco
if backup_df is None:
    try:
        backup_query = select(historico_tbl)
        backup_data = session.execute(backup_query).fetchall()
        backup_df = pd.DataFrame(backup_data)
        log_jsonl_entry("BACKUP", "Dados de backup lidos do banco de dados", {
            "total_registros": len(backup_df)
        })
        print(f"Backup do banco carregado: {len(backup_df)} registros")
    except Exception as e:
        log_jsonl_entry("ERRO", "Falha ao obter dados de backup do banco de dados", {"erro": str(e)})
        print(f"Erro ao buscar backup do banco: {e}")
        session.close()
        save_jsonl_log()
        raise

# ===== PREPARAÇÃO DO CONJUNTO DE DEDUPLICAÇÃO =====
print("Preparando dados de deduplicação...")
log_jsonl_entry("DEDUPLICACAO", "Preparando conjunto de deduplicação")

backup_duplicates_set = set()
if len(backup_df) > 0:
    for idx, row in backup_df.iterrows():
        # Normalizar os dados para comparação
        historico = str(row.get('Histórico', '')).strip() if pd.notna(row.get('Histórico')) else ''
        data = str(row.get('Data', '')).strip() if pd.notna(row.get('Data')) else ''
        id_cliente = str(row.get('Id do Cliente', '')).strip() if pd.notna(row.get('Id do Cliente')) else ''
        
        backup_duplicates_set.add((historico, data, id_cliente))

log_jsonl_entry("DEDUPLICACAO", "Conjunto de deduplicação preparado", {
    "total_combinacoes_unicas": len(backup_duplicates_set)
})

print(f"DEBUG: Conjunto de deduplicação tem {len(backup_duplicates_set)} combinações únicas")
if len(backup_duplicates_set) > 0:
    # Mostrar uma amostra das primeiras 3 duplicatas
    sample = list(backup_duplicates_set)[:3]
    print(f"  Amostra de duplicatas: {sample}")

# ===== LEITURA DO ARQUIVO EXCEL =====
print("Lendo arquivo de evolutions...")
log_jsonl_entry("ENTRADA", "Lendo arquivo Excel com registros de evolutions")

extension_file = glob.glob(f'{path_file}/evolutions*.xlsx')
if not extension_file:
    log_jsonl_entry("ERRO", "Nenhum arquivo evolutions*.xlsx encontrado", {"caminho_busca": path_file})
    print(f"Erro: Nenhum arquivo evolutions*.xlsx encontrado em {path_file}")
    session.close()
    save_jsonl_log()
    raise FileNotFoundError(f"Nenhum arquivo evolutions*.xlsx encontrado em {path_file}")

df = pd.read_excel(extension_file[0])
log_jsonl_entry("ENTRADA", "Arquivo Excel lido com sucesso", {
    "caminho_arquivo": extension_file[0],
    "total_linhas": len(df)
})

# ===== PROCESSAMENTO DOS REGISTROS =====
print("Sucesso! Inicializando migração de Evolutions...")
log_jsonl_entry("MIGRACAO", "Iniciando migração de evolutions")

inserted_count = 0
skipped_duplicates = 0
skipped_invalid = 0
skipped_other = 0
not_inserted_data = []
id_record = -60000

print(f"DEBUG: Total de registros a processar: {len(df)}")

for idx, row in df.iterrows():
    if idx % 1000 == 0 or idx == len(df) - 1:
        progress = round((idx / len(df)) * 100, 2)
        status = f"Processados: {idx}/{len(df)} | Inseridos: {inserted_count} | Duplicados: {skipped_duplicates} | Inválidos: {skipped_invalid} | Outros: {skipped_other} | Concluído: {progress}%"
        print(status)
        log_jsonl_entry("PROGRESSO", status)

    id_record -= 1
    
    # Validação 1: Verificar se ID do Histórico já existe
    existing_record = exists(session, id_record, 'Id do Histórico', Historico)
    if existing_record:
        skipped_other += 1
        row_dict = row.to_dict()
        row_dict['motivo_rejeicao'] = 'Id do Histórico já existe no banco de dados'
        not_inserted_data.append(row_dict)
        log_jsonl_entry("REJEICAO", "Id do Histórico já existe", {
            "id_historico": id_record,
            "linha_excel": idx
        })
        if idx % 5000 == 0:
            print(f"  DEBUG: Rejeitado por ID duplicado na linha {idx}")
        continue

    # Validação 2: Verificar campos de evolução
    text = verify_nan(row['OBSERVACAO'])
    weight = verify_nan(row['PESO'])
    height = verify_nan(row['ALTURA'])
    pressure = verify_nan(row['PRESSAOARTERIAL'])

    # Construir registro concatenando campos
    record = ''
    if weight:
        record += f'Peso: {weight}<br>'
    if height:
        record += f'Altura: {height}<br>'
    if pressure:
        record += f'Pressão Arterial: {pressure}<br>'
    if text:
        record += f'<br>{text}'

    if record == "" or record.strip() == '':
        skipped_invalid += 1
        row_dict = row.to_dict()
        row_dict['motivo_rejeicao'] = 'Histórico vazio ou inválido'
        not_inserted_data.append(row_dict)
        log_jsonl_entry("REJEICAO", "Histórico vazio ou inválido", {
            "linha_excel": idx
        })
        if idx % 5000 == 0:
            print(f"  DEBUG: Rejeitado por histórico vazio na linha {idx}")
        continue

    record = record.replace('_x000D_','')

    # Validação 3: Processar data
    date_str = verify_nan(row['DATA'])
    if date_str in ['', None]:
        date = '1900-01-01'
    else:
        try:
            if isinstance(date_str, str):
                date_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                date = date_obj.strftime('%Y-%m-%d %H:%M:%S')
            else:
                date = date_str.strftime('%Y-%m-%d %H:%M:%S')
            
            if not is_valid_date(date, '%Y-%m-%d %H:%M:%S'):
                date = '1900-01-01'
        except (TypeError, ValueError) as e:
            date = '1900-01-01'

    # Validação 4: Verificar ID do paciente
    id_patient = verify_nan(row['FICHAPACIENTEID'])
    if id_patient in ['', None]:
        skipped_invalid += 1
        row_dict = row.to_dict()
        row_dict['motivo_rejeicao'] = 'Id do paciente vazio'
        not_inserted_data.append(row_dict)
        log_jsonl_entry("REJEICAO", "Id do paciente vazio", {
            "linha_excel": idx
        })
        if idx % 5000 == 0:
            print(f"  DEBUG: Rejeitado por ID paciente vazio na linha {idx}")
        continue

    # Validação 5: Verificar duplicação antes de inserir
    historico_normalized = str(record).strip()
    data_normalized = str(date).strip()
    id_cliente_normalized = str(id_patient).strip()
    
    duplicate_key = (historico_normalized, data_normalized, id_cliente_normalized)
    
    if duplicate_key in backup_duplicates_set:
        skipped_duplicates += 1
        row_dict = row.to_dict()
        row_dict['motivo_rejeicao'] = 'Registro duplicado: [Histórico], [Data] e [Id do Cliente] já existem no banco de dados'
        not_inserted_data.append(row_dict)
        log_jsonl_entry("REJEICAO", "Registro duplicado detectado", {
            "id_cliente": id_patient,
            "data": date,
            "historico_preview": record[:100] + "..." if len(record) > 100 else record,
            "linha_excel": idx
        })
        if idx % 5000 == 0:
            print(f"  DEBUG: Rejeitado por duplicação na linha {idx}")
        continue

    # ===== INSERÇÃO DO REGISTRO =====
    try:
        new_record = Historico(
            Data=date,
        )
        setattr(new_record, "Histórico", bindparam(None, value=record, type_=UnicodeText()))
        setattr(new_record, "Id do Histórico", id_record)
        setattr(new_record, "Id do Cliente", id_patient)
        setattr(new_record, "Id do Usuário", 0)
        
        session.add(new_record)
        inserted_count += 1
        
        if idx % 5000 == 0:
            print(f"  DEBUG: Registro inserido com sucesso na linha {idx}")
        
        log_jsonl_entry("INSERCAO", "Registro inserido com sucesso", {
            "id_historico": id_record,
            "id_cliente": id_patient,
            "data": date,
            "historico_preview": record[:100] + "..." if len(record) > 100 else record,
            "linha_excel": idx,
            "campos_origem": {
                "peso": weight,
                "altura": height,
                "pressao_arterial": pressure,
                "observacao": text[:50] + "..." if text and len(text) > 50 else text
            }
        })

        if inserted_count % 1000 == 0:
            session.commit()
            log_jsonl_entry("COMMIT", f"Commit realizado após {inserted_count} inserções")

    except Exception as e:
        skipped_other += 1
        row_dict = row.to_dict()
        row_dict['motivo_rejeicao'] = f'Erro ao inserir: {str(e)}'
        not_inserted_data.append(row_dict)
        if idx % 5000 == 0:
            print(f"  DEBUG: ERRO ao processar linha {idx}: {str(e)[:100]}")
        log_jsonl_entry("ERRO", "Erro ao inserir registro", {
            "erro": str(e),
            "id_cliente": id_patient,
            "linha_excel": idx
        })
        continue

# ===== FINALIZAÇÃO E COMMIT =====
print("Finalizando migração...")
session.commit()
session.close()

log_jsonl_entry("MIGRACAO", "Migração concluída e banco de dados commitado", {
    "total_inseridos": inserted_count,
    "total_duplicados": skipped_duplicates,
    "total_invalidos": skipped_invalid,
    "total_outros_erros": skipped_other,
    "total_rejeitados": len(not_inserted_data),
    "total_processados": len(df)
})

# ===== GERAÇÃO DE LOGS =====
print("Migração concluída! Gerando logs...")
print(f"{inserted_count} novos históricos foram inseridos com sucesso!")
print(f"{skipped_duplicates} históricos foram pulados por duplicação!")
if skipped_invalid > 0:
    print(f"{skipped_invalid} históricos foram rejeitados por dados inválidos.")
if skipped_other > 0:
    print(f"{skipped_other} históricos foram rejeitados por outros motivos.")

# Salvar logs em Excel (compatível com código antigo)
if not_inserted_data:
    create_log(not_inserted_data, log_folder, "log_not_inserted_record_evolutions.xlsx")

# ===== RESUMO FINAL =====
execution_end = datetime.now()
duration = (execution_end - execution_start).total_seconds()

summary = {
    "timestamp_inicio": execution_start.isoformat(),
    "timestamp_fim": execution_end.isoformat(),
    "duracao_segundos": duration,
    "arquivo_evolutions": extension_file[0],
    "arquivo_backup_usado": backup_files[-1] if backup_files else "Dados lidos do banco de dados",
    "total_linhas_excel": len(df),
    "total_inseridos": inserted_count,
    "total_duplicados": skipped_duplicates,
    "total_invalidos": skipped_invalid,
    "total_outros_erros": skipped_other,
    "taxa_sucesso_percentual": round((inserted_count / len(df)) * 100, 2) if len(df) > 0 else 0,
    "resumo": f"De {len(df)} registros processados, {inserted_count} foram inseridos com sucesso. {skipped_duplicates} foram pulados por duplicação, {skipped_invalid} por dados inválidos, e {skipped_other} por outros motivos."
}

log_jsonl_entry("RESUMO", "Resumo final da execução", summary)

# ===== SALVAR LOG JSONL =====
log_path = save_jsonl_log()
print(f"\nLog JSONL salvo em: {log_path}")
print(f"\nResumo da Execução:")
print(f"  - Total de registros inseridos: {inserted_count}")
print(f"  - Total de registros duplicados: {skipped_duplicates}")
print(f"  - Total de registros inválidos: {skipped_invalid}")
print(f"  - Total de registros com outros erros: {skipped_other}")
print(f"  - Taxa de sucesso: {summary['taxa_sucesso_percentual']}%")
print(f"  - Tempo de execução: {duration:.2f} segundos")
