import pandas as pd
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
import urllib
from utils.utils import create_log, is_valid_date
from datetime import datetime
import unicodedata

def remover_acentos(texto):
    if not isinstance(texto, str):
        return texto
    return ''.join(
        c for c in unicodedata.normalize('NFKD', texto)
        if not unicodedata.combining(c)
    )

def append_to_log(log_data, record, status, motivo=""):
    log_data.append({
        'Id do Cliente': getattr(record, 'Id do Cliente', ''),
        'Id da Assinatura': getattr(record, 'Id da Assinatura', ''),
        'Nome': getattr(record, 'Nome', ''),
        'Situação': status,
        'Celular': getattr(record, 'Celular', ''),
        'Email': getattr(record, 'Email', ''),
        'RG': getattr(record, 'RG', ''),
        'CPF/CGC': getattr(record, 'CPF/CGC', ''),
        'Nascimento': getattr(record, 'Nascimento', ''),
        'Sexo': getattr(record, 'Sexo', ''),
        'Endereço Residencial': getattr(record, 'Endereço Residencial', ''),
        'Telefone Residencial': getattr(record, 'Telefone Residencial', ''),
        'Telefone Residencial 1': getattr(record, 'Telefone Residencial 1', ''),
        'Telefone Comercial': getattr(record, 'Telefone Comercial', ''),
        'Estado Residencial': getattr(record, 'Estado Residencial', ''),
        'Cidade Residencial': getattr(record, 'Cidade Residencial', ''),
        'Bairro Residencial': getattr(record, 'Bairro Residencial', ''),
        'Cep Residencial': getattr(record, 'Cep Residencial', ''),
        'Profissão': getattr(record, 'Profissão', ''),
        'Referências': getattr(record, 'Referências', ''),
        'Id do Convênio': getattr(record, 'Id do Convênio', ''),
        'Mãe': getattr(record, 'Mãe', ''),
        'Pai': getattr(record, 'Pai', ''),
        'Como conheceu': getattr(record, 'Como conheceu', ''),
        'Indicado por': getattr(record, 'Indicado por', ''),
        'Contato': getattr(record, 'Contato', ''),
        'Tags': getattr(record, 'Tags', ''),
        'Exclui_Mkt': getattr(record, 'Exclui_Mkt', ''),
        'Nome Social': getattr(record, 'Nome Social', ''),
        'NumeroCNS': getattr(record, 'NumeroCNS', ''),
        'Tipo': getattr(record, 'Tipo', ''),
        'Observações': getattr(record, 'Observações', ''),
        'Motivo': motivo,
        'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

def is_valid_rg(rg):
    return isinstance(rg, str) and 8 <= len(rg) <= 14 and rg is not None

def is_valid_celular(celular):
    return isinstance(celular, str) and len(celular) >= 9 and celular.isdigit() and celular is not None

def update_related_records(record, table, name_column, target_id, log_sql_data):
    try:
        items = session.query(table).filter(getattr(table, name_column) == getattr(record, 'Id do Cliente')).all()
        for item in items:
            setattr(item, name_column, target_id)
            log_sql_data.append({
                'SQL Query': f"UPDATE {table} SET {name_column} = {target_id} WHERE {name_column} = {getattr(record, 'Id do Cliente')}",
                'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
    except Exception as e:
        print(f"Erro ao atualizar IDs na tabela {table}: {e}")

def merge_record_data(target_record, duplicate_record):
    try:
        if (getattr(target_record, 'Celular', '') in ['', None]) and is_valid_celular(getattr(duplicate_record, 'Celular')):
            setattr(target_record, 'Celular', getattr(duplicate_record, 'Celular'))
        if (getattr(target_record, 'RG', '') in ['', None]) and is_valid_rg(getattr(duplicate_record, 'RG')):
            setattr(target_record, 'RG', getattr(duplicate_record, 'RG'))
        if (getattr(target_record, 'CPF/CGC', '') in ['', None]) and isinstance(getattr(duplicate_record, 'CPF/CGC', ''), str) and len(getattr(duplicate_record, 'CPF/CGC', '')) == 11:
            setattr(target_record, 'CPF/CGC', getattr(duplicate_record, 'CPF/CGC', ''))
        if (getattr(target_record, 'Nascimento', '') in ['', None]) and is_valid_date(getattr(duplicate_record, 'Nascimento')):
            setattr(target_record, 'Nascimento', getattr(duplicate_record, 'Nascimento'))
        if (getattr(target_record, 'Email', '') in ['', None]) and isinstance(getattr(duplicate_record, 'Email'), str) and '@' in getattr(duplicate_record, 'Email'):
            setattr(target_record, 'Email', getattr(duplicate_record, 'Email'))
        if (getattr(target_record, 'Endereço Residencial', '') in ['', None]) and isinstance(getattr(duplicate_record, 'Endereço Residencial'), str):
            setattr(target_record, 'Endereço Residencial', getattr(duplicate_record, 'Endereço Residencial'))
        if isinstance(getattr(duplicate_record, 'Observações'), str) and getattr(duplicate_record, 'Observações'):
            obs = getattr(target_record, 'Observações') or ''
            obs += f"      {getattr(duplicate_record, 'Observações')}"
            setattr(target_record, 'Observações', obs)
        if (getattr(target_record, 'Telefone Residencial', '') in ['', None]) and isinstance(getattr(duplicate_record, 'Telefone Residencial'), str):
            setattr(target_record, 'Telefone Residencial', getattr(duplicate_record, 'Telefone Residencial'))
        if (getattr(target_record, 'Telefone Residencial 1', '') in ['', None]) and isinstance(getattr(duplicate_record, 'Telefone Residencial 1'), str):
            setattr(target_record, 'Telefone Residencial 1', getattr(duplicate_record, 'Telefone Residencial 1'))
        if (getattr(target_record, 'Telefone Comercial', '') in ['', None]) and isinstance(getattr(duplicate_record, 'Telefone Comercial'), str):
            setattr(target_record, 'Telefone Comercial', getattr(duplicate_record, 'Telefone Comercial'))
        if (getattr(target_record, 'Sexo', '') in ['', None]) and isinstance(getattr(duplicate_record, 'Sexo'), str):
            setattr(target_record, 'Sexo', getattr(duplicate_record, 'Sexo'))
        if (getattr(target_record, 'Id da Assinatura', '') in ['', None]) and isinstance(getattr(duplicate_record, 'Id da Assinatura'), int):
            setattr(target_record, 'Id da Assinatura', getattr(duplicate_record, 'Id da Assinatura'))
        if (getattr(target_record, 'Estado Residencial', '') in ['', None]) and isinstance(getattr(duplicate_record, 'Estado Residencial'), str):
            setattr(target_record, 'Estado Residencial', getattr(duplicate_record, 'Estado Residencial'))
        if (getattr(target_record, 'Cidade Residencial', '') in ['', None]) and isinstance(getattr(duplicate_record, 'Cidade Residencial'), str):
            setattr(target_record, 'Cidade Residencial', getattr(duplicate_record, 'Cidade Residencial'))
        if (getattr(target_record, 'Bairro Residencial', '') in ['', None]) and isinstance(getattr(duplicate_record, 'Bairro Residencial'), str):
            setattr(target_record, 'Bairro Residencial', getattr(duplicate_record, 'Bairro Residencial'))
        if (getattr(target_record, 'Cep Residencial', '') in ['', None]) and isinstance(getattr(duplicate_record, 'Cep Residencial'), str):
            setattr(target_record, 'Cep Residencial', getattr(duplicate_record, 'Cep Residencial'))
        if (getattr(target_record, 'Profissão', '') in ['', None]) and isinstance(getattr(duplicate_record, 'Profissão'), str):
            setattr(target_record, 'Profissão', getattr(duplicate_record, 'Profissão'))
        if (getattr(target_record, 'Referências', '') in ['', None]) and isinstance(getattr(duplicate_record, 'Referências'), str):
            setattr(target_record, 'Referências', getattr(duplicate_record, 'Referências'))
        if (getattr(target_record, 'Id do Convênio', '') in ['', None]) and isinstance(getattr(duplicate_record, 'Id do Convênio'), int):
            setattr(target_record, 'Id do Convênio', getattr(duplicate_record, 'Id do Convênio'))
        if (getattr(target_record, 'Mãe', '') in ['', None]) and isinstance(getattr(duplicate_record, 'Mãe'), str):
            setattr(target_record, 'Mãe', getattr(duplicate_record, 'Mãe'))
        if (getattr(target_record, 'Pai', '') in ['', None]) and isinstance(getattr(duplicate_record, 'Pai'), str):
            setattr(target_record, 'Pai', getattr(duplicate_record, 'Pai'))
        if (getattr(target_record, 'Como conheceu', '') in ['', None]) and isinstance(getattr(duplicate_record, 'Como conheceu'), str):
            setattr(target_record, 'Como conheceu', getattr(duplicate_record, 'Como conheceu'))
        if (getattr(target_record, 'Indicado por', '') in ['', None]) and isinstance(getattr(duplicate_record, 'Indicado por'), str):
            setattr(target_record, 'Indicado por', getattr(duplicate_record, 'Indicado por'))
        if (getattr(target_record, 'Contato', '') in ['', None]) and isinstance(getattr(duplicate_record, 'Contato'), str):
            setattr(target_record, 'Contato', getattr(duplicate_record, 'Contato'))
        if (getattr(target_record, 'Tags', '') in ['', None]) and isinstance(getattr(duplicate_record, 'Tags'), str):
            setattr(target_record, 'Tags', getattr(duplicate_record, 'Tags'))
        if (getattr(target_record, 'Exclui_Mkt', '') in ['', None]) and isinstance(getattr(duplicate_record, 'Exclui_Mkt'), str):
            setattr(target_record, 'Exclui_Mkt', getattr(duplicate_record, 'Exclui_Mkt'))
        if (getattr(target_record, 'Nome Social', '') in ['', None]) and isinstance(getattr(duplicate_record, 'Nome Social'), str):
            setattr(target_record, 'Nome Social', getattr(duplicate_record, 'Nome Social'))
        if (getattr(target_record, 'NumeroCNS', '') in ['', None]) and isinstance(getattr(duplicate_record, 'NumeroCNS'), str):
            setattr(target_record, 'NumeroCNS', getattr(duplicate_record, 'NumeroCNS'))
        if (getattr(target_record, 'Tipo', '') in ['', None]) and isinstance(getattr(duplicate_record, 'Tipo'), str):
            setattr(target_record, 'Tipo', getattr(duplicate_record, 'Tipo'))
    except Exception as e:
        print(f"Erro ao mesclar dados do registro: {e}")

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
log_path = input("Informe o caminho da pasta onde quer salvar o arquivo de log: ").strip()

print("Conectando no Banco de dados...")

try:
    DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"
    engine = create_engine(DATABASE_URL)
    Base = automap_base()
    Base.prepare(autoload_with=engine)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    Contatos = Base.classes.Contatos
    HistoricoClientes = Base.classes['Histórico de Clientes']
    Agenda = Base.classes.Agenda
    Antiaging = Base.classes.Antiaging
    AntiagingAlimentos = Base.classes['Antiaging Alimentos']
    Atendimentos = Base.classes.Atendimentos
    Histórico_Detalhes = Base.classes['Histórico_Detalhes']
    Biospace = Base.classes.Biospace
    GuiaMedicacao = Base.classes['Guia de Medicação']
    LogdeTelefonemas = Base.classes['LogdeTelefonemas']
    EsteticaFacial = Base.classes['Estetica_facial']
except Exception as e:
    print(f"Erro ao conectar ao banco de dados: {e}")
    exit()

print("Sucesso! Começando unificação de registros...")

try:
    df = pd.read_sql(session.query(Contatos).statement, session.bind)
    df['Nome'] = df['Nome'].astype(str).str.upper().str.replace(' ', '', regex=False)
    df['Nome_Normalizado'] = df['Nome'].apply(remover_acentos)
    df['Nascimento'] = df['Nascimento'].astype(str).str.strip()
except Exception as e:
    print(f"Erro ao ler os dados do banco de dados: {e}")
    exit()

log_data = []
log_sql_data = []
log_not_unified = []

# Agrupa por nome normalizado
for nome_normalizado, grupo in df.groupby('Nome_Normalizado'):
    if len(grupo) < 2:
        continue  # Só interessa se houver mais de um com o mesmo nome

    nascimentos_unicos = grupo['Nascimento'].unique()
    grupo_1900 = grupo[grupo['Nascimento'] == '1900-01-01']
    grupo_validos = grupo[grupo['Nascimento'] != '1900-01-01']

    # Caso 1: Existem registros com nascimento 1900-01-01 e outros com nascimento válido
    if not grupo_1900.empty and not grupo_validos.empty:
        for _, row in grupo_1900.iterrows():
            append_to_log(
                log_not_unified,
                row,
                'Não unificado',
                motivo='Data de nascimento igual a 1900-01-01'
            )

    # Caso 2: Existem dois ou mais registros com o mesmo nome, mas nascimentos diferentes (e nenhum deles é 1900-01-01)
    if len(grupo_validos['Nascimento'].unique()) > 1:
        for _, row in grupo_validos.iterrows():
            append_to_log(
                log_not_unified,
                row,
                'Não unificado',
                motivo='Data de nascimento diferente de outro registro com mesmo nome'
            )

# Agora, só unifica quem tem mesmo nome normalizado e mesma data de nascimento (e nascimento != 1900-01-01)
mask_validas = df['Nascimento'] != '1900-01-01'
duplicates = df[mask_validas & df.duplicated(subset=['Nome_Normalizado', 'Nascimento'], keep=False)]
unified_df = duplicates.groupby(['Nome_Normalizado', 'Nascimento']).first().reset_index()

# Para controle de quais registros já foram unificados
unificados_set = set(zip(unified_df['Nome_Normalizado'], unified_df['Nascimento']))

for _, row in unified_df.iterrows():
    nome_normalizado = row['Nome_Normalizado']
    nascimento = row['Nascimento']
    print(f"Processando registro: Nome={nome_normalizado}, Nascimento={nascimento}")

    grupo = df[(df['Nome_Normalizado'] == nome_normalizado) & (df['Nascimento'] == nascimento)]
    ids_grupo = grupo['Id do Cliente'].tolist()
    duplicate_records = session.query(Contatos).filter(getattr(Contatos, "Id do Cliente").in_(ids_grupo)).all()

    if len(duplicate_records) < 2:
        continue

    append_to_log(log_data, duplicate_records[0], 'Registro original')
    target_id = getattr(duplicate_records[0], 'Id do Cliente')

    for record in duplicate_records[1:]:
        merge_record_data(duplicate_records[0], record)
        update_related_records(record, HistoricoClientes, 'Id do Cliente', target_id, log_sql_data)
        update_related_records(record, Agenda, 'Vinculado a', target_id, log_sql_data)
        update_related_records(record, Antiaging, 'Id do Cliente', target_id, log_sql_data)
        update_related_records(record, AntiagingAlimentos, 'Id do Cliente', target_id, log_sql_data)
        update_related_records(record, Atendimentos, 'Id do cliente', target_id, log_sql_data)
        update_related_records(record, Histórico_Detalhes, 'Id do Cliente', target_id, log_sql_data)
        update_related_records(record, Biospace, 'Id do Cliente', target_id, log_sql_data)
        update_related_records(record, GuiaMedicacao, 'Id do Cliente', target_id, log_sql_data)
        update_related_records(record, LogdeTelefonemas, 'Id do cliente', target_id, log_sql_data)
        update_related_records(record, EsteticaFacial, 'Id do Cliente', target_id, log_sql_data)
        session.commit()

    append_to_log(log_data, duplicate_records[0], 'Registro final unificado')

    for record in duplicate_records[1:]:
        append_to_log(log_data, record, 'Registro duplicado/removido')
        log_sql_data.append({
            'SQL Query': f"DELETE FROM Contatos WHERE Id do Cliente = {getattr(record, 'Id do Cliente')}",
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        try:
            session.delete(record)
        except Exception as e:
            print(f"Erro ao deletar registro: {e}")
    session.commit()

session.commit()
create_log(log_sql_data, log_path, "log_SQL_queries_unificacao.xlsx")
create_log(log_data, log_path, "log_contatos_unificacao.xlsx")
create_log(log_not_unified, log_path, "log_not_unified_sql_data.xlsx")
print("Unificação de registros concluída!")
print(f"Log Queries SQL, Log Contatos Unificação e Log Não Unificados salvos em: {log_path}")

session.close()