import datetime
import os
import pandas as pd
from sqlalchemy import create_engine, event, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
import urllib
from utils.utils import create_log, is_valid_date

def append_to_log(log_data, record, status):
    log_data.append({'Id do Cliente': getattr(record, 'Id do Cliente'),
                     'Id da Assinatura': getattr(record, 'Id da Assinatura'),
                     'Nome': getattr(record, 'Nome'),
                     'Situação': status,
                     'Celular': getattr(record, 'Celular'),
                     'Email': getattr(record, 'Email'),
                     'RG': getattr(record, 'RG'),
                     'CPF/CGC': getattr(record, 'CPF/CGC'),
                     'Nascimento': getattr(record, 'Nascimento'),
                     'Sexo': getattr(record, 'Sexo'),
                     'Endereço Residencial': getattr(record, 'Endereço Residencial'),
                     'Telefone Residencial': getattr(record, 'Telefone Residencial'),
                     'Observações': getattr(record, 'Observações')})

def is_valid_rg(rg):
    """ Verifica se o RG tem uma quantidade aceitável para ser válido. """
    return isinstance(rg, str) and len(rg) >= 8 and len(rg) <= 14 and rg != None

def is_valid_celular(celular):
    """ Verifica se o celular tem uma quantidade aceitável para ser válido. """
    return isinstance(celular, str) and len(celular) >= 9 and celular.isdigit() and celular != None

def update_related_records(record, table, name_column, target_id, log_sql_data):
    """ Atualiza os IDs das tabelas """
    try:
        items = session.query(table).filter(getattr(table, name_column) == getattr(record, 'Id do Cliente')).all()
        for item in items:
            setattr(item, name_column, target_id)
            log_sql_data.append({
                'SQL Query': f"UPDATE {table} SET {name_column} = {target_id} WHERE {name_column} = {getattr(record, 'Id do Cliente')}",
                'Timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
    except Exception as e: 
        print(f"Erro ao atualizar IDs na tabela {table}: {e}")

def unify_duplicates(group):
    group = group.iloc[0]
    return group

def merge_record_data(target_record, duplicate_record):
    """ Função para preencher dados ausentes no registro principal com dados dos registros duplicados """
    try:
    
        if (getattr(target_record, 'Celular', '') == '' or getattr(target_record, 'Celular', '') == None) and is_valid_celular(getattr(duplicate_record, 'Celular')):
            setattr(target_record, 'Celular', getattr(duplicate_record, 'Celular'))

        if (getattr(target_record, 'RG', '') == '' or getattr(target_record, 'RG', '') == None) and is_valid_rg(getattr(duplicate_record, 'RG')): 
            setattr(target_record, 'RG', getattr(duplicate_record, 'RG'))
        
        if (getattr(target_record, 'CPF/CGC', '') == '' or getattr(target_record, 'CPF/CGC', '') == None) and isinstance(getattr(duplicate_record, 'CPF/CGC', ''), str) and len(getattr(duplicate_record, 'CPF/CGC', '')) == 11:
            setattr(target_record, 'CPF/CGC', getattr(duplicate_record, 'CPF/CGC', ''))

        if (getattr(target_record, 'Nascimento', '') == '' or getattr(target_record, 'Nascimento', '') == None) and is_valid_date(getattr(duplicate_record, 'Nascimento')):
            setattr(target_record, 'Nascimento', getattr(duplicate_record, 'Nascimento'))
        
        if (getattr(target_record, 'Email', '') == '' or getattr(target_record, 'Email', '') == None) and isinstance(getattr(duplicate_record, 'Email'), str) and '@' in getattr(duplicate_record, 'Email'):
            setattr(target_record, 'Email', getattr(duplicate_record, 'Email'))

        if (getattr(target_record, 'Endereço Residencial', '') == '' or getattr(target_record, 'Endereço Residencial', '') == None) and isinstance(getattr(duplicate_record, 'Endereço Residencial'), str):
            setattr(target_record, 'Endereço Residencial', getattr(duplicate_record, 'Endereço Residencial'))

        if isinstance(getattr(duplicate_record, 'Observações'), str) and len(getattr(duplicate_record, 'Observações')) > 0 and getattr(duplicate_record, 'Observações') != None:
            obs = getattr(target_record, 'Observações')
            obs += f"      {getattr(duplicate_record, 'Observações')}"
            setattr(target_record, 'Observações', obs)

        if (getattr(target_record, 'Telefone Residencial', '') == '' or getattr(target_record, 'Telefone Residencial', '') == None) and isinstance(getattr(duplicate_record, 'Telefone Residencial'), str):
            setattr(target_record, 'Telefone Residencial', getattr(duplicate_record, 'Telefone Residencial'))

        if (getattr(target_record, 'Telefone Residencial 1', '') == '' or getattr(target_record, 'Telefone Residencial 1', '') == None) and isinstance(getattr(duplicate_record, 'Telefone Residencial 1'), str):
            setattr(target_record, 'Telefone Residencial 1', getattr(duplicate_record, 'Telefone Residencial 1'))

        if (getattr(target_record, 'Telefone Comercial', '') == '' or getattr(target_record, 'Telefone Comercial', '') == None) and isinstance(getattr(duplicate_record, 'Telefone Comercial'), str):
            setattr(target_record, 'Telefone Comercial', getattr(duplicate_record, 'Telefone Comercial'))
        
        if (getattr(target_record, 'Sexo', '') == '' or getattr(target_record, 'Sexo', '') == None) and isinstance(getattr(duplicate_record, 'Sexo'), str):
            setattr(target_record, 'Sexo', getattr(duplicate_record, 'Sexo'))

        if (getattr(target_record, 'Id da Assinatura', '') == '' or getattr(target_record, 'Id da Assinatura', '') == None) and isinstance(getattr(duplicate_record, 'Id da Assinatura'), int):
            setattr(target_record, 'Id da Assinatura', getattr(duplicate_record, 'Id da Assinatura'))

        if (getattr(target_record, 'Estado Residencial', '') == '' or getattr(target_record, 'Estado Residencial', '') == None) and isinstance(getattr(duplicate_record, 'Estado Residencial'), str):
            setattr(target_record, 'Estado Residencial', getattr(duplicate_record, 'Estado Residencial'))

        if (getattr(target_record, 'Cidade Residencial', '') == '' or getattr(target_record, 'Cidade Residencial', '') == None) and isinstance(getattr(duplicate_record, 'Cidade Residencial'), str):
            setattr(target_record, 'Cidade Residencial', getattr(duplicate_record, 'Cidade Residencial'))

        if (getattr(target_record, 'Bairro Residencial', '') == '' or getattr(target_record, 'Bairro Residencial', '') == None) and isinstance(getattr(duplicate_record, 'Bairro Residencial'), str):
            setattr(target_record, 'Bairro Residencial', getattr(duplicate_record, 'Bairro Residencial'))
        
        if (getattr(target_record, 'Cep Residencial', '') == '' or getattr(target_record, 'Cep Residencial', '') == None) and isinstance(getattr(duplicate_record, 'Cep Residencial'), str):
            setattr(target_record, 'Cep Residencial', getattr(duplicate_record, 'Cep Residencial'))

        if (getattr(target_record, 'Profissão', '') == '' or getattr(target_record, 'Profissão', '') == None) and isinstance(getattr(duplicate_record, 'Profissão'), str):
            setattr(target_record, 'Profissão', getattr(duplicate_record, 'Profissão'))
        
        if (getattr(target_record, 'Referências', '') == '' or getattr(target_record, 'Referências', '') == None) and isinstance(getattr(duplicate_record, 'Referências'), str):
            setattr(target_record, 'Referências', getattr(duplicate_record, 'Referências'))
        
        if (getattr(target_record, 'Id do Convênio', '') == '' or getattr(target_record, 'Id do Convênio', '') == None) and isinstance(getattr(duplicate_record, 'Id do Convênio'), int):
            setattr(target_record, 'Id do Convênio', getattr(duplicate_record, 'Id do Convênio'))

        if (getattr(target_record, 'Mãe', '') == '' or getattr(target_record, 'Mãe', '') == None) and isinstance(getattr(duplicate_record, 'Mãe'), str):
            setattr(target_record, 'Mãe', getattr(duplicate_record, 'Mãe'))
        
        if (getattr(target_record, 'Pai', '') == '' or getattr(target_record, 'Pai', '') == None) and isinstance(getattr(duplicate_record, 'Pai'), str):
            setattr(target_record, 'Pai', getattr(duplicate_record, 'Pai'))

        if (getattr(target_record, 'Como conheceu', '') == '' or getattr(target_record, 'Como conheceu', '') == None) and isinstance(getattr(duplicate_record, 'Como conheceu'), str):
            setattr(target_record, 'Como conheceu', getattr(duplicate_record, 'Como conheceu'))
        
        if (getattr(target_record, 'Indicado por', '') == '' or getattr(target_record, 'Indicado por', '') == None) and isinstance(getattr(duplicate_record, 'Indicado por'), str):
            setattr(target_record, 'Indicado por', getattr(duplicate_record, 'Indicado por'))
        
        if (getattr(target_record, 'Contato', '') == '' or getattr(target_record, 'Contato', '') == None) and isinstance(getattr(duplicate_record, 'Contato'), str):
            setattr(target_record, 'Contato', getattr(duplicate_record, 'Contato'))
        
        if (getattr(target_record, 'Tags', '') == '' or getattr(target_record, 'Tags', '') == None) and isinstance(getattr(duplicate_record, 'Tags'), str):
            setattr(target_record, 'Tags', getattr(duplicate_record, 'Tags'))
        
        if (getattr(target_record, 'Exclui_Mkt', '') == '' or getattr(target_record, 'Exclui_Mkt', '') == None) and isinstance(getattr(duplicate_record, 'Exclui_Mkt'), str):
            setattr(target_record, 'Exclui_Mkt', getattr(duplicate_record, 'Exclui_Mkt'))
        
        if (getattr(target_record, 'Nome Social', '') == '' or getattr(target_record, 'Nome Social', '') == None) and isinstance(getattr(duplicate_record, 'Nome Social'), str):
            setattr(target_record, 'Nome Social', getattr(duplicate_record, 'Nome Social'))
        
        if (getattr(target_record, 'NumeroCNS', '') == '' or getattr(target_record, 'NumeroCNS', '') == None) and isinstance(getattr(duplicate_record, 'NumeroCNS'), str):
            setattr(target_record, 'NumeroCNS', getattr(duplicate_record, 'NumeroCNS'))
            
        if (getattr(target_record, 'Tipo', '') == '' or getattr(target_record, 'Tipo', '') == None) and isinstance(getattr(duplicate_record, 'Tipo'), str):
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
    df['Nome'] = df['Nome'].str.upper()
    df['Nome'] = df['Nome'].str.replace(' ', '', regex=False)

    duplicates = df[df.duplicated(subset=['Nome'], keep=False)]

    unified_df = duplicates.groupby('Nome').apply(unify_duplicates)
except Exception as e:
    print(f"Erro ao ler os dados do banco de dados: {e}")
    exit()

log_data = []
log_sql_data = []

for index, row in unified_df.iterrows():
    original_id = row['Id do Cliente']
    nome = row['Nome']

    try:

        duplicate_records = session.query(Contatos).filter(
            func.upper(func.replace(Contatos.Nome,' ', '')) == nome.replace(' ', '').upper()
        ).all()
    
    except Exception as e: 
        print(f"Erro ao buscar registros duplicados: {e}")
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
            'Timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        try:
            session.delete(record)
        except Exception as e:
            print(f"Erro ao deletar registro: {e}")

    session.commit()

session.commit()

create_log(log_sql_data, log_path, "log_SQL_queries_unificacao.xlsx")
create_log(log_data, log_path, "log_contatos_unificacao.xlsx")

print("Unificação de registros concluída!")
print(f"Log SQL e Log Contatos Unificação salvo em: {log_path}")

session.close()