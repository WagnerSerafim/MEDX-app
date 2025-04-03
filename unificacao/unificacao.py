import datetime
import pandas as pd
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
import urllib

sql_log = []

def is_valid_rg(rg):
    """ Verifica se o RG tem uma quantidade aceitável para ser válido. """
    return isinstance(rg, str) and len(rg) >= 8 and len(rg) <= 14

def is_valid_celular(celular):
    """ Verifica se o celular tem uma quantidade aceitável para ser válido. """
    return isinstance(celular, str) and len(celular) >= 9 and celular.isdigit()

def is_valid_data(data):
    """ Verifica se a data de nascimento é válida e no formato correto. """
    try:
        datetime.strptime(str(data), "%d/%m/%Y")
        return True
    except:
        return False

def update_related_records(record, table, name_column, target_id):
    items = session.query(table).filter(getattr(table, name_column) == getattr(record, 'Id do Cliente')).all()
    for item in items:
        setattr(item, name_column, target_id)

def unify_duplicates(group):
    group = group.iloc[0]
    return group

def merge_record_data(target_record, duplicate_record):
    """ Função para preencher dados ausentes no registro principal com dados dos registros duplicados """
    if (getattr(target_record, 'Celular', '') == '' or getattr(target_record, 'Celular', '') == None) and is_valid_celular(getattr(duplicate_record, 'Celular')):
        setattr(target_record, 'Celular', getattr(duplicate_record, 'Celular'))

    if (getattr(target_record, 'RG', '') == '' or getattr(target_record, 'RG', '') == None) and is_valid_rg(getattr(duplicate_record, 'RG')): 
        setattr(target_record, 'RG', getattr(duplicate_record, 'RG'))
    
    if (getattr(target_record, 'CPF/CGC', '') == '' or getattr(target_record, 'CPF/CGC', '') == None) and isinstance(getattr(duplicate_record, 'CPF/CGC', ''), str) and len(getattr(duplicate_record, 'CPF/CGC', '')) == 11:
        setattr(target_record, 'CPF/CGC', getattr(duplicate_record, 'CPF/CGC', ''))

    if (getattr(target_record, 'Nascimento', '') == '' or getattr(target_record, 'Nascimento', '') == None) and is_valid_data(getattr(duplicate_record, 'Nascimento')):
        setattr(target_record, 'Nascimento', getattr(duplicate_record, 'Nascimento'))
    
    if (getattr(target_record, 'Email', '') == '' or getattr(target_record, 'Email', '') == None) and isinstance(getattr(duplicate_record, 'Email'), str) and '@' in getattr(duplicate_record, 'Email'):
        setattr(target_record, 'Email', getattr(duplicate_record, 'Email'))

    if (getattr(target_record, 'Endereço Residencial', '') == '' or getattr(target_record, 'Endereço Residencial', '') == None) and isinstance(getattr(duplicate_record, 'Endereço Residencial'), str):
        setattr(target_record, 'Endereço Residencial', getattr(duplicate_record, 'Endereço Residencial'))

    if isinstance(getattr(duplicate_record, 'Observações'), str) and len(getattr(duplicate_record, 'Observações')) > 0:
        obs = getattr(target_record, 'Observações')
        obs += f"      {getattr(duplicate_record, 'Observações')}"
        setattr(target_record, 'Observações', obs)

    if (getattr(target_record, 'Telefone Residencial', '') == '' or getattr(target_record, 'Telefone Residencial', '') == None) and isinstance(getattr(duplicate_record, 'Telefone Residencial'), str):
        setattr(target_record, 'Telefone Residencial', getattr(duplicate_record, 'Telefone Residencial'))

    if (getattr(target_record, 'Sexo', '') == '' or getattr(target_record, 'Sexo', '') == None) and isinstance(getattr(duplicate_record, 'Sexo'), str):
        setattr(target_record, 'Sexo', getattr(duplicate_record, 'Sexo'))

    if (getattr(target_record, 'Id da Assinatura', '') == '' or getattr(target_record, 'Id da Assinatura', '') == None) and isinstance(getattr(duplicate_record, 'Id da Assinatura'), str):
        setattr(target_record, 'Id da Assinatura', getattr(duplicate_record, 'Id da Assinatura'))

def log_sql_query(conn, cursor, statement, parameters, context, executemany):
    """ Função para registrar os comandos SQL executados no log """
    sql_log.append((statement, parameters))

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL, echo=True)
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

df = pd.read_sql(session.query(Contatos).statement, session.bind)

duplicates = df[df.duplicated(subset=['Nome'], keep=False)]

unified_df = duplicates.groupby('Nome').apply(unify_duplicates)

for index, row in unified_df.iterrows():
    original_id = row['Id do Cliente']
    nome = row['Nome']

    duplicate_records = session.query(Contatos).filter(Contatos.Nome == nome).all()

    target_id = getattr(duplicate_records[0], 'Id do Cliente')

    for record in duplicate_records[1:]:
        merge_record_data(duplicate_records[0], record)

        update_related_records(record, HistoricoClientes, 'Id do Cliente', target_id)
        update_related_records(record, Agenda, 'Vinculado a', target_id)
        update_related_records(record, Antiaging, 'Id do Cliente', target_id)
        update_related_records(record, AntiagingAlimentos, 'Id do Cliente', target_id)
        update_related_records(record, Atendimentos, 'Id do cliente', target_id)
        update_related_records(record, Histórico_Detalhes, 'Id do Cliente', target_id)
        update_related_records(record, Biospace, 'Id do Cliente', target_id)
        update_related_records(record, GuiaMedicacao, 'Id do Cliente', target_id)
        update_related_records(record, LogdeTelefonemas, 'Id do cliente', target_id)
        update_related_records(record, EsteticaFacial, 'Id do Cliente', target_id)

        session.commit()

    for record in duplicate_records[1:]:
        session.delete(record)

    session.commit()

session.commit()

log_df = pd.DataFrame(sql_log, columns=["SQL Query", "Parameters"])
log_file_path = "sql_log.xlsx"
log_df.to_excel(log_file_path, index=False)

print("Unificação de registros concluída!")
print(f"Log SQL salvo em: {log_file_path}")

session.close()