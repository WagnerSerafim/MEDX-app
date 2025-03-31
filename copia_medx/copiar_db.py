from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
import urllib

# Conexão com o banco de origem (banco 1)
sid_source = input("Informe o SoftwareID do banco de origem: ")
password_source = urllib.parse.quote_plus(input("Informe a senha do banco de origem: "))
dbase_source = input("Informe o DATABASE do banco de origem: ")

DATABASE_URL_SOURCE = f"mssql+pyodbc://Medizin_{sid_source}:{password_source}@medxserver.database.windows.net:1433/{dbase_source}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

# Conexão com o banco de destino (banco 2)
sid_dest = input("Informe o SoftwareID do banco de destino: ")
password_dest = urllib.parse.quote_plus(input("Informe a senha do banco de destino: "))
dbase_dest = input("Informe o DATABASE do banco de destino: ")

DATABASE_URL_DEST = f"mssql+pyodbc://Medizin_{sid_dest}:{password_dest}@medxserver.database.windows.net:1433/{dbase_dest}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

# Criar conexões e sessões para os dois bancos
engine_source = create_engine(DATABASE_URL_SOURCE)
engine_dest = create_engine(DATABASE_URL_DEST)

Base = automap_base()
Base.prepare(autoload_with=engine_source)  # Preparar para o banco de origem

SessionLocal_source = sessionmaker(bind=engine_source)
SessionLocal_dest = sessionmaker(bind=engine_dest)

session_source = SessionLocal_source()
session_dest = SessionLocal_dest()

# Obter as classes das tabelas
Contatos = Base.classes.Contatos
HistoricoClientes = Base.classes['Histórico de Clientes']
Agenda = Base.classes.Agenda
Atendimentos = Base.classes.Atendimentos

# Consultar dados do banco de origem (banco 1)
contatos_data = session_source.query(Contatos).all()
historicos_data = session_source.query(HistoricoClientes).all()
agenda_data = session_source.query(Agenda).all()
atendimentos_data = session_source.query(Atendimentos).all()

# Inserir os dados no banco de destino (banco 2)
for contato in contatos_data:
    # Criar um novo registro de Contato para o banco de destino
    new_contato = Contatos(
        **{column.name: getattr(contato, column.name) for column in Contatos.__table__.columns}
    )
    session_dest.add(new_contato)

for historico in historicos_data:
    # Criar um novo registro de Histórico para o banco de destino
    new_historico = HistoricoClientes(
        **{column.name: getattr(historico, column.name) for column in HistoricoClientes.__table__.columns}
    )
    session_dest.add(new_historico)

for agenda in agenda_data:
    # Criar um novo registro de Agenda para o banco de destino
    new_agenda = Agenda(
        **{column.name: getattr(agenda, column.name) for column in Agenda.__table__.columns}
    )
    session_dest.add(new_agenda)

for atendimento in atendimentos_data:
    # Criar um novo registro de Atendimentos para o banco de destino
    new_atendimento = Atendimentos(
        **{column.name: getattr(atendimento, column.name) for column in Atendimentos.__table__.columns}
    )
    session_dest.add(new_atendimento)

# Commit para salvar as inserções
session_dest.commit()

print("Dados copiados com sucesso do banco 1 para o banco 2!")

# Fechar as sessões
session_source.close()
session_dest.close()
