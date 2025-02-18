from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd

# Configuração do banco de dados
DATABASE_URL = "mssql+pyodbc://Medizin_32373:658$JQxn@medxserver.database.windows.net:1433/MEDX31?driver=ODBC+Driver+17+for+SQL+Server"

# Criar engine
engine = create_engine(DATABASE_URL)

# Habilitar mapeamento automático (reflection)
Base = automap_base()
Base.prepare(engine, reflect=True)  # Descobrir tabelas automaticamente

# Criar sessão
SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

# Acessar a classe correspondente à tabela "Contatos"
HistoricoClientes = getattr(Base.classes, "Histórico_de_Clientes")

df = pd.read_excel("C:\Users\WJSur\Downloads\FERNANDA_FERNANDES.xlsx")
print(df.head())
# Criar um novo contato
novo_contato = HistoricoClientes(
    Nome="Matheus Ferro",
    Sexo="M",
    Nascimento=datetime.strptime("27/01/1988", "%d/%m/%Y")  # Converter string para data
)

# Adicionar à sessão e salvar no banco
session.add(novo_contato)
session.commit()

print("Novo contato inserido com sucesso!")

# Fechar sessão
session.close()
