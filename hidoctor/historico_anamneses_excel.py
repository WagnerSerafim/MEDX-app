from bs4 import BeautifulSoup
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
from striprtf.striprtf import rtf_to_text
import urllib


sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase= input("Informe o DATABASE: ")

#DATABASE_URL = "mssql+pyodbc://Medizin_32373:658$JQxn@medxserver.database.windows.net:1433/MEDX31?driver=ODBC+Driver+17+for+SQL+Server"    #DEBUG
DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)


Base = automap_base()
Base.prepare(autoload_with=engine)

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()


HistoricoClientes = getattr(Base.classes, "Histórico de Clientes")

excel_name = input("Arquivo Excel a ser lido: ").strip()
df = pd.read_excel(excel_name)

for index,row in df.iterrows():
    
    novo_historico = HistoricoClientes(
        Histórico=rtf_to_text(row["Texto_Anamnese"]),
        Data=row["Anam_Date"]
    )
    setattr(novo_historico, "Id do Cliente", row["ID_Pac"])
    setattr(novo_historico, "Id do Usuário", 0)
    setattr(novo_historico, "Id do Histórico", (0-row["ID_Anam"]))
    
    session.add(novo_historico)

session.commit()

print("Novos históricos inseridos com sucesso!")


session.close()
