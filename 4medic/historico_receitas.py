from bs4 import BeautifulSoup
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd


def clean_html(html):
    if pd.isna(html):
        return ""
    
    soup = BeautifulSoup(html, "html.parser")

    for button in soup.find_all("button"):
        button.decompose() 
    
    for tag in soup.find_all(attrs={"contenteditable": True}):
        del tag["contenteditable"]

    return str(soup)


sid = input("Informe o SoftwareID: ")
password = input("Informe a senha: ")
dbase= input("Informe o DATABASE: ")

#DATABASE_URL = "mssql+pyodbc://Medizin_32373:658$JQxn@medxserver.database.windows.net:1433/MEDX31?driver=ODBC+Driver+17+for+SQL+Server"    //DEBUG
DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server"

engine = create_engine(DATABASE_URL)


Base = automap_base()
Base.prepare(engine, reflect=True)


SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()


HistoricoClientes = getattr(Base.classes, "Histórico de Clientes")

df = pd.read_excel(r"E:\Migracoes\4Medic\Schema_32292\backup_651340\backup_651340\csv\receitas.xlsx")

for index,row in df.iterrows():
    html_clean = clean_html(row["HTML_RECEITA"])
    novo_historico = HistoricoClientes(
        Histórico=html_clean,
        Data=row["DATA_RECEITA"]
    )
    setattr(novo_historico, "Id do Cliente", row["ID_PACIENTE"])
    setattr(novo_historico, "Id do Usuário", 0)
    setattr(novo_historico, "Id do Histórico", row["ID_RECEITA"])
    
    session.add(novo_historico)

session.commit()

print("Novos históricos inseridos com sucesso!")


session.close()
