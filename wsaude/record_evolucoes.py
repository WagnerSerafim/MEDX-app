import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import urllib

def is_valid_date(date_str):
    """ Verifica se a data é válida e diferente de '0000-00-00' """
    if pd.isna(date_str) or date_str in ["", "0000-00-00"]:
        return False
    try:
        date_obj = datetime.strptime(str(date_str), "%d-%m-%Y") 
        return 1900 <= date_obj.year <= 2100  
    except ValueError:
        return False 

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine)

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

HistoricoClientes = getattr(Base.classes, "Histórico de Clientes")

excel_file = input("Informe o caminho do arquivo evolucoes.py: ")
log_folder = os.path.dirname(excel_file)

df = pd.read_excel(excel_file)
df = df.fillna(value="")

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
cont = -1
cont2 = 0

limit_date = datetime.strptime("04/10/2024", "%d/%m/%Y")

for index, row in df.iterrows():
    
    record = row["Texto"]
    if record == "":
        continue

    # Certifique-se de pegar apenas os primeiros 16 caracteres da data e hora
    raw_date_str = str(row["Inserido em"][:16])  # Exemplo: "01/02/2018 13:02"

    # Imprimir o valor extraído para verificação
    print(f"RAW DATE STRING: {raw_date_str}")

    try:
        # Verificar se a data está no formato esperado (DD/MM/YYYY HH:MM)
        date = datetime.strptime(raw_date_str, "%d/%m/%Y %H:%M")
    except Exception as e:
        print(f"Erro ao processar a data {raw_date_str}: {e}")
        date = datetime.strptime("01/01/1900 00:00", "%d/%m/%Y %H:%M")  # Valor padrão se ocorrer erro

    print(f"DATA: {date} & row: {raw_date_str}")  # Mostra a data convertida e a string original

    if date <= limit_date:
        cont2 += 1
        continue  

    new_record = HistoricoClientes(
        Histórico=record,
        Data=date,
    )

    setattr(new_record, "Id do Cliente", row["Nº Interno Paciente"])
    setattr(new_record, "Id do Histórico", cont)
    setattr(new_record, "Id do Usuário", 0)

    log_data.append({
        "Id do Histórico": cont,
        "Id do Cliente": row["Nº Interno Paciente"],
        "Data": date,
        "Histórico": record,
        "Id do Usuário": 0,
    })
    cont -= 1
    session.add(new_record)

session.commit()

print("Novos Históricos inseridos com sucesso!")
print(f"CONT: {cont2}")

session.close()

log_df = pd.DataFrame(log_data)
log_file_path = os.path.join(log_folder, "record_evolucoes_log.xlsx")
log_df.to_excel(log_file_path, index=False)
