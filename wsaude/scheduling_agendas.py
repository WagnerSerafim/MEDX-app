import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import pandas as pd
import urllib

def get_valid_date(date_str):
    
    if pd.isna(date_str) or date_str in ["", "0000-00-00"]:
        return "01/01/1900"
    
    try:
        date_obj = datetime.strptime(str(date_str), "%d-%m-%Y")
        if 1900 <= date_obj.year <= 2100:
            return date_obj.strftime("%d/%m/%Y")
        else:
            return "01/01/1900"
    except ValueError:
        return "01/01/1900"


sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase= input("Informe o DATABASE: ")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine) 

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Agenda = Base.classes.Agenda

excel_file = input("Informe a pasta do arquivo agendas.xlsx: ").strip()                     
log_folder = os.path.dirname(excel_file)

df = pd.read_excel(excel_file)
df = df.fillna(value="")

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
repeated_ids_count = 0

for index, row in df.iterrows():

    if pd.isna(row["Nº Interno Agenda"]) or row["Nº Interno Agenda"] == "":
        continue

    elif pd.isna(row["Data"]) or row["Data"] == "":
        continue

    elif pd.isna(row["Hora Marcada"]) or row["Hora Marcada"] == "":
        continue

    existing_schedule = session.query(Agenda).filter(getattr(Agenda, "Id do Agendamento") == row["Nº Interno Agenda"]).first()

    if existing_schedule:
        repeated_ids_count += 1
        continue

    description = row["Paciente"]
    
    start_date = get_valid_date(row["Data"])

    # Concatenando data e hora para criar a string completa
    start_time_str = f"{start_date} {row['Hora Marcada']}"
    
    # Convertendo a string para datetime
    start_time = datetime.strptime(start_time_str, "%d/%m/%Y %H:%M")
    
    # Adicionando 30 minutos ao início
    end_time = start_time + timedelta(minutes=30)

    # Criando o novo agendamento
    new_schedulling = Agenda(
        Descrição=description,
        Início=start_time,
        Final=end_time,
        Status=1,
    )

    setattr(new_schedulling, "Id do Agendamento", row["Nº Interno Agenda"])
    setattr(new_schedulling, "Vinculado a", row["Nº Interno Paciente"])
    setattr(new_schedulling, "Id do Usuário", 1)
    
    log_data.append({
        "Id do Agendamento": row["Nº Interno Agenda"],
        "Vinculado a": row["Nº Interno Paciente"],
        "Id do Usuário": 1,
        "Início": start_time,
        "Final": end_time,
        "Descrição": description,
        "Status" : 1
    })

    session.add(new_schedulling)

session.commit()

print(f"Novos agendamentos inseridos com sucesso! {len(df) - repeated_ids_count} registros inseridos.")
print(f"{repeated_ids_count} registros com ID repetido foram ignorados.")

session.close()

log_df = pd.DataFrame(log_data)
log_file_path = os.path.join(log_folder, "log_schedulling_agendas.xlsx")
log_df.to_excel(log_file_path, index=False)
