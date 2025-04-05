import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import urllib

def get_valid_date(date_str):
    
    if date_str in ["", "0000-00-00", None]:
        return ""
    
    try:
        date_obj = datetime.strptime(str(date_str), "%Y-%m-%d %H:%M:%S")
        if 1900 <= date_obj.year <= 2100:
            return date_obj.strftime("%Y/%m/%d %H:%M:%S")
        else:
            return ""
    except ValueError:
        return ""


sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase= input("Informe o DATABASE: ")
excel_file = input("Informe a pasta do arquivo dados.xlsx: ").strip()   

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

print("Conectando no Banco de dados...")

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine) 

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

Agenda = Base.classes.Agenda
Contatos = Base.classes.Contatos

print("Sucesso! Começando migração de agendamentos...")
                  
log_folder = os.path.dirname(excel_file)

df = pd.read_excel(excel_file, sheet_name="agenda")
df = df.fillna(value="")

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

log_data = []
cont = 0 
for index, row in df.iterrows():

    existing_patient = session.query(Contatos).filter(getattr(Contatos, "Referências")==row["id_paciente"]).first()
    if existing_patient:
        id_patient = getattr(existing_patient, "Id do Cliente")
    else:
        print(f"Paciente com ID {row['id_paciente']} não encontrado.")
        continue
    

    print(row['inicio'])
    beginning = get_valid_date(row['inicio'])
    print(f"Beggining: {beginning}")
    ending = get_valid_date(row['fim'])

    if (beginning == "" or beginning == None) or (ending == "" or ending == None):
        continue


    description = f"{row['paciente']} {row['procedimento']} {row['cirurgia']}"
    status = 1
    id_user = row['profissional'] #Não tem ID o profissional responsável pela agenda



    new_schedulling = Agenda(
        Descrição=description,
        Início=beginning,
        Final=ending,
        Status=1,
    )

    # setattr(new_schedulling, "Id do Agendamento", id_schedule)
    setattr(new_schedulling, "Vinculado a", id_patient)
    setattr(new_schedulling, "Id do Usuário", id_user)
    
    log_data.append({
        # "Id do Agendamento": id_schedule,
        "Vinculado a": id_patient,
        "Id do Usuário": id_user,
        "Início": beginning,
        "Final": ending,
        "Descrição": description,
        "Status" : 1
    })

    session.add(new_schedulling)
    cont+=1
    if cont % 1000 == 0:
        session.commit()
        print(f"{cont} agendamentos comitados com sucesso!")

session.commit()

print(f"{cont} novos agendamentos foram inseridos com sucesso!")

session.close()

log_df = pd.DataFrame(log_data)
log_file_path = os.path.join(log_folder, "log_scheduling_agenda.xlsx")
log_df.to_excel(log_file_path, index=False)
