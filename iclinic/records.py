import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import urllib


def truncate_value(value, max_length):
    """Se o valor for maior que max_length, ele será truncado"""
    if pd.isna(value):
        return None
    return str(value)[:max_length] 

def get_record(json):
    """Extraindo o histórico do JSON"""
    record = ""

    if json["block"]:
        record = json["block"][0]["tab"] + "<br><br>"
        for i in json["block"]:
            record +=  f"{json['block'][i]['name']}: "
            if isinstance(json["block"][i]["value"], list):
                for j in json["block"][i]["value"]:
                    values += f"{json["block"][i]["value"][j]}, "
                values = values[:-2]
                record += f"{values} <br><br>"

            else:
                if json["block"][i]["unity"]:
                    record += f"{json["block"][i]["value"]} {json["block"][i]["unity"]} <br>"
                else:
                    record += f"{json["block"][i]["value"]} <br>"
    
    if json["aditional"]:
        if record != "":
            record += "<br><br>"
        
        record += "Texto(s) adicional(ais): <br>"
        for i in json["aditional"]:
            record += f"- {json["aditional"][i]["aditional_text"]}"
        
    return record


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

log_folder = input("Informe a pasta onde deseja salvar o arquivo de log: ").strip()

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

records_csv = input("Arquivo CSV de Histórico: ").strip()
df = pd.read_csv(records_csv, sep=None, engine='python')

df.replace("json::","",inplace=True)

log_data = []

for index, row in df.iterrows():

    if(pd.isna(row["start_time"] or row["start_time"] == "")):
        hour = datetime.strptime("00:00", "%H:%M")
    else:
        hour = row["start_time"]

    if (pd.isna(row["date"]) or row["date"] == ""): 
        date = datetime.strptime("01/01/1900 00:00", "%d/%m/%Y %H:%M")
    else:
        date = f"{row["date"]} {hour}"

    
    if pd.isna(row["eventblock_pack"]):
        record = get_record(row["eventblock_pack"])

    new_record = HistoricoClientes(
        Histórico=record,
        Data=date,
    )

    setattr(new_record, "Id do Cliente", row["patient_id"])
    setattr(new_record, "Id do Histórico", row["pk"])
    setattr(new_record, "Id do Usuário", 0)
    
    log_data.append({
        "Id do Histórico": row["pk"],
        "Id do Cliente": row["patient_id"],
        "Data": date,
        "Histórico": sex,
        "Id do Usuário": 0,
        })

    session.add(new_record)

session.commit()

print("Novos contatos inseridos com sucesso!")

session.close()

log_df = pd.DataFrame(log_data)
log_file_path = os.path.join(log_folder, "patients_log.xlsx")
log_df.to_excel(log_file_path, index=False)