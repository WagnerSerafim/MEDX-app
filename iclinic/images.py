import json
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import urllib


def get_images(json):

    images = []
    if isinstance(json['image'], list):
        for item in json['image']:
            if 'image' in item:
                image = item['image']
                images.append(image)

    return images

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase= input("Informe o DATABASE: ")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine) 

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

HistoricoClientes = getattr(Base.classes, "Histórico de Clientes")

images_csv = input("Arquivo CSV de Histórico: ").strip()
df = pd.read_csv(images_csv, sep=None, engine='python')
df["eventblock_pack"] = df["eventblock_pack"].astype(str).str.replace(r'^json::', '', regex=True)

log_folder = input("Informe a pasta onde deseja salvar o arquivo de log: ").strip()
if not os.path.exists(log_folder):
    os.makedirs(log_folder)
 
log_data = []
notInserted = []
count = 0
for index, row in df.iterrows():
    if(pd.isna(row["start_time"] or row["start_time"] == "")):
        hour = datetime.strptime("00:00", "%H:%M")
    else:
        hour = row["start_time"]

    if (pd.isna(row["date"]) or row["date"] == ""): 
        date = datetime.strptime("01/01/1900 00:00", "%d/%m/%Y %H:%M")
    else:
        date = f"{row["date"]} {hour}"

    if not pd.isna(row["eventblock_pack"]) and isinstance(row["eventblock_pack"], str):
        try:
            json_data = json.loads(row["eventblock_pack"])
            if 'image' in json_data:
                images = get_images(json_data)
            else:
                continue
        except json.JSONDecodeError:
            print(f"Erro ao decodificar JSON na linha {index + 2}. Pulando...")
            continue
    else:
        continue

    for image in images:
        count += 1
        new_image = HistoricoClientes(
            Histórico=image,
            Data=date,
            Classe=image
        )

        setattr(new_image, "Id do Cliente", row["patient_id"])
        setattr(new_image, "Id do Histórico", 0 - count)
        setattr(new_image, "Id do Usuário", 0)

        log_data.append({
            "Id do Histórico": row["pk"],
            "Id do Cliente": row["patient_id"],
            "Data": date,
            "Histórico": image,
            "Id do Usuário": 0,
            })
        
        session.add(new_image)
    
session.commit()

print("Históricos inseridos com sucesso!")

session.close()

log_df = pd.DataFrame(log_data)
log_file_path = os.path.join(log_folder, "images_log.xlsx")
log_df.to_excel(log_file_path, index=False)