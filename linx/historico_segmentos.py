import glob
from bs4 import BeautifulSoup
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
from striprtf.striprtf import rtf_to_text
import urllib
import os
from utils.utils import create_log, is_valid_date, exists

def get_historic(row):
    historic_columns = {
        "QueixaPrincipal", "HistoriaMolestiaAtual", "HistoriaMorbidaPregressa", "AntecedenteMorbidoFamiliar", 
        "MedicamentoEmUso", "ExameFisico", "HistoriaMorbidaSocial", "AntecedentesObstetricos", "AntecedentesMamarios", 
        "AntecedentesContraceptivos", "ExameSegmentar", "Mama", "Vulva", "Especular", "Toque", "DescricaoCicloMenstrual"
    }

    filtered_data = row.dropna().filter(items=historic_columns)
    cleaned_data = {col: str(val).replace("_x000D_", "<br>").strip() for col, val in filtered_data.items()}
    return "<br>".join(f"{col}: {val}" for col, val in cleaned_data.items() if val)

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase= input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine)

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

HistoricoClientes = getattr(Base.classes, "Histórico de Clientes")

print("Sucesso! Inicializando migração de Históricos...")

historico_excel = glob.glob(f'{path_file}/SeguimentosGeraisNormais.xlsx')
prontuario_excel = glob.glob(f'{path_file}/SeguimentosGerais.xlsx')

df_historic = pd.read_excel(historico_excel[0])
df = df_historic.replace('None', '')

df_medical_record = pd.read_excel(prontuario_excel[0])
df_medical_record = df_medical_record.replace('None', '')

log_folder = path_file

log_data = []
inserted_cont=0
not_inserted_data = []
not_inserted_cont = 0

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

mapping_id_patient = pd.Series(df_medical_record["PacienteID"].values, index=df_medical_record["SeguimentoGeralNormalID"]).to_dict()

for index, row in df_medical_record.iterrows():
    
    existing_record = exists(session, 0 - row["SeguimentoGeralID"], "Id do Histórico", HistoricoClientes)
    if existing_record:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Id do histórico já existe no Banco de Dados'
        not_inserted_data.append(row_dict)
        continue

    if row["SeguimentoGeralNormalID"] == 0:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'SeguimentoGeralNormalID é 0'
        not_inserted_data.append(row_dict)
        continue

    historic_data = df_historic[df_historic["SeguimentoGeralNormalID"] == row["SeguimentoGeralNormalID"]]

    if historic_data.empty:
        not_inserted_cont += 1
        row_dict = row.to_dict()
        row_dict['Motivo'] = 'Não há histórico associado a este SeguimentoGeralNormalID'
        not_inserted_data.append(row_dict)
        continue
    else:
        formatted_historic = get_historic(historic_data.iloc[0])
    
    new_record = HistoricoClientes( 
        Histórico=formatted_historic, 
        Data=row["DataConsulta"]  
    )    
    setattr(new_record, "Id do Cliente", row["PacienteID"]) 
    setattr(new_record, "Id do Usuário", 0)
    setattr(new_record, "Id do Histórico", (0 - row["SeguimentoGeralID"] ))

    log_data.append({
        "Id Cliente": row["PacienteID"],
        "Id Histórico": 0 - row["SeguimentoGeralID"],
        "Data": row["DataConsulta"],
        "Histórico": formatted_historic,
        "Id do Usuário": 0,
    })
    session.add(new_record)
    inserted_cont+=1

    if inserted_cont % 100 == 0:
        session.commit()

session.commit()

print(f"{inserted_cont} novos históricos foram inseridos com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} históricos não foram inseridos, verifique o log para mais detalhes.")

session.close()

create_log(log_data, log_folder, "log_inserted_records_seguimentos.xlsx")
create_log(not_inserted_data, log_folder, "log_not_inserted_records_seguimentos.xlsx")