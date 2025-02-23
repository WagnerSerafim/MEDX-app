from bs4 import BeautifulSoup
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
from striprtf.striprtf import rtf_to_text
import urllib
import os

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

#DATABASE_URL = "mssql+pyodbc://Medizin_32373:658$JQxn@medxserver.database.windows.net:1433/MEDX31?driver=ODBC+Driver+17+for+SQL+Server"    #DEBUG
DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine)

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

HistoricoClientes = getattr(Base.classes, "Histórico de Clientes")

historico_excel = input("Arquivo Excel Seguimentos Gerais Normais: ").strip()  
prontuario_excel = input("Arquivo Excel Seguimentos Gerais: ").strip()  
log_folder = input("Informe a pasta onde deseja salvar o arquivo de log: ").strip()

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

df_historic = pd.read_excel(historico_excel)  
df_medical_record = pd.read_excel(prontuario_excel)  

mapping_id_patient = pd.Series(df_medical_record["PacienteID"].values, index=df_medical_record["SeguimentoGeralNormalID"]).to_dict()

log_data = []

for index, row in df_medical_record.iterrows():
    if row["SeguimentoGeralNormalID"] == 0:
        continue

    historic_data = df_historic[df_historic["SeguimentoGeralNormalID"] == row["SeguimentoGeralNormalID"]]

    if historic_data.empty:
        formatted_historic = "Sem histórico disponível"
    else:
        formatted_historic = get_historic(historic_data.iloc[0])
    
    novo_historico = HistoricoClientes( 
        Histórico=formatted_historic, 
        Data=row["DataConsulta"]  
    )    
    setattr(novo_historico, "Id do Cliente", row["PacienteID"]) 
    setattr(novo_historico, "Id do Usuário", 0)
    setattr(novo_historico, "Id do Histórico", (0 - row["SeguimentoGeralID"] ))

    session.add(novo_historico)

    log_data.append({
        "Id Cliente": row["PacienteID"],
        "Id Histórico": 0 - row["SeguimentoGeralID"],
        "Data": row["DataConsulta"],
        "Histórico": formatted_historic
    })

session.commit()
session.close()

log_df = pd.DataFrame(log_data)
log_file_path = os.path.join(log_folder, "historic_seguimentos_log.xlsx")
log_df.to_excel(log_file_path, index=False)

print(f"✅ Novos históricos inseridos com sucesso! O arquivo de log foi salvo em: {log_file_path}")
