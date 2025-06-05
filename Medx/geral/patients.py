from datetime import datetime
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, select
import urllib
from utils.utils import exists, create_log, clean_caracters, truncate_value

def log_denied(patient, motivo):
    row_dict = {col.name: getattr(patient, col.name) for col in ContatosSrc.__table__.columns}
    row_dict["Motivo"] = motivo
    row_dict["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_not_inserted.append(row_dict)

print("Por favor, informe os dados do banco de origem.\n")

sid_src = input("Informe o SoftwareID do banco de origem: ")
password_src = urllib.parse.quote_plus(input("Informe a senha do banco de origem: "))
dbase_src = input("Informe o DATABASE do banco de origem: ")

print("Agora informe os dados do banco de destino.\n")

sid_dest = input("Informe o SoftwareID do banco de destino: ")
password_dest = urllib.parse.quote_plus(input("Informe a senha do banco de destino: "))
dbase_dest = input("Informe o DATABASE do banco de destino: ")

path_file = input("Informe o caminho da pasta onde ficarão os logs: ")

print("Conectando aos Bancos de Dados...")

DATABASE_SRC = f"mssql+pyodbc://Medizin_{sid_src}:{password_src}@medxserver.database.windows.net:1433/{dbase_src}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"
DATABASE_URL = f"mssql+pyodbc://Medizin_{sid_dest}:{password_dest}@medxserver.database.windows.net:1433/{dbase_dest}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine_src = create_engine(DATABASE_SRC)
engine_dest = create_engine(DATABASE_URL)

BaseSrc = automap_base()
BaseSrc.prepare(autoload_with=engine_src)
ContatosSrc = getattr(BaseSrc.classes, "Contatos")

BaseDest = automap_base()
BaseDest.prepare(autoload_with=engine_dest)
ContatosDest = getattr(BaseDest.classes, "Contatos")

SessionSrc = sessionmaker(bind=engine_src)
SessionDest = sessionmaker(bind=engine_dest)

session_src = SessionSrc()
session_dest = SessionDest()

print("Sucesso! Inicializando migração de Contatos...")

log_inserted = []
log_not_inserted = []
valid_data = []

batch_size = 1000
last_id = -99999999999

log_folder = path_file

if not os.path.exists(log_folder):
    os.makedirs(log_folder)

print("Iniciando leitura e validação em blocos...")

while True:
    stmt = (
        select(ContatosSrc)
        .where(getattr(ContatosSrc, "Id do Cliente") > last_id)
        .order_by(getattr(ContatosSrc, "Id do Cliente"))
        .limit(batch_size)
    )
    patients = session_src.execute(stmt).scalars().all()

    if not patients:
        break

    for patient in patients:
        try:
            id_patient = getattr(patient, "Id do Cliente", None)
            last_id = max(last_id, id_patient)

            if exists(session_dest, id_patient, "Id do Cliente", ContatosDest):
                log_denied(patient, "Id do Cliente já existe no banco de destino")
                continue
            
            patient_data = {
                "Nome": patient.Nome,
                "Nascimento": patient.Nascimento,
                "Sexo": patient.Sexo,
                "Celular": patient.Celular,
                "Email": patient.Email,
                "Id do Cliente": id_patient,
                "CPF/CGC": getattr(patient, "CPF/CGC", ''),
                "Cep Residencial": getattr(patient, "Cep Residencial", ''),
                "Endereço Residencial": getattr(patient, "Endereço Residencial", ''),
                "Endereço Comercial": getattr(patient, "Endereço Comercial", ''),
                "Bairro Residencial": getattr(patient, "Bairro Residencial", ''),
                "Cidade Residencial": getattr(patient, "Cidade Residencial", ''),
                "Estado Residencial": getattr(patient, "Estado Residencial", ''),
                "Telefone Residencial": getattr(patient, "Telefone Residencial", ''),
                "Telefone Residencial 1": getattr(patient, "Telefone Residencial 1", ''),
                "Telefone Comercial": getattr(patient, "Telefone Comercial", ''),
                "Profissão": getattr(patient, "Profissão", ''),
                "Pai": getattr(patient, "Pai", ''),
                "Mãe": getattr(patient, "Mãe", ''),
                "RG": getattr(patient, "RG", ''),
                "Cidade Comercial": getattr(patient, "Cidade Comercial", ''),
                "Estado Comercial": getattr(patient, "Estado Comercial", ''),
                "Cep Comercial": getattr(patient, "Cep Comercial", ''),
                "País Residencial": getattr(patient, "País Residencial", ''),
                "País Comercial": getattr(patient, "País Comercial", ''),
                "Estado Civil": getattr(patient, "Estado Civil", ''),
                "Observações": getattr(patient, "Observações", ''),
                "Bairro Comercial": getattr(patient, "Bairro Comercial", ''),
                "PáginaWeb": getattr(patient, "Página Web", ''),
                "Referências": getattr(patient, "Referências", ''),
                "Filhos": getattr(patient, "Filhos", ''),
                "Empresa": getattr(patient, "Empresa", ''),
                "Tipo" : getattr(patient, "Tipo", ''),
                "Mala Direta": getattr(patient, "Mala Direta", ''),
                "Id do Convênio": getattr(patient, "Id do Convênio", ''),
                "VIP": getattr(patient, "VIP", ''),
                "Número da Matrícula": getattr(patient, "Número da Matrícula", ''),
                "Histórico Familiar IAM AVC antes 50 anos": getattr(patient, "Histórico Familiar IAM AVC antes 50 anos", ''),
                "Região": getattr(patient, "Região", ''),
                "Escolaridade": getattr(patient, "Escolaridade", ''),
                "Religião": getattr(patient, "Religião", ''),
                "Co-Morbidade": getattr(patient, "Co-Morbidade", ''),
                "Fadiga": getattr(patient, "Fadiga", ''),
                "Fumante": getattr(patient, "Fumante", ''),
                "LastEditDate": getattr(patient, "LastEditDate", None),
                "CreationDate": getattr(patient, "CreationDate", None),
                "Cônjuge": getattr(patient, "Cônjuge", ''),
                "Acompanhante": getattr(patient, "Acompanhante", ''),
                "Contato" : getattr(patient, "Contato", ''),
                "Exclui_Mkt": getattr(patient, "Exclui_Mkt", ''),
                "Tags" : getattr(patient, "Tags", ''),
                "NumeroCNS": getattr(patient, "NumeroCNS", ''),
                "Nome Social": getattr(patient, "Nome Social", ''),
                "FaceDescriptor": getattr(patient, "FaceDescriptor", ''),
                "Como Conheceu": getattr(patient, "Como Conheceu", ''),
                "Indicado por": getattr(patient, "Indicado por", ''),
            }

            valid_data.append(patient_data)

            log_inserted.append(patient_data)

        except Exception as e:
            log_denied(patient, f"Erro inesperado: {str(e)}")

print("Inserindo dados no banco de destino...")

try:
    with session_dest.begin():
        for i in range(0, len(valid_data), 100):
            batch = valid_data[i:i+100]
            session_dest.bulk_insert_mappings(ContatosDest, batch)
    print(f"{len(valid_data)} contatos inseridos com sucesso!")
except Exception as e:
    print("Erro na inserção. Transação revertida.")
    print(str(e))

session_src.close()
session_dest.close()

print("Migração concluída. Criando logs...")

create_log(log_inserted, log_folder, "log_inserted_patients.xlsx")
create_log(log_not_inserted, log_folder, "log_not_inserted_patients.xlsx")

print("Logs criados com sucesso!\n")

print(f"Total de contatos inseridos: {len(log_inserted)}")
print(f"Total de contatos não inseridos: {len(log_not_inserted)}")

