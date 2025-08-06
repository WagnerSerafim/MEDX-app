from datetime import datetime
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, select, text
import urllib
from utils.utils import exists, create_log, clean_caracters

log_inserted = []
log_not_inserted = []
batch_size = 500

def log_denied(patient, motivo):
    row_dict = {col.name: getattr(patient, col.name) for col in ContatosSrc.__table__.columns}
    row_dict["Motivo"] = motivo
    row_dict["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_not_inserted.append(row_dict)

def generate_valid_data(session_src, session_dest, ContatosSrc, ContatosDest, ids_clientes):
    last_id = -99999999999
    total_validated = 0

    while True:
        stmt = (
            select(ContatosSrc)
            .where(getattr(ContatosSrc, "Id do Cliente").in_(ids_clientes))
            .where(getattr(ContatosSrc, "Id do Cliente") > last_id)
            .order_by(getattr(ContatosSrc, "Id do Cliente"))
            .limit(batch_size)
        )
        patients = session_src.execute(stmt).scalars().all()

        if not patients:
            break

        for patient in patients:
            try:
                id_patient = getattr(patient, "Id do Cliente", '')
                last_id = max(last_id, id_patient)

                if exists(session_dest, id_patient, "Id do Cliente", ContatosDest):
                    log_denied(patient, "Id do Cliente já existe no banco de destino")
                    continue

                email = clean_caracters(getattr(patient, "Email", ''))
                observations = clean_caracters(getattr(patient, "Observações", ''))

                patient_data = {
                    "Nome": patient.Nome,
                    "Nascimento": patient.Nascimento,
                    "Sexo": patient.Sexo,
                    "Celular": patient.Celular,
                    "Email": email,
                    "Id do Cliente": id_patient,
                    "CPF/CGC": str(getattr(patient, "CPF/CGC", '')),
                    "Cep Residencial": str(getattr(patient, "Cep Residencial", '')),
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
                    "Observações": observations,
                    "Bairro Comercial": getattr(patient, "Bairro Comercial", ''),
                    "PáginadaWeb": getattr(patient, "PáginadaWeb", ''),
                    "Referências": getattr(patient, "Referências", ''),
                    "Filhos": getattr(patient, "Filhos", ''),
                    "Empresa": getattr(patient, "Empresa", ''),
                    "Tipo": getattr(patient, "Tipo", ''),
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
                    "CreationDate": getattr(patient, "CreationDate", None),
                    "Cônjugue": getattr(patient, "Cônjugue", ''),
                    "Acompanhante": getattr(patient, "Acompanhante", ''),
                    "Contato": getattr(patient, "Contato", ''),
                    "Exclui_Mkt": getattr(patient, "Exclui_Mkt", ''),
                    "Tags": getattr(patient, "Tags", ''),
                    "NumeroCNS": getattr(patient, "NumeroCNS", ''),
                    "Nome Social": getattr(patient, "Nome Social", ''),
                    "FaceDescriptor": getattr(patient, "FaceDescriptor", ''),
                    "Como conheceu": getattr(patient, "Como conheceu", ''),
                    "Indicado por": getattr(patient, "Indicado por", ''),
                }

                total_validated += 1
                if total_validated % 500 == 0:
                    print(f"Validados: {total_validated} pacientes...")

                yield patient_data

            except Exception as e:
                log_denied(patient, f"Erro inesperado: {str(e)}")

print("Por favor, informe os dados do banco de origem.\n")
sid_src = input("Informe o SoftwareID do banco de origem: ")
password_src = urllib.parse.quote_plus(input("Informe a senha do banco de origem: "))
dbase_src = input("Informe o DATABASE do banco de origem: ")

print("Agora informe os dados do banco de destino.\n")
sid_dest = input("Informe o SoftwareID do banco de destino: ")
password_dest = urllib.parse.quote_plus(input("Informe a senha do banco de destino: "))
dbase_dest = input("Informe o DATABASE do banco de destino: ")

id_usuario = input("Informe o(s) Id(s) do Usuário para filtrar os pacientes (separe por vírgula ou espaço): ").strip()
ids_usuario_list = [i.strip() for i in id_usuario.replace(',', ' ').split() if i.strip()]
ids_usuario_sql = ', '.join(ids_usuario_list)

path_file = input("Informe o caminho da pasta onde ficarão os logs: ")

DATABASE_SRC = f"mssql+pyodbc://Medizin_{sid_src}:{password_src}@medxserver.database.windows.net:1433/{dbase_src}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"
engine_src = create_engine(DATABASE_SRC)
BaseSrc = automap_base()
BaseSrc.prepare(autoload_with=engine_src)
ContatosSrc = getattr(BaseSrc.classes, "Contatos")
SessionSrc = sessionmaker(bind=engine_src)
session_src = SessionSrc()


with engine_src.connect() as conn:
    query = f"""
        SELECT DISTINCT [Id do Cliente] FROM [Histórico de Clientes] WHERE [Id do Usuário] IN ({ids_usuario_sql})
        UNION
        SELECT DISTINCT [Vinculado a] FROM [AGENDA] WHERE [Id do Usuário] IN ({ids_usuario_sql})
    """
    result = conn.execute(text(query))
    ids_clientes = [row[0] for row in result if row[0] is not None]

if not ids_clientes:
    print("Nenhum paciente relacionado ao(s) Id(s) do Usuário informado(s).")
    exit()

print(f"Serão migrados {len(ids_clientes)} pacientes relacionados ao(s) Id(s) do Usuário informado(s).")

if not os.path.exists(path_file):
    os.makedirs(path_file)

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid_dest}:{password_dest}@medxserver.database.windows.net:1433/{dbase_dest}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

engine_dest = create_engine(DATABASE_URL)

BaseDest = automap_base()
BaseDest.prepare(autoload_with=engine_dest)
ContatosDest = getattr(BaseDest.classes, "Contatos")

SessionDest = sessionmaker(bind=engine_dest)
session_dest = SessionDest()

print("Sucesso! Inicializando migração de Contatos...")

with SessionDest() as temp_session:
    try:
        buffer = []
        total = 0
        for patient_data in generate_valid_data(session_src, temp_session, ContatosSrc, ContatosDest, ids_clientes):
            buffer.append(patient_data)

            if len(buffer) == batch_size:
                temp_session.bulk_insert_mappings(ContatosDest, buffer)
                log_inserted.extend(buffer)
                total += len(buffer)
                print(f"Inseridos: {total} registros ({round((total / (total + len(log_not_inserted))) * 100, 2)}%)")
                buffer.clear()

        if buffer:
            temp_session.bulk_insert_mappings(ContatosDest, buffer)
            log_inserted.extend(buffer)
            total += len(buffer)
            print(f"Inseridos: {total} registros ({round((total / (total + len(log_not_inserted))) * 100, 2)}%) - 100%")
        
        temp_session.commit()

    except Exception as e:
        temp_session.rollback()
        print("\nErro na inserção. Transação revertida.")
        print(f'ERROR: {str(e)}')

session_src.close()

print("Migração concluída. Criando logs...")
create_log(log_inserted, path_file, "log_inserted_patients.xlsx")
create_log(log_not_inserted, path_file, "log_not_inserted_patients.xlsx")
print("Logs criados com sucesso!\n")
print(f"Total de contatos inseridos: {len(log_inserted)}")
print(f"Total de contatos não inseridos: {len(log_not_inserted)}")
