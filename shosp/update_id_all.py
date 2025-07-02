import glob
import os
from collections import defaultdict
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import urllib
from utils.utils import create_log

# Inputs
sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
path_file = input("Informe o caminho da pasta que contém os arquivos: ")

# Conexão
print("Conectando no Banco de Dados...")
DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"
engine = create_engine(DATABASE_URL)

Base = automap_base()
Base.prepare(autoload_with=engine)

SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

# Tabelas
Contatos = getattr(Base.classes, "Contatos")
Historico = getattr(Base.classes, "Histórico de Clientes")
Agenda = getattr(Base.classes, "Agenda")
ExamesResultados = getattr(Base.classes, "Exames_resultados")

# Leitura dos arquivos
print("Lendo arquivos de backup...")
todos_arquivos = glob.glob(f'{path_file}/all_bkps.xlsx')
df_antigo = pd.read_excel(todos_arquivos[0], sheet_name='contatos_bkp').replace('None', '')
df_novo = pd.read_excel(todos_arquivos[0], sheet_name='contatos_novo_bkp').replace('None', '')

# Garantir que a coluna 'Nascimento' está em formato datetime
df_antigo['Nascimento'] = pd.to_datetime(df_antigo['Nascimento'], errors='coerce')
df_novo['Nascimento'] = pd.to_datetime(df_novo['Nascimento'], errors='coerce')

# Agrupamento por (Nome, Nascimento)
def agrupar_por_nome_nascimento(df):
    grupos = defaultdict(list)
    for _, row in df.iterrows():
        chave = (row['Nome'], row['Nascimento'])
        grupos[chave].append({
            'Id': row['Id do Cliente'],
            'row': row
        })
    return grupos

antigos_grupo = agrupar_por_nome_nascimento(df_antigo)
novos_grupo = agrupar_por_nome_nascimento(df_novo)

ids_novos_existentes = set(df_novo['Id do Cliente'])

# Mapeia ID antigo -> novo com base em regras de correspondência
id_antigo_para_novo = {}

for chave in antigos_grupo:
    lista_antigos = antigos_grupo.get(chave, [])
    lista_novos = novos_grupo.get(chave, [])

    for i, antigo in enumerate(lista_antigos):
        if i < len(lista_novos):
            id_antigo = antigo['Id']
            id_novo = lista_novos[i]['Id']
            # Pula se id antigo já existe na nova tabela
            if id_antigo == id_novo:
                continue
            id_antigo_para_novo[id_antigo] = id_novo

# Logs
log_folder = path_file
os.makedirs(log_folder, exist_ok=True)

log_data = []
not_inserted_data = []
inserted_cont = 0
not_inserted_cont = 0

# Função para atualizar tabelas
def atualizar_tabela(model, campo_id):
    global inserted_cont, not_inserted_cont
    for id_antigo, id_novo in id_antigo_para_novo.items():
        try:
            registros = session.query(model).filter(getattr(model, campo_id) == id_antigo).all()
            for reg in registros:
                setattr(reg, campo_id, id_novo)
                log_data.append({
                    "Tabela": model.__table__.name,
                    "Id Antigo": id_antigo,
                    "Id Novo": id_novo
                })
                inserted_cont += 1
        except Exception as e:
            not_inserted_data.append({
                "Tabela": model.__table__.name,
                "Id Antigo": id_antigo,
                "Erro": str(e)
            })
            not_inserted_cont += 1

# Aplicar em cada tabela
print("Atualizando IDs em Histórico de Clientes...")
atualizar_tabela(Historico, "Id do Cliente")

print("Atualizando IDs em Agenda...")
atualizar_tabela(Agenda, "Vinculado a")

print("Atualizando IDs em Exames_resultados...")
atualizar_tabela(ExamesResultados, "Id do Paciente")

# Finaliza sessão
session.commit()
session.close()

# Logs em arquivos Excel
create_log(log_data, log_folder, "log_update_id_all_patients.xlsx")
create_log(not_inserted_data, log_folder, "log_not_update_id_all_patients.xlsx")

# Feedback final
print(f"{inserted_cont} ids de clientes foram atualizados com sucesso!")
if not_inserted_cont > 0:
    print(f"{not_inserted_cont} ids não foram atualizados, verifique o log para mais detalhes.")
