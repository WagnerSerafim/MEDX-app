import os
import json
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
import urllib

sid = input("Informe o SoftwareID: ")
password = urllib.parse.quote_plus(input("Informe a senha: "))
dbase = input("Informe o DATABASE: ")
backup_dir = input("Informe o diretório para salvar os backups: ").strip()

# Input dos Ids dos usuários
ids_usuarios = []
while True:
    id_usuario = input("Digite o Id do usuário para backup (ou 'N' para parar): ").strip()
    if id_usuario.upper() == 'N' or id_usuario == '':
        break
    ids_usuarios.append(id_usuario)

if not ids_usuarios:
    print("Nenhum Id de usuário informado. Encerrando.")
    exit()

print("Iniciando a conexão com o banco de dados...")

DATABASE_URL = f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"
engine = create_engine(DATABASE_URL)
Base = automap_base()
Base.prepare(autoload_with=engine)
SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

# Busca nome dos usuários
usuarios_nomes = {}
for id_usuario in ids_usuarios:
    result = session.execute(text(f"SELECT [Usuário] FROM [Usuários] WHERE [Id do Usuário] = :id"), {"id": id_usuario})
    row = result.fetchone()
    nome = row[0] if row else f"usuario_{id_usuario}"
    usuarios_nomes[id_usuario] = nome

for id_usuario in ids_usuarios:
    nome_usuario = usuarios_nomes[id_usuario]
    pasta_usuario = os.path.join(backup_dir, nome_usuario)
    if not os.path.exists(pasta_usuario):
        os.makedirs(pasta_usuario)

    print(f"\nExportando dados do usuário: {nome_usuario} (Id: {id_usuario})")

    # Agenda
    result = session.execute(text(f"SELECT * FROM [Agenda] WHERE [Id do Usuário] = :id"), {"id": id_usuario})
    agenda = [dict(row._mapping) for row in result]
    with open(os.path.join(pasta_usuario, "Agenda.json"), "w", encoding="utf-8") as f:
        json.dump(agenda, f, ensure_ascii=False, indent=2, default=str)

    # Histórico de Clientes
    result = session.execute(text(f"SELECT * FROM [Histórico de Clientes] WHERE [Id do Usuário] = :id"), {"id": id_usuario})
    historico_clientes = [dict(row._mapping) for row in result]
    with open(os.path.join(pasta_usuario, "HistoricoClientes.json"), "w", encoding="utf-8") as f:
        json.dump(historico_clientes, f, ensure_ascii=False, indent=2, default=str)

    # Atendimentos
    result = session.execute(text(f"SELECT * FROM [Atendimentos] WHERE [Id do Usuário] = :id"), {"id": id_usuario})
    atendimentos = [dict(row._mapping) for row in result]
    with open(os.path.join(pasta_usuario, "Atendimentos.json"), "w", encoding="utf-8") as f:
        json.dump(atendimentos, f, ensure_ascii=False, indent=2, default=str)

    # Autodocs
    result = session.execute(text(f"SELECT * FROM [Autodocs] WHERE [Bloqueio] = :id"), {"id": id_usuario})
    autodocs = [dict(row._mapping) for row in result]
    with open(os.path.join(pasta_usuario, "Autodocs.json"), "w", encoding="utf-8") as f:
        json.dump(autodocs, f, ensure_ascii=False, indent=2, default=str)

    # Ids dos Clientes
    result = session.execute(text(f"""
        SELECT DISTINCT [Id do Cliente] FROM [Histórico de Clientes] WHERE [Id do Usuário] = :id
        UNION
        SELECT DISTINCT [Vinculado a] FROM [Agenda] WHERE [Id do Usuário] = :id
    """), {"id": id_usuario})
    ids_clientes = [str(row[0]) for row in result if row[0] is not None]

    # Contatos
    if ids_clientes:
        ids_clientes_str = ",".join([f"'{idc}'" for idc in ids_clientes])
        result = session.execute(text(f"SELECT * FROM [Contatos] WHERE [Id do Cliente] IN ({ids_clientes_str})"))
        contatos = [dict(row._mapping) for row in result]
        with open(os.path.join(pasta_usuario, "Contatos.json"), "w", encoding="utf-8") as f:
            json.dump(contatos, f, ensure_ascii=False, indent=2, default=str)

        # Diagnóstico-QP
        result = session.execute(text(f"SELECT * FROM [Diagnóstico-QP]"))
        diagnostico_qp = [dict(row._mapping) for row in result]
        with open(os.path.join(pasta_usuario, "DiagnosticoQP.json"), "w", encoding="utf-8") as f:
            json.dump(diagnostico_qp, f, ensure_ascii=False, indent=2, default=str)

        # Exames_resultados
        result = session.execute(text(f"SELECT * FROM [Exames_resultados] WHERE [Id do Paciente] IN ({ids_clientes_str})"))
        exames_resultados = [dict(row._mapping) for row in result]
        with open(os.path.join(pasta_usuario, "ExamesResultados.json"), "w", encoding="utf-8") as f:
            json.dump(exames_resultados, f, ensure_ascii=False, indent=2, default=str)

        # Ids dos exames
        ids_exames = [str(row['Id do Exame']) for row in exames_resultados if 'Id do Exame' in row and row['Id do Exame'] is not None]
        if ids_exames:
            ids_exames_str = ",".join([f"'{ide}'" for ide in ids_exames])
            result = session.execute(text(f"SELECT * FROM [Exames_modelo] WHERE [Id do Exame] IN ({ids_exames_str})"))
            exames_modelo = [dict(row._mapping) for row in result]
            with open(os.path.join(pasta_usuario, "ExamesModelo.json"), "w", encoding="utf-8") as f:
                json.dump(exames_modelo, f, ensure_ascii=False, indent=2, default=str)

    # Arquivo txt com campo Classe do Histórico de Clientes
    classes = [row['Classe'] for row in historico_clientes if 'Classe' in row and row['Classe'] not in [None, '', 'NULL']]
    txt_path = os.path.join(pasta_usuario, "classes_historico_clientes.txt")
    with open(txt_path, "w", encoding="utf-8") as f:
        for classe in classes:
            f.write(str(classe) + "\n")

print("\nBackup específico concluído!")
session.close()