import re
import urllib
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


TAMANHO_MAX_ITEM = 100
ID_UN_PADRAO = -687642721
TIPO_MOVIMENTACAO_ENTRADA = "E"
ABA_ESTOQUE = "IMPLANTES"


def limpar_texto(valor):
    if pd.isna(valor):
        return None

    if isinstance(valor, float) and valor.is_integer():
        valor = int(valor)

    valor = str(valor).strip()

    if valor == "":
        return None

    if valor.endswith(".0"):
        valor = valor[:-2]

    return valor


def converter_numero(valor, padrao=None):
    if pd.isna(valor):
        return padrao

    valor = str(valor).strip()

    if valor == "":
        return padrao

    match = re.search(r"-?\d+(?:[,.]\d+)?", valor)

    if not match:
        return padrao

    numero = float(match.group(0).replace(",", "."))

    if numero.is_integer():
        return int(numero)

    return numero


def converter_data_excel(valor):
    if pd.isna(valor):
        return None

    if isinstance(valor, (datetime, pd.Timestamp)):
        return valor.to_pydatetime() if isinstance(valor, pd.Timestamp) else valor

    valor_str = str(valor).strip()

    if valor_str == "":
        return None

    try:
        if valor_str.replace(".", "", 1).isdigit():
            serial = int(float(valor_str))
            return datetime(1899, 12, 30) + timedelta(days=serial)
    except Exception:
        pass

    formatos = [
        "%d/%m/%Y",
        "%Y-%m-%d",
        "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%m/%y",
        "%m/%Y",
    ]

    for formato in formatos:
        try:
            return datetime.strptime(valor_str, formato)
        except Exception:
            continue

    return None


def gerar_validade_mm_aa(valor):
    data = converter_data_excel(valor)

    if data is None:
        return None

    return data.strftime("%m/%y")


def truncar_com_sufixo(valor, sufixo=None, limite=TAMANHO_MAX_ITEM):
    valor = valor.strip()

    if not sufixo:
        return valor[:limite]

    espaco_sufixo = len(sufixo)
    return f"{valor[:limite - espaco_sufixo]}{sufixo}"


def gerar_nome_item(produto, contagem_nomes):
    chave = produto.upper()
    ocorrencia = contagem_nomes.get(chave, 0)
    contagem_nomes[chave] = ocorrencia + 1

    sufixo = None if ocorrencia == 0 else f" ({ocorrencia})"
    return truncar_com_sufixo(produto, sufixo)


def normalizar_colunas(df):
    df.columns = [str(col).strip().upper() for col in df.columns]
    return df


def validar_colunas(df):
    colunas_obrigatorias = [
        "PRODUTO",
        "LOTE",
        "VALIDADE",
        "QUANTIDADE FISICO",
    ]

    for coluna in colunas_obrigatorias:
        if coluna not in df.columns:
            raise ValueError(f"Coluna obrigatória não encontrada no Excel: {coluna}")


def preparar_linhas(df):
    linhas = []
    contagem_nomes = {}

    for indice_excel, row in df.iterrows():
        produto = limpar_texto(row.get("PRODUTO"))

        if not produto:
            continue

        item = gerar_nome_item(produto, contagem_nomes)
        lote = limpar_texto(row.get("LOTE"))
        validade = gerar_validade_mm_aa(row.get("VALIDADE"))
        quantidade_fisico = converter_numero(row.get("QUANTIDADE FISICO"), padrao=0)

        linhas.append({
            "LinhaExcel": indice_excel + 2,
            "Item": item,
            "Lote": lote,
            "Validade": validade,
            "Qtd": quantidade_fisico,
        })

    return linhas


def diagnosticar_linhas(linhas):
    total_qtd = sum((linha["Qtd"] or 0) for linha in linhas)
    itens_zerados = sum(1 for linha in linhas if (linha["Qtd"] or 0) == 0)
    itens_com_lote = sum(1 for linha in linhas if linha["Lote"])
    itens_com_validade = sum(1 for linha in linhas if linha["Validade"])

    print("\n--- Resumo da migração preparada ---")
    print(f"Itens preparados: {len(linhas)}")
    print(f"Soma das quantidades físicas: {total_qtd}")
    print(f"Itens com quantidade 0: {itens_zerados}")
    print(f"Itens com lote: {itens_com_lote}")
    print(f"Itens com validade: {itens_com_validade}")
    print("-" * 60)

    maiores_nomes = sorted(linhas, key=lambda x: len(x["Item"]), reverse=True)[:5]

    print("\n--- Maiores nomes após tratamento ---")
    for linha in maiores_nomes:
        print(f"{len(linha['Item'])} caracteres | Linha {linha['LinhaExcel']} | {linha['Item']}")
    print("-" * 60)


def buscar_limites_colunas(engine):
    sql = text("""
        SELECT
            TABLE_NAME,
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME IN (
            'Estoque',
            'Estoque Movimentação',
            'Estoque Movimentação Itens'
        )
          AND COLUMN_NAME IN (
              'Item',
              'Lote',
              'Validade',
              'Tipo'
          )
        ORDER BY TABLE_NAME, COLUMN_NAME
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql).mappings().all()

    limites = {}

    print("\n--- Estrutura dos campos de texto no banco ---")

    for row in rows:
        tabela = row["TABLE_NAME"]
        coluna = row["COLUMN_NAME"]
        tipo = row["DATA_TYPE"]
        tamanho = row["CHARACTER_MAXIMUM_LENGTH"]
        nullable = row["IS_NULLABLE"]

        limites[(tabela, coluna)] = tamanho

        print(f"{tabela}.{coluna}: {tipo}({tamanho}) | Nullable: {nullable}")

    print("-" * 60)

    return limites


def validar_tamanhos(linhas, limites):
    campos = [
        ("Estoque", "Item", "Item"),
        ("Estoque", "Lote", "Lote"),
        ("Estoque", "Validade", "Validade"),
        ("Estoque Movimentação Itens", "Lote", "Lote"),
        ("Estoque Movimentação Itens", "Validade", "Validade"),
    ]

    erros = []

    for linha in linhas:
        for tabela, coluna, campo_python in campos:
            limite = limites.get((tabela, coluna))
            valor = linha.get(campo_python)

            if valor is None:
                continue

            if limite is None or limite == -1:
                continue

            tamanho = len(str(valor))

            if tamanho > limite:
                erros.append({
                    "linha_excel": linha["LinhaExcel"],
                    "tabela": tabela,
                    "coluna": coluna,
                    "limite": limite,
                    "tamanho": tamanho,
                    "valor": valor,
                    "item": linha.get("Item"),
                })

    if not erros:
        return

    print("\nERRO: Existem valores maiores que o limite da coluna no banco.")
    print("Revise os casos abaixo antes de inserir:\n")

    for erro in erros:
        print(
            f"Linha Excel {erro['linha_excel']} | "
            f"Tabela: {erro['tabela']} | "
            f"Coluna: {erro['coluna']} | "
            f"Limite: {erro['limite']} | "
            f"Tamanho: {erro['tamanho']} | "
            f"Valor: {erro['valor']} | "
            f"Item: {erro['item']}"
        )

    raise ValueError("Migração abortada por valores maiores que o limite da coluna.")


sid = input("Informe o SoftwareID: ").strip()
password = urllib.parse.quote_plus(input("Informe a senha: ").strip())
dbase = input("Informe o DATABASE: ").strip()
path_excel = input("Informe o caminho do Excel: ").strip()

DATABASE_URL = (
    f"mssql+pyodbc://Medizin_{sid}:{password}"
    f"@medxserver.database.windows.net:1433/{dbase}"
    f"?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"
)

engine = create_engine(
    DATABASE_URL,
    fast_executemany=True,
    pool_pre_ping=True,
)

df = pd.read_excel(path_excel, sheet_name=ABA_ESTOQUE, engine="openpyxl")
df = normalizar_colunas(df)
validar_colunas(df)

linhas = preparar_linhas(df)

if not linhas:
    raise ValueError("Nenhum item válido encontrado no Excel.")

diagnosticar_linhas(linhas)

limites = buscar_limites_colunas(engine)
validar_tamanhos(linhas, limites)

insert_estoque_sql = text("""
    INSERT INTO [Estoque] (
        [Item],
        [Lote],
        [Validade],
        [Qtd],
        [IsOculto]
    )
    OUTPUT INSERTED.[Id do Item] AS [Id do Item]
    VALUES (
        :Item,
        :Lote,
        :Validade,
        :Qtd,
        0
    )
""")

insert_movimentacao_sql = text("""
    INSERT INTO [Estoque Movimentação] (
        [Id do Contato],
        [Data],
        [Id da UN],
        [Tipo],
        [Observação],
        [Total da Movimentação],
        [Número do Documento]
    )
    OUTPUT INSERTED.[Id da Movimentação] AS [Id da Movimentação]
    VALUES (
        NULL,
        :Data,
        :IdUN,
        :Tipo,
        NULL,
        :TotalMovimentacao,
        NULL
    )
""")

insert_movimentacao_item_sql = text("""
    INSERT INTO [Estoque Movimentação Itens] (
        [Id da Movimentação],
        [Id do Item],
        [Lote],
        [Validade],
        [Qtd]
    )
    VALUES (
        :IdMovimentacao,
        :IdItem,
        :Lote,
        :Validade,
        :Qtd
    )
""")

print("\nIniciando inserção no banco...")

data_movimentacao = datetime.now()
inseridos = 0

with engine.begin() as conn:
    for i, linha in enumerate(linhas, start=1):
        try:
            estoque_result = conn.execute(insert_estoque_sql, {
                "Item": linha["Item"],
                "Lote": linha["Lote"],
                "Validade": linha["Validade"],
                "Qtd": linha["Qtd"],
            }).mappings().first()

            id_item = estoque_result["Id do Item"]

            movimentacao_result = conn.execute(insert_movimentacao_sql, {
                "Data": data_movimentacao,
                "IdUN": ID_UN_PADRAO,
                "Tipo": TIPO_MOVIMENTACAO_ENTRADA,
                "TotalMovimentacao": linha["Qtd"],
            }).mappings().first()

            id_movimentacao = movimentacao_result["Id da Movimentação"]

            conn.execute(insert_movimentacao_item_sql, {
                "IdMovimentacao": id_movimentacao,
                "IdItem": id_item,
                "Lote": linha["Lote"],
                "Validade": linha["Validade"],
                "Qtd": linha["Qtd"],
            })

            inseridos += 1

            if inseridos % 100 == 0:
                print(f"Inseridos: {inseridos}/{len(linhas)}")

        except SQLAlchemyError as e:
            print("\nERRO AO INSERIR LINHA:")
            print(f"Índice preparado: {i}")
            print(f"Linha Excel: {linha['LinhaExcel']}")
            print(linha)

            print("\nTamanhos dos campos de texto:")
            print(f"Item: {len(str(linha['Item'])) if linha['Item'] else 0}")
            print(f"Lote: {len(str(linha['Lote'])) if linha['Lote'] else 0}")
            print(f"Validade: {len(str(linha['Validade'])) if linha['Validade'] else 0}")

            print("\nERRO ORIGINAL:")
            print(e)

            raise

print(f"\nMigração concluída com sucesso. Itens inseridos: {inseridos}")
