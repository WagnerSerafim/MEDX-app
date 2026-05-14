from pathlib import Path
from datetime import datetime, date, time
import re
import urllib.parse

import mysql.connector
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


# ============================================================
# CONFIGURAÇÕES MYSQL - BANCO ONDE ESTÁ A TABELA imagens
# ============================================================

MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = ""
MYSQL_DATABASE = "steticclub_conex_spa"


# ============================================================
# CONFIGURAÇÕES SQL SERVER - BANCO DA MEDX
# ============================================================

SID = "36432"
SQLSERVER_PASSWORD = "140@TRes"
SQLSERVER_DATABASE = "MEDX35"

SQLSERVER_HOST = "medxserver.database.windows.net"
SQLSERVER_SCHEMA = f"schema_{SID}"


# ============================================================
# CONFIGURAÇÕES DA IMPORTAÇÃO
# ============================================================

PASTA_IMAGENS = Path(r"D:\Medxdata\36432")

TABELA_HISTORICO = "Histórico de Clientes"

BATCH_SIZE = 500

# Em alguns ambientes, fast_executemany pode causar estouro de memória
# ao enviar lotes grandes para o SQL Server.
SQLSERVER_FAST_EXECUTEMANY = False

# Se True, não insere duplicado quando já existir mesmo [Id do Cliente] + [Classe]
EVITAR_DUPLICADOS = True

# Se True, apenas mostra o que faria, mas não insere nada
DRY_RUN = False


def limpar_nome_arquivo(valor) -> str:
    if valor is None:
        return ""

    if isinstance(valor, bytes):
        valor = valor.decode("latin1", errors="ignore")

    valor = str(valor).strip()
    valor = re.sub(r'[<>:"/\\|?*]', "_", valor)
    valor = re.sub(r"\s+", " ", valor)

    return valor[:120]


def detectar_extensao_por_assinatura(assinatura, ext_banco=None) -> str:
    """
    Detecta a extensão usando a assinatura do BLOB.
    Aqui a assinatura pode vir como:
    - bytes reais: b'\\xff\\xd8\\xff'
    - texto hexadecimal: b'ffd8ff...'
    """

    if assinatura is None:
        assinatura = b""

    if isinstance(assinatura, str):
        assinatura = assinatura.encode("ascii", errors="ignore")

    if isinstance(assinatura, memoryview):
        assinatura = assinatura.tobytes()

    assinatura = bytes(assinatura)
    inicio = assinatura[:40].lower()

    # Caso tenha vindo como texto hexadecimal
    if inicio.startswith(b"ffd8ff"):
        return ".jpg"

    if inicio.startswith(b"89504e47"):
        return ".png"

    if inicio.startswith(b"47494638"):
        return ".gif"

    if inicio.startswith(b"25504446"):
        return ".pdf"

    if inicio.startswith(b"424d"):
        return ".bmp"

    if inicio.startswith(b"52494646"):
        return ".webp"

    # Caso tenha vindo como bytes reais
    if assinatura.startswith(b"\xff\xd8\xff"):
        return ".jpg"

    if assinatura.startswith(b"\x89PNG\r\n\x1a\n"):
        return ".png"

    if assinatura.startswith(b"GIF87a") or assinatura.startswith(b"GIF89a"):
        return ".gif"

    if assinatura.startswith(b"%PDF"):
        return ".pdf"

    if assinatura.startswith(b"BM"):
        return ".bmp"

    if assinatura.startswith(b"RIFF") and b"WEBP" in assinatura[:32]:
        return ".webp"

    ext_banco = limpar_nome_arquivo(ext_banco).lower().replace(".", "")

    mapa_ext = {
        "image/jpeg": ".jpg",
        "image/jpg": ".jpg",
        "jpeg": ".jpg",
        "jpg": ".jpg",
        "image/png": ".png",
        "png": ".png",
        "image/gif": ".gif",
        "gif": ".gif",
        "application/pdf": ".pdf",
        "pdf": ".pdf",
        "bmp": ".bmp",
        "image/bmp": ".bmp",
        "webp": ".webp",
        "image/webp": ".webp",
    }

    if ext_banco in mapa_ext:
        return mapa_ext[ext_banco]

    if "/" in ext_banco:
        ext_banco = ext_banco.split("/")[-1]

    if ext_banco:
        return f".{ext_banco}"

    return ".bin"


def montar_data(data_valor, hora_valor) -> datetime:
    """
    Monta [Data] no formato correto para SQL Server:
    yyyy-mm-dd hh:mm:ss
    """

    if data_valor is None:
        return datetime.now().replace(microsecond=0)

    if isinstance(data_valor, datetime):
        data_base = data_valor.date()
    elif isinstance(data_valor, date):
        data_base = data_valor
    else:
        texto = str(data_valor).strip()
        data_base = datetime.strptime(texto[:10], "%Y-%m-%d").date()

    if hora_valor is None:
        hora_base = time(0, 0, 0)
    elif isinstance(hora_valor, time):
        hora_base = hora_valor.replace(microsecond=0)
    else:
        texto_hora = str(hora_valor).strip()
        hora_base = datetime.strptime(texto_hora[:8], "%H:%M:%S").time()

    return datetime.combine(data_base, hora_base)


def montar_nome_arquivo(row) -> str:
    id_imagem = row["id"]
    cpf = limpar_nome_arquivo(row["cpf"] or "sem_cpf")
    descricao = limpar_nome_arquivo(row["descricao"] or "imagem")
    arquivo = limpar_nome_arquivo(row["arquivo"] or "arquivo")

    ext = detectar_extensao_por_assinatura(row["assinatura"], row["ext"])

    nome_base = limpar_nome_arquivo(
        f"{id_imagem}_{cpf}_{descricao}_{arquivo}"
    )

    return nome_base + ext


def conectar_mysql():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
    )


def conectar_sqlserver():
    password_encoded = urllib.parse.quote_plus(SQLSERVER_PASSWORD)

    database_url = (
        f"mssql+pyodbc://Medizin_{SID}:{password_encoded}"
        f"@{SQLSERVER_HOST}:1433/{SQLSERVER_DATABASE}"
        "?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"
    )

    return create_engine(
        database_url,
        fast_executemany=SQLSERVER_FAST_EXECUTEMANY,
        pool_pre_ping=True,
    )


def buscar_metadados_mysql():
    conn = conectar_mysql()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT
            id,
            pessoa,
            descricao,
            arquivo,
            ext,
            cpf,
            data,
            hora,
            LEFT(imagem, 40) AS assinatura
        FROM imagens
        WHERE imagem IS NOT NULL
        ORDER BY id
    """)

    for row in cursor:
        yield row

    cursor.close()
    conn.close()


def inserir_lote_sqlserver(engine, lote):
    if not lote:
        return 0

    tabela = f"[{SQLSERVER_SCHEMA}].[{TABELA_HISTORICO}]"

    if EVITAR_DUPLICADOS:
        sql = text(f"""
            INSERT INTO {tabela}
            (
                [Id do Cliente],
                [Data],
                [Id do Usuário],
                [Histórico],
                [Classe]
            )
            SELECT
                :id_cliente,
                :data,
                :id_usuario,
                :historico,
                :classe
            WHERE NOT EXISTS (
                SELECT 1
                FROM {tabela}
                WHERE [Id do Cliente] = :id_cliente
                  AND [Classe] = :classe
            )
        """)
    else:
        sql = text(f"""
            INSERT INTO {tabela}
            (
                [Id do Cliente],
                [Data],
                [Id do Usuário],
                [Histórico],
                [Classe]
            )
            VALUES
            (
                :id_cliente,
                :data,
                :id_usuario,
                :historico,
                :classe
            )
        """)

    try:
        with engine.begin() as conn:
            result = conn.execute(sql, lote)

        return result.rowcount if result.rowcount is not None else len(lote)
    except MemoryError:
        # Faz fallback automático dividindo o lote para reduzir o consumo
        # de memória no executemany do driver ODBC.
        if len(lote) <= 1:
            raise

        meio = len(lote) // 2
        inseridos_esquerda = inserir_lote_sqlserver(engine, lote[:meio])
        inseridos_direita = inserir_lote_sqlserver(engine, lote[meio:])
        return inseridos_esquerda + inseridos_direita


def main():
    print("Iniciando leitura dos metadados no MySQL...")

    registros = []
    total_lidos = 0
    total_sem_pessoa = 0
    total_arquivo_nao_encontrado = 0

    for row in buscar_metadados_mysql():
        total_lidos += 1

        id_cliente = row["pessoa"]

        if id_cliente is None:
            total_sem_pessoa += 1
            continue

        nome_arquivo = montar_nome_arquivo(row)
        caminho_arquivo = PASTA_IMAGENS / nome_arquivo

        if not caminho_arquivo.exists():
            total_arquivo_nao_encontrado += 1
            print(f"Arquivo não encontrado, ignorando: {nome_arquivo}")
            continue

        data_historico = montar_data(row["data"], row["hora"])

        registros.append({
            "id_cliente": int(id_cliente),
            "data": data_historico,
            "id_usuario": 0,
            "historico": nome_arquivo,
            "classe": nome_arquivo,
        })

    print("\nResumo da preparação:")
    print(f"Registros lidos no MySQL: {total_lidos}")
    print(f"Registros preparados para inserir: {len(registros)}")
    print(f"Registros sem pessoa: {total_sem_pessoa}")
    print(f"Arquivos não encontrados na pasta: {total_arquivo_nao_encontrado}")

    if DRY_RUN:
        print("\nDRY_RUN=True, nada foi inserido no SQL Server.")
        if registros:
            print("\nExemplo do primeiro registro preparado:")
            print(registros[0])
        return

    if not registros:
        print("\nNenhum registro para inserir.")
        return

    print("\nConectando no SQL Server...")
    engine = conectar_sqlserver()

    total_inseridos = 0

    for i in range(0, len(registros), BATCH_SIZE):
        lote = registros[i:i + BATCH_SIZE]
        inseridos_lote = inserir_lote_sqlserver(engine, lote)
        total_inseridos += inseridos_lote

        print(
            f"Lote {i // BATCH_SIZE + 1}: "
            f"{len(lote)} processados | "
            f"{inseridos_lote} inseridos"
        )

    engine.dispose()

    print("\nFinalizado.")
    print(f"Total preparado: {len(registros)}")
    print(f"Total inserido: {total_inseridos}")


if __name__ == "__main__":
    main()