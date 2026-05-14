from pathlib import Path
import re
import json
from datetime import datetime

import mysql.connector


# =========================
# CONFIGURAÇÕES FIXAS
# =========================

MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = ""
MYSQL_DATABASE = "steticclub_conex_spa"

PASTA_IMAGENS = Path(r"D:\Medxdata\imagens_exportadas_v2")

PREFIXO = "36432"

DRY_RUN = False

LOG_PATH = PASTA_IMAGENS / "log_renomeacao_fotos_perfil_v2.jsonl"


def conectar_mysql():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
    )


def escrever_log(payload: dict):
    payload["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")


def buscar_fotos_perfil():
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
            hora
        FROM imagens
        WHERE imagem IS NOT NULL
          AND (
                LOWER(TRIM(COALESCE(arquivo, ''))) LIKE '%foto%'
             OR LOWER(TRIM(COALESCE(descricao, ''))) LIKE '%foto%'
          )
        ORDER BY id
    """)

    for row in cursor:
        yield row

    cursor.close()
    conn.close()


def localizar_arquivo_por_id(id_imagem: int) -> Path | None:
    """
    Localiza qualquer arquivo exportado que comece com:
    {id_imagem}_

    Exemplo:
    7325_185.572.307-70_foto_fotos_foto.jpg
    """

    encontrados = list(PASTA_IMAGENS.glob(f"{id_imagem}_*"))

    if len(encontrados) == 1:
        return encontrados[0]

    if len(encontrados) > 1:
        # Pega o primeiro, mas registra conflito depois se necessário
        return encontrados[0]

    return None


def caminho_final_unico(id_paciente: int, ext: str) -> Path:
    """
    Gera:
    36432-{id_paciente}.jpg

    Se já existir, gera:
    36432-{id_paciente}_2.jpg
    36432-{id_paciente}_3.jpg
    etc.
    """

    caminho = PASTA_IMAGENS / f"{PREFIXO}-{id_paciente}{ext}"

    if not caminho.exists():
        return caminho

    contador = 2

    while True:
        novo = PASTA_IMAGENS / f"{PREFIXO}-{id_paciente}_{contador}{ext}"

        if not novo.exists():
            return novo

        contador += 1


def main():
    total_lidos = 0
    total_renomeados = 0
    total_ja_renomeados = 0
    total_sem_pessoa = 0
    total_nao_encontrados = 0
    total_erros = 0

    print("Iniciando renomeação das fotos de perfil...")

    for row in buscar_fotos_perfil():
        total_lidos += 1

        id_imagem = row["id"]
        id_paciente = row["pessoa"]

        if id_paciente is None:
            total_sem_pessoa += 1

            escrever_log({
                "status": "sem_pessoa",
                "id_imagem": id_imagem,
                "id_paciente": None,
                "descricao": row.get("descricao"),
                "arquivo": row.get("arquivo"),
            })

            continue

        caminho_atual = localizar_arquivo_por_id(id_imagem)

        if not caminho_atual:
            total_nao_encontrados += 1

            escrever_log({
                "status": "arquivo_nao_encontrado",
                "id_imagem": id_imagem,
                "id_paciente": id_paciente,
                "descricao": row.get("descricao"),
                "arquivo": row.get("arquivo"),
                "mensagem": f"Nenhum arquivo encontrado começando com {id_imagem}_",
            })

            print(f"Arquivo não encontrado para id_imagem={id_imagem}")
            continue

        if caminho_atual.name.startswith(f"{PREFIXO}-{id_paciente}"):
            total_ja_renomeados += 1

            escrever_log({
                "status": "ja_renomeado",
                "id_imagem": id_imagem,
                "id_paciente": id_paciente,
                "arquivo_atual": caminho_atual.name,
            })

            continue

        ext = caminho_atual.suffix.lower() or ".jpg"
        caminho_novo = caminho_final_unico(id_paciente, ext)

        try:
            if not DRY_RUN:
                caminho_atual.rename(caminho_novo)

            total_renomeados += 1

            escrever_log({
                "status": "renomeado" if not DRY_RUN else "simulado",
                "id_imagem": id_imagem,
                "id_paciente": id_paciente,
                "descricao": row.get("descricao"),
                "arquivo": row.get("arquivo"),
                "arquivo_atual": caminho_atual.name,
                "arquivo_novo": caminho_novo.name,
                "caminho_atual": str(caminho_atual),
                "caminho_novo": str(caminho_novo),
            })

            print(f"{caminho_atual.name} -> {caminho_novo.name}")

        except Exception as e:
            total_erros += 1

            escrever_log({
                "status": "erro",
                "id_imagem": id_imagem,
                "id_paciente": id_paciente,
                "arquivo_atual": caminho_atual.name,
                "arquivo_novo": caminho_novo.name,
                "erro": str(e),
            })

            print(f"Erro ao renomear {caminho_atual.name}: {e}")

    print("\nFinalizado.")
    print(f"Registros de foto lidos no MySQL: {total_lidos}")
    print(f"Arquivos renomeados: {total_renomeados}")
    print(f"Arquivos já renomeados: {total_ja_renomeados}")
    print(f"Registros sem pessoa: {total_sem_pessoa}")
    print(f"Arquivos não encontrados: {total_nao_encontrados}")
    print(f"Erros: {total_erros}")
    print(f"Log: {LOG_PATH}")


if __name__ == "__main__":
    main()