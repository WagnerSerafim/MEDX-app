import os
import re
import json
import pyodbc
from datetime import datetime

def sanitize_filename(name: str) -> str:
    """
    Remove caracteres inválidos para nome de arquivo no Windows e normaliza espaços.
    """
    if name is None:
        name = ""
    name = str(name).strip()
    # substitui caracteres inválidos: \ / : * ? " < > |
    name = re.sub(r'[\\/:*?"<>|]+', "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name or "sem_titulo"

def ensure_unique_path(base_dir: str, filename: str, ext: str) -> str:
    """
    Garante que não vai sobrescrever arquivo. Se existir, adiciona _1, _2, etc.
    """
    candidate = os.path.join(base_dir, f"{filename}{ext}")
    if not os.path.exists(candidate):
        return candidate

    i = 1
    while True:
        candidate = os.path.join(base_dir, f"{filename}_{i}{ext}")
        if not os.path.exists(candidate):
            return candidate
        i += 1

def detect_image_extension(data: bytes) -> str:
    """
    Detecta extensão básica pelo header.
    JPG: FF D8 FF
    PNG: 89 50 4E 47 0D 0A 1A 0A
    GIF: 47 49 46 38
    BMP: 42 4D
    """
    if not data:
        return ".bin"
    if data.startswith(b"\xFF\xD8\xFF"):
        return ".jpg"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return ".png"
    if data.startswith(b"GIF8"):
        return ".gif"
    if data.startswith(b"BM"):
        return ".bmp"
    return ".bin"

def bytes_from_sql_hex_string(s: str) -> bytes:
    """
    Converte string no formato '0xFFD8...' para bytes.
    Se já vier bytes do SQL Server, isso não será usado.
    """
    if s is None:
        return b""
    s = s.strip()
    if s.lower().startswith("0x"):
        s = s[2:]
    # remove espaços, quebras de linha etc
    s = re.sub(r"\s+", "", s)
    if not s:
        return b""
    return bytes.fromhex(s)

def format_dt(dt_value) -> str:
    """
    Converte DATEINSERT para 'Y-m-d H:M:S'.
    Aceita datetime, string ou outros formatos retornados pelo pyodbc.
    """
    if dt_value is None:
        return ""
    if isinstance(dt_value, datetime):
        return dt_value.strftime("%Y-%m-%d %H:%M:%S")
    # se vier como string tipo '2017-02-06 00:00:00.000'
    s = str(dt_value).strip()
    # tenta parsear com milissegundos
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    # fallback: corta milissegundos se existirem
    if "." in s:
        s = s.split(".", 1)[0]
    return s

def main():
    # ===== INPUT DO DIRETÓRIO =====
    out_dir = input("Informe o diretório para salvar as imagens: ").strip().strip('"')
    if not out_dir:
        raise ValueError("Diretório inválido.")
    os.makedirs(out_dir, exist_ok=True)

    # ===== CONEXÃO (Windows Auth) =====
    server = r"localhost\MSSQLSERVER01"
    database = "bancolocal"

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
    )

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # ===== BUSCA DOS DADOS =====
    # Ajuste os nomes dos campos caso tenham collation/case/typos diferentes
    query = """
        SELECT PCOD, IMAGECOD, DATEINSERT, TITLE, [IMAGE]
        FROM dbo.CLINI_04
        WHERE [IMAGE] IS NOT NULL
    """
    cursor.execute(query)

    results_json = []
    total = 0
    saved = 0

    for row in cursor.fetchall():
        total += 1

        pcod = row.PCOD
        imagecod = row.IMAGECOD
        dateinsert = row.DATEINSERT
        title = row.TITLE
        image_field = row.IMAGE

        safe_title = sanitize_filename(title)

        # ===== CONVERTE O CAMPO IMAGE PARA BYTES =====
        # No SQL Server, geralmente varbinary vem como bytes direto no pyodbc.
        # Se por algum motivo vier string '0x....', converte.
        if isinstance(image_field, (bytes, bytearray, memoryview)):
            img_bytes = bytes(image_field)
        else:
            img_bytes = bytes_from_sql_hex_string(str(image_field))

        if not img_bytes:
            # pula registros sem conteúdo
            continue

        ext = detect_image_extension(img_bytes)
        file_path = ensure_unique_path(out_dir, safe_title, ext)

        # ===== SALVA A IMAGEM =====
        with open(file_path, "wb") as f:
            f.write(img_bytes)
        saved += 1

        # ===== MONTA O JSON =====
        item = {
            "Id do Cliente": pcod,
            "Id do Histórico": imagecod,
            "Data": format_dt(dateinsert),
            "Histórico": title,
            "Classe": os.path.abspath(file_path),
        }
        results_json.append(item)

    conn.close()

    # ===== SALVA JSON NO MESMO DIRETÓRIO =====
    json_path = os.path.join(out_dir, "clini_04_export.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results_json, f, ensure_ascii=False, indent=2)

    print(f"\nPronto!")
    print(f"Registros lidos: {total}")
    print(f"Imagens salvas: {saved}")
    print(f"JSON gerado em: {json_path}")

if __name__ == "__main__":
    main()