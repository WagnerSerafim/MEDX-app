from pathlib import Path
import re
import mysql.connector


# =========================
# CONFIGURAÇÕES FIXAS
# =========================

HOST = "localhost"
PORT = 3306
USER = "root"
PASSWORD = ""
DATABASE = "steticclub_conex_spa"
OUTPUT_DIR = r"D:\Medxdata\imagens_exportadas_v2"


def limpar_nome_arquivo(valor) -> str:
    if valor is None:
        return ""

    if isinstance(valor, bytes):
        valor = valor.decode("latin1", errors="ignore")

    valor = str(valor).strip()
    valor = re.sub(r'[<>:"/\\|?*]', "_", valor)
    valor = re.sub(r"\s+", " ", valor)

    return valor[:120]


def normalizar_blob(blob) -> bytes:
    if blob is None:
        return b""

    if isinstance(blob, memoryview):
        blob = blob.tobytes()

    if isinstance(blob, bytearray):
        blob = bytes(blob)

    if isinstance(blob, str):
        blob = blob.encode("ascii", errors="ignore")

    blob = bytes(blob)

    inicio = blob[:40].lower()

    parece_hex_textual = (
        inicio.startswith(b"ffd8ff")      # JPG
        or inicio.startswith(b"89504e47") # PNG
        or inicio.startswith(b"47494638") # GIF
        or inicio.startswith(b"25504446") # PDF
        or inicio.startswith(b"424d")     # BMP
        or inicio.startswith(b"52494646") # WEBP/RIFF
    )

    if parece_hex_textual:
        texto_hex = blob.decode("ascii", errors="ignore").strip()
        texto_hex = "".join(texto_hex.split())
        texto_hex = texto_hex.replace("0x", "").replace("0X", "")

        try:
            return bytes.fromhex(texto_hex)
        except ValueError:
            return blob

    return blob


def detectar_extensao(blob: bytes, ext_banco=None) -> str:
    if blob.startswith(b"\xff\xd8\xff"):
        return ".jpg"

    if blob.startswith(b"\x89PNG\r\n\x1a\n"):
        return ".png"

    if blob.startswith(b"GIF87a") or blob.startswith(b"GIF89a"):
        return ".gif"

    if blob.startswith(b"%PDF"):
        return ".pdf"

    if blob.startswith(b"BM"):
        return ".bmp"

    if blob.startswith(b"RIFF") and b"WEBP" in blob[:32]:
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


def caminho_unico(pasta: Path, nome_arquivo: str) -> Path:
    caminho = pasta / nome_arquivo

    if not caminho.exists():
        return caminho

    stem = caminho.stem
    suffix = caminho.suffix

    contador = 1

    while True:
        novo = pasta / f"{stem}_{contador}{suffix}"
        if not novo.exists():
            return novo

        contador += 1


def main():
    output_dir = Path(OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)

    conn = mysql.connector.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
    )

    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            id,
            imagem,
            pessoa,
            descricao,
            arquivo,
            ext,
            cpf,
            data,
            hora
        FROM imagens
        WHERE imagem IS NOT NULL
        ORDER BY id
    """)

    total_lidos = 0
    total_exportados = 0
    total_vazios = 0
    total_bin = 0

    for row in cursor:
        total_lidos += 1

        id_imagem = row[0]
        imagem = row[1]
        descricao = row[3]
        arquivo = row[4]
        ext_banco = row[5]
        cpf = row[6]

        blob = normalizar_blob(imagem)

        if not blob:
            total_vazios += 1
            continue

        ext = detectar_extensao(blob, ext_banco)

        if ext == ".bin":
            total_bin += 1

        cpf_limpo = limpar_nome_arquivo(cpf or "sem_cpf")
        descricao_limpa = limpar_nome_arquivo(descricao or "imagem")
        arquivo_limpo = limpar_nome_arquivo(arquivo or "arquivo")

        nome_base = limpar_nome_arquivo(
            f"{id_imagem}_{cpf_limpo}_{descricao_limpa}_{arquivo_limpo}"
        )

        nome_arquivo = nome_base + ext
        caminho_saida = caminho_unico(output_dir, nome_arquivo)

        with caminho_saida.open("wb") as f:
            f.write(blob)

        total_exportados += 1

        if total_exportados % 100 == 0:
            print(f"{total_exportados} arquivos exportados...")

    cursor.close()
    conn.close()

    print("\nFinalizado.")
    print(f"Registros lidos: {total_lidos}")
    print(f"Arquivos exportados: {total_exportados}")
    print(f"BLOBs vazios: {total_vazios}")
    print(f"Arquivos desconhecidos .bin: {total_bin}")
    print(f"Pasta de saída: {output_dir}")


if __name__ == "__main__":
    main()