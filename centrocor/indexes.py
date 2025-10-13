import pyodbc
import re
import binascii
import logging
from pathlib import Path
from typing import Optional, Tuple

# ============ CONFIG ============
SERVER = "PC-WAGNER"
DATABASE = "REPOSITORIO"
TABLE = "dbo.tblAnexoCliente"

COL_BIN  = "strAnexo"          # conteúdo (VARBINARY ou hex textual)
COL_NAME = "strAnexoArquivo"   # nome do arquivo

DEST_FILES_DIR = Path(r"E:\Medxdata\31362")

ONLY_IMAGES = True            # True = salva apenas imagens; False = salva qualquer tipo
SKIP_IF_FILE_EXISTS = True    # não regrava se já existir
BATCH_SIZE = 10               # lote pequeno para reduzir pico de memória
VERBOSE = True

# ============ LOG ============
logging.basicConfig(
    level=logging.INFO if VERBOSE else logging.WARNING,
    format="%(levelname)s | %(message)s"
)
log = logging.getLogger("export_anexos")

# ============ CONEXÃO ============
def pick_sqlserver_driver() -> str:
    preferred = [
        "ODBC Driver 19 for SQL Server",
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "SQL Server",
    ]
    installed = set(pyodbc.drivers())
    for d in preferred:
        if d in installed:
            return d
    raise RuntimeError(f"Instale um driver ODBC do SQL Server: {preferred}")

def conn_str(server: str, database: str) -> str:
    driver = pick_sqlserver_driver()
    return (
        f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};"
        f"Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes;"
    )

# ============ BINÁRIO / DETECÇÃO ============
HEXSET = set("0123456789abcdefABCDEF")

def normalize_hex_string(s: str) -> str:
    s = s.strip()
    if s.lower().startswith("0x"):
        s = s[2:]
    s = re.sub(r"\s+", "", s)
    for i, ch in enumerate(s):
        if ch not in HEXSET:
            raise ValueError(f"caractere não-hex na posição {i}: {repr(ch)}")
    if len(s) % 2:
        raise ValueError(f"quantidade ímpar de dígitos hex ({len(s)})")
    return s

def to_bytes(val) -> bytes:
    if isinstance(val, (bytes, bytearray, memoryview)):
        return bytes(val)
    return binascii.unhexlify(normalize_hex_string(str(val)))

def guess_ext(b: bytes) -> str:
    # imagens
    if b.startswith(b"\x89PNG\r\n\x1a\n"): return ".png"
    if b.startswith(b"\xff\xd8\xff"):      return ".jpg"   # JPEG
    if b.startswith(b"GIF8"):              return ".gif"
    if b.startswith(b"BM"):                return ".bmp"   # BMP
    if b[:4] in (b"II*\x00", b"MM\x00*"):  return ".tiff"  # TIFF
    if b[:4] == b"RIFF" and b[8:12] == b"WEBP": return ".webp"
    # outros comuns
    if b.startswith(b"%PDF"):              return ".pdf"
    if b[:4] == b"PK\x03\x04":            return ".zip"
    return ".bin"

def is_image_ext(ext: str) -> bool:
    return ext.lower() in {".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tiff", ".webp", ".pdf"}

# ============ NOMES ============
INVALID_WIN = r'<>:"/\\|?*\x00-\x1F'
invalid_re = re.compile(f"[{INVALID_WIN}]")

def sanitize_filename(name: str, max_len: int = 150) -> str:
    name = (name or "").strip()
    if not name:
        name = "anexo"
    name = invalid_re.sub("_", name).rstrip(". ")
    if not name:
        name = "anexo"
    if len(name) > max_len:
        name = name[:max_len].rstrip()
    return name

def split_name_ext(name: str) -> Tuple[str, str]:
    p = Path(name)
    base = p.stem
    ext = "".join(p.suffixes)
    return base if base else name, ext

def unique_path(base_dir: Path, base_name: str, ext: str) -> Path:
    path = base_dir / f"{base_name}{ext}"
    if not path.exists():
        return path
    i = 1
    while True:
        cand = base_dir / f"{base_name}-{i:03d}{ext}"
        if not cand.exists():
            return cand
        i += 1

# ============ EXPORT ============
def export_files_only():
    DEST_FILES_DIR.mkdir(parents=True, exist_ok=True)

    cs = conn_str(SERVER, DATABASE)
    total = saved = skipped = 0

    with pyodbc.connect(cs, autocommit=True) as conn:
        cur = conn.cursor()
        cur.arraysize = BATCH_SIZE

        # leitura leve (aceita leitura "suja")
        cur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;")

        sql = (
            f"SELECT {COL_BIN}, {COL_NAME} "
            f"FROM {TABLE} WITH (NOLOCK) "
            f"WHERE {COL_BIN} IS NOT NULL "
            f"ORDER BY (SELECT NULL);"
        )
        log.info(f"Executando em lotes: {sql}")
        cur.execute(sql)

        while True:
            rows = cur.fetchmany(BATCH_SIZE)
            if not rows:
                break
            for (binval, nome_arquivo_raw) in rows:
                total += 1

                # decodifica o blob (sem manter nada na memória depois)
                try:
                    b = to_bytes(binval)
                except Exception as e:
                    skipped += 1
                    log.warning(f"[{total}] erro ao converter binário: {e}")
                    continue

                ext_detect = guess_ext(b)
                if ONLY_IMAGES and not is_image_ext(ext_detect):
                    skipped += 1
                    log.info(f"[{total}] não é imagem ({ext_detect}), pulando.")
                    continue

                # nome do arquivo a partir de strAnexoArquivo
                nome_arquivo = sanitize_filename(str(nome_arquivo_raw))
                base, ext_from_name = split_name_ext(nome_arquivo)

                # prefere extensão do nome se for coerente/imagética; senão usa a detectada
                final_ext = ext_detect
                if ext_from_name and (ext_from_name.lower() == ext_detect.lower() or is_image_ext(ext_from_name)):
                    final_ext = ext_from_name

                out_path = unique_path(DEST_FILES_DIR, base, final_ext)

                if SKIP_IF_FILE_EXISTS and out_path.exists():
                    saved += 1
                    continue

                out_path.write_bytes(b)
                saved += 1
                if saved % 100 == 0:
                    log.info(f"{saved} arquivos salvos...")

    print(f"Concluído. Processados: {total} | salvos: {saved} | pulados: {skipped}")
    print(f"Arquivos em: {DEST_FILES_DIR}")

if __name__ == "__main__":
    export_files_only()
