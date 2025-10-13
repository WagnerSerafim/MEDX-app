import pyodbc
import re
import binascii
import logging
from pathlib import Path

# ============ CONFIG ============
SERVER = "PC-WAGNER"
DATABASE = "BIODATA"
TABLE = "dbo.tblFoto"

COL_BIN = "imgFoto"        # conteúdo da imagem (varbinary ou texto hex)
COL_ID  = "intClienteId"   # para compor o nome do arquivo

DEST_DIR = Path(r"E:\Medxdata\31362")

ONLY_IMAGES = True          # True = garante que só salva imagens
SKIP_IF_EXISTS = True       # não regrava se já existir
BATCH_SIZE = 10             # lote pequeno => baixo pico de memória
VERBOSE = True

# ============ LOG ============
logging.basicConfig(
    level=logging.INFO if VERBOSE else logging.WARNING,
    format="%(levelname)s | %(message)s"
)
log = logging.getLogger("export_fotos")

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

# ============ HEX/BIN ============
HEXSET = set("0123456789abcdefABCDEF")

def normalize_hex_string(s: str) -> str:
    """
    Limpa e valida string hex. Se virar vazio (ex.: '0x'), retorna ''.
    """
    if s is None:
        return ""
    s = s.strip()
    if s.lower().startswith("0x"):
        s = s[2:]
    s = re.sub(r"\s+", "", s)
    if s == "":
        return ""
    for i, ch in enumerate(s):
        if ch not in HEXSET:
            raise ValueError(f"caractere não-hex na posição {i}: {repr(ch)}")
    if len(s) % 2:
        raise ValueError(f"quantidade ímpar de dígitos hex ({len(s)})")
    return s

def to_bytes(val) -> bytes | None:
    """
    Converte campo (varbinary ou texto hex) em bytes.
    Retorna None para conteúdos vazios ('0x', '', varbinary length 0).
    """
    if val is None:
        return None
    if isinstance(val, (bytes, bytearray, memoryview)):
        b = bytes(val)
        return b if len(b) > 0 else None
    s = normalize_hex_string(str(val))
    if s == "":
        return None
    return binascii.unhexlify(s)

def guess_image_ext(b: bytes) -> str | None:
    # formatos de imagem mais comuns
    if b.startswith(b"\x89PNG\r\n\x1a\n"): return ".png"
    if b.startswith(b"\xff\xd8\xff"):      return ".jpg"   # JPEG
    if b.startswith(b"GIF8"):              return ".gif"
    if b.startswith(b"BM"):                return ".bmp"   # BMP
    if b[:4] in (b"II*\x00", b"MM\x00*"):  return ".tiff"  # TIFF
    if b[:4] == b"RIFF" and b[8:12] == b"WEBP": return ".webp"
    return None  # não reconhecido como imagem

# ============ NOMES ============
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
def export_fotos():
    DEST_DIR.mkdir(parents=True, exist_ok=True)

    cs = conn_str(SERVER, DATABASE)
    total = saved = skipped = 0

    with pyodbc.connect(cs, autocommit=True) as conn:
        cur = conn.cursor()
        cur.arraysize = BATCH_SIZE
        cur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;")

        # Busca só as colunas necessárias
        sql = (
            f"SELECT {COL_BIN}, {COL_ID} "
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

            for (binval, cliente_id) in rows:
                total += 1

                # converte; ignora vazio ('0x' / varbinary 0 bytes)
                try:
                    b = to_bytes(binval)
                except Exception as e:
                    skipped += 1
                    log.warning(f"[{total}] erro ao converter binário: {e}")
                    continue
                if not b:
                    skipped += 1
                    # '0x' ou vazio
                    continue

                ext = guess_image_ext(b)
                if ONLY_IMAGES and ext is None:
                    skipped += 1
                    # não é imagem reconhecida
                    continue
                if ext is None:
                    # se quiser salvar mesmo não reconhecido, troque para '.bin'
                    ext = ".bin"

                base_name = f"31362-{cliente_id}"
                out_path = unique_path(DEST_DIR, base_name, ext)

                if SKIP_IF_EXISTS and out_path.exists():
                    saved += 1
                    continue

                out_path.write_bytes(b)
                saved += 1

                if saved % 100 == 0:
                    log.info(f"{saved} arquivos salvos...")

    print(f"Concluído. Processados: {total} | salvos: {saved} | pulados/ignorados: {skipped}")
    print(f"Arquivos em: {DEST_DIR}")

if __name__ == "__main__":
    export_fotos()
