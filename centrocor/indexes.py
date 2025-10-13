import pyodbc, re, binascii, logging, json, shutil
from pathlib import Path
from typing import Tuple, Optional

# ============ CONFIG ============
SERVER = "PC-WAGNER"
DATABASE = "REPOSITORIO"
TABLE = "dbo.tblAnexoCliente"

COL_BIN  = "strAnexo"          # blob (VARBINARY/hex textual)
COL_NAME = "strAnexoArquivo"   # nome base do arquivo
PK_COL   = "intAnexoClienteId"        # <<< AJUSTE para a chave primária crescente

DEST_DIR = Path(r"E:\Medxdata\31362")

# O que salvar
ALLOW_IMAGES = True
ALLOW_PDF    = True            # <<< Coloque False se NÃO quiser PDFs
ALLOWED_EXTRA = set()          # ex.: {".zip"} se quiser salvar .zip etc.

# Execução
BATCH_SIZE = 50
SKIP_IF_EXISTS = True          # pula se já houver arquivo (qualquer variação)
VERBOSE = True

# Proteção de disco: parar se espaço livre < 1 GB (ajuste)
MIN_FREE_BYTES = 1 * 1024 * 1024 * 1024

# ============ LOG ============
logging.basicConfig(level=logging.INFO if VERBOSE else logging.WARNING,
                    format="%(levelname)s | %(message)s")
log = logging.getLogger("export_anexos")

# ============ CONEXÃO ============
def pick_sqlserver_driver() -> str:
    preferred = ["ODBC Driver 19 for SQL Server","ODBC Driver 18 for SQL Server",
                 "ODBC Driver 17 for SQL Server","SQL Server"]
    installed = set(pyodbc.drivers())
    for d in preferred:
        if d in installed:
            return d
    raise RuntimeError(f"Instale um driver ODBC do SQL Server: {preferred}")

def conn_str(server: str, database: str) -> str:
    driver = pick_sqlserver_driver()
    return (f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};"
            f"Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes;")

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

def detect_ext(b: bytes) -> Optional[str]:
    # imagens
    if b.startswith(b"\x89PNG\r\n\x1a\n"): return ".png"
    if b.startswith(b"\xff\xd8\xff"):      return ".jpg"   # JPEG
    if b.startswith(b"GIF8"):              return ".gif"
    if b.startswith(b"BM"):                return ".bmp"
    if b[:4] in (b"II*\x00", b"MM\x00*"):  return ".tiff"  # TIFF
    if b[:4] == b"RIFF" and b[8:12] == b"WEBP": return ".webp"
    # pdf
    if b.startswith(b"%PDF"):              return ".pdf"
    # zip etc. (caso queira permitir via ALLOWED_EXTRA)
    if b[:4] == b"PK\x03\x04":            return ".zip"
    return None

def is_allowed_ext(ext: Optional[str]) -> bool:
    if ext is None: 
        return False
    e = ext.lower()
    if ALLOW_IMAGES and e in {".png",".jpg",".jpeg",".gif",".bmp",".tiff",".webp"}:
        return True
    if ALLOW_PDF and e == ".pdf":
        return True
    if e in {x.lower() for x in ALLOWED_EXTRA}:
        return True
    return False

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
    return name[:max_len].rstrip()

def split_name_ext(name: str) -> Tuple[str, str]:
    p = Path(name)
    base = p.stem
    ext = "".join(p.suffixes)
    return base if base else name, ext

def any_variant_exists(base_dir: Path, base_name: str, ext: str) -> bool:
    if (base_dir / f"{base_name}{ext}").exists():
        return True
    return any(base_dir.glob(f"{base_name}-*{ext}"))

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

# ============ CHECKPOINT / DISCO ============
def checkpoint_path() -> Path:
    return DEST_DIR / f".resume_{TABLE}.json"

def load_checkpoint() -> Optional[int]:
    p = checkpoint_path()
    if not p.exists():
        return None
    try:
        return int(json.loads(p.read_text(encoding="utf-8")).get("last_id"))
    except Exception:
        return None

def save_checkpoint(last_id: int):
    checkpoint_path().write_text(json.dumps({"last_id": last_id}, ensure_ascii=False),
                                 encoding="utf-8")

def has_free_space(min_free_bytes: int) -> bool:
    try:
        usage = shutil.disk_usage(DEST_DIR.drive or DEST_DIR.anchor)
        return usage.free >= min_free_bytes
    except Exception:
        return True

# ============ EXPORT ============
def export_files_with_resume():
    if not PK_COL:
        raise RuntimeError("Defina PK_COL com a chave primária crescente da tabela.")
    DEST_DIR.mkdir(parents=True, exist_ok=True)

    last_id = load_checkpoint()
    if last_id is not None:
        log.info(f"Retomando a partir de {PK_COL} > {last_id}")
    else:
        log.info("Iniciando do começo (sem checkpoint).")

    total = saved = skipped = 0
    max_id_seen = last_id or 0

    with pyodbc.connect(conn_str(SERVER, DATABASE), autocommit=True) as conn:
        cur = conn.cursor()
        cur.arraysize = BATCH_SIZE
        cur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;")

        while True:
            if not has_free_space(MIN_FREE_BYTES):
                log.error("Pouco espaço livre. Salvando checkpoint e saindo.")
                save_checkpoint(max_id_seen)
                break

            sql = (
                f"SELECT TOP ({BATCH_SIZE}) {PK_COL}, {COL_BIN}, {COL_NAME} "
                f"FROM {TABLE} WITH (NOLOCK) "
                f"WHERE {COL_BIN} IS NOT NULL AND {PK_COL} > ? "
                f"ORDER BY {PK_COL} ASC;"
            )
            rows = cur.execute(sql, max_id_seen).fetchall()
            if not rows:
                save_checkpoint(max_id_seen)
                break

            for (pk, binval, name_raw) in rows:
                max_id_seen = pk
                total += 1

                # decode blob
                try:
                    b = to_bytes(binval)
                except Exception:
                    skipped += 1
                    continue
                if not b:
                    skipped += 1
                    continue

                ext = detect_ext(b)
                if not is_allowed_ext(ext):
                    skipped += 1
                    continue

                # preferir a extensão do nome se for coerente; senão usar detectada
                name_clean = sanitize_filename(str(name_raw))
                base, ext_from_name = split_name_ext(name_clean)
                final_ext = ext
                if ext_from_name and ext_from_name.lower() == (ext or "").lower():
                    final_ext = ext_from_name
                elif ext_from_name and (ext is None):
                    final_ext = ext_from_name  # confia no nome quando detecção é None

                # pular se já existir qualquer variação (retomada sem duplicar)
                if SKIP_IF_EXISTS and any_variant_exists(DEST_DIR, base, final_ext):
                    saved += 1
                    continue

                out_path = unique_path(DEST_DIR, base, final_ext)
                out_path.write_bytes(b)
                saved += 1

                if saved % 100 == 0:
                    log.info(f"{saved} arquivos salvos... (last {PK_COL}={max_id_seen})")

            save_checkpoint(max_id_seen)

    print(f"Fim. Processados: {total} | salvos: {saved} | pulados: {skipped}")
    print(f"Checkpoint: {checkpoint_path()}")
    print(f"Pasta: {DEST_DIR}")

if __name__ == "__main__":
    export_files_with_resume()
