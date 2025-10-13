import pyodbc
import logging
from pathlib import Path
from datetime import datetime, date, time

# ===== CONFIG =====
SERVER = "PC-WAGNER"
DATABASE = "REPOSITORIO"
TABLE = "dbo.tblAnexoCliente"

# Colunas para o Excel (na ordem pedida)
COLS = {
    "Classe": "strAnexoArquivo",
    "Histórico": "strDescricao",
    "Data": "datAnexo",
    "Id do Cliente": "intClienteId",
}

DEST_XLSX_PATH = Path(r"E:\Migracoes\Schema_31362\anexos_export.xlsx")
BATCH_SIZE = 500
VERBOSE = True

logging.basicConfig(level=logging.INFO if VERBOSE else logging.WARNING,
                    format="%(levelname)s | %(message)s")
log = logging.getLogger("excel_export")

# ===== Conexão =====
def pick_sqlserver_driver():
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

def conn_str(server, database):
    driver = pick_sqlserver_driver()
    return (f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};"
            f"Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes;")

# ===== Datas =====
def to_datetime(v):
    """Converte o valor retornado do banco para datetime (para aplicar formato no Excel)."""
    if isinstance(v, datetime):
        return v
    if isinstance(v, date):
        return datetime.combine(v, time(0, 0, 0))
    if isinstance(v, str):
        s = v.strip().replace('Z', '')
        # tenta ISO completo (com hora)
        try:
            return datetime.fromisoformat(s)
        except Exception:
            pass
        # tenta só a data
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d")
        except Exception:
            return None
    return None

# ===== Exportação =====
def export_excel_only():
    from xlsxwriter import Workbook

    DEST_XLSX_PATH.parent.mkdir(parents=True, exist_ok=True)
    wb = None
    try:
        # Workbook streaming (memória constante)
        wb = Workbook(DEST_XLSX_PATH.as_posix(), {'constant_memory': True})
        ws = wb.add_worksheet("Anexos")

        # Cabeçalhos
        headers = list(COLS.keys())
        for c, h in enumerate(headers):
            ws.write(0, c, h)

        # Formato para data+hora
        date_fmt = wb.add_format({'num_format': 'yyyy-mm-dd hh:mm:ss'})

        # Consulta leve (sem strAnexo), em lotes, sem bloquear
        sql = (
            "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; "
            f"SELECT {', '.join(COLS.values())} "
            f"FROM {TABLE} WITH (NOLOCK) "
            "ORDER BY (SELECT NULL);"
        )

        total = 0
        next_row = 1
        with pyodbc.connect(conn_str(SERVER, DATABASE), autocommit=True) as conn:
            cur = conn.cursor()
            cur.arraysize = BATCH_SIZE
            log.info("Executando consulta (sem strAnexo)...")
            cur.execute(sql)

            while True:
                rows = cur.fetchmany(BATCH_SIZE)
                if not rows:
                    break

                for r in rows:
                    total += 1
                    classe, historico, data_val, id_cli = r

                    # Classe / Histórico
                    ws.write(next_row, 0, classe)
                    ws.write(next_row, 1, historico)

                    # Data com hora (se possível)
                    dt = to_datetime(data_val)
                    if dt is not None:
                        ws.write_datetime(next_row, 2, dt, date_fmt)
                    else:
                        # se não for possível converter, grava texto bruto
                        ws.write(next_row, 2, "" if data_val is None else str(data_val))

                    # Id do Cliente
                    ws.write(next_row, 3, id_cli)

                    next_row += 1

        log.info(f"Linhas escritas: {total}")
        print(f"OK! Excel salvo em: {DEST_XLSX_PATH}")

    finally:
        # fecha sempre, mesmo se ocorrer erro no meio
        if wb is not None:
            wb.close()

if __name__ == "__main__":
    export_excel_only()
