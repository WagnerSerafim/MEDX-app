import urllib

from sqlalchemy import create_engine


def build_source_engine() -> tuple:
    print("Informe os dados do banco de origem (MEDSYSTEM / SQL Server local)")
    source_server = input("Servidor/instância (ex: localhost\\MSSQLSERVER01): ").strip() or "localhost\\MSSQLSERVER01"
    source_database = input("Database de origem [BKP_Albanita]: ").strip() or "BKP_Albanita"

    source_url = (
        f"mssql+pyodbc://@{source_server}/{source_database}"
        "?driver=ODBC+Driver+17+for+SQL+Server"
        "&Trusted_Connection=yes"
        "&Encrypt=yes"
        "&TrustServerCertificate=yes"
    )
    return create_engine(source_url), source_database


def build_target_engine() -> tuple:
    print("\nInforme os dados do banco de destino (MEDX Azure)")
    sid = input("Informe o SoftwareID: ").strip()
    password = urllib.parse.quote_plus(input("Informe a senha: "))
    dbase = input("Informe o DATABASE: ").strip()

    target_url = (
        f"mssql+pyodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}"
        "?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"
    )
    return create_engine(target_url, fast_executemany=True), sid, dbase
