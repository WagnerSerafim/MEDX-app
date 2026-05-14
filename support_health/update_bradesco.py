import os
import json
import urllib.parse
from datetime import datetime

import pandas as pd
import pyodbc


CONVENIO_ID_BRADESCO = 1399454438
CSV_PATH = r"D:\Migracoes\35893_Easy_Health\pacientes.csv"
CHUNK_SIZE = 2000


def agora_iso():
    return datetime.now().isoformat(timespec="seconds")


def garantir_pasta_logs(base_dir: str) -> dict:
    logs_dir = os.path.join(base_dir, "LOGS")
    jsonl_dir = os.path.join(logs_dir, "jsonl")
    os.makedirs(jsonl_dir, exist_ok=True)

    return {
        "logs_dir": logs_dir,
        "jsonl_dir": jsonl_dir,
        "execucao": os.path.join(jsonl_dir, "execucao_convenio_bradesco.jsonl"),
        "alteracoes": os.path.join(jsonl_dir, "contatos_convenio_bradesco_alterados.jsonl"),
        "ignorados": os.path.join(jsonl_dir, "contatos_convenio_bradesco_ignorados.jsonl"),
        "resumo": os.path.join(jsonl_dir, "resumo_convenio_bradesco.jsonl"),
    }


def escrever_jsonl(caminho: str, registros):
    if not registros:
        return
    with open(caminho, "a", encoding="utf-8") as f:
        for r in registros:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]


def conectar_sqlserver():
    sid = input("Informe o SoftwareID: ").strip()
    password = urllib.parse.quote_plus(input("Informe a senha: ").strip())
    dbase = input("Informe o DATABASE: ").strip()

    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=medxserver.database.windows.net,1433;"
        f"DATABASE={dbase};"
        f"UID=Medizin_{sid};"
        f"PWD={urllib.parse.unquote_plus(password)};"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
    )

    return pyodbc.connect(conn_str, autocommit=False), sid, dbase


def ler_csv_filtrar_bradesco(csv_path: str) -> pd.DataFrame:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {csv_path}")

    tentativas = [
        {"sep": ",", "encoding": "utf-8"},
        {"sep": ";", "encoding": "utf-8"},
        {"sep": ";", "encoding": "latin1"},
        {"sep": ",", "encoding": "latin1"},
    ]

    df = None
    ultimo_erro = None

    for t in tentativas:
        try:
            df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, **t)
            if "id_paciente" in df.columns and "convenio" in df.columns:
                break
        except Exception as e:
            ultimo_erro = e

    if df is None or "id_paciente" not in df.columns or "convenio" not in df.columns:
        raise ValueError(
            "Não foi possível ler o CSV corretamente ou as colunas "
            "'id_paciente' e 'convenio' não foram encontradas."
        ) from ultimo_erro

    df["id_paciente"] = df["id_paciente"].astype(str).str.strip()
    df["convenio"] = df["convenio"].astype(str).str.strip()

    df = df[
        df["id_paciente"].ne("")
        & df["convenio"].str.contains("bradesco", case=False, na=False)
    ].copy()

    df = df.drop_duplicates(subset=["id_paciente"])

    return df[["id_paciente", "convenio"]]


def criar_tabela_temporaria(cursor):
    cursor.execute("""
        IF OBJECT_ID('tempdb..#PacientesBradesco') IS NOT NULL
            DROP TABLE #PacientesBradesco;

        CREATE TABLE #PacientesBradesco (
            id_paciente BIGINT NOT NULL PRIMARY KEY,
            convenio_origem NVARCHAR(255) NULL
        );
    """)


def inserir_lote_temp(cursor, lote):
    if not lote:
        return

    dados = [(int(x["id_paciente"]), x["convenio"]) for x in lote]

    cursor.fast_executemany = True
    cursor.executemany("""
        INSERT INTO #PacientesBradesco (id_paciente, convenio_origem)
        VALUES (?, ?)
    """, dados)


def buscar_previa_alteracoes(cursor):
    cursor.execute("""
        SELECT
            c.[Id do Cliente] AS id_cliente,
            c.[Id do Convênio] AS convenio_atual,
            p.convenio_origem
        FROM Contatos c
        INNER JOIN #PacientesBradesco p
            ON p.id_paciente = c.[Id do Cliente]
        WHERE
            c.[Id do Convênio] IS NULL
            OR LTRIM(RTRIM(CAST(c.[Id do Convênio] AS NVARCHAR(255)))) = ''
    """)
    cols = [col[0] for col in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def buscar_ignorados(cursor):
    cursor.execute("""
        SELECT
            c.[Id do Cliente] AS id_cliente,
            c.[Id do Convênio] AS convenio_atual,
            p.convenio_origem,
            'Id do Convênio já preenchido' AS motivo
        FROM Contatos c
        INNER JOIN #PacientesBradesco p
            ON p.id_paciente = c.[Id do Cliente]
        WHERE
            c.[Id do Convênio] IS NOT NULL
            AND LTRIM(RTRIM(CAST(c.[Id do Convênio] AS NVARCHAR(255)))) <> ''
    """)
    cols = [col[0] for col in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def buscar_nao_encontrados(cursor):
    cursor.execute("""
        SELECT
            p.id_paciente,
            p.convenio_origem,
            'Paciente Bradesco não encontrado na tabela Contatos' AS motivo
        FROM #PacientesBradesco p
        LEFT JOIN Contatos c
            ON p.id_paciente = c.[Id do Cliente]
        WHERE c.[Id do Cliente] IS NULL
    """)
    cols = [col[0] for col in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def atualizar_convenio(cursor):
    cursor.execute("""
        UPDATE c
           SET c.[Id do Convênio] = ?
        FROM Contatos c
        INNER JOIN #PacientesBradesco p
            ON p.id_paciente = c.[Id do Cliente]
        WHERE
            c.[Id do Convênio] IS NULL
            OR LTRIM(RTRIM(CAST(c.[Id do Convênio] AS NVARCHAR(255)))) = ''
    """, CONVENIO_ID_BRADESCO)

    return cursor.rowcount


def main():
    base_dir = os.path.dirname(CSV_PATH)
    caminhos_logs = garantir_pasta_logs(base_dir)

    escrever_jsonl(caminhos_logs["execucao"], [{
        "timestamp": agora_iso(),
        "evento": "inicio_execucao",
        "arquivo_csv": CSV_PATH,
        "convenio_destino": CONVENIO_ID_BRADESCO,
        "chunk_size": CHUNK_SIZE
    }])

    conn = None

    try:
        df = ler_csv_filtrar_bradesco(CSV_PATH)
        total_bradesco_csv = len(df)

        escrever_jsonl(caminhos_logs["execucao"], [{
            "timestamp": agora_iso(),
            "evento": "csv_lido",
            "total_pacientes_bradesco_csv": total_bradesco_csv
        }])

        if df.empty:
            escrever_jsonl(caminhos_logs["resumo"], [{
                "timestamp": agora_iso(),
                "status": "sem_dados",
                "mensagem": "Nenhum paciente com convênio contendo 'Bradesco' foi encontrado no CSV."
            }])
            print("Nenhum paciente Bradesco encontrado no CSV.")
            return

        conn, sid, dbase = conectar_sqlserver()
        cursor = conn.cursor()

        total_alterados = 0
        total_ignorados = 0
        total_nao_encontrados = 0

        linhas = df.to_dict(orient="records")

        for idx, lote in enumerate(chunked(linhas, CHUNK_SIZE), start=1):
            criar_tabela_temporaria(cursor)
            inserir_lote_temp(cursor, lote)

            previas = buscar_previa_alteracoes(cursor)
            ignorados = buscar_ignorados(cursor)
            nao_encontrados = buscar_nao_encontrados(cursor)

            qtd_update = atualizar_convenio(cursor)

            conn.commit()

            timestamp_lote = agora_iso()

            alterados_lote = []
            for item in previas:
                alterados_lote.append({
                    "timestamp": timestamp_lote,
                    "lote": idx,
                    "id_cliente": item["id_cliente"],
                    "convenio_origem_csv": item["convenio_origem"],
                    "valor_antigo_id_convenio": item["convenio_atual"],
                    "valor_novo_id_convenio": CONVENIO_ID_BRADESCO,
                    "acao": "update"
                })

            ignorados_lote = []
            for item in ignorados:
                ignorados_lote.append({
                    "timestamp": timestamp_lote,
                    "lote": idx,
                    "id_cliente": item["id_cliente"],
                    "convenio_origem_csv": item["convenio_origem"],
                    "valor_atual_id_convenio": item["convenio_atual"],
                    "motivo": item["motivo"],
                    "acao": "ignorado"
                })

            nao_encontrados_lote = []
            for item in nao_encontrados:
                nao_encontrados_lote.append({
                    "timestamp": timestamp_lote,
                    "lote": idx,
                    "id_cliente": item["id_paciente"],
                    "convenio_origem_csv": item["convenio_origem"],
                    "motivo": item["motivo"],
                    "acao": "nao_encontrado"
                })

            escrever_jsonl(caminhos_logs["alteracoes"], alterados_lote)
            escrever_jsonl(caminhos_logs["ignorados"], ignorados_lote + nao_encontrados_lote)

            total_alterados += len(alterados_lote)
            total_ignorados += len(ignorados_lote)
            total_nao_encontrados += len(nao_encontrados_lote)

            escrever_jsonl(caminhos_logs["execucao"], [{
                "timestamp": timestamp_lote,
                "evento": "lote_processado",
                "lote": idx,
                "registros_no_lote": len(lote),
                "previstos_para_update": len(previas),
                "updates_executados_reportados": qtd_update,
                "ignorados": len(ignorados_lote),
                "nao_encontrados": len(nao_encontrados_lote)
            }])

        resumo = {
            "timestamp": agora_iso(),
            "status": "concluido",
            "software_id": sid,
            "database": dbase,
            "arquivo_csv": CSV_PATH,
            "convenio_destino": CONVENIO_ID_BRADESCO,
            "total_pacientes_bradesco_csv": total_bradesco_csv,
            "total_alterados": total_alterados,
            "total_ignorados_id_convenio_preenchido": total_ignorados,
            "total_nao_encontrados_em_contatos": total_nao_encontrados,
            "log_alteracoes": caminhos_logs["alteracoes"],
            "log_ignorados": caminhos_logs["ignorados"],
            "log_execucao": caminhos_logs["execucao"]
        }

        escrever_jsonl(caminhos_logs["resumo"], [resumo])

        print("\nProcesso concluído com sucesso.")
        print(json.dumps(resumo, ensure_ascii=False, indent=2))

    except Exception as e:
        if conn:
            conn.rollback()

        erro = {
            "timestamp": agora_iso(),
            "status": "erro",
            "mensagem": str(e)
        }
        escrever_jsonl(caminhos_logs["resumo"], [erro])
        escrever_jsonl(caminhos_logs["execucao"], [{
            "timestamp": agora_iso(),
            "evento": "erro",
            "mensagem": str(e)
        }])

        print("Erro durante a execução:")
        print(str(e))

    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()