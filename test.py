import asyncio
import json
import os
import sys
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool


TARGET_FILE_NAME = "log_contatos_unificados.xlsx"


def normalize_text(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().upper()


def normalize_situacao(value) -> str:
    return normalize_text(value)


def normalize_nome(value) -> str:
    return normalize_text(value)


def safe_int(value):
    if pd.isna(value):
        return None
    try:
        return int(float(str(value).strip()))
    except Exception:
        return None


def find_target_excel_files(base_path: str, target_file_name: str = TARGET_FILE_NAME) -> List[Path]:
    root = Path(base_path)
    if not root.exists():
        raise FileNotFoundError(f"Caminho não encontrado: {base_path}")

    if not root.is_dir():
        raise NotADirectoryError(f"O caminho informado não é uma pasta: {base_path}")

    matched_files = []
    for file_path in root.rglob(target_file_name):
        if file_path.is_file() and not file_path.name.startswith("~$"):
            matched_files.append(file_path)

    return sorted(matched_files)


def extract_mappings_from_dataframe(df: pd.DataFrame, source_file: str) -> Tuple[List[Dict], List[Dict]]:
    required_columns = ["Id do Cliente", "Situação", "Nome"]
    missing = [col for col in required_columns if col not in df.columns]

    if missing:
        raise ValueError(
            f"Arquivo '{source_file}' não contém as colunas obrigatórias: {missing}"
        )

    work_df = df[required_columns].copy()
    work_df["Id do Cliente"] = work_df["Id do Cliente"].apply(safe_int)
    work_df["Situação_norm"] = work_df["Situação"].apply(normalize_situacao)
    work_df["Nome_norm"] = work_df["Nome"].apply(normalize_nome)

    work_df = work_df[
        (work_df["Nome_norm"] != "") &
        (work_df["Id do Cliente"].notna()) &
        (work_df["Situação_norm"] != "")
    ].copy()

    mappings = []
    diagnostics = []

    for nome_norm, group in work_df.groupby("Nome_norm", dropna=False):
        group_records = group.to_dict(orient="records")

        final_rows = group[group["Situação_norm"] == "REGISTRO FINAL UNIFICADO"]
        removed_rows = group[group["Situação_norm"] == "REGISTRO DUPLICADO/REMOVIDO"]

        if final_rows.empty and removed_rows.empty:
            continue

        if final_rows.empty:
            diagnostics.append({
                "event": "nome_sem_registro_final_unificado",
                "source_file": source_file,
                "nome": nome_norm,
                "records_found": group_records,
                "timestamp": datetime.now().isoformat()
            })
            continue

        if removed_rows.empty:
            diagnostics.append({
                "event": "nome_sem_registro_duplicado_removido",
                "source_file": source_file,
                "nome": nome_norm,
                "records_found": group_records,
                "timestamp": datetime.now().isoformat()
            })
            continue

        if len(final_rows) > 1:
            diagnostics.append({
                "event": "multiplos_registros_finais_unificados",
                "source_file": source_file,
                "nome": nome_norm,
                "records_found": group_records,
                "timestamp": datetime.now().isoformat()
            })
            continue

        final_id = safe_int(final_rows.iloc[0]["Id do Cliente"])
        final_nome_original = str(final_rows.iloc[0]["Nome"])

        if final_id is None:
            diagnostics.append({
                "event": "id_final_invalido",
                "source_file": source_file,
                "nome": nome_norm,
                "records_found": group_records,
                "timestamp": datetime.now().isoformat()
            })
            continue

        for _, removed_row in removed_rows.iterrows():
            old_id = safe_int(removed_row["Id do Cliente"])

            if old_id is None:
                diagnostics.append({
                    "event": "id_removido_invalido",
                    "source_file": source_file,
                    "nome": nome_norm,
                    "record": dict(removed_row),
                    "timestamp": datetime.now().isoformat()
                })
                continue

            if old_id == final_id:
                diagnostics.append({
                    "event": "id_removido_igual_ao_id_final",
                    "source_file": source_file,
                    "nome": nome_norm,
                    "old_id": old_id,
                    "final_id": final_id,
                    "timestamp": datetime.now().isoformat()
                })
                continue

            mappings.append({
                "source_file": source_file,
                "nome": final_nome_original,
                "nome_normalizado": nome_norm,
                "old_id": old_id,
                "new_id": final_id,
                "old_situacao": "Registro duplicado/removido",
                "new_situacao": "Registro final unificado",
                "timestamp": datetime.now().isoformat()
            })

    return mappings, diagnostics


async def process_database_updates(database_url: str, mappings: List[Dict], log_path: str):
    engine = create_async_engine(
        database_url,
        echo=False,
        poolclass=NullPool,
        future=True,
    )

    total_updated_rows = 0
    processed_pairs = set()

    async with engine.connect() as conn:
        for item in mappings:
            pair_key = (item["old_id"], item["new_id"])
            if pair_key in processed_pairs:
                with open(log_path, "a", encoding="utf-8") as log_file:
                    log_file.write(json.dumps({
                        "event": "mapping_duplicado_ignorado",
                        **item,
                        "timestamp": datetime.now().isoformat()
                    }, ensure_ascii=False) + "\n")
                continue

            processed_pairs.add(pair_key)

            try:
                async with conn.begin():
                    count_stmt = text("""
                        SELECT COUNT(*) AS total
                        FROM [Exames_resultados]
                        WHERE [Id do Paciente] = :old_id
                    """)

                    count_result = await conn.execute(count_stmt, {"old_id": item["old_id"]})
                    affected_before = count_result.scalar_one()

                    if affected_before == 0:
                        with open(log_path, "a", encoding="utf-8") as log_file:
                            log_file.write(json.dumps({
                                "event": "nenhum_registro_para_atualizar",
                                **item,
                                "affected_rows_before": 0,
                                "affected_rows_after": 0,
                                "timestamp": datetime.now().isoformat()
                            }, ensure_ascii=False) + "\n")
                        continue

                    update_stmt = text("""
                        UPDATE [Exames_resultados]
                        SET [Id do Paciente] = :new_id
                        WHERE [Id do Paciente] = :old_id
                    """)

                    update_result = await conn.execute(update_stmt, {
                        "old_id": item["old_id"],
                        "new_id": item["new_id"]
                    })

                    affected_after = update_result.rowcount if update_result.rowcount is not None else affected_before
                    total_updated_rows += affected_after

                    with open(log_path, "a", encoding="utf-8") as log_file:
                        log_file.write(json.dumps({
                            "event": "update_realizado",
                            **item,
                            "affected_rows_before": affected_before,
                            "affected_rows_after": affected_after,
                            "timestamp": datetime.now().isoformat()
                        }, ensure_ascii=False) + "\n")

            except Exception as e:
                with open(log_path, "a", encoding="utf-8") as log_file:
                    log_file.write(json.dumps({
                        "event": "erro_ao_atualizar",
                        **item,
                        "error": str(e),
                        "timestamp": datetime.now().isoformat()
                    }, ensure_ascii=False) + "\n")

    await engine.dispose()
    return total_updated_rows


async def main():
    sid = input("Informe o SoftwareID: ")
    password = urllib.parse.quote_plus(input("Informe a senha: "))
    dbase = input("Informe o DATABASE: ")
    path_file = input("Informe o caminho da pasta que contém os arquivos: ")

    database_url = f"mssql+aioodbc://Medizin_{sid}:{password}@medxserver.database.windows.net:1433/{dbase}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no"

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.abspath(f"log_atualizacao_exames_resultados_{timestamp}.jsonl")

    with open(log_path, "a", encoding="utf-8") as log_file:
        log_file.write(json.dumps({
            "event": "inicio_processamento",
            "target_file_name": TARGET_FILE_NAME,
            "path_file": path_file,
            "database": dbase,
            "timestamp": datetime.now().isoformat()
        }, ensure_ascii=False) + "\n")

    try:
        excel_files = find_target_excel_files(path_file, TARGET_FILE_NAME)

        if not excel_files:
            with open(log_path, "a", encoding="utf-8") as log_file:
                log_file.write(json.dumps({
                    "event": "arquivo_alvo_nao_encontrado",
                    "target_file_name": TARGET_FILE_NAME,
                    "path_file": path_file,
                    "timestamp": datetime.now().isoformat()
                }, ensure_ascii=False) + "\n")

            print(f"Nenhum arquivo '{TARGET_FILE_NAME}' foi encontrado dentro da pasta informada.")
            print(f"Log gerado em: {log_path}")
            return

        all_mappings = []
        seen_file_errors = False

        for excel_file in excel_files:
            try:
                df = pd.read_excel(excel_file)
                mappings, diagnostics = extract_mappings_from_dataframe(df, str(excel_file))
                all_mappings.extend(mappings)

                with open(log_path, "a", encoding="utf-8") as log_file:
                    log_file.write(json.dumps({
                        "event": "arquivo_lido",
                        "source_file": str(excel_file),
                        "rows": int(len(df)),
                        "mappings_found": len(mappings),
                        "diagnostics_found": len(diagnostics),
                        "timestamp": datetime.now().isoformat()
                    }, ensure_ascii=False) + "\n")

                    for diag in diagnostics:
                        log_file.write(json.dumps(diag, ensure_ascii=False) + "\n")

            except Exception as e:
                seen_file_errors = True
                with open(log_path, "a", encoding="utf-8") as log_file:
                    log_file.write(json.dumps({
                        "event": "erro_ao_ler_arquivo",
                        "source_file": str(excel_file),
                        "error": str(e),
                        "timestamp": datetime.now().isoformat()
                    }, ensure_ascii=False) + "\n")

        if not all_mappings:
            with open(log_path, "a", encoding="utf-8") as log_file:
                log_file.write(json.dumps({
                    "event": "nenhum_mapping_encontrado",
                    "target_file_name": TARGET_FILE_NAME,
                    "total_files_found": len(excel_files),
                    "timestamp": datetime.now().isoformat()
                }, ensure_ascii=False) + "\n")

            print("Nenhum vínculo válido de unificação foi encontrado.")
            print(f"Log gerado em: {log_path}")
            return

        total_updated_rows = await process_database_updates(database_url, all_mappings, log_path)

        with open(log_path, "a", encoding="utf-8") as log_file:
            log_file.write(json.dumps({
                "event": "fim_processamento",
                "target_file_name": TARGET_FILE_NAME,
                "total_files_found": len(excel_files),
                "total_mappings": len(all_mappings),
                "total_updated_rows": total_updated_rows,
                "seen_file_errors": seen_file_errors,
                "timestamp": datetime.now().isoformat()
            }, ensure_ascii=False) + "\n")

        print("Processamento concluído com sucesso.")
        print(f"Arquivos '{TARGET_FILE_NAME}' encontrados: {len(excel_files)}")
        print(f"Mapeamentos encontrados: {len(all_mappings)}")
        print(f"Total de registros atualizados em [Exames_resultados]: {total_updated_rows}")
        print(f"Log gerado em: {log_path}")

    except Exception as e:
        with open(log_path, "a", encoding="utf-8") as log_file:
            log_file.write(json.dumps({
                "event": "erro_fatal",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }, ensure_ascii=False) + "\n")

        print(f"Erro fatal: {e}")
        print(f"Consulte o log em: {log_path}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())