import os

from sqlalchemy import MetaData, Table, insert, text
from sqlalchemy.orm import sessionmaker

from medsystem.core.db import build_source_engine, build_target_engine
from medsystem.core.helpers import mount_phone, normalize_birthdate, normalize_sex
from medsystem.core.logs import write_jsonl_log
from utils.utils import clean_string, limpar_cpf, limpar_numero, truncate_value, verify_nan


BATCH_SIZE = 1000



def main():
    source_engine, source_database = build_source_engine()
    target_engine, sid, dbase = build_target_engine()
    log_folder = input("Informe a pasta para salvar os logs: ").strip()
    os.makedirs(log_folder, exist_ok=True)

    print("Conectando e preparando metadados de destino...")
    metadata = MetaData()
    contatos_tbl = Table("Contatos", metadata, schema=f"schema_{sid}", autoload_with=target_engine)

    SessionLocal = sessionmaker(bind=target_engine, future=True)

    inserted_log = []
    not_inserted_log = []

    inserted_count = 0
    not_inserted_count = 0
    total_processed = 0

    with SessionLocal() as session:
        existing_ids_result = session.execute(text(f"SELECT [Id do Cliente] FROM [schema_{sid}].[Contatos]"))
        existing_ids = {str(row[0]) for row in existing_ids_result if row[0] is not None}

        print(f"IDs existentes carregados em memória: {len(existing_ids)}")

        source_query = text(
            """
            SELECT
                [Código] AS CODIGO,
                [CodSinco] AS CODSINCO,
                [Nome] AS NOME,
                [Sexo] AS SEXO,
                [CPF] AS CPF,
                [Data de Nascimento] AS DATA_NASCIMENTO,
                [Endereço] AS ENDERECO,
                [Numero_Logradouro] AS NUMERO,
                [Complemento_Logradouro] AS COMPLEMENTO,
                [Bairro] AS BAIRRO,
                [Cidade] AS CIDADE,
                COALESCE([UF], [Estado]) AS ESTADO,
                [CEP] AS CEP,
                [DDDTelefone1] AS DDD1,
                [Telefone 1] AS TELEFONE1,
                [DDDTelefone2] AS DDD2,
                [Telefone 2] AS TELEFONE2,
                [E-Mail] AS EMAIL,
                [Observações] AS OBSERVACOES,
                [Pai] AS PAI,
                [RG] AS RG
            FROM [dbo].[SWClinica]
            WHERE [Apagado] = 0 OR [Apagado] IS NULL
            ORDER BY [Código]
            """
        )

        payload = []

        with source_engine.connect().execution_options(stream_results=True) as source_conn:
            rows = source_conn.execute(source_query).mappings()

            for row in rows:
                total_processed += 1

                try:
                    id_patient = limpar_numero(verify_nan(row["CODIGO"]))
                    if id_patient is None:
                        not_inserted_count += 1
                        not_inserted_log.append({**dict(row), "Motivo": "Id do Cliente vazio"})
                        continue

                    id_key = str(id_patient)
                    if id_key in existing_ids:
                        not_inserted_count += 1
                        not_inserted_log.append({**dict(row), "Motivo": "Id do Cliente já existe"})
                        continue

                    name = verify_nan(row["NOME"])
                    if name is None:
                        not_inserted_count += 1
                        not_inserted_log.append({**dict(row), "Motivo": "Nome do Paciente vazio"})
                        continue

                    clean_name = clean_string(name)
                    birthday = normalize_birthdate(row["DATA_NASCIMENTO"])
                    sex = normalize_sex(row["SEXO"])

                    telephone = mount_phone(row["DDD1"], row["TELEFONE1"])
                    cellphone = mount_phone(row["DDD2"], row["TELEFONE2"])

                    email = verify_nan(row["EMAIL"])
                    cpf = limpar_cpf(verify_nan(row["CPF"]))
                    rg = limpar_numero(verify_nan(row["RG"]))
                    cep = limpar_numero(verify_nan(row["CEP"]))
                    complement = verify_nan(row["COMPLEMENTO"])
                    neighbourhood = verify_nan(row["BAIRRO"])
                    city = verify_nan(row["CIDADE"])
                    state = verify_nan(row["ESTADO"])
                    father = verify_nan(row["PAI"])
                    observation = verify_nan(row["OBSERVACOES"])

                    address = verify_nan(row["ENDERECO"])
                    number = limpar_numero(verify_nan(row["NUMERO"]))
                    if address and number:
                        address = f"{address} {number}"

                    rec = {
                        "Id do Cliente": id_patient,
                        "Nome": truncate_value(clean_name, 50),
                        "Nascimento": birthday,
                        "Sexo": sex,
                        "Celular": truncate_value(cellphone, 25),
                        "Email": truncate_value(email, 100),
                        "CPF/CGC": truncate_value(cpf, 25),
                        "Observação": truncate_value(observation, 255),
                        "Referências": limpar_numero(verify_nan(row["CODSINCO"])),
                        "Cep Residencial": truncate_value(cep, 10),
                        "Endereço Residencial": truncate_value(address, 50),
                        "Endereço Comercial": truncate_value(complement, 50),
                        "Bairro Residencial": truncate_value(neighbourhood, 25),
                        "Cidade Residencial": truncate_value(city, 25),
                        "Estado Residencial": truncate_value(state, 2),
                        "Telefone Residencial": truncate_value(telephone, 25),
                        "Pai": truncate_value(father, 50),
                        "RG": truncate_value(rg, 25),
                    }

                    payload.append((id_key, rec))

                    if len(payload) >= BATCH_SIZE:
                        try:
                            records_to_insert = [item[1] for item in payload]
                            session.execute(insert(contatos_tbl), records_to_insert)
                            session.commit()
                            inserted_count += len(records_to_insert)
                            inserted_log.extend(records_to_insert)
                            for committed_id, _ in payload:
                                existing_ids.add(committed_id)
                            payload.clear()
                        except Exception as e:
                            session.rollback()
                            for _, failed in payload:
                                not_inserted_count += 1
                                not_inserted_log.append({**failed, "Motivo": f"Falha no commit do lote: {e}"})
                            payload.clear()

                    if total_processed % 1000 == 0:
                        print(
                            f"Processados: {total_processed} | "
                            f"Inseridos: {inserted_count} | "
                            f"Não inseridos: {not_inserted_count}"
                        )

                except Exception as e:
                    not_inserted_count += 1
                    not_inserted_log.append({**dict(row), "Motivo": str(e)})

        if payload:
            try:
                records_to_insert = [item[1] for item in payload]
                session.execute(insert(contatos_tbl), records_to_insert)
                session.commit()
                inserted_count += len(records_to_insert)
                inserted_log.extend(records_to_insert)
                for committed_id, _ in payload:
                    existing_ids.add(committed_id)
                payload.clear()
            except Exception as e:
                session.rollback()
                for _, failed in payload:
                    not_inserted_count += 1
                    not_inserted_log.append({**failed, "Motivo": f"Falha no commit do lote final: {e}"})
                payload.clear()

    inserted_log_path = write_jsonl_log(inserted_log, log_folder, f"log_inserted_patients_medsystem_{dbase}.jsonl")
    not_inserted_log_path = write_jsonl_log(
        not_inserted_log,
        log_folder,
        f"log_not_inserted_patients_medsystem_{dbase}.jsonl",
    )

    print("\nMigração finalizada!")
    print(f"Origem: {source_database}")
    print(f"Total processados: {total_processed}")
    print(f"Inseridos: {inserted_count}")
    print(f"Não inseridos: {not_inserted_count}")
    print(f"Log de inseridos: {inserted_log_path}")
    print(f"Log de não inseridos: {not_inserted_log_path}")


if __name__ == "__main__":
    main()
