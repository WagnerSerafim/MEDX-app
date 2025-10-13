import os
import pandas as pd

# Solicita o diretório ao usuário
base_dir = input("Informe o caminho do diretório base: ").strip()

registros = []

for pasta in os.listdir(base_dir):
    pasta_path = os.path.join(base_dir, pasta)
    if not os.path.isdir(pasta_path):
        continue

    # Extrai o id do cliente do nome da pasta (após o "_")
    try:
        id_cliente = pasta.split("_", 1)[1]
    except IndexError:
        id_cliente = ""

    for arquivo in os.listdir(pasta_path):
        if arquivo == "folder.fld":
            continue

        classe = f"{pasta}/{arquivo}"
        registros.append({
            "Id do cliente": id_cliente,
            "Histórico": arquivo,
            "Classe": classe,
            "Data": "1900-01-01 00:00:00"
        })

# Cria o DataFrame e salva em Excel
df = pd.DataFrame(registros)
excel_path = os.path.join(base_dir, "planilha_historicos.xlsx")
df.to_excel(excel_path, index=False)

print(f"Planilha criada em: {excel_path}")