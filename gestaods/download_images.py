import os
import pandas as pd
import requests
from urllib.parse import urlparse

# Caminho do Excel
excel_path = r"D:\Migracoes\Schema_36275_GestaoDS\fa160b989dc4c1daed94aa5209912280 (2)\pacientes_arquivos.xlsx"

# Pasta onde os arquivos serão salvos
output_folder = r"D:\Migracoes\Schema_36275_GestaoDS\fa160b989dc4c1daed94aa5209912280 (2)\anexos"

# Cria a pasta se ela não existir
os.makedirs(output_folder, exist_ok=True)

# Lê o Excel
df = pd.read_excel(excel_path)

# Verifica se existe a coluna 'Arquivo'
if "Arquivo" not in df.columns:
    raise Exception("A coluna 'Arquivo' não existe no Excel.")

for index, url in enumerate(df["Arquivo"]):
    if pd.isna(url):
        continue

    try:
        # Pega nome original do arquivo da URL
        filename = os.path.basename(urlparse(url).path)

        # Caminho completo para salvar
        save_path = os.path.join(output_folder, filename)

        print(f"Baixando ({index+1}): {filename}")

        # Faz o download
        response = requests.get(url, timeout=30)

        # Salva o arquivo
        with open(save_path, "wb") as f:
            f.write(response.content)

    except Exception as e:
        print(f"Erro ao baixar {url}: {e}")

print("\n✔️ Download concluído!")
