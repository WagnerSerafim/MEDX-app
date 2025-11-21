import os
import pandas as pd
import requests
from urllib.parse import urlparse

# Caminho do Excel
excel_path = r"D:\Migracoes\Schema_36275_GestaoDS\fa160b989dc4c1daed94aa5209912280 (2)\pacientes.xlsx"

# Pasta onde os arquivos serão salvos
output_folder = r"D:\Migracoes\Schema_36275_GestaoDS\fa160b989dc4c1daed94aa5209912280 (2)\fotos"

# Cria a pasta se ela não existir
os.makedirs(output_folder, exist_ok=True)

# Lê o Excel
df = pd.read_excel(excel_path)

# Verifica se existe a coluna 'Foto'
if "Foto" not in df.columns:
    raise Exception("A coluna 'Foto' não existe no Excel.")

for index, row in df.iterrows():
    url = row["Foto"]
    cod = row["Cod Paciente"]

    if pd.isna(url):
        continue

    try:
        # Extrai extensão da imagem pela URL
        ext = os.path.splitext(urlparse(url).path)[1]  # ".jpeg", ".jpg", ".png", etc.

        # Nome correto do arquivo
        filename = f"36275-{cod}{ext}"

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
