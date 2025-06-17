import dask.dataframe as dd
import pandas as pd
import csv

csv.field_size_limit(10000000)

# Caminho do arquivo CSV
input_csv = r"C:\Users\WJSur\Documents\migração_amigo\export_3754\records.csv"

# Defina o arquivo de saída
output_csv = r"C:\Users\WJSur\Documents\migração_amigo\export_3754\records_2.csv"

# Abrir o arquivo CSV original e criar um arquivo de saída limpo
with open(input_csv, 'r', encoding='utf-8') as infile, open(output_csv, 'w', newline='', encoding='utf-8') as outfile:
    # Criar o leitor e escritor CSV
    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    
    # Escrever o cabeçalho no arquivo de saída
    writer.writeheader()
    
    # Iterar sobre as linhas do arquivo
    for row in reader:
        # Se a coluna 'text' existir, substitua as quebras de linha por espaços
        if 'text' in row:
            row['text'] = row['text'].replace('\n', ' ').replace('\r', ' ')
        
        # Escrever a linha limpa no arquivo de saída
        writer.writerow(row)

# Agora o arquivo 'arquivo_limpo.csv' está sem quebras de linha na coluna 'text'
print(f"Arquivo limpo foi salvo como {output_csv}")


# Lê o CSV com Dask, ajustando os parâmetros de leitura
df = dd.read_csv(output_csv, dtype={'extra': 'object', 'subtype': 'object', 'id': int, 'title': 'object', 'created_at': 'object', 'updated_at': 'object', 'deleted_at': 'object', 'user_id': int, 'patient_id': int, 'record_type_id': 'object', 'record_subtype_id': 'object', 'record_extra_id': 'object', 'record_extra_subtype_id': 'object'}, 
                 quoting=1, on_bad_lines='skip', engine='python', assume_missing=True)

df = df.replace({r'\n': ' '}, regex=True)

print(f"Total de partições: {df.npartitions}")

# Conta o número de linhas
num_linhas = df.compute().shape[0]
print(f"Total de linhas no arquivo (computado): {num_linhas}")

