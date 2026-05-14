from pathlib import Path

path = Path(r"C:\Users\WJSur\Downloads\stetic_imagens\stetic_imagens.sql")

maior_linha = 0
linha_numero = 0
total_linhas = 0
insert_lines = 0

with path.open("rb") as f:
    for i, line in enumerate(f, start=1):
        total_linhas += 1
        size = len(line)

        if line.lstrip().upper().startswith(b"INSERT INTO"):
            insert_lines += 1

        if size > maior_linha:
            maior_linha = size
            linha_numero = i

print(f"Total de linhas: {total_linhas}")
print(f"Linhas de INSERT: {insert_lines}")
print(f"Maior linha: {maior_linha / 1024 / 1024:.2f} MB")
print(f"Número da maior linha: {linha_numero}")