"""
Corrige CSVs onde campos com ponto e vírgula estão entre aspas simples,
trocando para aspas duplas e garantindo que o arquivo possa ser lido corretamente.
"""
import re

import pandas as pd

input_path = r"E:\Migracoes\Schema_30911_gestaoDs\e25f794b35edd6f000afa3445004349d\atendimentos.csv"
output_path = r"E:\Migracoes\Schema_30911_gestaoDs\e25f794b35edd6f000afa3445004349d\atendimentos_corrigido.csv"

with open(input_path, 'r', encoding='utf-8') as infile, open(output_path, 'w', encoding='utf-8', newline='') as outfile:
    for line in infile:
        # Substitui aspas simples por aspas duplas apenas em campos (não no cabeçalho)
        # Exemplo: ;'campo;com;pv';  => ;"campo;com;pv";
        # Só troca se o campo começa e termina com aspas simples
        fixed_line = re.sub(r";'([^']*)'", r';"\1"', line)
        # Também cobre o caso do campo ser o último da linha
        fixed_line = re.sub(r"'([^']*)'\s*$", r'"\1"\n', fixed_line)
        outfile.write(fixed_line)


df = pd.read_csv(output_path, sep=';', engine='python')
print(df.head())