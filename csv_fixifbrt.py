# Este script lê o arquivo CSV e substitui a aspa simples final por aspas duplas em cada linha.

input_path = r"e:\Migracoes\Schema_30911_gestaoDs\e25f794b35edd6f000afa3445004349d\atendimentos_corrigido.csv"
output_path = r"e:\Migracoes\Schema_30911_gestaoDs\e25f794b35edd6f000afa3445004349d\atendimentos_corrigido_ok.csv"

with open(input_path, "r", encoding="utf-8") as fin, open(output_path, "w", encoding="utf-8") as fout:
	for line in fin:
		# Remove o \n temporariamente para checar o último caractere real
		stripped = line.rstrip('\n\r')
		if stripped.endswith("'"):
			stripped = stripped[:-1] + '"'
			# Adiciona o \n de volta
			fout.write(stripped + '\n')
		else:
			fout.write(line)