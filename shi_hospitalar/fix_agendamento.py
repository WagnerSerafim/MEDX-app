import csv

input_file = r"C:\Users\WJSur\Documents\migracoes_07_07_25\32110\Agendamento.csv"
output_file = r"C:\Users\WJSur\Documents\migracoes_07_07_25\32110\Agendamento_fixed.csv"

with open(input_file, mode='r', encoding='utf-8') as infile, open(output_file, mode='w', encoding='utf-8', newline='') as outfile:
    reader = csv.reader(infile, delimiter=';')
    writer = csv.writer(outfile, delimiter=';')

    for row in reader:
        if row:  # ignora linhas vazias
            # Adiciona aspas apenas ao primeiro campo
            row[0] = f'"{row[0]}"'
        writer.writerow(row)

print(f'✅ Arquivo corrigido salvo em: {output_file}')
