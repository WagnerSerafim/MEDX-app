import csv

def limpar_quebras_csv(input_path, output_path):
    with open(input_path, 'r', encoding='utf-8') as f:
        conteudo = f.read()

    def substituir_quebras(texto):
        resultado = []
        dentro_de_aspas = False
        i = 0
        while i < len(texto):
            char = texto[i]
            if char == '"':
                if i + 1 < len(texto) and texto[i+1] == '"':
                    resultado.append('"')
                    i += 1
                else:
                    dentro_de_aspas = not dentro_de_aspas
                    resultado.append(char)
            elif char == '\n' and dentro_de_aspas:
                resultado.append('<br>')
            else:
                resultado.append(char)
            i += 1
        return ''.join(resultado)

    conteudo_limpo = substituir_quebras(conteudo)

    # Garante aspas duplas nos campos com <br>
    linhas = []
    reader = csv.reader(conteudo_limpo.splitlines())
    for row in reader:
        nova_row = []
        for campo in row:
            if '<br>' in campo and not (campo.startswith('"') and campo.endswith('"')):
                campo = f'"{campo.replace("\"", "\"\"")}"'
            nova_row.append(campo)
        linhas.append(nova_row)

    with open(output_path, 'w', encoding='utf-8', newline='') as f_out:
        writer = csv.writer(f_out)
        writer.writerows(linhas)

# Exemplo de uso:
limpar_quebras_csv(
    r'E:\Migracoes\Schema_35896\BKP_do_Sistema\BKP do Sistema\consultas realizadas.csv',
    r'E:\Migracoes\Schema_35896\BKP_do_Sistema\BKP do Sistema\consultas_realizadas_corrigido.csv'
)