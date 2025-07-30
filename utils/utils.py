from datetime import datetime
import os
import re
import pandas as pd
import math

def verify_nan(value):
    """Verifica se o valor é NaN ou None e retorna None."""
    if value in [None, '', 'None', 'nan', 'NaN', 'NAN', 'NULL', 'null'] or pd.isna(value):
        return ''
    return value

def exists(session, id, id_table, table):
    return session.query(table).filter(getattr(table, id_table)==id).first()

def is_valid_date(date_str, date_format):
    if date_str in ["", None]:
        return False
    # Converte para string se for datetime ou pd.Timestamp
    if isinstance(date_str, (datetime, pd.Timestamp)):
        date_str = date_str.strftime(date_format)
    try:
        date_str = str(date_str)
        if "/" in date_str:
            date_str = date_str.replace("/", "-")
        date_obj = datetime.strptime(date_str, date_format)
        if date_format in ["%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"]:
            if (1900 <= date_obj.year <= 2100) and (1 <= date_obj.month <= 12) and (1 <= date_obj.day <= 31) and \
               (0 <= date_obj.hour <= 23) and (0 <= date_obj.minute <= 59) and (0 <= date_obj.second <= 59):
                return True
        elif date_format in ["%d-%m-%Y %H:%M", "%Y-%m-%d %H:%M"]:
            if (1900 <= date_obj.year <= 2100) and (1 <= date_obj.month <= 12) and (1 <= date_obj.day <= 31) and \
               (0 <= date_obj.hour <= 23) and (0 <= date_obj.minute <= 59):
                return True
        else:
            if (1900 <= date_obj.year <= 2100) and (1 <= date_obj.month <= 12) and (1 <= date_obj.day <= 31):
                return True
    except ValueError as e:
        print(f"Erro de valor na data {date_str}: {e}")
        return False
    except TypeError as e:
        print(f"Erro de tipo na data {date_str}: {e}")
        return False

def truncate_value(value, max_length):
    """Se o valor for maior que max_length, ele será truncado"""
    if value is None or value == "":
        return None
    return str(value)[:max_length]

def replace_null_with_empty_string(data):
    """Substitui valores nulos por strings vazias JSONs, serve para listas e dicionários num geral."""

    if isinstance(data, dict): 
        return {key: replace_null_with_empty_string(value) for key, value in data.items()}
    elif isinstance(data, list):  
        return [replace_null_with_empty_string(item) for item in data]
    elif data is None:
        return ""
    else:
        return data
    
def create_log(log_data, log_folder, log_name):
    """Cria um arquivo de log.xlsx na mesma pasta do arquivo de entrada."""
    log_df = pd.DataFrame(log_data)
    log_path = os.path.join(log_folder, log_name)
    log_df.to_excel(log_path, index=False)

def verify_column_exists(column_name, df, row):
        """ Verifica se a coluna existe no DataFrame"""
        if column_name in df.columns:
            generic_var = row[column_name]
            return generic_var
        else:
            return ''

def clean_value(value):
    # Converte 'nan', '', None, numpy.nan para None
    if value in [None, '', 'None']:
        return None
    try:
        if isinstance(value, float) and math.isnan(value):
            return None
    except:
        pass
    if str(value).lower() == 'nan':
        return None
    return value

def clean_caracters(value):
    if isinstance(value, str):
        # Remove caracteres de controle ilegais no Excel (exceto \t, \n)
        return re.sub(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]", "", value)
    return value

def fixing_csv(input_path, output_path):
    """
    Corrige CSVs onde campos possuem aspas duplas internas não escapadas,
    escapando corretamente para evitar erro de parsing no pandas.
    """
    import re

    def escape_inner_quotes(field):
        # Remove as aspas duplas do início/fim (delimitadoras)
        if field.startswith('"') and field.endswith('"'):
            inner = field[1:-1]
            # Escapa aspas duplas internas
            inner = inner.replace('"', '""')
            return f'"{inner}"'
        return field

    with open(input_path, 'r', encoding='utf-8') as infile, open(output_path, 'w', encoding='utf-8', newline='') as outfile:
        for line in infile:
            # Divide a linha em campos, considerando aspas duplas
            fields = []
            current = ''
            in_quotes = False
            for c in line:
                if c == '"':
                    in_quotes = not in_quotes
                    current += c
                elif c == ';' and not in_quotes:
                    fields.append(current)
                    current = ''
                else:
                    current += c
            if current:
                fields.append(current.rstrip('\n'))

            # Escapa aspas duplas internas em cada campo delimitado por aspas duplas
            fields = [escape_inner_quotes(f) for f in fields]
            outfile.write(';'.join(fields) + '\n')



