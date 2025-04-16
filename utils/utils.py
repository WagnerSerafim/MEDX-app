from datetime import datetime
import os

import pandas as pd

def exists(session, id, id_table, table):
    return session.query(table).filter(getattr(table, id_table)==id).first()

def is_valid_date(date_str, date_format):
    if date_str in ["", None]:
        return False
    try:
        if "/" in date_str:
            date_str = date_str.replace("/", "-")
        
        date_obj = datetime.strptime(str(date_str), date_format)
        
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
    except ValueError:
        return False
    
    except TypeError:
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