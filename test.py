import pandas as pd
from datetime import datetime


df = pd.read_excel(r"E:\Migracoes\Schema_35317_Linx\Extracao_74489_6909\Pacientes.xlsx")

date_str = df['DataNascimento'][3][:10].strip()
print(date_str)


date = datetime.strptime(date_str, '%m/%d/%Y')
date = date.strftime('%Y/%m/%d')

print(date)
print(len(df))
