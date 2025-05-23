import pandas as pd


df = pd.read_xml(r"E:\Migracoes\Schema_32622_MedWeb\data-133907148038772453\paciente.xml")

print(df.head())
print(df.columns)
print(len(df))
