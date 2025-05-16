import pandas as pd


df = pd.read_xml(r"E:\Análise Backup\george valda\data-133907148038772453\files.xml")

print(df.head())
print(len(df))
