import pandas as pd
import csv

csv.field_size_limit(2**31-1)
chunk_size = 100000  # Number of rows per output file

reader = pd.read_csv(
    r"E:\Migracoes\Schema_12654\Exportação de dados\Exportação de dados\CSV\HISTORICOLAUDO_202504251317.csv",
    sep=';', engine='python', chunksize=chunk_size
)

for i, chunk in enumerate(reader):
    out_path = f"E:/Migracoes/Schema_12654/Exportação de dados/Exportação de dados/CSV/HISTORICOLAUDO_202504251317_part{i+1}.csv"
    chunk.to_csv(out_path, sep=';', index=False)
    print(f"Saved {out_path}")