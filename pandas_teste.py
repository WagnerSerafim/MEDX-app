import pandas as pd

df = pd.read_csv("C:\\Users\\WJSur\\Documents\\iclinic_files\\06-12-2024-event_record.csv")
df["eventblock_pack"] = df["eventblock_pack"].str.replace("json::","")
print(df["eventblock_pack"][0])