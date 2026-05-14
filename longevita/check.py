from pathlib import Path

arquivo = Path(r"D:\Medxdata\imagens_exportadas\240_052.414.096-02_prontuario_200_191_15_02_2022_jpg_JPG.jpg")

with arquivo.open("rb") as f:
    inicio = f.read(20)

print(inicio)
print(inicio.hex())