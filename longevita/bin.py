from pathlib import Path

pasta = Path(r"D:\Medxdata\imagens_exportadas")


def detectar_extensao_por_assinatura(path: Path) -> str | None:
    with path.open("rb") as f:
        inicio = f.read(32)

    if inicio.startswith(b"\xff\xd8\xff"):
        return ".jpg"

    if inicio.startswith(b"\x89PNG\r\n\x1a\n"):
        return ".png"

    if inicio.startswith(b"GIF87a") or inicio.startswith(b"GIF89a"):
        return ".gif"

    if inicio.startswith(b"%PDF"):
        return ".pdf"

    if inicio.startswith(b"BM"):
        return ".bmp"

    if inicio.startswith(b"RIFF") and b"WEBP" in inicio:
        return ".webp"

    if inicio.startswith(b"II*\x00") or inicio.startswith(b"MM\x00*"):
        return ".tif"

    return None


total_bin = 0
renomeados = 0
desconhecidos = 0

for arquivo in pasta.rglob("*.bin"):
    total_bin += 1

    nova_ext = detectar_extensao_por_assinatura(arquivo)

    if not nova_ext:
        desconhecidos += 1
        print(f"Desconhecido: {arquivo.name}")
        continue

    novo_caminho = arquivo.with_suffix(nova_ext)

    contador = 1
    while novo_caminho.exists():
        novo_caminho = arquivo.with_name(f"{arquivo.stem}_{contador}{nova_ext}")
        contador += 1

    arquivo.rename(novo_caminho)
    renomeados += 1
    print(f"Renomeado: {arquivo.name} -> {novo_caminho.name}")

print("\nResumo:")
print(f"Arquivos .bin encontrados: {total_bin}")
print(f"Renomeados: {renomeados}")
print(f"Continuaram desconhecidos: {desconhecidos}")