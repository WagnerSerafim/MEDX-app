from pathlib import Path
import shutil


def remover_mais(nome: str) -> str:
    return nome.replace("+", "")


def garantir_nome_unico(caminho: Path) -> Path:
    if not caminho.exists():
        return caminho

    stem = caminho.stem
    suffix = caminho.suffix
    parent = caminho.parent
    contador = 1

    while True:
        candidato = parent / f"{stem}({contador}){suffix}"
        if not candidato.exists():
            return candidato
        contador += 1


def renomear_pastas(diretorio_origem: Path):
    """
    Renomeia apenas as pastas, das mais profundas para as mais rasas.
    """
    pastas = sorted(
        [p for p in diretorio_origem.rglob("*") if p.is_dir()],
        key=lambda p: len(p.parts),
        reverse=True
    )

    for pasta in pastas:
        novo_nome = remover_mais(pasta.name)
        if novo_nome != pasta.name:
            novo_caminho = pasta.with_name(novo_nome)

            if novo_caminho.exists():
                print(f"[AVISO] Pasta destino já existe, ignorando: {novo_caminho}")
                continue

            try:
                pasta.rename(novo_caminho)
                print(f"[PASTA RENOMEADA] {pasta} -> {novo_caminho}")
            except Exception as e:
                print(f"[ERRO] Falha ao renomear pasta {pasta}: {e}")


def processar_arquivos(origem: Path, destino_base: Path):
    """
    Renomeia os arquivos com '+' e copia somente esses arquivos
    para o destino, preservando a estrutura relativa.
    """
    arquivos = list(origem.rglob("*"))

    for arquivo in arquivos:
        if not arquivo.exists() or not arquivo.is_file():
            continue

        nome_original = arquivo.name
        novo_nome = remover_mais(nome_original)
        caminho_atual = arquivo

        if novo_nome != nome_original:
            novo_caminho = arquivo.with_name(novo_nome)

            if novo_caminho.exists():
                novo_caminho = garantir_nome_unico(novo_caminho)

            try:
                arquivo.rename(novo_caminho)
                print(f"[ARQUIVO RENOMEADO] {arquivo} -> {novo_caminho}")
                caminho_atual = novo_caminho
            except Exception as e:
                print(f"[ERRO] Falha ao renomear arquivo {arquivo}: {e}")
                continue

            relativo = caminho_atual.relative_to(origem)
            destino_final = destino_base / relativo
            destino_final.parent.mkdir(parents=True, exist_ok=True)
            destino_final = garantir_nome_unico(destino_final)

            try:
                shutil.copy2(str(caminho_atual), str(destino_final))
                print(f"[COPIADO] {caminho_atual} -> {destino_final}")
            except Exception as e:
                print(f"[ERRO] Falha ao copiar {caminho_atual}: {e}")


def processar_diretorio(diretorio_origem: str, diretorio_destino: str):
    origem = Path(diretorio_origem).resolve()
    destino = Path(diretorio_destino).resolve()

    if not origem.exists() or not origem.is_dir():
        raise ValueError(f"Diretório de origem inválido: {origem}")

    destino.mkdir(parents=True, exist_ok=True)

    print(f"[INÍCIO] Origem: {origem}")
    print(f"[INÍCIO] Destino: {destino}")

    renomear_pastas(origem)
    processar_arquivos(origem, destino)

    print("[FIM] Processamento concluído.")


if __name__ == "__main__":
    diretorio_origem = input("Informe o diretório de origem: ").strip().strip('"')
    diretorio_destino = input("Informe o diretório de destino: ").strip().strip('"')

    processar_diretorio(diretorio_origem, diretorio_destino)