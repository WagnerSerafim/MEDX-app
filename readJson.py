import argparse
import json
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import TextIO


DEFAULT_INPUT = Path(
    r"D:\wagner\backups\Backup_35412\Histórico de Clientes.json"
)
DEFAULT_CLIENT_ID = 26900
CHUNK_SIZE = 1024 * 1024 * 4


def _read_more(file: TextIO, buffer: str, eof: bool) -> tuple[str, bool]:
    if eof:
        return buffer, eof

    chunk = file.read(CHUNK_SIZE)
    if not chunk:
        return buffer, True

    return buffer + chunk, False


def iter_json_items(path: Path) -> Iterator[dict]:
    decoder = json.JSONDecoder()
    buffer = ""
    pos = 0
    eof = False
    inside_array = False
    started = False

    with path.open("r", encoding="utf-8-sig", newline="") as file:
        while True:
            if pos > CHUNK_SIZE and pos > len(buffer) // 2:
                buffer = buffer[pos:]
                pos = 0

            while pos >= len(buffer) and not eof:
                buffer, eof = _read_more(file, buffer, eof)

            while pos < len(buffer) and buffer[pos].isspace():
                pos += 1

            if pos >= len(buffer):
                if eof:
                    return
                continue

            if not started:
                if buffer[pos] == "[":
                    inside_array = True
                    pos += 1
                started = True
                continue

            if inside_array:
                while True:
                    while pos < len(buffer) and buffer[pos].isspace():
                        pos += 1

                    if pos >= len(buffer):
                        break

                    if buffer[pos] == ",":
                        pos += 1
                        continue

                    if buffer[pos] == "]": 
                        return

                    break

                if pos >= len(buffer):
                    if eof:
                        raise ValueError("JSON array terminou de forma inesperada.")
                    continue

            try:
                item, next_pos = decoder.raw_decode(buffer, pos)
            except json.JSONDecodeError:
                if eof:
                    raise
                buffer, eof = _read_more(file, buffer, eof)
                continue

            pos = next_pos

            if isinstance(item, dict):
                yield item
            elif not inside_array:
                raise ValueError("O JSON precisa ser um array de objetos ou objetos JSON sequenciais.")


def count_non_null_field(input_path: Path, field: str) -> int:
    count = 0
    for item in iter_json_items(input_path):
        value = item.get(field)
        if value is None:
            continue
        if isinstance(value, str) and value.strip() == "":
            continue
        count += 1
    return count


def sample_non_null_field(input_path: Path, field: str, limit: int) -> list[dict]:
    samples: list[dict] = []
    for item in iter_json_items(input_path):
        value = item.get(field)
        if value is None:
            continue
        if isinstance(value, str) and value.strip() == "":
            continue
        samples.append(item)
        if len(samples) >= limit:
            break
    return samples


def write_matching_items(input_path: Path, output: TextIO, client_id: int) -> int:
    count = 0
    output.write("[")

    for item in iter_json_items(input_path):
        if item.get("Id do Cliente") != client_id:
            continue

        if count:
            output.write(",\n")
        else:
            output.write("\n")

        json.dump(item, output, ensure_ascii=False)
        count += 1

    if count:
        output.write("\n")
    output.write("]\n")
    return count


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Filtra Histórico_de_Clientes.json por Id do Cliente em modo streaming."
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--client-id", type=int, default=DEFAULT_CLIENT_ID)
    parser.add_argument(
        "--output",
        type=Path,
        help="Arquivo de saída. Se omitido, imprime o JSON filtrado no terminal.",
    )
    parser.add_argument(
        "--count-field",
        type=str,
        help="Conta quantos itens possuem o campo informado com valor não nulo/não vazio.",
    )
    parser.add_argument(
        "--sample-field",
        type=str,
        help="Retorna exemplos de itens onde o campo informado é não nulo/não vazio.",
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=10,
        help="Quantidade de exemplos a retornar com --sample-field (padrão: 10).",
    )
    args = parser.parse_args()

    if args.sample_field:
        samples = sample_non_null_field(args.input, args.sample_field, args.sample_limit)
        out_stream = args.output.open("w", encoding="utf-8", newline="") if args.output else sys.stdout
        try:
            json.dump(samples, out_stream, ensure_ascii=False, indent=2)
            out_stream.write("\n")
        finally:
            if args.output:
                out_stream.close()
        print(
            f"{len(samples)} exemplo(ns) com '{args.sample_field}' não nulo retornado(s).",
            file=sys.stderr,
        )
        return 0

    if args.count_field:
        count = count_non_null_field(args.input, args.count_field)
        print(f"{count} item(ns) com '{args.count_field}' não nulo em {args.input}")
        return 0

    if args.output:
        with args.output.open("w", encoding="utf-8", newline="") as output:
            count = write_matching_items(args.input, output, args.client_id)
        print(f"{count} item(ns) encontrados em {args.output}", file=sys.stderr)
        return 0

    count = write_matching_items(args.input, sys.stdout, args.client_id)
    print(f"{count} item(ns) encontrados", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
