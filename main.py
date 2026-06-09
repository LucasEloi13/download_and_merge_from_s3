import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Literal

from source.s3 import S3Source

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

import os
from dotenv import load_dotenv
load_dotenv(override=True)

DEFAULT_BUCKET = os.getenv('AWS_S3_BUCKET')
DEFAULT_PREFIX = os.getenv('AWS_S3_PREFIX')
DEFAULT_RAW_DIR = "raw_files"
DEFAULT_INPUT_FORMAT: Literal["json_array", "json_objects"] = "json_array"
DEFAULT_FILE_SUFFIX = ".json"


def default_output_file() -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"output/{timestamp}.json"


def stream_objects_to_array(source_path: Path, destination, first_item: bool, input_format: str) -> bool:
    in_string = False
    is_escaped = False
    object_depth = 0
    current_object = bytearray()

    with source_path.open("rb") as source_file:
        for chunk in iter(lambda: source_file.read(1024 * 1024), b""):
            for byte in chunk:
                char = chr(byte)

                if object_depth == 0:
                    if input_format == "json_array":
                        if char.isspace() or char in {",", "[", "]"}:
                            continue
                    else:
                        if char.isspace():
                            continue
                    if char == "{":
                        current_object.clear()
                        current_object.append(byte)
                        object_depth = 1
                    continue

                current_object.append(byte)

                if in_string:
                    if is_escaped:
                        is_escaped = False
                    elif char == "\\":
                        is_escaped = True
                    elif char == '"':
                        in_string = False
                    continue

                if char == '"':
                    in_string = True
                    continue

                if char == "{":
                    object_depth += 1
                elif char == "}":
                    object_depth -= 1

                    if object_depth == 0:
                        if not first_item:
                            destination.write(b",\n")
                        destination.write(current_object)
                        first_item = False

    return first_item


def merge_raw_files(raw_files: list[Path], output_path: Path, input_format: str) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    first_item = True
    with output_path.open("wb") as destination:
        destination.write(b"[\n")
        for raw_file in raw_files:
            logger.info("Convertendo arquivo para array final (%s): %s", input_format, raw_file)
            first_item = stream_objects_to_array(raw_file, destination, first_item, input_format)

        destination.write(b"\n]\n")


def download_and_join_files(
    bucket: str,
    prefix: str,
    output_file: str,
    input_format: Literal["json_array", "json_objects"] = DEFAULT_INPUT_FORMAT,
    raw_dir: str = DEFAULT_RAW_DIR,
    file_suffix: str | None = DEFAULT_FILE_SUFFIX,
    limit: int | None = None,
    show_progress: bool = True,
    s3_source: S3Source | None = None,
) -> list[Path]:
    if not bucket:
        raise ValueError("Bucket não informado. Use --bucket ou defina AWS_S3_BUCKET.")
    if prefix is None:
        raise ValueError("Prefix não informado. Use --prefix ou defina AWS_S3_PREFIX.")

    source = s3_source or S3Source()
    raw_dir_path = Path(raw_dir)
    raw_dir_path.mkdir(parents=True, exist_ok=True)
    downloaded_raw_files: list[Path] = []

    source.connect()
    try:
        discovered_keys = source.list_keys(bucket=bucket, prefix=prefix, suffix=file_suffix)
        discovered_keys.sort()
        selected_keys = discovered_keys[:limit] if limit else discovered_keys

        logger.info(
            "Encontradas %s keys em s3://%s/%s (baixando %s)",
            len(discovered_keys),
            bucket,
            prefix,
            len(selected_keys),
        )

        for key in selected_keys:
            logger.info("Baixando: s3://%s/%s", bucket, key)
            destination = raw_dir_path / Path(key).name
            try:
                downloaded_file = source.download_object_to_file(
                    key=key,
                    bucket=bucket,
                    destination_path=destination,
                    show_progress=show_progress,
                )
                downloaded_raw_files.append(downloaded_file)
            except FileNotFoundError:
                logger.warning("Pulando key ausente: s3://%s/%s", bucket, key)
                continue
    finally:
        source.close()

    if not downloaded_raw_files:
        logger.warning("Nenhum arquivo foi baixado com sucesso; merge não será executado")
        return []

    output_path = Path(output_file)
    logger.info("Iniciando merge de %s arquivos em %s", len(downloaded_raw_files), raw_dir_path)
    merge_raw_files(downloaded_raw_files, output_path, input_format=input_format)

    logger.info("Arquivo consolidado salvo em: %s", output_path)
    return downloaded_raw_files


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Baixa arquivos JSON do S3 e consolida em um único JSON"
    )
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--prefix", default=DEFAULT_PREFIX)
    parser.add_argument(
        "--output",
        default=default_output_file(),
        help="Caminho do arquivo de saída consolidado",
    )
    parser.add_argument(
        "--input-format",
        choices=["json_array", "json_objects"],
        default=DEFAULT_INPUT_FORMAT,
        help="Formato dos arquivos de entrada: json_array ([{...}]) ou json_objects (um objeto por linha)",
    )
    parser.add_argument(
        "--file-suffix",
        default=DEFAULT_FILE_SUFFIX,
        help="Filtra as keys descobertas por sufixo (ex.: .json). Use vazio para não filtrar.",
    )
    parser.add_argument(
        "--raw-dir",
        default=DEFAULT_RAW_DIR,
        help="Diretório para salvar os arquivos brutos baixados do S3",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Quantidade de arquivos para processar (ex.: 2 para teste rápido)",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Desativa a barra de progresso do download",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    download_and_join_files(
        bucket=args.bucket,
        prefix=args.prefix,
        output_file=args.output,
        input_format=args.input_format,
        raw_dir=args.raw_dir,
        file_suffix=args.file_suffix or None,
        limit=args.limit,
        show_progress=not args.no_progress,
    )


if __name__ == "__main__":
    main()
