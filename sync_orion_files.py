"""Herramienta para copiar archivos desde el servidor Orion hacia S3.

Este script se conecta a un servidor NAS mediante SFTP, muestra los
archivos disponibles en el origen que no existen en el bucket de S3 y
permite seleccionar cuáles se desean copiar.
"""

from __future__ import annotations

import argparse
import logging
import os
import stat
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List, Sequence, Set, Tuple

import boto3
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
import paramiko


# Valores por defecto proporcionados en la consigna.
DEFAULT_SFTP_HOST = os.environ.get("ORION_SFTP_HOST", "10.18.239.220")
DEFAULT_SFTP_PORT = int(os.environ.get("ORION_SFTP_PORT", "28232"))
DEFAULT_SFTP_USER = os.environ.get("ORION_SFTP_USER", "orion")
DEFAULT_SFTP_PASSWORD = os.environ.get("ORION_SFTP_PASSWORD", "Ori0n*")
DEFAULT_SFTP_PATH = os.environ.get("ORION_SFTP_PATH", "/home/orion/orion1")

DEFAULT_S3_ACCESS_KEY = os.environ.get(
    "ORION_S3_ACCESS_KEY", "ZCR20411QNWVYFLQDKEH"
)
DEFAULT_S3_SECRET_KEY = os.environ.get(
    "ORION_S3_SECRET_KEY", "MueSwCBEYUDPgB9JO9hv89RgVSvBpuC8bHanxtt3"
)
DEFAULT_S3_REGION = os.environ.get("ORION_S3_REGION", "us-east-1")
DEFAULT_S3_ENDPOINT = os.environ.get(
    "ORION_S3_ENDPOINT", "http://dc-s3.justiciasalta.gov.ar"
)
DEFAULT_S3_BUCKET = os.environ.get("ORION_S3_BUCKET", "s3-test-01")
DEFAULT_S3_PREFIX = os.environ.get("ORION_S3_PREFIX", "videosorion")


@dataclass(frozen=True)
class RemoteFile:
    """Representa un archivo disponible en el servidor SFTP."""

    relative_path: str
    absolute_path: str
    size: int
    modified_time: datetime

    def as_display_row(self) -> Sequence[str]:
        return (
            self.relative_path,
            f"{self.size:,} bytes",
            self.modified_time.strftime("%Y-%m-%d %H:%M:%S"),
        )


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sincroniza archivos desde el servidor Orion hacia un bucket de S3",
    )

    parser.add_argument("--sftp-host", default=DEFAULT_SFTP_HOST)
    parser.add_argument("--sftp-port", type=int, default=DEFAULT_SFTP_PORT)
    parser.add_argument("--sftp-user", default=DEFAULT_SFTP_USER)
    parser.add_argument("--sftp-password", default=DEFAULT_SFTP_PASSWORD)
    parser.add_argument("--sftp-path", default=DEFAULT_SFTP_PATH)

    parser.add_argument("--s3-access-key", default=DEFAULT_S3_ACCESS_KEY)
    parser.add_argument("--s3-secret-key", default=DEFAULT_S3_SECRET_KEY)
    parser.add_argument("--s3-region", default=DEFAULT_S3_REGION)
    parser.add_argument("--s3-endpoint", default=DEFAULT_S3_ENDPOINT)
    parser.add_argument("--s3-bucket", default=DEFAULT_S3_BUCKET)
    parser.add_argument("--s3-prefix", default=DEFAULT_S3_PREFIX)

    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Incluye subdirectorios al listar archivos del NAS",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Muestra información adicional de depuración",
    )
    return parser


def connect_sftp(
    host: str, port: int, username: str, password: str
) -> Tuple[paramiko.SSHClient, paramiko.SFTPClient]:
    logging.debug("Conectando al servidor SFTP %s:%s", host, port)
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(hostname=host, port=port, username=username, password=password)
    sftp_client = ssh_client.open_sftp()
    return ssh_client, sftp_client


def list_remote_files(
    sftp: paramiko.SFTPClient, base_path: str, recursive: bool = True
) -> List[RemoteFile]:
    """Obtiene la lista de archivos en el NAS."""

    logging.debug("Listando archivos en %s (recursivo=%s)", base_path, recursive)
    files: List[RemoteFile] = []

    def _walk(current_path: str, relative_prefix: str = "") -> None:
        try:
            entries = sftp.listdir_attr(current_path)
        except FileNotFoundError:
            logging.error("No se encontró la ruta remota: %s", current_path)
            return
        except OSError as error:
            logging.error("No se pudo listar %s: %s", current_path, error)
            return

        for entry in entries:
            full_path = f"{current_path.rstrip('/')}/{entry.filename}"
            relative_path = (
                f"{relative_prefix}/{entry.filename}" if relative_prefix else entry.filename
            )
            if stat.S_ISDIR(entry.st_mode):
                if recursive:
                    _walk(full_path, relative_path)
                else:
                    logging.debug("Omitiendo directorio %s", full_path)
                continue

            files.append(
                RemoteFile(
                    relative_path=relative_path,
                    absolute_path=full_path,
                    size=entry.st_size,
                    modified_time=datetime.fromtimestamp(entry.st_mtime),
                )
            )

    _walk(base_path)
    files.sort(key=lambda item: item.relative_path.lower())
    logging.info("Se detectaron %s archivos en el origen", len(files))
    return files


def list_s3_objects(
    client: BaseClient, bucket: str, prefix: str
) -> Set[str]:
    """Retorna el conjunto de rutas relativas existentes en S3."""

    normalized_prefix = prefix.strip("/")
    if normalized_prefix:
        list_prefix = f"{normalized_prefix}/"
    else:
        list_prefix = ""

    paginator = client.get_paginator("list_objects_v2")
    existing: Set[str] = set()

    logging.debug(
        "Listando objetos en bucket=%s con prefijo=%s", bucket, list_prefix or "<raíz>"
    )

    pagination_kwargs = {"Bucket": bucket}
    if list_prefix:
        pagination_kwargs["Prefix"] = list_prefix

    for page in paginator.paginate(**pagination_kwargs):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            if list_prefix:
                relative = key[len(list_prefix) :]
            else:
                relative = key
            existing.add(relative)

    logging.info(
        "Se detectaron %s archivos existentes en S3 bajo el prefijo '%s'",
        len(existing),
        normalized_prefix,
    )
    return existing


def display_destination(existing: Iterable[str], prefix: str) -> None:
    normalized_prefix = prefix.strip("/")
    print("\n===== Archivos actualmente en S3 =====")
    if not normalized_prefix:
        print("(Bucket en raíz)")
    else:
        print(f"Prefijo: {normalized_prefix}")

    sorted_existing = sorted(existing)
    if not sorted_existing:
        print("No hay archivos en destino.")
        return

    for index, key in enumerate(sorted_existing, start=1):
        display_key = f"{normalized_prefix}/{key}" if normalized_prefix else key
        print(f"{index:3d}. {display_key}")


def display_sources(files: Sequence[RemoteFile]) -> None:
    print("\n===== Archivos disponibles en el origen =====")
    if not files:
        print("No hay archivos nuevos para copiar.")
        return

    header = f"{'#':>3s}  {'Ruta relativa':<60s}  {'Tamaño':>15s}  {'Modificación':>20s}"
    print(header)
    print("-" * len(header))
    for idx, remote_file in enumerate(files, start=1):
        relative, size, mtime = remote_file.as_display_row()
        print(f"{idx:3d}. {relative:<60.60s}  {size:>15s}  {mtime:>20s}")


def prompt_selection(total: int) -> List[int]:
    if total == 0:
        return []

    while True:
        selection = input(
            "Ingrese los números de los archivos a copiar (ej. '1 2 5' o 'todos'): "
        ).strip()
        if not selection:
            print("Debe ingresar al menos un índice.")
            continue

        if selection.lower() in {"todos", "all"}:
            return list(range(1, total + 1))

        try:
            chosen = sorted({int(value) for value in selection.replace(",", " ").split()})
        except ValueError:
            print("Entrada inválida. Utilice números separados por espacios o 'todos'.")
            continue

        if any(index < 1 or index > total for index in chosen):
            print(f"Los índices deben estar entre 1 y {total}.")
            continue

        return chosen


def upload_files(
    sftp: paramiko.SFTPClient,
    client: BaseClient,
    bucket: str,
    prefix: str,
    files: Sequence[RemoteFile],
    selected_indexes: Sequence[int],
) -> None:
    normalized_prefix = prefix.strip("/")
    for index in selected_indexes:
        remote_file = files[index - 1]
        object_key = f"{normalized_prefix}/{remote_file.relative_path}" if normalized_prefix else remote_file.relative_path
        logging.info(
            "Copiando %s hacia s3://%s/%s", remote_file.absolute_path, bucket, object_key
        )

        try:
            with sftp.open(remote_file.absolute_path, "rb") as remote_stream:
                client.upload_fileobj(remote_stream, bucket, object_key)
        except (OSError, BotoCoreError, ClientError) as error:
            logging.error("Error al copiar %s: %s", remote_file.relative_path, error)
        else:
            logging.info("Archivo %s copiado correctamente.", remote_file.relative_path)


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()
    configure_logging(args.verbose)

    s3_client = boto3.client(
        "s3",
        region_name=args.s3_region,
        endpoint_url=args.s3_endpoint,
        aws_access_key_id=args.s3_access_key,
        aws_secret_access_key=args.s3_secret_key,
        config=Config(signature_version="s3v4"),
    )

    try:
        ssh_client, sftp_client = connect_sftp(
            host=args.sftp_host,
            port=args.sftp_port,
            username=args.sftp_user,
            password=args.sftp_password,
        )
    except (paramiko.SSHException, OSError) as error:
        logging.error("No fue posible establecer la conexión SFTP: %s", error)
        return

    try:
        existing_objects = list_s3_objects(s3_client, args.s3_bucket, args.s3_prefix)
        display_destination(existing_objects, args.s3_prefix)

        remote_files = list_remote_files(
            sftp_client, args.sftp_path, recursive=args.recursive
        )
        filtered_files = [
            remote_file
            for remote_file in remote_files
            if remote_file.relative_path not in existing_objects
        ]

        display_sources(filtered_files)

        if not filtered_files:
            print("No hay archivos para copiar.")
            return

        indexes = prompt_selection(len(filtered_files))
        if not indexes:
            print("No se seleccionaron archivos.")
            return

        upload_files(
            sftp_client,
            s3_client,
            args.s3_bucket,
            args.s3_prefix,
            filtered_files,
            indexes,
        )
    finally:
        sftp_client.close()
        ssh_client.close()


if __name__ == "__main__":
    main()
