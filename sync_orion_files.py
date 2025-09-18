#!/usr/bin/env python3
"""Herramienta para sincronizar archivos de un servidor SFTP a un bucket de S3.

Incluye lógica de tolerancia a diferentes codificaciones de nombres de archivo
para evitar errores ``UnicodeDecodeError`` durante el recorrido de directorios.
El módulo expone una API basada en ``run_sync`` que puede reutilizarse desde
otros contextos (por ejemplo, la interfaz web incluida en ``webapp.py``) y
mantiene la funcionalidad de ejecución desde la línea de comandos.
"""
from __future__ import annotations

import argparse
import dataclasses
import logging
import os
import posixpath
import stat
import sys
from dataclasses import dataclass, field
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

import boto3
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
import paramiko
from paramiko.client import SSHClient
from paramiko.common import DEBUG
from paramiko.sftp_attr import SFTPAttributes
from paramiko.sftp_client import (
    CMD_CLOSE,
    CMD_HANDLE,
    CMD_NAME,
    CMD_OPENDIR,
    CMD_READDIR,
    SFTPError,
)

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - ``python-dotenv`` es opcional.
    load_dotenv = None

logger = logging.getLogger("sync_orion")


@dataclass
class SyncConfig:
    """Configuración principal para la sincronización."""

    sftp_host: str
    sftp_port: int = 22
    sftp_username: str = ""
    sftp_password: Optional[str] = None
    sftp_private_key: Optional[str] = None
    sftp_passphrase: Optional[str] = None
    sftp_base_path: str = "."
    sftp_encodings: Sequence[str] = field(
        default_factory=lambda: ("utf-8", "latin-1", "cp1252")
    )
    s3_bucket: str = ""
    s3_prefix: str = ""
    aws_region: Optional[str] = None
    delete_remote_after_upload: bool = False
    allowed_extensions: Optional[Sequence[str]] = None

    def normalized_prefix(self) -> str:
        prefix = self.s3_prefix.strip("/")
        return prefix

    def remote_base(self) -> str:
        base = self.sftp_base_path.strip()
        return base if base else "."


@dataclass
class SyncOptions:
    """Opciones para modificar el comportamiento de ``run_sync``."""

    dry_run: bool = False
    list_only: bool = False


@dataclass
class RemoteFile:
    """Representa un archivo localizado en el servidor remoto."""

    path: str
    size: int
    mtime: float

    def relative_to(self, base_path: str) -> str:
        relative = posixpath.relpath(self.path, base_path)
        return "." if relative == "." else relative


@dataclass
class SyncSummary:
    """Resumen de la ejecución de ``run_sync``."""

    encoding_used: str
    total_remote_files: int
    uploaded_files: int
    skipped_existing: int
    deleted_remote: int
    dry_run: bool
    list_only: bool


class SyncError(RuntimeError):
    """Error de alto nivel para fallos durante la sincronización."""


def _setup_logger(verbose: bool) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(levelname)s: %(message)s")
    )
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)


def _load_private_key(config: SyncConfig) -> Optional[paramiko.PKey]:
    if not config.sftp_private_key:
        return None
    key_path = os.path.expanduser(config.sftp_private_key)
    if not os.path.exists(key_path):
        raise SyncError(f"La clave privada '{key_path}' no existe")
    try:
        return paramiko.RSAKey.from_private_key_file(
            key_path, password=config.sftp_passphrase
        )
    except paramiko.PasswordRequiredException as exc:  # pragma: no cover
        raise SyncError("Se requiere passphrase para la clave privada") from exc
    except paramiko.SSHException as exc:  # pragma: no cover
        raise SyncError(
            "No se pudo cargar la clave privada proporcionada"
        ) from exc


def _connect_ssh(config: SyncConfig) -> SSHClient:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    private_key = _load_private_key(config)

    try:
        client.connect(
            hostname=config.sftp_host,
            port=config.sftp_port,
            username=config.sftp_username or None,
            password=config.sftp_password or None,
            pkey=private_key,
            passphrase=config.sftp_passphrase or None,
            allow_agent=True,
            look_for_keys=bool(not config.sftp_private_key),
        )
    except paramiko.SSHException as exc:  # pragma: no cover
        raise SyncError("No se pudo establecer la conexión SSH") from exc

    return client


def _create_sftp_client(
    ssh_client: SSHClient, encoding: str
) -> paramiko.SFTPClient:
    transport = ssh_client.get_transport()
    if transport is None:  # pragma: no cover
        raise SyncError("No se pudo obtener el transporte SSH")
    try:
        # Paramiko >= 3.4 acepta ``encoding`` directamente.
        return paramiko.SFTPClient.from_transport(transport, encoding=encoding)
    except TypeError:
        # Versiones anteriores no soportan el parámetro ``encoding``.
        return paramiko.SFTPClient.from_transport(transport)


def _listdir_attr_with_encoding(
    sftp: paramiko.SFTPClient, path: str, encoding: str
) -> List[SFTPAttributes]:
    path_bytes = path.encode(encoding) if isinstance(path, str) else path
    adjusted = sftp._adjust_cwd(path_bytes)
    sftp._log(DEBUG, "listdir({!r})".format(adjusted))
    t, msg = sftp._request(CMD_OPENDIR, adjusted)
    if t != CMD_HANDLE:
        raise SFTPError("Expected handle")
    handle = msg.get_binary()
    filelist: List[SFTPAttributes] = []
    try:
        while True:
            try:
                t, msg = sftp._request(CMD_READDIR, handle)
            except EOFError:
                break
            if t != CMD_NAME:
                raise SFTPError("Expected name response")
            count = msg.get_int()
            for _ in range(count):
                filename_bytes = msg.get_string()
                longname_bytes = msg.get_string()
                filename = filename_bytes.decode(encoding)
                longname = longname_bytes.decode(encoding, errors="replace")
                attr = SFTPAttributes._from_msg(msg, filename, longname)
                if filename not in (".", ".."):
                    filelist.append(attr)
    finally:
        sftp._request(CMD_CLOSE, handle)
    return filelist


def _iter_encodings(config: SyncConfig) -> Iterator[str]:
    seen = set()
    for encoding in config.sftp_encodings:
        encoding = encoding.strip()
        if not encoding:
            continue
        lowered = encoding.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        yield encoding
    if "utf-8" not in seen:
        yield "utf-8"


def _collect_remote_files(
    sftp: paramiko.SFTPClient,
    base_path: str,
    allowed_exts: Optional[Sequence[str]],
    encoding: str,
) -> List[RemoteFile]:
    allowed = None
    if allowed_exts:
        allowed = {
            ext if ext.startswith(".") else f".{ext}"
            for ext in (e.lower().strip() for e in allowed_exts)
            if ext
        }

    files: List[RemoteFile] = []

    def _walk(current_path: str) -> None:
        entries = _listdir_attr_with_encoding(sftp, current_path, encoding)
        for attr in entries:
            name = attr.filename
            if name in (".", ".."):
                continue
            full_path = posixpath.join(current_path, name)
            if stat.S_ISDIR(attr.st_mode):
                _walk(full_path)
                continue
            if allowed and posixpath.splitext(name)[1].lower() not in allowed:
                continue
            files.append(
                RemoteFile(path=full_path, size=attr.st_size, mtime=attr.st_mtime)
            )

    _walk(base_path)
    return files


def _list_remote_with_fallback(
    ssh_client: SSHClient, config: SyncConfig
) -> Tuple[paramiko.SFTPClient, List[RemoteFile], str]:
    last_exc: Optional[Exception] = None
    base_path = config.remote_base()
    for encoding in _iter_encodings(config):
        logger.info(
            "Intentando listar archivos usando la codificación '%s'", encoding
        )
        sftp = _create_sftp_client(ssh_client, encoding)
        try:
            files = _collect_remote_files(
                sftp, base_path, config.allowed_extensions, encoding
            )
            logger.info(
                "Se detectaron %d archivos remotos (codificación '%s')",
                len(files),
                encoding,
            )
            return sftp, files, encoding
        except UnicodeDecodeError as exc:
            logger.warning(
                "Fallo al decodificar nombres de archivo con la codificación '%s'.",
                encoding,
            )
            last_exc = exc
            sftp.close()
        except FileNotFoundError as exc:
            sftp.close()
            raise SyncError(
                f"La ruta remota '{base_path}' no existe o no es accesible"
            ) from exc
    if last_exc:
        raise SyncError(
            "Ninguna de las codificaciones configuradas permitió listar el servidor"
        ) from last_exc
    raise SyncError("No se pudo crear una conexión SFTP válida")


def _list_existing_objects(
    s3_client, bucket: str, prefix: str
) -> Dict[str, int]:
    paginator = s3_client.get_paginator("list_objects_v2")
    kwargs = {"Bucket": bucket}
    if prefix:
        kwargs["Prefix"] = prefix
    existing: Dict[str, int] = {}
    for page in paginator.paginate(**kwargs):
        for obj in page.get("Contents", []):
            existing[obj["Key"]] = obj.get("Size", 0)
    return existing


def _remote_to_s3_key(remote: RemoteFile, base_path: str, prefix: str) -> str:
    relative = remote.relative_to(base_path)
    if relative == ".":
        relative = posixpath.basename(remote.path)
    key = relative.replace("\\", "/")
    if prefix:
        key = f"{prefix}/{key}"
    return key


def _encode_remote_path(path: str, encoding: str) -> bytes:
    return path.encode(encoding)


def run_sync(config: SyncConfig, options: Optional[SyncOptions] = None) -> SyncSummary:
    options = options or SyncOptions()
    if not config.sftp_host:
        raise SyncError("Debe especificarse el host del servidor SFTP")
    if not config.s3_bucket:
        raise SyncError("Debe especificarse el bucket de destino en S3")

    logger.debug("Configuración empleada: %s", dataclasses.asdict(config))

    ssh_client = _connect_ssh(config)
    logger.info("Conexión SSH establecida con %s", config.sftp_host)

    try:
        sftp, remote_files, encoding = _list_remote_with_fallback(ssh_client, config)
    except Exception:
        ssh_client.close()
        raise

    existing_objects: Dict[str, int] = {}
    s3_client = None
    if not options.list_only:
        try:
            session_kwargs = {}
            if config.aws_region:
                session_kwargs["region_name"] = config.aws_region
            session = boto3.session.Session(**session_kwargs)
            s3_client = session.client("s3")
            existing_objects = _list_existing_objects(
                s3_client, config.s3_bucket, config.normalized_prefix()
            )
            logger.info(
                "Se detectaron %d archivos existentes en S3 bajo el prefijo '%s'",
                len(existing_objects),
                config.normalized_prefix() or "<sin prefijo>",
            )
        except (BotoCoreError, ClientError, NoCredentialsError) as exc:
            sftp.close()
            ssh_client.close()
            raise SyncError("No se pudo inicializar el cliente de S3") from exc
    else:
        logger.info(
            "Ejecutando en modo 'solo listar'; no se consultará S3 ni se subirá nada."
        )

    uploaded = 0
    skipped = 0
    deleted = 0

    base_path = config.remote_base()
    prefix = config.normalized_prefix()

    try:
        if options.list_only:
            for remote in remote_files:
                logger.info("%s (%.2f MB)", remote.path, remote.size / (1024 * 1024))
        else:
            assert s3_client is not None
            for remote in remote_files:
                key = _remote_to_s3_key(remote, base_path, prefix)
                if key in existing_objects:
                    skipped += 1
                    logger.debug(
                        "Omitiendo '%s' porque ya existe en S3 con tamaño %d",
                        key,
                        existing_objects[key],
                    )
                    continue
                if options.dry_run:
                    uploaded += 1
                    logger.info("[DRY-RUN] Se subiría '%s'", key)
                    continue
                logger.info("Subiendo '%s' a s3://%s", key, config.s3_bucket)
                remote_path_bytes = _encode_remote_path(remote.path, encoding)
                with sftp.file(remote_path_bytes, "rb") as remote_fp:
                    s3_client.upload_fileobj(remote_fp, config.s3_bucket, key)
                uploaded += 1
                if config.delete_remote_after_upload:
                    sftp.remove(remote_path_bytes)
                    deleted += 1
                    logger.info("Archivo remoto '%s' eliminado tras la carga", remote.path)
    finally:
        sftp.close()
        ssh_client.close()

    return SyncSummary(
        encoding_used=encoding,
        total_remote_files=len(remote_files),
        uploaded_files=uploaded,
        skipped_existing=skipped,
        deleted_remote=deleted,
        dry_run=options.dry_run,
        list_only=options.list_only,
    )


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sincroniza archivos desde un servidor SFTP hacia un bucket de S3"
    )
    parser.add_argument("--env-file", help="Ruta a un archivo .env con variables")
    parser.add_argument("--dry-run", action="store_true", help="Simula la carga a S3")
    parser.add_argument(
        "--list-only",
        action="store_true",
        help="Solo lista los archivos encontrados sin subirlos",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Muestra información detallada"
    )
    return parser.parse_args(argv)


def _load_env_file(env_file: Optional[str]) -> None:
    if not env_file:
        return
    if load_dotenv is None:
        raise SyncError(
            "Se indicó un archivo .env pero python-dotenv no está instalado"
        )
    if not load_dotenv(env_file):
        raise SyncError(f"No se pudo cargar el archivo de entorno '{env_file}'")


def config_from_env() -> SyncConfig:
    def _split(value: Optional[str]) -> Optional[List[str]]:
        if not value:
            return None
        parts = [item.strip() for item in value.split(",")]
        return [item for item in parts if item]

    return SyncConfig(
        sftp_host=os.environ.get("SFTP_HOST", ""),
        sftp_port=int(os.environ.get("SFTP_PORT", "22")),
        sftp_username=os.environ.get("SFTP_USERNAME", ""),
        sftp_password=os.environ.get("SFTP_PASSWORD") or None,
        sftp_private_key=os.environ.get("SFTP_PRIVATE_KEY") or None,
        sftp_passphrase=os.environ.get("SFTP_PASSPHRASE") or None,
        sftp_base_path=os.environ.get("SFTP_BASE_PATH", "."),
        sftp_encodings=_split(os.environ.get("SFTP_ENCODINGS"))
        or ("utf-8", "latin-1", "cp1252"),
        s3_bucket=os.environ.get("S3_BUCKET", ""),
        s3_prefix=os.environ.get("S3_PREFIX", ""),
        aws_region=os.environ.get("AWS_REGION") or None,
        delete_remote_after_upload=
            os.environ.get("DELETE_REMOTE_AFTER_UPLOAD", "false").lower()
            in {"1", "true", "yes", "on"},
        allowed_extensions=_split(os.environ.get("ALLOWED_EXTENSIONS")),
    )


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = _parse_args(argv)
    _setup_logger(args.verbose)
    _load_env_file(args.env_file)

    config = config_from_env()
    options = SyncOptions(dry_run=args.dry_run, list_only=args.list_only)

    try:
        summary = run_sync(config, options)
    except SyncError as exc:
        logger.error("%s", exc)
        raise SystemExit(1) from exc

    logger.info("===== Resumen =====")
    logger.info("Codificación utilizada: %s", summary.encoding_used)
    logger.info("Archivos remotos encontrados: %d", summary.total_remote_files)
    if not summary.list_only:
        logger.info("Archivos subidos: %d", summary.uploaded_files)
        logger.info("Archivos omitidos por existir: %d", summary.skipped_existing)
        if config.delete_remote_after_upload:
            logger.info("Archivos remotos eliminados: %d", summary.deleted_remote)
    if summary.dry_run:
        logger.info("La ejecución fue un simulacro (dry-run)")


if __name__ == "__main__":
    main()
