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
from threading import Event
from datetime import datetime
from dataclasses import dataclass, field
from typing import Callable, Dict, Iterator, List, Optional, Sequence, Set, Tuple

import boto3
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
import paramiko
from paramiko.client import SSHClient
from paramiko.sftp_attr import SFTPAttributes

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - ``python-dotenv`` es opcional.
    load_dotenv = None

logger = logging.getLogger("sync_orion")

S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL", "http://dc-s3.justiciasalta.gov.ar")


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
    allowed_extensions: Optional[Sequence[str]] = field(
        default_factory=lambda: (".webm", ".mp4")
    )

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
class FileProgress:
    """Representa el avance de la transferencia de un archivo."""

    remote_path: str
    s3_key: str
    size: int
    transferred: int = 0

    @property
    def percentage(self) -> float:
        if self.size <= 0:
            return 100.0
        return min(100.0, (self.transferred / self.size) * 100.0)


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
    remote_files: List[RemoteFile] = field(default_factory=list)
    existing_objects: Dict[str, int] = field(default_factory=dict)
    progress: List[FileProgress] = field(default_factory=list)
    total_bytes: int = 0
    total_transferred: int = 0
    selected_files: int = 0

    @property
    def total_percentage(self) -> float:
        if self.total_bytes <= 0:
            return 100.0
        return min(100.0, (self.total_transferred / self.total_bytes) * 100.0)


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


def _override_paramiko_encoding(encoding: str) -> Callable[[], None]:
    """Sobrescribe temporalmente la codificación usada internamente por Paramiko."""

    # ``paramiko.util.b`` y ``paramiko.util.u`` son las funciones responsables de
    # convertir entre ``str`` y ``bytes``.  Paramiko no expone una API pública para
    # ajustar la codificación utilizada, por lo que la única opción es reemplazar
    # dinámicamente dichas funciones durante la vida útil del cliente SFTP.
    import paramiko.util  # importación local para evitar efectos globales.

    original_b = paramiko.util.b
    original_u = paramiko.util.u

    def patched_b(value, encoding: str = encoding):
        return original_b(value, encoding=encoding)

    def patched_u(value, encoding: str = encoding):
        try:
            return original_u(value, encoding=encoding)
        except UnicodeDecodeError:
            logger.warning(
                "Fallo al decodificar valor con la codificación '%s'; "
                "aplicando modo tolerante",
                encoding,
            )
            try:
                return original_u(
                    value, encoding=encoding, errors="surrogateescape"
                )
            except TypeError:
                if isinstance(value, bytes):
                    return value.decode(encoding, errors="surrogateescape")
                return original_u(value, encoding=encoding)

    paramiko.util.b = patched_b
    paramiko.util.u = patched_u

    def restore() -> None:
        paramiko.util.b = original_b
        paramiko.util.u = original_u

    return restore


def _create_sftp_client(
    ssh_client: SSHClient, encoding: str
) -> paramiko.SFTPClient:
    transport = ssh_client.get_transport()
    if transport is None:  # pragma: no cover
        raise SyncError("No se pudo obtener el transporte SSH")
    restore_encoding = _override_paramiko_encoding(encoding)
    try:
        sftp = paramiko.SFTPClient.from_transport(transport)
    except Exception:
        restore_encoding()
        raise

    original_close = sftp.close
    setattr(sftp, "_tolerant_encoding", encoding)

    def close_with_restore():
        try:
            original_close()
        finally:
            restore_encoding()
            if hasattr(sftp, "_tolerant_encoding"):
                delattr(sftp, "_tolerant_encoding")

    sftp.close = close_with_restore  # type: ignore[assignment]
    return sftp


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


def _decode_filename(raw: bytes, encoding: Optional[str]) -> str:
    candidates: List[str] = []
    if encoding:
        candidates.append(encoding)
    lowered = {item.lower() for item in candidates}
    if "utf-8" not in lowered:
        candidates.append("utf-8")
        lowered.add("utf-8")
    if "latin-1" not in lowered:
        candidates.append("latin-1")

    last_error: Optional[UnicodeDecodeError] = None
    for candidate in candidates:
        try:
            return raw.decode(candidate)
        except UnicodeDecodeError as exc:
            last_error = exc
            logger.warning(
                "Fallo al decodificar nombre con la codificación '%s'; "
                "aplicando modo tolerante",
                candidate,
            )
            try:
                return raw.decode(candidate, errors="surrogateescape")
            except UnicodeDecodeError:
                continue

    if last_error:
        logger.debug("Imposible decodificar nombre de archivo", exc_info=last_error)
    return raw.decode("utf-8", errors="surrogateescape")


def _listdir_attr_tolerant(
    sftp: paramiko.SFTPClient, path: str
) -> List[SFTPAttributes]:
    encoding = getattr(sftp, "_tolerant_encoding", None)
    adjusted_path = sftp._adjust_cwd(path)  # type: ignore[attr-defined]
    handle = None
    filelist: List[SFTPAttributes] = []

    try:
        t, msg = sftp._request(paramiko.sftp.CMD_OPENDIR, adjusted_path)  # type: ignore[attr-defined]
        if t != paramiko.sftp.CMD_HANDLE:  # type: ignore[attr-defined]
            raise paramiko.SFTPError("Expected handle")
        handle = msg.get_binary()
        while True:
            try:
                t, msg = sftp._request(paramiko.sftp.CMD_READDIR, handle)  # type: ignore[attr-defined]
            except EOFError:
                break
            if t != paramiko.sftp.CMD_NAME:  # type: ignore[attr-defined]
                raise paramiko.SFTPError("Expected name response")
            count = msg.get_int()
            for _ in range(count):
                filename_raw = msg.get_binary()
                longname_raw = msg.get_binary()
                filename = _decode_filename(filename_raw, encoding)
                longname = _decode_filename(longname_raw, encoding)
                attr = SFTPAttributes._from_msg(msg, filename, longname)
                if filename not in (".", ".."):
                    filelist.append(attr)
    finally:
        if handle is not None:
            try:
                sftp._request(paramiko.sftp.CMD_CLOSE, handle)  # type: ignore[attr-defined]
            except Exception:
                logger.debug(
                    "No se pudo cerrar el handle de listado en modo tolerante",
                    exc_info=True,
                )

    return filelist


def _collect_remote_files(
    sftp: paramiko.SFTPClient, base_path: str, allowed_exts: Optional[Sequence[str]]
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
        try:
            entries = sftp.listdir_attr(current_path)
        except UnicodeError:
            logger.warning(
                "Fallo al listar '%s' con la codificación actual; reintentando en modo tolerante",
                current_path,
            )
            entries = _listdir_attr_tolerant(sftp, current_path)
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
                sftp, base_path, config.allowed_extensions
            )
            logger.info(
                "Se detectaron %d archivos remotos (codificación '%s')",
                len(files),
                encoding,
            )
            return sftp, files, encoding
        except UnicodeError as exc:
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


def build_path_variants(path: str, base_path: str) -> Set[str]:
    """Genera formas equivalentes de una ruta remota.

    Permite comparar rutas provenientes del formulario web (que pueden estar
    normalizadas de forma distinta) con las rutas completas obtenidas durante el
    recorrido del SFTP.
    """

    cleaned = (path or "").strip()
    if not cleaned:
        return set()

    normalized_base = posixpath.normpath(base_path or "")
    if normalized_base in {"", "."}:
        normalized_base = ""

    variants = {cleaned}
    normalized = posixpath.normpath(cleaned)
    variants.add(normalized)

    if normalized_base and (
        normalized == normalized_base
        or normalized.startswith(f"{normalized_base}{posixpath.sep}")
    ):
        try:
            relative = posixpath.relpath(cleaned, normalized_base)
        except ValueError:
            # Si las rutas no comparten prefijo el valor relativo no es útil.
            pass
        else:
            variants.add(relative)
            variants.add(posixpath.normpath(relative))

    # Eliminamos duplicados vacíos o referidos al directorio actual.
    return {variant for variant in variants if variant and variant != "."}


def _create_s3_client(config: SyncConfig):
    session_kwargs = {}
    if config.aws_region:
        session_kwargs["region_name"] = config.aws_region
    session = boto3.session.Session(**session_kwargs)
    client_kwargs = {}
    if S3_ENDPOINT_URL:
        client_kwargs["endpoint_url"] = S3_ENDPOINT_URL
    return session.client("s3", **client_kwargs)


def build_sync_plan(
    config: SyncConfig,
) -> Tuple[str, List[RemoteFile], Dict[str, int]]:
    """Obtiene la lista de archivos remotos y los objetos existentes en S3."""

    if not config.sftp_host:
        raise SyncError("Debe especificarse el host del servidor SFTP")
    if not config.s3_bucket:
        raise SyncError("Debe especificarse el bucket de destino en S3")

    ssh_client = _connect_ssh(config)
    logger.info("Conexión SSH establecida con %s", config.sftp_host)

    sftp: Optional[paramiko.SFTPClient] = None
    try:
        sftp, remote_files, encoding = _list_remote_with_fallback(ssh_client, config)
    finally:
        if sftp is not None:
            try:
                sftp.close()
            except Exception:
                logger.debug(
                    "No se pudo cerrar el cliente SFTP tras el listado", exc_info=True
                )
        ssh_client.close()

    try:
        s3_client = _create_s3_client(config)
        existing_objects = _list_existing_objects(
            s3_client, config.s3_bucket, config.normalized_prefix()
        )
        logger.info(
            "Se detectaron %d archivos existentes en S3 bajo el prefijo '%s'",
            len(existing_objects),
            config.normalized_prefix() or "<sin prefijo>",
        )
    except (BotoCoreError, ClientError, NoCredentialsError) as exc:
        raise SyncError("No se pudo inicializar el cliente de S3") from exc

    return encoding, remote_files, existing_objects


def run_sync(
    config: SyncConfig,
    options: Optional[SyncOptions] = None,
    selected_remote_paths: Optional[Sequence[str]] = None,
    stop_event: Optional[Event] = None,
    progress_callback: Optional[Callable[[FileProgress, str], None]] = None,
) -> SyncSummary:
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
            s3_client = _create_s3_client(config)
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

    selection_lookup: Optional[Dict[str, str]] = None
    remaining_selection: Set[str] = set()
    if selected_remote_paths is not None:
        lookup: Dict[str, str] = {}
        for raw_path in selected_remote_paths:
            normalized_input = (raw_path or "").strip()
            if not normalized_input:
                continue
            for variant in build_path_variants(normalized_input, base_path):
                lookup.setdefault(variant, normalized_input)
        if lookup:
            selection_lookup = lookup
            remaining_selection = set(lookup.values())

    selected_remote: List[RemoteFile] = []
    for remote in remote_files:
        matched_key: Optional[str] = None
        if selection_lookup is not None:
            for variant in build_path_variants(remote.path, base_path):
                if variant in selection_lookup:
                    matched_key = selection_lookup[variant]
                    break
            if matched_key is None:
                continue
        selected_remote.append(remote)
        if matched_key is not None:
            remaining_selection.discard(matched_key)

    if remaining_selection:
        logger.warning(
            "Los siguientes archivos seleccionados no se encontraron en el origen: %s",
            ", ".join(sorted(remaining_selection)),
        )

    transfer_progress: List[FileProgress] = []
    total_bytes = 0
    total_transferred = 0

    def _notify(status: str, entry: FileProgress) -> None:
        if progress_callback is None:
            return
        try:
            progress_callback(entry, status)
        except Exception:  # pragma: no cover - defensivo
            logger.debug(
                "Error al notificar el progreso; se ignora la actualización.",
                exc_info=True,
            )

    stop_notified = False

    def _stop_requested() -> bool:
        nonlocal stop_notified
        if stop_event is not None and stop_event.is_set():
            if not stop_notified:
                logger.info(
                    "Se solicitó detener la copia; no se iniciarán nuevas transferencias después del archivo actual."
                )
                stop_notified = True
            return True
        return False

    try:
        if options.list_only:
            for remote in remote_files:
                logger.info("%s (%.2f MB)", remote.path, remote.size / (1024 * 1024))
        else:
            assert s3_client is not None
            transfer_candidates: List[Tuple[RemoteFile, str]] = []
            for remote in selected_remote:
                key = _remote_to_s3_key(remote, base_path, prefix)
                if key in existing_objects:
                    skipped += 1
                    logger.debug(
                        "Omitiendo '%s' porque ya existe en S3 con tamaño %d",
                        key,
                        existing_objects[key],
                    )
                    continue
                transfer_candidates.append((remote, key))
            total_bytes = sum(remote.size for remote, _ in transfer_candidates)
            for remote, key in transfer_candidates:
                if _stop_requested():
                    break
                entry = FileProgress(remote_path=remote.path, s3_key=key, size=remote.size)
                transfer_progress.append(entry)
                _notify("start", entry)
                if options.dry_run:
                    uploaded += 1
                    entry.transferred = remote.size
                    total_transferred = min(
                        total_bytes, total_transferred + remote.size
                    )
                    logger.info("[DRY-RUN] Se subiría '%s'", key)
                    _notify("finish", entry)
                    if _stop_requested():
                        break
                    continue
                logger.info("Subiendo '%s' a s3://%s", key, config.s3_bucket)

                def _callback(bytes_amount: int, entry: FileProgress = entry) -> None:
                    nonlocal total_transferred
                    entry.transferred = min(
                        entry.size, entry.transferred + int(bytes_amount)
                    )
                    total_transferred = min(
                        total_bytes, total_transferred + int(bytes_amount)
                    )

                try:
                    with sftp.file(remote.path, "rb") as remote_fp:
                        s3_client.upload_fileobj(
                            remote_fp, config.s3_bucket, key, Callback=_callback
                        )
                except FileNotFoundError:
                    logger.warning(
                        "El archivo remoto '%s' ya no está disponible; se omitirá.",
                        remote.path,
                    )
                    try:
                        transfer_progress.remove(entry)
                    except ValueError:
                        pass
                    total_bytes = max(0, total_bytes - remote.size)
                    if total_transferred > total_bytes:
                        total_transferred = total_bytes
                    skipped += 1
                    _notify("skipped", entry)
                    if _stop_requested():
                        break
                    continue
                if entry.transferred < entry.size:
                    delta = entry.size - entry.transferred
                    entry.transferred = entry.size
                    total_transferred = min(total_bytes, total_transferred + delta)
                uploaded += 1
                existing_objects[key] = remote.size
                if config.delete_remote_after_upload:
                    sftp.remove(remote.path)
                    deleted += 1
                    logger.info(
                        "Archivo remoto '%s' eliminado tras la carga", remote.path
                    )
                _notify("finish", entry)
                if _stop_requested():
                    break
    finally:
        sftp.close()
        ssh_client.close()

    summary = SyncSummary(
        encoding_used=encoding,
        total_remote_files=len(remote_files),
        uploaded_files=uploaded,
        skipped_existing=skipped,
        deleted_remote=deleted,
        dry_run=options.dry_run,
        list_only=options.list_only,
        remote_files=list(remote_files),
        existing_objects=existing_objects,
        progress=transfer_progress,
        total_bytes=total_bytes,
        total_transferred=total_transferred,
        selected_files=len(selected_remote),
    )
    if options.dry_run and total_bytes and total_transferred < total_bytes:
        summary.total_transferred = total_bytes
        for entry in summary.progress:
            entry.transferred = entry.size
    return summary


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
        if value is None:
            return None
        stripped = value.strip()
        if not stripped:
            return None
        if stripped == "*":
            return []
        parts = [item.strip() for item in value.split(",")]
        filtered = [item for item in parts if item]
        return filtered or None

    allowed = _split(os.environ.get("ALLOWED_EXTENSIONS"))

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
        allowed_extensions=None if allowed == [] else allowed or (".webm", ".mp4"),
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
    logger.info("Archivos seleccionados para subir: %d", summary.selected_files)
    if not summary.list_only:
        logger.info("Archivos subidos: %d", summary.uploaded_files)
        logger.info("Archivos omitidos por existir: %d", summary.skipped_existing)
        logger.info(
            "Bytes transferidos: %d de %d",
            summary.total_transferred,
            summary.total_bytes,
        )
        if config.delete_remote_after_upload:
            logger.info("Archivos remotos eliminados: %d", summary.deleted_remote)
    if summary.dry_run:
        logger.info("La ejecución fue un simulacro (dry-run)")


if __name__ == "__main__":
    main()
