"""Aplicación web para ejecutar ``sync_orion_files`` desde el navegador."""
from __future__ import annotations

import dataclasses
import logging
import os
import posixpath
import threading
import traceback
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

try:  # pragma: no cover - dependencias opcionales
    import boto3
except Exception:  # pragma: no cover - boto3 no disponible en el entorno
    boto3 = None

try:  # pragma: no cover - dependencias opcionales
    from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
except Exception:  # pragma: no cover - botocore no disponible
    BotoCoreError = ClientError = NoCredentialsError = Exception
from flask import Flask, jsonify, render_template, request

try:
    import psycopg2
    from psycopg2 import OperationalError, sql
    from psycopg2.extras import execute_values
except Exception:  # pragma: no cover - fallback si el driver no está disponible
    psycopg2 = None
    OperationalError = Exception
    sql = None
    execute_values = None

from sync_orion_files import (
    FileProgress,
    RemoteFile,
    SyncConfig,
    SyncError,
    SyncOptions,
    S3_ENDPOINT_URL,
    build_sync_plan,
    config_from_env,
    run_sync,
)
from genexus_utils import generate_genexus_guid

DEFAULT_GESTOR_COLUMNS: Tuple[str, ...] = (
    "sgddocid",
    "sgddocnombre",
    "sgddoctipo",
    "sgddoctamano",
    "sgddocfecalta",
    "sgddocubicfisica",
    "sgddocurl",
    "sgddocusuarioalta",
    "sgddocpublico",
    "sgddocapporigen",
)

app = Flask(__name__)
app.config.setdefault("SECRET_KEY", "cambia-esta-clave")


class _BufferLogHandler(logging.Handler):
    """Handler en memoria para capturar logs de la sincronización."""

    def __init__(self) -> None:
        super().__init__()
        self._lines: List[str] = []
        self.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

    def emit(self, record: logging.LogRecord) -> None:  # pragma: no cover - handler
        try:
            self._lines.append(self.format(record))
        except Exception:  # pragma: no cover - no interrumpir la vista
            pass

    @property
    def text(self) -> str:
        return "\n".join(self._lines)


def _parse_bool(value: Optional[str]) -> bool:
    if not value:
        return False
    return value.lower() in {"1", "true", "yes", "on"}


def _split_csv(value: str) -> List[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _format_size(num_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    size = float(num_bytes)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"


def _remote_to_key(remote: RemoteFile, base_path: str, prefix: str) -> str:
    relative = remote.relative_to(base_path)
    if relative == ".":
        relative = posixpath.basename(remote.path)
    key = relative.replace("\\", "/")
    if prefix:
        key = f"{prefix}/{key}"
    return key


def _build_plan_context(
    config: SyncConfig,
    encoding: str,
    remote_files: Sequence[RemoteFile],
    existing_objects: Dict[str, int],
) -> Dict[str, Any]:
    base_path = config.remote_base()
    prefix = config.normalized_prefix()
    missing_files: List[Dict[str, Any]] = []
    total_missing = 0

    for remote in remote_files:
        key = _remote_to_key(remote, base_path, prefix)
        entry = {
            "remote_path": remote.path,
            "s3_key": key,
            "size": remote.size,
            "size_label": _format_size(remote.size),
        }
        if key not in existing_objects:
            missing_files.append(entry)
            total_missing += remote.size

    existing_list = [
        {
            "key": key,
            "size": size,
            "size_label": _format_size(size),
        }
        for key, size in sorted(existing_objects.items())
    ]

    return {
        "encoding": encoding,
        "remote_total": len(remote_files),
        "missing_files": missing_files,
        "missing_count": len(missing_files),
        "total_missing_bytes": total_missing,
        "total_missing_label": _format_size(total_missing),
        "existing_files": existing_list,
        "existing_count": len(existing_list),
        "base_path": base_path,
    }


class CopyManager:
    """Gestiona la ejecución de copias en segundo plano."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._stop_event: Optional[threading.Event] = None
        self._state: Dict[str, Any] = {
            "status": "idle",
            "current_file": None,
            "message": None,
            "summary": None,
            "error": None,
            "logs": "",
            "plan": None,
            "needs_refresh": False,
        }

    def snapshot(self, consume_refresh: bool = False) -> Dict[str, Any]:
        with self._lock:
            data = {
                "status": self._state["status"],
                "current_file": self._state["current_file"],
                "message": self._state["message"],
                "summary": self._state["summary"],
                "error": self._state["error"],
                "logs": self._state["logs"],
                "plan": self._state["plan"],
            }
            if consume_refresh and self._state["needs_refresh"]:
                data["needs_refresh"] = True
                self._state["needs_refresh"] = False
            else:
                data["needs_refresh"] = False
            return data

    def is_active(self) -> bool:
        with self._lock:
            return self._thread is not None and self._thread.is_alive()

    def update_plan(self, plan: Optional[Dict[str, Any]]) -> None:
        with self._lock:
            self._state["plan"] = plan

    def start(
        self,
        config: SyncConfig,
        options: SyncOptions,
        remote_paths: Optional[Sequence[str]],
        plan: Optional[Dict[str, Any]],
    ) -> bool:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return False
            self._stop_event = threading.Event()
            self._state.update(
                {
                    "status": "running",
                    "message": "Se inició la copia de archivos.",
                    "current_file": None,
                    "summary": None,
                    "error": None,
                    "logs": "",
                    "plan": plan,
                    "needs_refresh": False,
                }
            )
            thread = threading.Thread(
                target=self._worker,
                args=(
                    dataclasses.replace(config),
                    dataclasses.replace(options),
                    list(remote_paths or []),
                    self._stop_event,
                ),
                daemon=True,
            )
            self._thread = thread
            thread.start()
            return True

    def request_stop(self) -> bool:
        with self._lock:
            if (
                self._thread is not None
                and self._thread.is_alive()
                and self._stop_event is not None
            ):
                self._stop_event.set()
                self._state.update(
                    {
                        "status": "stopping",
                        "message": "Se detendrá al finalizar el archivo actual y se actualizarán las listas.",
                    }
                )
                return True
            return False

    def _worker(
        self,
        config: SyncConfig,
        options: SyncOptions,
        remote_paths: Sequence[str],
        stop_event: threading.Event,
    ) -> None:
        handler = _BufferLogHandler()
        sync_logger = logging.getLogger("sync_orion")
        previous_level = sync_logger.level
        sync_logger.addHandler(handler)
        sync_logger.setLevel(logging.INFO)

        def _progress(entry: FileProgress, status: str) -> None:
            with self._lock:
                if status == "start":
                    self._state["current_file"] = entry.remote_path
                elif status in {"finish", "skipped"}:
                    self._state["current_file"] = None

        try:
            summary = run_sync(
                config,
                options,
                selected_remote_paths=remote_paths or None,
                stop_event=stop_event,
                progress_callback=_progress,
            )
            plan_context = _build_plan_context(
                config,
                summary.encoding_used,
                summary.remote_files,
                summary.existing_objects,
            )
            with self._lock:
                self._state.update(
                    {
                        "summary": summary,
                        "logs": handler.text,
                        "status": "completed"
                        if not stop_event.is_set()
                        else "stopped",
                        "message": (
                            "Copia finalizada correctamente."
                            if not stop_event.is_set()
                            else "La copia se detuvo tras finalizar el archivo en curso."
                        ),
                        "plan": plan_context,
                        "current_file": None,
                        "needs_refresh": True,
                    }
                )
        except Exception as exc:
            extra_logs = traceback.format_exc()
            with self._lock:
                self._state.update(
                    {
                        "error": str(exc),
                        "logs": f"{handler.text}\n\n{extra_logs}".strip(),
                        "status": "error",
                        "message": "Ocurrió un error durante la copia.",
                        "current_file": None,
                        "needs_refresh": True,
                    }
                )
        finally:
            sync_logger.removeHandler(handler)
            sync_logger.setLevel(previous_level)
            with self._lock:
                self._thread = None
                self._stop_event = None


def _status_label(status: str) -> str:
    mapping = {
        "idle": "Sin actividad",
        "running": "Copiando archivos",
        "stopping": "Deteniendo copia",
        "stopped": "Copia detenida",
        "completed": "Copia finalizada",
        "error": "Error en la copia",
    }
    return mapping.get(status, "Sin información")


copy_manager = CopyManager()


@app.template_filter("human_size")
def _human_size_filter(value: Any) -> str:
    try:
        return _format_size(int(value))
    except (TypeError, ValueError):
        return "-"


@app.route("/")
def menu():
    return render_template("menu.html")


@app.route("/incluir", methods=["GET", "POST"])
def include_view():
    config = config_from_env()
    s3_bucket = config.s3_bucket
    s3_prefix = config.normalized_prefix()

    connection_status = False
    error_message: Optional[str] = None
    operation_message: Optional[str] = None
    operation_error: Optional[str] = None
    existing_matches: List[str] = []
    added_records: List[str] = []

    if psycopg2 is None:
        error_message = "El controlador de PostgreSQL no está disponible en el entorno."
    else:
        try:
            connection = psycopg2.connect(
                dbname="gestor",
                user="usrgestor",
                password="Gestor97",
                host="10.18.250.250",
                port=5432,
                connect_timeout=5,
            )
        except OperationalError as exc:  # pragma: no cover - depende del entorno
            error_message = str(exc)
            connection = None
        else:
            connection_status = True

        if connection is not None:
            fetch_failed = False
            try:
                registered = _fetch_registered_files(connection)
            except Exception as exc:  # pragma: no cover - depende del esquema real
                operation_error = (
                    "No se pudieron obtener los archivos registrados en la base. "
                    f"Detalle: {exc}"
                )
                registered = {}
                fetch_failed = True

            if (
                not fetch_failed
                and request.method == "POST"
                and request.form.get("operation") == "incorporate"
            ):
                try:
                    (
                        existing_matches,
                        added_records,
                        operation_message,
                        new_registry,
                    ) = _incorporate_files(
                        connection, config, registered
                    )
                except RuntimeError as exc:
                    operation_error = str(exc)
                except Exception as exc:  # pragma: no cover - errores dependientes del entorno
                    connection.rollback()
                    operation_error = (
                        "Ocurrió un error al incorporar archivos en la base: "
                        f"{exc}"
                    )
                else:
                    connection.commit()
                    # Actualizamos el conjunto local para reflejar las inserciones realizadas.
                    registered.update(new_registry)
            connection.close()

            if not existing_matches and not added_records and registered:
                # Si no hubo acción reciente, mostramos el listado actual como referencia.
                existing_matches = sorted(registered.values())

    return render_template(
        "incluir.html",
        connection_status=connection_status,
        error_message=error_message,
        operation_message=operation_message,
        operation_error=operation_error,
        existing_matches=existing_matches,
        added_records=added_records,
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
    )


def _gestor_table_identifiers() -> Tuple[str, str, Tuple[str, ...]]:
    """Obtiene el esquema, tabla y columnas configurados para la base gestor."""

    table_path = os.environ.get("GESTOR_TABLE", "sgdpjs")
    schema = os.environ.get("GESTOR_SCHEMA")

    columns_raw = os.environ.get("GESTOR_COLUMNS")
    if columns_raw:
        column_names = [col.strip() for col in columns_raw.split(",") if col.strip()]
    else:
        legacy_column = os.environ.get("GESTOR_COLUMN")
        if legacy_column:
            column_names = [col.strip() for col in legacy_column.split(",") if col.strip()]
        else:
            column_names = list(DEFAULT_GESTOR_COLUMNS)

    if not column_names:
        raise RuntimeError(
            "No se configuraron columnas válidas para la tabla de gestor."
        )

    if schema:
        table_name = table_path
    elif "." in table_path:
        schema, table_name = table_path.split(".", 1)
    else:
        schema = "public"
        table_name = table_path

    normalized_columns = {col.lower() for col in column_names}
    if table_name.lower() == "sgdpjs" and "sgddocid" not in normalized_columns:
        raise RuntimeError(
            "La tabla 'sgdpjs' requiere la columna 'sgddocid'. "
            "Incluya 'sgddocid' en la variable GESTOR_COLUMNS para generar los identificadores."
        )

    return schema, table_name, tuple(column_names)


def _fetch_registered_files(connection) -> Dict[Tuple[str, Optional[str]], str]:
    """Obtiene los archivos ya registrados indexados por nombre y extensión."""

    if sql is None:
        return {}

    schema, table_name, columns = _gestor_table_identifiers()
    table_identifier = sql.Identifier(schema, table_name)
    column_identifiers = [sql.Identifier(column) for column in columns]

    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL("SELECT {columns} FROM {table}").format(
                columns=sql.SQL(", ").join(column_identifiers),
                table=table_identifier,
            )
        )
        rows = cursor.fetchall()

    columns_lower = [column.lower() for column in columns]
    try:
        name_index = columns_lower.index("sgddocnombre")
    except ValueError:
        name_index = None
    try:
        type_index = columns_lower.index("sgddoctipo")
    except ValueError:
        type_index = None

    registered: Dict[Tuple[str, Optional[str]], str] = {}
    for row in rows:
        if not row:
            continue
        entry = _format_registered_entry(row, columns, name_index, type_index)
        if entry:
            display, lookup_key = entry
            registered.setdefault(lookup_key, display)

    return registered


def _format_registered_entry(
    row: Sequence[Any],
    columns: Sequence[str],
    name_index: Optional[int],
    type_index: Optional[int],
) -> Optional[Tuple[str, Tuple[str, Optional[str]]]]:
    if not row:
        return None

    if name_index is not None:
        raw_name = row[name_index] if name_index < len(row) else None
        name = str(raw_name).strip() if raw_name is not None else ""
        if not name:
            return None
        extension: Optional[str] = None
        if type_index is not None and type_index < len(row):
            extension_raw = row[type_index]
            if extension_raw not in (None, ""):
                cleaned = str(extension_raw).strip()
                extension = cleaned.lower() or None
        display = name if not extension else f"{name}.{extension}"
        return display, _build_lookup_key(name, extension)

    if len(columns) == 1:
        value = row[0]
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        return text, (text.lower(), None)

    if len(columns) == 2:
        name = str(row[0]).strip() if row[0] is not None else ""
        extension_raw = row[1]
        if not name:
            return None
        if extension_raw in (None, ""):
            return name, _build_lookup_key(name, None)
        extension = str(extension_raw).strip().lower()
        display = f"{name}.{extension}" if extension else name
        return display, _build_lookup_key(name, extension)

    parts: List[str] = []
    for value in row:
        if value in (None, ""):
            continue
        text = str(value).strip()
        if text:
            parts.append(text)
    if not parts:
        return None
    display = " - ".join(parts)
    return display, (display.lower(), None)


@dataclasses.dataclass
class _S3ObjectInfo:
    key: str
    size: Optional[int]
    last_modified: Optional[datetime]


@dataclasses.dataclass
class _PreparedRecord:
    display: str
    values: Tuple[Any, ...]
    lookup_key: Tuple[str, Optional[str]]


def _build_lookup_key(name: str, extension: Optional[str]) -> Tuple[str, Optional[str]]:
    normalized_name = name.strip().lower()
    normalized_extension = extension.strip().lower() if extension else None
    return normalized_name, normalized_extension


def _size_in_kilobytes(size: Optional[int]) -> Decimal:
    if size in (None, 0):
        return Decimal("0.00000")
    return (Decimal(int(size)) / Decimal(1024)).quantize(
        Decimal("0.00001"), rounding=ROUND_HALF_UP
    )


def _ensure_utc(value: Optional[datetime]) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _generate_presigned_url(
    s3_client,
    config: SyncConfig,
    key: str,
    expires_in: int,
) -> str:
    return s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": config.s3_bucket, "Key": key},
        ExpiresIn=expires_in,
    )


def _normalize_object_for_table(
    obj: _S3ObjectInfo,
    columns: Sequence[str],
    config: SyncConfig,
    s3_client,
    *,
    expires_in: int = 7 * 24 * 60 * 60,
) -> Optional[_PreparedRecord]:
    key = obj.key
    if not key or key.endswith("/"):
        return None

    filename = posixpath.basename(key)
    name, extension = posixpath.splitext(filename)
    name = name.strip()
    if not name:
        return None
    cleaned_extension = extension.lstrip(".") or None
    if cleaned_extension is not None:
        cleaned_extension = cleaned_extension.lower()

    lookup_key = _build_lookup_key(name, cleaned_extension)
    display = name if not cleaned_extension else f"{name}.{cleaned_extension}"

    needs_url = any(column.lower() == "sgddocurl" for column in columns)
    url: Optional[str] = None
    if needs_url:
        url = _generate_presigned_url(s3_client, config, key, expires_in)

    size_value = _size_in_kilobytes(obj.size)
    timestamp = _ensure_utc(obj.last_modified)
    physical_location = f"s3://{config.s3_bucket}/{key}"

    values: List[Any] = []
    for column in columns:
        lower = column.lower()
        if lower == "sgddocid":
            values.append(generate_genexus_guid())
        elif lower == "sgddocnombre":
            values.append(name)
        elif lower == "sgddocnombreoriginal":
            values.append(name)
        elif lower == "sgddoctipo":
            values.append(cleaned_extension)
        elif lower == "sgddotamano":
            values.append(size_value)
        elif lower == "sgddocfecalta":
            values.append(timestamp)
        elif lower in {"sgddocubfisica", "sgddocubicfisica"}:
            values.append(physical_location)
        elif lower == "sgddocurl":
            values.append(url)
        elif lower == "sgddocusuarioalta":
            values.append("gestor")
        elif lower == "sgddocpublico":
            values.append(True)
        elif lower == "sgddocapporigen":
            values.append("gestor")
        else:
            values.append(None)

    return _PreparedRecord(display=display, values=tuple(values), lookup_key=lookup_key)


def _create_s3_client(config: SyncConfig):
    if boto3 is None:  # pragma: no cover - entorno sin dependencia opcional
        raise RuntimeError("La librería 'boto3' no está instalada en el entorno actual.")

    session_kwargs: Dict[str, Any] = {}
    if config.aws_region:
        session_kwargs["region_name"] = config.aws_region
    session = boto3.session.Session(**session_kwargs)

    client_kwargs: Dict[str, Any] = {}
    if S3_ENDPOINT_URL:
        client_kwargs["endpoint_url"] = S3_ENDPOINT_URL
    return session.client("s3", **client_kwargs)


def _list_s3_objects(
    config: SyncConfig, s3_client
) -> List[_S3ObjectInfo]:
    """Lista los objetos disponibles en S3 según la configuración actual."""

    if not config.s3_bucket:
        raise RuntimeError(
            "Debe configurar la variable S3_BUCKET para listar los archivos disponibles."
        )

    paginator = s3_client.get_paginator("list_objects_v2")
    request_kwargs: Dict[str, Any] = {"Bucket": config.s3_bucket}
    prefix = config.normalized_prefix()
    if prefix:
        request_kwargs["Prefix"] = prefix

    objects: List[_S3ObjectInfo] = []
    for page in paginator.paginate(**request_kwargs):
        contents = page.get("Contents", [])
        for obj in contents:
            key = obj.get("Key")
            if not key:
                continue
            size = obj.get("Size")
            last_modified = obj.get("LastModified")
            objects.append(
                _S3ObjectInfo(
                    key=str(key),
                    size=int(size) if size is not None else None,
                    last_modified=last_modified,
                )
            )
    return objects


def _insert_new_records(connection, values: Iterable[Tuple[Any, ...]]) -> int:
    """Inserta los registros indicados en la tabla configurada de la base gestor."""

    if sql is None or execute_values is None:
        raise RuntimeError("El controlador de PostgreSQL no está disponible.")

    prepared = [tuple(row) for row in values]
    if not prepared:
        return 0

    schema, table_name, columns = _gestor_table_identifiers()
    if any(len(row) != len(columns) for row in prepared):
        raise RuntimeError(
            "Los datos a insertar no coinciden con la cantidad de columnas configuradas."
        )

    column_identifiers = [sql.Identifier(column) for column in columns]

    with connection.cursor() as cursor:
        query = sql.SQL(
            "INSERT INTO {table} ({columns}) VALUES %s ON CONFLICT DO NOTHING"
        ).format(
            table=sql.Identifier(schema, table_name),
            columns=sql.SQL(", ").join(column_identifiers),
        )
        execute_values(cursor, query, prepared)
    return len(prepared)


def _incorporate_files(
    connection,
    config: SyncConfig,
    registered: Dict[Tuple[str, Optional[str]], str],
) -> Tuple[List[str], List[str], str, Dict[Tuple[str, Optional[str]], str]]:
    """Clasifica e inserta los archivos faltantes en la base gestor."""

    try:
        s3_client = _create_s3_client(config)
    except RuntimeError:
        raise
    except Exception as exc:  # pragma: no cover - inicialización dependiente del entorno
        raise RuntimeError("No se pudo inicializar el cliente de S3.") from exc

    try:
        s3_objects = _list_s3_objects(config, s3_client)
    except (BotoCoreError, ClientError, NoCredentialsError) as exc:
        raise RuntimeError(
            "No se pudo obtener el listado de objetos desde S3. "
            "Verifique las credenciales y el bucket configurado."
        ) from exc

    _, _, columns = _gestor_table_identifiers()
    normalized_entries: List[_PreparedRecord] = []
    for obj in s3_objects:
        normalized = _normalize_object_for_table(obj, columns, config, s3_client)
        if not normalized:
            continue
        normalized_entries.append(normalized)

    already_registered = sorted(
        registered.get(entry.lookup_key, entry.display)
        for entry in normalized_entries
        if entry.lookup_key in registered
    )
    to_insert = [
        entry for entry in normalized_entries if entry.lookup_key not in registered
    ]

    inserted_count = _insert_new_records(
        connection, (entry.values for entry in to_insert)
    )
    added_records = sorted(entry.display for entry in to_insert)

    if inserted_count:
        message = (
            f"Se incorporaron {inserted_count} archivos nuevos a la base gestor."
        )
    else:
        message = "No se encontraron archivos nuevos para incorporar."

    new_registry = {entry.lookup_key: entry.display for entry in to_insert}
    return already_registered, added_records, message, new_registry


@app.route("/copy", methods=["GET", "POST"])
def copy_view():
    defaults = config_from_env()
    form_context = {
        "sftp_host": defaults.sftp_host,
        "sftp_port": defaults.sftp_port,
        "sftp_username": defaults.sftp_username,
        "sftp_password": "",
        "sftp_private_key": defaults.sftp_private_key or "",
        "sftp_passphrase": "",
        "sftp_base_path": defaults.sftp_base_path,
        "sftp_encodings": ", ".join(defaults.sftp_encodings),
        "s3_bucket": defaults.s3_bucket,
        "s3_prefix": defaults.s3_prefix,
        "aws_region": defaults.aws_region or "",
        "delete_remote_after_upload": defaults.delete_remote_after_upload,
        "allowed_extensions": (
            "*"
            if defaults.allowed_extensions is None
            else ", ".join(defaults.allowed_extensions)
        ),
        "dry_run": False,
        "list_only": False,
    }
    plan: Optional[Dict[str, Any]] = None
    message: Optional[str] = None
    error: Optional[str] = None
    extra_logs: Optional[str] = None

    if request.method == "POST":
        form = request.form
        encodings = _split_csv(form.get("sftp_encodings", "utf-8,latin-1,cp1252"))
        allowed_raw = form.get("allowed_extensions", "")
        allowed_exts = _split_csv(allowed_raw)
        if allowed_raw.strip() == "*":
            allowed_for_config: Optional[Sequence[str]] = None
        elif allowed_exts:
            allowed_for_config = allowed_exts
        else:
            allowed_for_config = defaults.allowed_extensions
        private_key_raw = (form.get("sftp_private_key") or "").strip()
        private_key = private_key_raw or (defaults.sftp_private_key or None)

        password_raw = form.get("sftp_password", "")
        if password_raw:
            password = password_raw
        elif private_key:
            password = None
        else:
            password = defaults.sftp_password

        passphrase_raw = form.get("sftp_passphrase", "")
        if passphrase_raw:
            passphrase = passphrase_raw
        elif private_key:
            passphrase = defaults.sftp_passphrase
        else:
            passphrase = None

        config = SyncConfig(
            sftp_host=form.get("sftp_host", "").strip(),
            sftp_port=int(form.get("sftp_port", defaults.sftp_port) or defaults.sftp_port),
            sftp_username=form.get("sftp_username", "").strip(),
            sftp_password=password,
            sftp_private_key=private_key,
            sftp_passphrase=passphrase,
            sftp_base_path=form.get("sftp_base_path", ".").strip() or ".",
            sftp_encodings=encodings or defaults.sftp_encodings,
            s3_bucket=form.get("s3_bucket", "").strip(),
            s3_prefix=form.get("s3_prefix", "").strip(),
            aws_region=form.get("aws_region") or None,
            delete_remote_after_upload=_parse_bool(form.get("delete_remote_after_upload")),
            allowed_extensions=allowed_for_config,
        )
        options = SyncOptions(
            dry_run=_parse_bool(form.get("dry_run")),
            list_only=_parse_bool(form.get("list_only")),
        )
        operation = form.get("operation", "list")

        try:
            if operation == "list":
                encoding, remote_files, existing_objects = build_sync_plan(config)
                plan = _build_plan_context(
                    config, encoding, remote_files, existing_objects
                )
                copy_manager.update_plan(plan)
                message = "Listado actualizado."
            elif operation == "start_copy":
                encoding, remote_files, existing_objects = build_sync_plan(config)
                plan = _build_plan_context(
                    config, encoding, remote_files, existing_objects
                )
                copy_manager.update_plan(plan)
                missing_paths = [
                    item["remote_path"] for item in plan.get("missing_files", [])
                ]
                if not missing_paths:
                    message = "No hay archivos pendientes para copiar."
                else:
                    started = copy_manager.start(
                        config, options, missing_paths, plan
                    )
                    if started:
                        message = "Se inició la copia de archivos."
                    else:
                        message = "Ya hay una copia en ejecución."
            elif operation == "stop_copy":
                if copy_manager.request_stop():
                    message = "Se detendrá al finalizar el archivo actual y se actualizarán las listas."
                else:
                    message = "No hay una copia en ejecución."
            else:
                encoding, remote_files, existing_objects = build_sync_plan(config)
                plan = _build_plan_context(
                    config, encoding, remote_files, existing_objects
                )
                copy_manager.update_plan(plan)
                message = "Listado actualizado."
        except SyncError as exc:
            error = str(exc)
        except Exception as exc:  # pragma: no cover - ruta defensiva
            app.logger.exception("Error inesperado durante la sincronización")
            error = f"Ocurrió un error inesperado: {exc}"
            extra_logs = traceback.format_exc()
        finally:
            form_context.update(
                {
                    "sftp_host": config.sftp_host,
                    "sftp_port": config.sftp_port,
                    "sftp_username": config.sftp_username,
                    "sftp_private_key": config.sftp_private_key or "",
                    "sftp_password": password_raw,
                    "sftp_passphrase": passphrase_raw,
                    "sftp_base_path": config.sftp_base_path,
                    "sftp_encodings": ", ".join(config.sftp_encodings),
                    "s3_bucket": config.s3_bucket,
                    "s3_prefix": config.s3_prefix,
                    "aws_region": config.aws_region or "",
                    "delete_remote_after_upload": config.delete_remote_after_upload,
                    "allowed_extensions": (
                        "*"
                        if config.allowed_extensions is None
                        else ", ".join(config.allowed_extensions)
                    ),
                    "dry_run": options.dry_run,
                    "list_only": options.list_only,
                }
            )

    copy_state = copy_manager.snapshot()
    summary = copy_state.get("summary")
    state_logs = copy_state.get("logs", "")
    if extra_logs:
        logs = f"{state_logs}\n\n{extra_logs}" if state_logs else extra_logs
    else:
        logs = state_logs
    status = copy_state.get("status", "idle")
    status_label = _status_label(status)
    current_file = copy_state.get("current_file")
    if plan is None:
        plan = copy_state.get("plan")
    if not message:
        message = copy_state.get("message")
    if not error:
        error = copy_state.get("error")

    context = {
        "form": form_context,
        "plan": plan,
        "summary": summary,
        "logs": logs,
        "error": error,
        "message": message,
        "status": status,
        "status_label": status_label,
        "current_file": current_file,
        "disable_start": status in {"running", "stopping"},
        "disable_stop": status not in {"running", "stopping"},
    }
    return render_template("copy.html", **context)


@app.route("/copy/status")
def copy_status():
    state = copy_manager.snapshot(consume_refresh=True)
    status = state.get("status", "idle")
    return jsonify(
        {
            "status": status,
            "status_label": _status_label(status),
            "current_file": state.get("current_file"),
            "message": state.get("message"),
            "error": state.get("error"),
            "should_refresh": bool(state.get("needs_refresh")),
        }
    )


@app.route("/health")
def healthcheck():
    return {"status": "ok"}


if __name__ == "__main__":  # pragma: no cover
    app.run(debug=True, host="0.0.0.0", port=5000)
