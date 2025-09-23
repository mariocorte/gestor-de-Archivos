"""Aplicación web para ejecutar ``sync_orion_files`` desde el navegador."""
from __future__ import annotations

import dataclasses
import logging
import posixpath
import threading
import traceback
from typing import Any, Dict, List, Optional, Sequence, Tuple

from flask import Flask, jsonify, render_template, request

try:
    import psycopg2
    from psycopg2 import OperationalError
except Exception:  # pragma: no cover - fallback if driver missing at runtime
    psycopg2 = None
    OperationalError = Exception

from sync_orion_files import (
    FileProgress,
    RemoteFile,
    SyncConfig,
    SyncError,
    SyncOptions,
    build_sync_plan,
    config_from_env,
    run_sync,
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


@app.route("/incluir")
def include_view():
    connection_status = False
    error_message: Optional[str] = None

    if psycopg2 is None:
        error_message = "El controlador de PostgreSQL no está disponible en el entorno."
    else:
        try:
            connection = psycopg2.connect(
                dbname="sgdpjs",
                user="usrgestor",
                password="Gestor97",
                host="10.18.250.251",
                port=5432,
                connect_timeout=5,
            )
        except OperationalError as exc:  # pragma: no cover - depende del entorno
            error_message = str(exc)
        else:
            connection_status = True
            connection.close()

    return render_template(
        "incluir.html",
        connection_status=connection_status,
        error_message=error_message,
    )


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
