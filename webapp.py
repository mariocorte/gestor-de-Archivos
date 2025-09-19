"""Aplicación web para ejecutar ``sync_orion_files`` desde el navegador."""
from __future__ import annotations

import logging
import posixpath
import traceback
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from flask import Flask, render_template, request

from sync_orion_files import (
    RemoteFile,
    SyncConfig,
    SyncError,
    SyncOptions,
    build_path_variants,
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


def _apply_selection_to_plan(plan: Dict[str, Any], selected_paths: Sequence[str]) -> None:
    base_path = plan.get("base_path", "")
    size_lookup: Dict[str, Tuple[str, int]] = {}
    for item in plan["missing_files"]:
        for variant in build_path_variants(item["remote_path"], base_path):
            size_lookup.setdefault(variant, (item["remote_path"], item["size"]))

    matched_paths: Set[str] = set()
    selected_total = 0
    for path in selected_paths:
        for variant in build_path_variants(path, base_path):
            entry = size_lookup.get(variant)
            if entry is None:
                continue
            remote_path, size = entry
            if remote_path in matched_paths:
                continue
            matched_paths.add(remote_path)
            selected_total += size
            break

    plan["selected_total_bytes"] = selected_total
    plan["selected_total_label"] = _format_size(selected_total)


@app.template_filter("human_size")
def _human_size_filter(value: Any) -> str:
    try:
        return _format_size(int(value))
    except (TypeError, ValueError):
        return "-"


@app.route("/", methods=["GET", "POST"])
def index():
    defaults = config_from_env()
    context = {
        "form": {
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
            "operation": "list",
        },
        "summary": None,
        "logs": "",
        "error": None,
        "plan": None,
        "selected_files": [],
    }

    if request.method == "POST":
        form = request.form
        encodings = _split_csv(form.get("sftp_encodings", "utf-8,latin-1,cp1252"))
        allowed_raw = form.get("allowed_extensions", "")
        allowed_exts = _split_csv(allowed_raw)
        if allowed_raw.strip() == "*":
            allowed_for_config = None
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
            delete_remote_after_upload=_parse_bool(
                form.get("delete_remote_after_upload")
            ),
            allowed_extensions=allowed_for_config,
        )
        options = SyncOptions(
            dry_run=_parse_bool(form.get("dry_run")),
            list_only=_parse_bool(form.get("list_only")),
        )

        operation = form.get("operation", "list")
        selected_files = list(form.getlist("selected_files"))
        context["selected_files"] = selected_files
        context["form"]["operation"] = operation

        handler = _BufferLogHandler()
        sync_logger = logging.getLogger("sync_orion")
        previous_level = sync_logger.level
        sync_logger.setLevel(logging.INFO)
        sync_logger.addHandler(handler)

        plan: Optional[Dict[str, Any]] = None

        extra_logs: Optional[str] = None

        try:
            if operation == "sync":
                if not selected_files:
                    context["error"] = "Selecciona al menos un archivo para copiar."
                    encoding, remote_files, existing_objects = build_sync_plan(config)
                    plan = _build_plan_context(
                        config, encoding, remote_files, existing_objects
                    )
                else:
                    summary = run_sync(
                        config, options, selected_remote_paths=selected_files
                    )
                    context["summary"] = summary
                    plan = _build_plan_context(
                        config,
                        summary.encoding_used,
                        summary.remote_files,
                        summary.existing_objects,
                    )
                    selected_files = [
                        item["remote_path"] for item in plan["missing_files"]
                    ]
                    context["selected_files"] = selected_files
            else:
                encoding, remote_files, existing_objects = build_sync_plan(config)
                plan = _build_plan_context(
                    config, encoding, remote_files, existing_objects
                )
                if not selected_files:
                    selected_files = [
                        item["remote_path"] for item in plan["missing_files"]
                    ]
                    context["selected_files"] = selected_files
        except SyncError as exc:
            context["error"] = str(exc)
        except Exception as exc:  # pragma: no cover - ruta defensiva
            app.logger.exception("Error inesperado durante la sincronización")
            context["error"] = f"Ocurrió un error inesperado: {exc}"
            extra_logs = traceback.format_exc()
        finally:
            sync_logger.removeHandler(handler)
            sync_logger.setLevel(previous_level)
            logs = handler.text
            if extra_logs:
                logs = f"{logs}\n\n{extra_logs}" if logs else extra_logs
            context["logs"] = logs
            context["form"].update({
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
                "operation": operation,
            })
            if plan is not None:
                _apply_selection_to_plan(plan, context["selected_files"])
                context["plan"] = plan

    return render_template("index.html", **context)


@app.route("/health")
def healthcheck():
    return {"status": "ok"}


if __name__ == "__main__":  # pragma: no cover
    app.run(debug=True, host="0.0.0.0", port=5000)
