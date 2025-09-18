"""Aplicación web para ejecutar ``sync_orion_files`` desde el navegador."""
from __future__ import annotations

import logging
from typing import List, Optional

from flask import Flask, render_template, request

from sync_orion_files import (
    SyncConfig,
    SyncError,
    SyncOptions,
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
        },
        "summary": None,
        "logs": "",
        "error": None,
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
        config = SyncConfig(
            sftp_host=form.get("sftp_host", "").strip(),
            sftp_port=int(form.get("sftp_port", defaults.sftp_port) or defaults.sftp_port),
            sftp_username=form.get("sftp_username", "").strip(),
            sftp_password=form.get("sftp_password") or None,
            sftp_private_key=form.get("sftp_private_key") or None,
            sftp_passphrase=form.get("sftp_passphrase") or None,
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

        handler = _BufferLogHandler()
        sync_logger = logging.getLogger("sync_orion")
        previous_level = sync_logger.level
        sync_logger.setLevel(logging.INFO)
        sync_logger.addHandler(handler)

        try:
            summary = run_sync(config, options)
            context["summary"] = summary
        except SyncError as exc:
            context["error"] = str(exc)
        finally:
            sync_logger.removeHandler(handler)
            sync_logger.setLevel(previous_level)
            context["logs"] = handler.text
            context["form"].update({
                "sftp_host": config.sftp_host,
                "sftp_port": config.sftp_port,
                "sftp_username": config.sftp_username,
                "sftp_private_key": config.sftp_private_key or "",
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
            })

    return render_template("index.html", **context)


@app.route("/health")
def healthcheck():
    return {"status": "ok"}


if __name__ == "__main__":  # pragma: no cover
    app.run(debug=True, host="0.0.0.0", port=5000)
