#!/usr/bin/env python3
"""Registra en ``sgdpjs`` los archivos existentes en un bucket de S3."""

from __future__ import annotations

import argparse
import logging
import os
import posixpath
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, Iterator, Optional, Sequence, Set, Tuple

import boto3
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
import psycopg2
from psycopg2.extensions import connection as PGConnection

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - ``python-dotenv`` es opcional.
    load_dotenv = None

from sync_orion_files import S3_ENDPOINT_URL, SyncConfig, config_from_env
from genexus_utils import generate_genexus_guid


logger = logging.getLogger("register_s3_documents")


class RegistrationError(RuntimeError):
    """Error de alto nivel para la sincronización de metadatos."""


@dataclass
class DatabaseConfig:
    """Parámetros de conexión a la base de datos."""

    host: str
    port: int
    dbname: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        """Construye la configuración tomando variables de entorno conocidas."""

        host = (
            os.environ.get("PGHOST")
            or os.environ.get("DB_HOST")
            or "10.18.250.250"
        )
        port_raw = os.environ.get("PGPORT") or os.environ.get("DB_PORT") or "5432"
        try:
            port = int(port_raw)
        except ValueError as exc:  # pragma: no cover - validado en tiempo de ejecución
            raise RegistrationError("El puerto de la base de datos no es válido") from exc
        dbname = (
            os.environ.get("PGDATABASE")
            or os.environ.get("DB_NAME")
            or "gestor"
        )
        user = os.environ.get("PGUSER") or os.environ.get("DB_USER") or "usrgestor"
        password = (
            os.environ.get("PGPASSWORD")
            or os.environ.get("DB_PASSWORD")
            or "Gestor97"
        )
        return cls(host=host, port=port, dbname=dbname, user=user, password=password)

    def to_connection_kwargs(self) -> Dict[str, object]:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
        }


def _load_env_file(env_file: Optional[str]) -> None:
    if not env_file:
        return
    if load_dotenv is None:
        raise RegistrationError(
            "Se indicó un archivo .env pero python-dotenv no está instalado"
        )
    if not load_dotenv(env_file):
        raise RegistrationError(f"No se pudo cargar el archivo de entorno '{env_file}'")


def _setup_logger(verbose: bool) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)


def _create_s3_client(config: SyncConfig):
    session_kwargs = {}
    if config.aws_region:
        session_kwargs["region_name"] = config.aws_region
    session = boto3.session.Session(**session_kwargs)
    client_kwargs = {}
    if S3_ENDPOINT_URL:
        client_kwargs["endpoint_url"] = S3_ENDPOINT_URL
    return session.client("s3", **client_kwargs)


def _iter_s3_objects(
    config: SyncConfig, client=None
) -> Iterator[Dict[str, object]]:
    if not config.s3_bucket:
        raise RegistrationError("Debe especificarse el bucket de destino en S3")
    if client is None:
        client = _create_s3_client(config)
    paginator = client.get_paginator("list_objects_v2")
    kwargs = {"Bucket": config.s3_bucket}
    prefix = config.normalized_prefix()
    if prefix:
        kwargs["Prefix"] = prefix
    for page in paginator.paginate(**kwargs):
        for obj in page.get("Contents", []):
            yield obj


def _normalize_name_and_extension(key: str) -> Optional[Tuple[str, Optional[str]]]:
    if not key or key.endswith("/"):
        return None
    filename = posixpath.basename(key)
    name, extension = posixpath.splitext(filename)
    if not name:
        return None
    cleaned_extension = extension.lstrip(".") or None
    if cleaned_extension is not None:
        cleaned_extension = cleaned_extension.lower()
    return name, cleaned_extension


def _size_in_kilobytes(size: Optional[int]) -> Decimal:
    if size in (None, 0):
        return Decimal("0.00000")
    return (Decimal(int(size)) / Decimal(1024)).quantize(
        Decimal("0.00001"), rounding=ROUND_HALF_UP
    )


def _ensure_utc(dt: Optional[datetime]) -> datetime:
    if dt is None:
        return datetime.now(timezone.utc)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _load_existing_pairs(conn: PGConnection) -> Set[Tuple[str, Optional[str]]]:
    with conn.cursor() as cur:
        cur.execute("SELECT sgddocnombre, sgddoctipo FROM sgdpjs")
        rows = cur.fetchall()
    existing: Set[Tuple[str, Optional[str]]] = set()
    for name, extension in rows:
        normalized = (
            (name or "").lower(),
            (extension.lower() if isinstance(extension, str) else None),
        )
        existing.add(normalized)
    return existing


def _generate_presigned_url(
    client, config: SyncConfig, key: str, expires_in: int
) -> str:
    return client.generate_presigned_url(
        "get_object",
        Params={"Bucket": config.s3_bucket, "Key": key},
        ExpiresIn=expires_in,
    )


def register_documents(
    config: SyncConfig,
    db_config: DatabaseConfig,
    *,
    expires_in: int = 7 * 24 * 60 * 60,
    dry_run: bool = False,
) -> Tuple[int, int]:
    if not config.s3_bucket:
        raise RegistrationError("Debe especificarse el bucket de destino en S3")

    client = _create_s3_client(config)
    total_existing = 0
    total_inserted = 0

    try:
        conn = psycopg2.connect(**db_config.to_connection_kwargs())
    except psycopg2.Error as exc:  # pragma: no cover - depende del entorno
        raise RegistrationError("No se pudo conectar a la base de datos") from exc

    try:
        with conn:
            existing_pairs = _load_existing_pairs(conn)
            logger.info(
                "Registros ya existentes detectados: %d", len(existing_pairs)
            )

            for obj in _iter_s3_objects(config, client):
                key = obj.get("Key")
                normalized = _normalize_name_and_extension(str(key))
                if not normalized:
                    continue
                name, extension = normalized
                lookup_key = (name.lower(), extension.lower() if extension else None)
                if lookup_key in existing_pairs:
                    total_existing += 1
                    continue

                raw_size = obj.get("Size")
                size_bytes = int(raw_size) if raw_size is not None else 0
                size = _size_in_kilobytes(size_bytes)
                timestamp = _ensure_utc(obj.get("LastModified"))
                url = _generate_presigned_url(client, config, str(key), expires_in)

                doc_id = generate_genexus_guid()
                physical_location = f"s3://{config.s3_bucket}/{key}"

                logger.debug(
                    "Insertando registro para '%s' (%s) - tamaño %s KB", name, key, size
                )

                if dry_run:
                    total_inserted += 1
                    existing_pairs.add(lookup_key)
                    continue

                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO sgdpjs (
                            sgddocid,
                            sgddocnombre,
                            sgddocnombreoriginal,
                            sgddoctipo,
                            sgddoctamano,
                            sgddocfecalta,
                            sgddocubicfisica,
                            sgddocurl,
                            sgddocusuarioalta,
                            sgddocpublico,
                            sgddocapporigen
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            doc_id,
                            name,
                            "original",
                            extension,
                            size,
                            timestamp,
                            physical_location,
                            url,
                            "gestor",
                            True,
                            "gestor",
                        ),
                    )

                total_inserted += 1
                existing_pairs.add(lookup_key)
    finally:
        conn.close()
    return total_inserted, total_existing


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Crea registros en sgdpjs para los archivos ya presentes en S3"
    )
    parser.add_argument("--env-file", help="Ruta a un archivo .env con variables")
    parser.add_argument(
        "--expires-in",
        type=int,
        default=7 * 24 * 60 * 60,
        help="Tiempo de vigencia en segundos para la URL firmada (por defecto 7 días)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simula la inserción sin modificar la base de datos",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Muestra información detallada"
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = _parse_args(argv)
    _setup_logger(args.verbose)
    _load_env_file(args.env_file)

    config = config_from_env()
    db_config = DatabaseConfig.from_env()

    try:
        inserted, existing = register_documents(
            config,
            db_config,
            expires_in=args.expires_in,
            dry_run=args.dry_run,
        )
    except RegistrationError as exc:
        logger.error("%s", exc)
        raise SystemExit(1) from exc
    except (BotoCoreError, ClientError, NoCredentialsError) as exc:
        logger.error("Error al consultar S3: %s", exc)
        raise SystemExit(2) from exc

    logger.info("Archivos ya registrados: %d", existing)
    logger.info("Nuevos registros %s: %d", "simulados" if args.dry_run else "insertados", inserted)
    if args.dry_run:
        logger.info("La ejecución se realizó en modo simulación; no se hicieron cambios.")


if __name__ == "__main__":
    main()
