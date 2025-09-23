"""Utilidades relacionadas con integraciones de GeneXus."""

from __future__ import annotations

import uuid


def generate_genexus_guid() -> str:
    """Genera un GUID con el formato habitual de GeneXus.

    GeneXus almacena los atributos de tipo GUID como cadenas en mayúsculas con
    guiones. El generador estándar de Python produce letras minúsculas, por lo
    que aquí se normaliza la representación.
    """

    return str(uuid.uuid4()).upper()
