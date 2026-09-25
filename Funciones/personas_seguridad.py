"""Memoria de personas consultadas (colección `personas_seguridad`).

2026-09-25: cada estudio que crea una empresa deja registro de la PERSONA
evaluada (cédula, nombres/apellidos informados, nombre VERIFICADO por la
cascada PGN→Policía y fecha de expedición de la cédula si alguna vez se
envió). Propósito: optimizar consultas futuras — el portal autollena los
datos con onBlur de la cédula y el POST del estudio usa la memoria cuando
el body no trae `fecha_expedicion` (delitos_sexuales) o nombres/apellidos
(rama_judicial / captcha de la PGN).

Aislamiento por tenant (decisión del usuario 2026-09-25): una empresa solo
ve/usa la memoria de las personas que ELLA consultó antes (array `empresas`).
Los datos de la persona son iguales para todos, pero no se filtra a un tenant
qué personas consultó otro.

Todo best-effort: un fallo de esta memoria JAMÁS tumba un estudio.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from bson import ObjectId

from bd.bd_cliente import bd_cliente

logger = logging.getLogger(__name__)

db = bd_cliente["integra"]
col_personas = db["personas_seguridad"]

_indices_creados = False


def asegurar_indices_personas() -> None:
    """Índices idempotentes (patrón asegurar_indices_cobro; tolera fallo)."""
    global _indices_creados
    if _indices_creados:
        return
    try:
        col_personas.create_index([("cedula", 1)], name="idx_persseg_cedula", unique=True)
        # Gate de visibilidad por empresa: el lookup filtra (cedula, empresas).
        col_personas.create_index(
            [("cedula", 1), ("empresas", 1)], name="idx_persseg_cedula_empresa"
        )
        _indices_creados = True
    except Exception as exc:
        logger.warning("Índices de personas_seguridad no se pudieron crear: %s", exc)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def registrar_consulta_persona(
    cedula: str,
    empresa_id: str | ObjectId,
    *,
    nombres: str | None = None,
    apellidos: str | None = None,
    nombre_consultado: str | None = None,
    fecha_expedicion: str | None = None,
) -> None:
    """Upsert NO destructivo de la persona consultada. Jamás lanza.

    - Campos opcionales solo se escriben si traen valor (una consulta sin
      nombres no borra los que ya había).
    - `nombre_consultado` es el VERIFICADO por el portal (cascada PGN→
      Policía): siempre gana sobre el anterior.
    """
    if not (cedula or "").strip():
        return
    try:
        empresa_oid = ObjectId(empresa_id) if not isinstance(empresa_id, ObjectId) else empresa_id
    except Exception:
        logger.warning("registrar_consulta_persona: empresa_id inválido (%r)", empresa_id)
        return
    ahora = _utcnow()
    set_dinamico: dict[str, Any] = {"ultima_consulta_en": ahora, "actualizado_en": ahora}
    if (nombres or "").strip():
        set_dinamico["nombres"] = nombres.strip()
    if (apellidos or "").strip():
        set_dinamico["apellidos"] = apellidos.strip()
    if (nombre_consultado or "").strip():
        set_dinamico["nombre_consultado"] = nombre_consultado.strip()
    if (fecha_expedicion or "").strip():
        set_dinamico["fecha_expedicion"] = fecha_expedicion.strip()
    try:
        col_personas.update_one(
            {"cedula": cedula.strip()},
            {
                "$set": set_dinamico,
                # total_consultas SOLO por $inc: $setOnInsert+$inc sobre el
                # mismo campo es operadores en conflicto para Mongo (error).
                "$setOnInsert": {"primera_consulta_en": ahora},
                "$inc": {"total_consultas": 1},
                "$addToSet": {"empresas": empresa_oid},
            },
            upsert=True,
        )
    except Exception as exc:
        logger.warning("Memoria de personas (%s) no se pudo actualizar: %s", cedula, exc)


def buscar_persona(cedula: str, empresa_id: str | ObjectId) -> dict | None:
    """Persona consultada antes por ESTA empresa (aislamiento por tenant).

    Retorna un dict acotado (sin `_id`, sin array de empresas) o None si la
    empresa no la ha consultado. Fallos de BD → None (best-effort).
    """
    if not (cedula or "").strip():
        return None
    try:
        empresa_oid = ObjectId(empresa_id) if not isinstance(empresa_id, ObjectId) else empresa_id
        doc = col_personas.find_one({"cedula": cedula.strip(), "empresas": empresa_oid})
    except Exception as exc:
        logger.warning("buscar_persona (%s) falló: %s", cedula, exc)
        return None
    if not doc:
        return None
    return {
        "nombres": doc.get("nombres") or "",
        "apellidos": doc.get("apellidos") or "",
        "nombre_consultado": doc.get("nombre_consultado") or "",
        "fecha_expedicion": doc.get("fecha_expedicion") or "",
        "total_consultas": doc.get("total_consultas") or 0,
        "ultima_consulta_en": doc.get("ultima_consulta_en"),
    }
