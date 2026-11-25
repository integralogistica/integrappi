# Efectos de temporada de la app (2026-11-25, 10.ª tanda).
#
# El ADMIN configura desde /Apariencia (grupo ADMINISTRACIÓN del hub) el
# efecto visual global de la aplicación — navidad (copos), halloween, fiestas
# patrias, celebración, san valentín, semana santa — y TODAS las páginas lo
# muestran (partículas + acento del header unificado). Fácil de prender y
# apagar: un solo documento.
#
# - GET /apariencia : PÚBLICO (las páginas lo consultan al montar, sin token)
#                     con cache en memoria de 60 s. Devuelve {efecto, etiqueta,
#                     actualizado_por, fecha} — los metadatos son informativos.
# - PUT /apariencia : solo ADMIN (patrón _resolver_usuario/_requiere de
#                     otros_costos). Audita quién/cuándo.
#
# Colección `config_apariencia` (una colección = un propósito): UN doc
# {_id: "global", efecto, actualizado_por, nombre, fecha}.

import logging
import time
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from bd.bd_cliente import bd_cliente
from rutas.otros_costos import _resolver_usuario, _requiere

logger = logging.getLogger("apariencia")

router = APIRouter(prefix="/apariencia", tags=["Apariencia"])

db = bd_cliente["integra"]
col_apariencia = db["config_apariencia"]

DOC_ID = "global"

# Catálogo de efectos (id → etiqueta visible). Espejo del frontend
# (src/Funciones/efectosTemporada.ts) — mantener sincronizados.
EFECTOS_VALIDOS = {
    "ninguno": "Ninguno",
    "navidad": "Navidad",
    "halloween": "Halloween",
    "patrias": "Fiestas patrias",
    "celebracion": "Celebración",
    "san_valentin": "San Valentín",
    "semana_santa": "Semana Santa",
}

# Cache en memoria del GET (el frontend lo consulta en cada carga de página):
# 60 s evita miles de round-trips a Mongo durante un día laboral.
_CACHE_TTL = 60
_cache: dict = {"datos": None, "ts": 0.0}


def _leer_config() -> dict:
    doc = col_apariencia.find_one({"_id": DOC_ID})
    if not doc:
        # Default: sin efecto (no se siembra nada — el GET siempre responde).
        return {"efecto": "ninguno", "actualizado_por": None, "nombre": None, "fecha": None}
    return {
        "efecto": doc.get("efecto", "ninguno"),
        "actualizado_por": doc.get("actualizado_por"),
        "nombre": doc.get("nombre"),
        "fecha": doc.get("fecha"),
    }


@router.get("")
async def obtener_apariencia():
    """Público: las páginas consultan el efecto vigente al montar."""
    ahora = time.monotonic()
    if _cache["datos"] is None or ahora - _cache["ts"] > _CACHE_TTL:
        try:
            _cache["datos"] = _leer_config()
            _cache["ts"] = ahora
        except Exception as e:
            logger.warning(f"[APARIENCIA] No se pudo leer la config: {e}")
            if _cache["datos"] is None:
                return {"efecto": "ninguno", "etiqueta": EFECTOS_VALIDOS["ninguno"]}
    datos = _cache["datos"]
    return {
        "efecto": datos["efecto"],
        "etiqueta": EFECTOS_VALIDOS.get(datos["efecto"], datos["efecto"]),
        "actualizado_por": datos.get("actualizado_por"),
        "nombre": datos.get("nombre"),
        "fecha": datos.get("fecha"),
    }


class AparienciaInput(BaseModel):
    efecto: str = Field(..., max_length=30)
    usuario: Optional[str] = Field(None, max_length=60, description="ADMIN que cambia el efecto (cookie)")


@router.put("")
async def actualizar_apariencia(data: AparienciaInput):
    """Solo ADMIN: enciende/apaga el efecto de temporada de TODA la app."""
    if not data.usuario:
        raise HTTPException(status_code=403, detail="Falta el usuario (cookie) para autorizar el cambio.")
    info = _resolver_usuario(data.usuario)
    _requiere(info, {"ADMIN"}, "cambiar la apariencia de la app")

    efecto = data.efecto.strip().lower()
    if efecto not in EFECTOS_VALIDOS:
        raise HTTPException(
            status_code=422,
            detail=f"Efecto inválido: '{efecto}'. Válidos: {', '.join(EFECTOS_VALIDOS)}",
        )

    col_apariencia.update_one(
        {"_id": DOC_ID},
        {"$set": {
            "efecto": efecto,
            "actualizado_por": info["usuario"],
            "nombre": info.get("nombre") or info["usuario"],
            "fecha": datetime.now(timezone.utc).isoformat(),
        }},
        upsert=True,
    )
    # Invalidar cache: el cambio debe verse de inmediato en cualquier página.
    _cache["datos"] = None
    _cache["ts"] = 0.0
    return {"mensaje": f"Efecto actualizado: {EFECTOS_VALIDOS[efecto]}", "efecto": efecto, "etiqueta": EFECTOS_VALIDOS[efecto]}
