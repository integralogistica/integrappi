"""API de Seguridad: consultas de antecedentes de conductores desde portales públicos.

- Manifiestos: Historial de Viajes del RNDC (Ministerio de Transporte) por
  cédula, ventana fija del último año, vía Funciones/bot_rndc2.py (captcha
  aritmético de texto). El canal por web service no está disponible para las
  credenciales actuales (proceso 12 deshabilitado — ver scripts/probar_rndc_placa.py).
- Procuraduría: certificado de antecedentes disciplinarios (Ley 1238: consulta
  expresamente habilitada a entidades públicas y privadas para aspirantes a
  cargos/contratos) vía Funciones/bot_procuraduria.py (captcha aritmético por
  regex o de conocimiento general con Gemini). El certificado PDF queda
  guardado y el veredicto se extrae del texto.

Cada consulta real se audita/cachea en `consultas_seguridad` (24h).
La identidad se resuelve por CORREO (login actual) o nombre de usuario contra
baseusuarios, con perfil SEGURIDAD o ADMIN.
"""
from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException, Query

from bd.bd_cliente import bd_cliente
from Funciones.bot_procuraduria import BotProcuraduriaError, consultar_antecedentes_sync
from Funciones.bot_rndc2 import BotRNDC2Error, consultar_historial_viajes_sync
from rutas.otros_costos import _requiere

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/seguridad", tags=["Seguridad"])

PERFILES_SEGURIDAD = {"SEGURIDAD", "ADMIN"}

# Ventana de consulta hacia atrás desde hoy. Últimos 12 meses (1 año).
DIAS_VENTANA = 365
# Caché: misma cédula dentro de esta ventana no vuelve a golpear el portal.
HORAS_CACHE = 24

db = bd_cliente["integra"]
col_consultas = db["consultas_seguridad"]
col_usuarios = db["baseusuarios"]
try:
    col_consultas.create_index([("tipo", 1), ("cedula", 1), ("consultado_en", -1)], name="idx_seg_tipo_cedula")
    col_consultas.create_index([("consultado_en", -1)], name="idx_seg_fecha")
except Exception as exc:
    logger.warning("No se pudo crear índices de consultas_seguridad: %s", exc)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _resolver_usuario(usuario_o_correo: str) -> dict:
    """Resuelve la identidad por CORREO (login actual) o, como fallback,
    por nombre de usuario — el patrón de otros_costos ampliado para el login
    por correo de baseusuarios.
    """
    valor = str(usuario_o_correo or "").strip()
    if not valor:
        raise HTTPException(status_code=401, detail="No autenticado")
    doc = None
    if "@" in valor:
        doc = col_usuarios.find_one({"correo": {"$regex": f"^{re.escape(valor.lower())}$", "$options": "i"}})
    if doc is None:
        doc = col_usuarios.find_one({"usuario": valor.upper()})
    if not doc:
        raise HTTPException(status_code=401, detail="Usuario no válido")
    if not doc.get("activo", True):
        raise HTTPException(status_code=403, detail="Usuario inactivo")
    return {
        "usuario": doc["usuario"],
        "perfil": (doc.get("perfil") or "").strip().upper(),
        "regional": doc.get("regional", ""),
        "nombre": doc.get("nombre", ""),
        "id": str(doc["_id"]),
    }


def _normalizar_cedula(valor: str) -> str:
    digitos = "".join(c for c in (valor or "") if c.isdigit())
    if not 3 <= len(digitos) <= 15:
        raise HTTPException(status_code=422, detail="La cédula debe tener entre 3 y 15 dígitos")
    return digitos


def _buscar_cache(tipo: str, cedula: str, force: bool):
    """Última consulta vigente (24h) del tipo+cédula, o None."""
    if force:
        return None
    return col_consultas.find_one(
        {"tipo": tipo, "cedula": cedula, "expira_en": {"$gt": _utcnow()}},
        sort=[("consultado_en", -1)],
    )


def _envolver_cache(doc: dict) -> dict:
    """Respuesta uniforme para un hit de caché (sin _id)."""
    doc.pop("_id", None)
    doc["cache"] = True
    return doc


def _nombre_del_certificado(texto: str) -> str:
    """Extrae el nombre del certificado del PDF/texto (p. ej. tras 'señor(a)')."""
    m = re.search(r"señor\(a\)\s+([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ\s]{5,60}?)\s+identificado", texto, re.IGNORECASE)
    return m.group(1).strip() if m else ""


@router.get("/manifiestos")
async def consultar_manifiestos(
    cedula: str = Query(..., min_length=3, max_length=20, description="Cédula del conductor"),
    usuario: str | None = Query(None, description="Correo o usuario que consulta"),
    force: bool = Query(False, description="Ignorar caché (vuelve a consultar el portal)"),
):
    """Manifiestos RNDC de un conductor (último año) por cédula."""
    info = _resolver_usuario(usuario)
    _requiere(info, PERFILES_SEGURIDAD, "consultar manifiestos de seguridad")

    cedula_norm = _normalizar_cedula(cedula)
    cache = _buscar_cache("manifiestos_rndc", cedula_norm, force)
    if cache:
        return _envolver_cache(cache)

    # Ventana fija: último año en el formato AAAA/MM/DD del portal
    hasta = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=5)  # hora Colombia
    desde = hasta - timedelta(days=DIAS_VENTANA)
    fecha_inicio = desde.strftime("%Y/%m/%d")
    fecha_fin = hasta.strftime("%Y/%m/%d")

    try:
        resultado = await asyncio.to_thread(
            consultar_historial_viajes_sync,
            cedula=cedula_norm,
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
        )
    except BotRNDC2Error as exc:
        logger.error("Bot RNDC2 falló para cédula %s: %s", cedula_norm, exc)
        raise HTTPException(status_code=502, detail=f"No fue posible consultar el portal RNDC: {exc}") from exc

    viajes = [v for v in resultado.get("viajes", []) if v.get("Nro. de Radicado", "").strip().isdigit() and len(v.get("Nro. de Radicado", "").strip()) >= 6]
    ahora = _utcnow()
    doc = {
        "tipo": "manifiestos_rndc",
        "cedula": cedula_norm,
        "desde": fecha_inicio,
        "hasta": fecha_fin,
        "viajes": viajes,
        "columnas": resultado.get("columnas", []),
        "total": len(viajes),
        "usuario": info["usuario"],
        "perfil": info["perfil"],
        "consultado_en": ahora,
        "expira_en": ahora + timedelta(hours=HORAS_CACHE),
        "forzado": bool(force),
    }
    try:
        col_consultas.insert_one(doc)
    except Exception as exc:
        logger.error("Consulta manifiestos %s no se pudo auditar: %s", cedula_norm, exc)

    return {
        "tipo": "manifiestos_rndc",
        "cedula": cedula_norm,
        "cache": False,
        "consultado_en": ahora,
        "desde": fecha_inicio,
        "hasta": fecha_fin,
        "total": len(viajes),
        "viajes": viajes,
    }


@router.get("/procuraduria")
async def consultar_procuraduria(
    cedula: str = Query(..., min_length=3, max_length=20, description="Cédula a consultar"),
    usuario: str | None = Query(None, description="Correo o usuario que consulta"),
    force: bool = Query(False, description="Ignorar caché (vuelve a consultar el portal)"),
):
    """Certificado de antecedentes disciplinarios de la Procuraduría (ordinario).

    La Ley 1238 de 2008 habilita a entidades públicas y privadas a consultar
    este certificado de aspirantes a cargos/contratos. Retorna el veredicto
    (no_registra) y la ruta del PDF oficial descargado.
    """
    info = _resolver_usuario(usuario)
    _requiere(info, PERFILES_SEGURIDAD, "consultar antecedentes de la Procuraduría")

    cedula_norm = _normalizar_cedula(cedula)
    cache = _buscar_cache("procuraduria", cedula_norm, force)
    if cache:
        return _envolver_cache(cache)

    try:
        resultado = await asyncio.to_thread(consultar_antecedentes_sync, cedula_norm)
    except BotProcuraduriaError as exc:
        logger.error("Bot Procuraduría falló para cédula %s: %s", cedula_norm, exc)
        raise HTTPException(status_code=502, detail=f"No fue posible consultar la Procuraduría: {exc}") from exc

    ahora = _utcnow()
    doc = {
        "tipo": "procuraduria",
        "cedula": cedula_norm,
        "no_registra": resultado.get("no_registra"),
        "mensaje": resultado.get("mensaje", ""),
        "nombre_certificado": _nombre_del_certificado(resultado.get("texto_pdf", "")),
        "pdf_ruta": resultado.get("pdf_ruta"),
        "pdf_tamano": len(resultado["pdf_bytes"]) if resultado.get("pdf_bytes") else 0,
        "usuario": info["usuario"],
        "perfil": info["perfil"],
        "consultado_en": ahora,
        "expira_en": ahora + timedelta(hours=HORAS_CACHE),
        "forzado": bool(force),
    }
    try:
        col_consultas.insert_one(doc)
    except Exception as exc:
        logger.error("Consulta procuraduría %s no se pudo auditar: %s", cedula_norm, exc)

    doc.pop("_id", None)
    return {
        "tipo": "procuraduria",
        "cedula": cedula_norm,
        "cache": False,
        "consultado_en": ahora,
        "no_registra": resultado.get("no_registra"),
        "mensaje": resultado.get("mensaje", ""),
        "nombre_certificado": doc["nombre_certificado"],
        "pdf_ruta": resultado.get("pdf_ruta"),
    }


@router.get("/historico")
async def listar_historico(
    usuario: str | None = Query(None, description="Correo o usuario que consulta"),
    cedula: str | None = Query(None, description="Filtrar por cédula consultada"),
    tipo: str | None = Query(None, description="Filtrar por tipo (manifiestos_rndc, procuraduria)"),
    limit: int = Query(50, ge=1, le=200),
    skip: int = Query(0, ge=0),
):
    """Auditoría de consultas de seguridad. No consulta portales."""
    info = _resolver_usuario(usuario)
    _requiere(info, PERFILES_SEGURIDAD, "ver el histórico de seguridad")
    query: dict = {}
    if tipo:
        query["tipo"] = tipo
    if cedula:
        query["cedula"] = _normalizar_cedula(cedula)
    total = col_consultas.count_documents(query)
    cursor = col_consultas.find(query, {"viajes": 0, "texto_resultado": 0}).sort("consultado_en", -1).skip(skip).limit(limit)
    items = []
    for doc in cursor:
        doc["id"] = str(doc.pop("_id"))
        items.append(doc)
    return {"total": total, "items": items}
