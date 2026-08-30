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
Autenticación (desde 2026-08-29): Bearer JWT del módulo de seguridad
(Funciones/auth_seguridad.py) — perfil SEGURIDAD o ADMIN con empresa activa.
El API de estudios consolidados (cédula → PDF) vive en rutas/seguridad_estudios.py.
"""
from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query

from bd.bd_cliente import bd_cliente
from Funciones.auth_seguridad import actor_actual
from Funciones.bot_policia import (
    BotPoliciaError,
    BotPoliciaSinCaptchaKey,
    consultar_antecedentes_policia_sync,
)
from Funciones.bot_procuraduria import BotProcuraduriaError, consultar_antecedentes_sync
from Funciones.bot_rndc2 import BotRNDC2Error, consultar_historial_viajes_sync
from Funciones.bot_runt import BotRuntError, BotRuntSinCaptchaKey, consultar_vehiculo_runt_sync

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


def _requiere_seguridad(actor: dict) -> None:
    """El actor (ya autenticado por Bearer) debe ser SEGURIDAD o ADMIN."""
    if actor["perfil"] not in PERFILES_SEGURIDAD:
        raise HTTPException(
            status_code=403,
            detail=f"Su perfil ({actor['perfil']}) no tiene permiso para usar el módulo de seguridad.",
        )


def _normalizar_cedula(valor: str) -> str:
    digitos = "".join(c for c in (valor or "") if c.isdigit())
    if not 3 <= len(digitos) <= 15:
        raise HTTPException(status_code=422, detail="La cédula debe tener entre 3 y 15 dígitos")
    return digitos


def _normalizar_placa(valor: str) -> str:
    """Placa colombiana normalizada: AAA123 (particular), AAA12A (moto) o
    AA1234 (formato viejo). El portal es la autoridad final; esto solo evita
    basura obvia en el input."""
    placa = re.sub(r"[^A-Za-z0-9]", "", valor or "").upper()
    if not re.fullmatch(r"[A-Z]{3}[0-9]{2}[0-9A-Z]|[A-Z]{2}[0-9]{4}", placa):
        raise HTTPException(
            status_code=422,
            detail="Placa inválida. Formatos: AAA123 (particular) o AAA12A (moto)",
        )
    return placa


def _buscar_cache(tipo: str, cedula: str, force: bool, *, placa: str | None = None):
    """Última consulta vigente (24h) del tipo+cédula, o None.

    RNDC con 0 viajes y sin confirmación del portal ('Consulta realizada')
    NO cuenta como caché válida: fue una respuesta incompleta que jamás debió
    guardarse (fix 2026-08-29) — se ignora y se vuelve al portal.

    Procuraduría SIN veredicto y SIN PDF (fix 2026-08-30): igual — una
    respuesta vacía así jamás debió cachearse; se ignora al leer.

    runt discrimina además por PLACA (una cédula puede tener varios vehículos):
    sin placa no hay identidad de caché → nunca hit (evita cross-contaminación
    entre placas de la misma cédula)."""
    if force:
        return None
    filtro = {"tipo": tipo, "cedula": cedula, "expira_en": {"$gt": _utcnow()}}
    if tipo == "runt":
        if not placa:
            return None
        filtro["placa"] = placa
    doc = col_consultas.find_one(filtro, sort=[("consultado_en", -1)])
    if (
        doc
        and tipo == "manifiestos_rndc"
        and not (doc.get("viajes") or [])
        and "consulta realizada" not in (doc.get("mensaje_portal") or "").lower()
    ):
        return None
    if (
        doc
        and tipo == "procuraduria"
        and doc.get("no_registra") is None
        and not (doc.get("pdf_tamano") or 0)
    ):
        return None
    return doc


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
    force: bool = Query(False, description="Ignorar caché (vuelve a consultar el portal)"),
    actor: dict = Depends(actor_actual),
):
    """Manifiestos RNDC de un conductor (último año) por cédula."""
    _requiere_seguridad(actor)

    cedula_norm = _normalizar_cedula(cedula)
    cache = _buscar_cache("manifiestos_rndc", cedula_norm, force)
    if cache:
        return _envolver_cache(cache)

    # Ventana fija: último año en el formato AAAA/MM/DD del portal.
    # Hora Colombia = UTC−5 (RESTAR 5 h). Sumarlas producía una fecha FUTURA
    # entre las 19:00–24:00 CO y el portal respondía sin módulo de consulta.
    hasta = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=5)  # hora Colombia
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
        "usuario": actor["usuario"],
        "perfil": actor["perfil"],
        "empresa_id": actor.get("empresa_id"),
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
    force: bool = Query(False, description="Ignorar caché (vuelve a consultar el portal)"),
    actor: dict = Depends(actor_actual),
):
    """Certificado de antecedentes disciplinarios de la Procuraduría (ordinario).

    La Ley 1238 de 2008 habilita a entidades públicas y privadas a consultar
    este certificado de aspirantes a cargos/contratos. Retorna el veredicto
    (no_registra) y la ruta del PDF oficial descargado.
    """
    _requiere_seguridad(actor)

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
        "usuario": actor["usuario"],
        "perfil": actor["perfil"],
        "empresa_id": actor.get("empresa_id"),
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


@router.get("/policia")
async def consultar_policia(
    cedula: str = Query(..., min_length=3, max_length=20, description="Cédula a consultar"),
    force: bool = Query(False, description="Ignorar caché (vuelve a consultar el portal)"),
    actor: dict = Depends(actor_actual),
):
    """Consulta de antecedentes judiciales de la Policía Nacional (por cédula).

    ⚠️ El portal (art. 94 del Decreto 019 de 2012) es de AUTOCONSULTA del
    titular y sus términos prohíben el acceso por terceros: la consulta se
    realiza bajo autorización previa del titular (Ley 1581 de 2012), cuya
    obligación de contar con ella es del solicitante. La fuente no viene
    habilitada por defecto en los estudios (opt-in por empresa).

    El resultado son las leyendas oficiales (Sentencia SU-458 de 2012):
    "NO TIENE ASUNTOS PENDIENTES CON LAS AUTORIDADES JUDICIALES" (no_registra
    true) o "ACTUALMENTE NO ES REQUERIDO POR AUTORIDAD JUDICIAL" (false). El
    portal NO genera PDF (Decreto 19 de 2012 art. 93). Captcha reCAPTCHA v2
    resuelto por servicio externo (SEGURIDAD_POLICIA_CAPTCHA_KEY).
    """
    _requiere_seguridad(actor)

    cedula_norm = _normalizar_cedula(cedula)
    cache = _buscar_cache("policia", cedula_norm, force)
    if cache:
        return _envolver_cache(cache)

    try:
        resultado = await asyncio.to_thread(consultar_antecedentes_policia_sync, cedula_norm)
    except BotPoliciaSinCaptchaKey as exc:
        logger.error("Bot Policía sin key de captcha: %s", exc)
        raise HTTPException(
            status_code=503,
            detail="Fuente policía no configurada: falta SEGURIDAD_POLICIA_CAPTCHA_KEY en el servidor",
        ) from exc
    except BotPoliciaError as exc:
        logger.error("Bot Policía falló para cédula %s: %s", cedula_norm, exc)
        raise HTTPException(status_code=502, detail=f"No fue posible consultar la Policía: {exc}") from exc

    ahora = _utcnow()
    doc = {
        "tipo": "policia",
        "cedula": cedula_norm,
        "no_registra": resultado.get("no_registra"),
        "mensaje": resultado.get("mensaje", ""),
        "nombre_consultado": resultado.get("nombre_consultado", ""),
        "pdf_ruta": resultado.get("pdf_ruta"),
        "pdf_tamano": len(resultado["pdf_bytes"]) if resultado.get("pdf_bytes") else 0,
        "usuario": actor["usuario"],
        "perfil": actor["perfil"],
        "empresa_id": actor.get("empresa_id"),
        "consultado_en": ahora,
        "expira_en": ahora + timedelta(hours=HORAS_CACHE),
        "forzado": bool(force),
    }
    try:
        col_consultas.insert_one(doc)
    except Exception as exc:
        logger.error("Consulta policía %s no se pudo auditar: %s", cedula_norm, exc)

    doc.pop("_id", None)
    return {
        "tipo": "policia",
        "cedula": cedula_norm,
        "cache": False,
        "consultado_en": ahora,
        "no_registra": resultado.get("no_registra"),
        "mensaje": resultado.get("mensaje", ""),
        "nombre_consultado": resultado.get("nombre_consultado", ""),
        "pdf_ruta": resultado.get("pdf_ruta"),
    }


@router.get("/runt")
async def consultar_runt(
    placa: str = Query(..., min_length=4, max_length=10, description="Placa del vehículo (AAA123 / AAA12A)"),
    cedula: str = Query(..., min_length=3, max_length=20, description="Cédula del propietario ACTIVO del vehículo"),
    force: bool = Query(False, description="Ignorar caché (vuelve a consultar el portal)"),
    actor: dict = Depends(actor_actual),
):
    """Consulta ciudadana de vehículo del RUNT (por placa + cédula del propietario).

    Canal: Portal Público de Consulta Ciudadana del RUNT (Mintransporte) —
    consulta abierta por placa con verificación de la cédula del propietario
    ACTIVO del vehículo. Devuelve los datos del vehículo (marca, línea, modelo,
    motor, chasis, VIN...) y la póliza SOAT con su vigencia. El portal NO
    genera un PDF consolidado (las descargas son por póliza/certificado).
    Captcha de imagen propio resuelto por servicio externo
    (SEGURIDAD_RUNT_CAPTCHA_KEY). El cacheo discrimina por (cedula, placa).
    """
    _requiere_seguridad(actor)

    cedula_norm = _normalizar_cedula(cedula)
    placa_norm = _normalizar_placa(placa)
    cache = _buscar_cache("runt", cedula_norm, force, placa=placa_norm)
    if cache:
        return _envolver_cache(cache)

    try:
        resultado = await asyncio.to_thread(consultar_vehiculo_runt_sync, placa_norm, cedula_norm)
    except BotRuntSinCaptchaKey as exc:
        logger.error("Bot RUNT sin key de captcha: %s", exc)
        raise HTTPException(
            status_code=503,
            detail="Fuente runt no configurada: falta SEGURIDAD_RUNT_CAPTCHA_KEY en el servidor",
        ) from exc
    except BotRuntError as exc:
        logger.error("Bot RUNT falló para placa %s: %s", placa_norm, exc)
        raise HTTPException(status_code=502, detail=f"No fue posible consultar el RUNT: {exc}") from exc

    ahora = _utcnow()
    doc = {
        "tipo": "runt",
        "cedula": cedula_norm,
        "placa": placa_norm,
        "no_registra": resultado.get("no_registra"),
        "mensaje": resultado.get("mensaje", ""),
        "datos_vehiculo": resultado.get("datos_vehiculo") or {},
        "soat": resultado.get("soat"),
        "polizas": (resultado.get("polizas") or [])[:10],
        "usuario": actor["usuario"],
        "perfil": actor["perfil"],
        "empresa_id": actor.get("empresa_id"),
        "consultado_en": ahora,
        "expira_en": ahora + timedelta(hours=HORAS_CACHE),
        "forzado": bool(force),
    }
    try:
        col_consultas.insert_one(doc)
    except Exception as exc:
        logger.error("Consulta runt %s no se pudo auditar: %s", placa_norm, exc)

    doc.pop("_id", None)
    return {
        "tipo": "runt",
        "cedula": cedula_norm,
        "placa": placa_norm,
        "cache": False,
        "consultado_en": ahora,
        "no_registra": resultado.get("no_registra"),
        "mensaje": resultado.get("mensaje", ""),
        "datos_vehiculo": resultado.get("datos_vehiculo") or {},
        "soat": resultado.get("soat"),
        "polizas": (resultado.get("polizas") or [])[:10],
    }


@router.get("/historico")
async def listar_historico(
    cedula: str | None = Query(None, description="Filtrar por cédula consultada"),
    tipo: str | None = Query(None, description="Filtrar por tipo (manifiestos_rndc, procuraduria, policia, runt)"),
    limit: int = Query(50, ge=1, le=200),
    skip: int = Query(0, ge=0),
    actor: dict = Depends(actor_actual),
):
    """Auditoría de consultas de seguridad. No consulta portales."""
    _requiere_seguridad(actor)
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
