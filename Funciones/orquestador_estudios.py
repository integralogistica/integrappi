"""Orquestador de Estudios de Seguridad: cédula → fuentes → PDF → GCS → Mongo.

Ejecuta las dos fuentes (RNDC manifiestos y Procuraduría) EN PARALELO —
`asyncio.gather` sobre `asyncio.to_thread` es seguro porque cada bot tiene su
propio `threading.Lock` de módulo (serializa consultas dentro de cada portal,
pero los portales entre sí van en paralelo).

Reglas del módulo:
  - El doc del estudio se crea ANTES de consultar (estado EN_PROGRESO): aunque
    todo falle después, queda trazabilidad de quién consultó qué y cuándo.
  - Una fuente que falla NUNCA aborta el estudio: queda como NO_DISPONIBLE
    (timeout/portal caído) o ERROR (fallo funcional del bot) en su sección.
  - Estado global: COMPLETADA solo si TODAS las fuentes terminaron bien;
    nunca se registra éxito cuando una fuente falló.
  - Reintentos AQUÍ (no en los bots): 1 reintento con backoff fijo, solo ante
    error del bot o timeout, y solo si queda presupuesto de pared.
  - Minimización de datos personales: NO se persisten texto_resultado,
    texto_pdf ni html de los bots; mensaje truncado a 300 chars.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable

from bd.bd_cliente import bd_cliente
from Funciones.bot_procuraduria import BotProcuraduriaError, consultar_antecedentes_sync
from Funciones.bot_rndc2 import BotRNDC2Error, consultar_historial_viajes_sync
from rutas.baseusuarios import BASEUSUARIOS_JWT_SECRET
from rutas.seguridad import (
    DIAS_VENTANA,
    HORAS_CACHE,
    _buscar_cache,
    _nombre_del_certificado,
    _utcnow,
    col_consultas,
)

logger = logging.getLogger(__name__)

# --- Configuración por entorno (ver plan; ninguna es secreta) ----------------
INTENTOS_FUENTE = int(os.getenv("SEGURIDAD_INTENTOS_FUENTE", "2"))        # totales (1 original + 1 reintento)
BACKOFF_MS = int(os.getenv("SEGURIDAD_BACKOFF_MS", "3000"))
TIMEOUT_FUENTE_S = float(os.getenv("SEGURIDAD_TIMEOUT_FUENTE_S", "150"))
RETENCION_DIAS = int(os.getenv("SEGURIDAD_RETENCION_DIAS", "730"))
MAX_VIAJES_DOC = int(os.getenv("SEGURIDAD_MAX_VIAJES_DOC", "500"))
MAX_MENSAJE = 300

FUENTES = ("manifiestos_rndc", "procuraduria")

# Evita apilar Chromium concurrentes en una instancia pequeña de Render.
_SEMAFORO_ESTUDIOS = asyncio.Semaphore(2)

db = bd_cliente["integra"]
col_estudios = db["estudios_seguridad"]


# --- Utilidades ---------------------------------------------------------------

def nuevo_consulta_id() -> str:
    return "ES-" + uuid.uuid4().hex[:12].upper()


def codigo_verificacion(consulta_id: str) -> str:
    """Código público impreso en el QR; derivado del secreto JWT (no adivinable
    sin él) para que nadie pueda enumerar estudios válidos."""
    semilla = f"{consulta_id}:{BASEUSUARIOS_JWT_SECRET}"
    return hashlib.sha256(semilla.encode("utf-8")).hexdigest()[:10].upper()


def enmascarar_cedula(cedula: str) -> str:
    cedula = str(cedula or "")
    if len(cedula) <= 4:
        return "*" * len(cedula)
    return f"{cedula[:2]}{'*' * (len(cedula) - 4)}{cedula[-2:]}"


def _ventana_rndc() -> tuple[str, str]:
    """Ventana fija de DIAS_VENTANA días en el formato AAAA/MM/DD del portal."""
    hasta = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=5)  # hora Colombia
    desde = hasta - timedelta(days=DIAS_VENTANA)
    return desde.strftime("%Y/%m/%d"), hasta.strftime("%Y/%m/%d")


# --- Ejecución de una fuente --------------------------------------------------

async def _llamar_con_reintento(
    nombre: str,
    cedula: str,
    invocar: Callable[[], Awaitable[dict]],
) -> tuple[dict | None, int, list[float], Exception | None]:
    """Ejecuta la fuente con presupuesto de pared y 1 reintento.

    Retorna (resultado | None, intentos, duraciones_s, ultima_excepcion | None).
    NUNCA levanta: si todos los intentos fallan, el error viaja en el 4.º valor
    y el llamador lo convierte en estado de la fuente.
    """
    intentos = 0
    duraciones: list[float] = []
    presupuesto = time.monotonic() + TIMEOUT_FUENTE_S
    ultima: Exception | None = None
    while intentos < INTENTOS_FUENTE:
        intentos += 1
        inicio = time.monotonic()
        try:
            resultado = await asyncio.wait_for(invocar(), timeout=max(5.0, presupuesto - time.monotonic()))
            duraciones.append(round(time.monotonic() - inicio, 2))
            return resultado, intentos, duraciones, None
        except Exception as exc:
            ultima = exc
            duraciones.append(round(time.monotonic() - inicio, 2))
            queda_tiempo = time.monotonic() + BACKOFF_MS / 1000 < presupuesto
            if intentos >= INTENTOS_FUENTE or not queda_tiempo:
                break
            logger.warning(
                "Fuente %s falló (intento %s/%s, %.1fs): %s — reintentando",
                nombre, intentos, INTENTOS_FUENTE, duraciones[-1], exc,
            )
            await asyncio.sleep(BACKOFF_MS / 1000)
    return None, intentos, duraciones, ultima


def _clasificar_error(exc: Exception) -> tuple[str, dict]:
    """(estado de la fuente, error {tipo, mensaje}) — NO_DISPONIBLE vs ERROR."""
    if isinstance(exc, asyncio.TimeoutError):
        return "NO_DISPONIBLE", {"tipo": "TimeoutError", "mensaje": f"La fuente no respondió en {TIMEOUT_FUENTE_S:.0f} s"}
    tipo = type(exc).__name__
    mensaje = str(exc)[:MAX_MENSAJE]
    return "ERROR", {"tipo": tipo, "mensaje": mensaje}


async def _ejecutar_fuente(nombre: str, cedula: str, actor: dict, forzar: bool) -> dict:
    """Ejecuta una fuente (caché → portal con reintento) y devuelve su sección
    lista para el doc del estudio. NUNCA lanza: una fuente caída queda
    registrada como NO_DISPONIBLE/ERROR."""
    seccion: dict[str, Any] = {
        "estado": "ERROR",
        "origen": None,
        "intentos": 0,
        "duraciones_s": [],
        "error": None,
        "consultado_en": _utcnow(),
        "cache_id": None,
    }

    # 1) Caché global 24h por (tipo, cédula): los datos del tercero son
    #    idénticos para cualquier empresa; la atribución vive en el estudio.
    cache = _buscar_cache(nombre, cedula, forzar)
    if cache:
        seccion.update({"estado": "EXITO", "origen": "cache", "intentos": 0, "cache_id": str(cache["_id"])})
        if nombre == "manifiestos_rndc":
            viajes = cache.get("viajes", [])[:MAX_VIAJES_DOC]
            seccion.update({
                "desde": cache.get("desde"), "hasta": cache.get("hasta"),
                "viajes": viajes, "columnas": cache.get("columnas", []),
                "total": cache.get("total", len(viajes)),
            })
        else:
            seccion.update({
                "no_registra": cache.get("no_registra"),
                "mensaje": (cache.get("mensaje") or "")[:MAX_MENSAJE],
                "nombre_certificado": cache.get("nombre_certificado", ""),
                "pdf_tamano": cache.get("pdf_tamano", 0),
            })
        return seccion

    # 2) Consulta real al portal.
    fecha_inicio = fecha_fin = None
    if nombre == "manifiestos_rndc":
        fecha_inicio, fecha_fin = _ventana_rndc()

        async def invocar() -> dict:
            return await asyncio.to_thread(
                consultar_historial_viajes_sync,
                cedula=cedula, fecha_inicio=fecha_inicio, fecha_fin=fecha_fin,
            )
    else:

        async def invocar() -> dict:
            return await asyncio.to_thread(consultar_antecedentes_sync, cedula)

    try:
        resultado, intentos, duraciones, error_exc = await _llamar_con_reintento(nombre, cedula, invocar)
    except Exception as exc:  # red de Mongo, etc.: igual queda auditada la fuente como ERROR
        estado, error = _clasificar_error(exc)
        seccion.update({"estado": estado, "intentos": 0, "duraciones_s": [], "error": error})
        logger.error("Fuente %s falló para cédula %s: %s", nombre, enmascarar_cedula(cedula), exc)
        return seccion
    seccion["intentos"] = intentos
    seccion["duraciones_s"] = duraciones
    if error_exc is not None:
        estado, error = _clasificar_error(error_exc)
        seccion.update({"estado": estado, "error": error})
        logger.error(
            "Fuente %s falló para cédula %s tras %s intentos: %s",
            nombre, enmascarar_cedula(cedula), intentos, error_exc,
        )
        return seccion
    seccion["origen"] = "portal"

    ahora = _utcnow()
    expira = ahora + timedelta(hours=HORAS_CACHE)
    if nombre == "manifiestos_rndc":
        viajes = [
            v for v in resultado.get("viajes", [])
            if str(v.get("Nro. de Radicado", "")).strip().isdigit()
            and len(str(v.get("Nro. de Radicado", "")).strip()) >= 6
        ]
        total = len(viajes)
        doc_cache = {
            "tipo": nombre, "cedula": cedula,
            "desde": fecha_inicio, "hasta": fecha_fin,
            "viajes": viajes, "columnas": resultado.get("columnas", []), "total": total,
            "usuario": actor["usuario"], "perfil": actor.get("perfil", ""),
            "empresa_id": actor.get("empresa_id"), "usuario_id": actor.get("usuario_id"),
            "consultado_en": ahora, "expira_en": expira, "forzado": bool(forzar),
        }
        try:
            col_consultas.insert_one(doc_cache)
            seccion["cache_id"] = str(doc_cache["_id"])
        except Exception as exc:
            logger.error("Caché manifiestos %s no se pudo auditar: %s", enmascarar_cedula(cedula), exc)
        seccion.update({
            "estado": "EXITO",
            "desde": fecha_inicio, "hasta": fecha_fin,
            "viajes": viajes[:MAX_VIAJES_DOC], "columnas": resultado.get("columnas", []),
            "total": total,
        })
    else:
        pdf_bytes = resultado.get("pdf_bytes") or b""
        nombre_cert = _nombre_del_certificado(resultado.get("texto_pdf", "") or "")
        no_registra = resultado.get("no_registra")
        doc_cache = {
            "tipo": nombre, "cedula": cedula,
            "no_registra": no_registra,
            "mensaje": (resultado.get("mensaje") or "")[:MAX_MENSAJE],
            "nombre_certificado": nombre_cert,
            "pdf_sha256": hashlib.sha256(pdf_bytes).hexdigest() if pdf_bytes else None,
            "pdf_tamano": len(pdf_bytes),
            "usuario": actor["usuario"], "perfil": actor.get("perfil", ""),
            "empresa_id": actor.get("empresa_id"), "usuario_id": actor.get("usuario_id"),
            "consultado_en": ahora, "expira_en": expira, "forzado": bool(forzar),
        }
        try:
            col_consultas.insert_one(doc_cache)
            seccion["cache_id"] = str(doc_cache["_id"])
        except Exception as exc:
            logger.error("Caché procuraduría %s no se pudo auditar: %s", enmascarar_cedula(cedula), exc)
        # ADVERTENCIA: el portal respondió pero el veredicto no fue legible
        # (no_registra None): el estudio no puede afirmar "limpio".
        estado = "EXITO" if no_registra is not None else "ADVERTENCIA"
        seccion.update({
            "estado": estado,
            "no_registra": no_registra,
            "mensaje": (resultado.get("mensaje") or "")[:MAX_MENSAJE],
            "nombre_certificado": nombre_cert,
            "pdf_sha256": doc_cache["pdf_sha256"],
            "pdf_tamano": len(pdf_bytes),
        })
        # El certificado oficial se sube como anexo desde la ruta.
        seccion["_pdf_bytes"] = pdf_bytes  # volátil: se sube a GCS y se descarta

    return seccion


# --- Estado global --------------------------------------------------------------

def calcular_estado_global(fuentes: dict) -> str:
    """Nunca COMPLETADA con fuente fallida (regla del módulo).
    Las fuentes DESHABILITADA no cuentan para el cálculo."""
    activas = [f for f in fuentes.values() if f.get("estado") != "DESHABILITADA"]
    estados = {f.get("estado") for f in activas}
    if not activas:
        return "ERROR"
    if estados <= {"EXITO"}:
        return "COMPLETADA"
    if estados <= {"EXITO", "ADVERTENCIA"}:
        return "COMPLETADA_CON_ADVERTENCIAS"
    if estados & {"EXITO", "ADVERTENCIA"}:
        return "PARCIAL"
    return "ERROR"


# --- Estudio completo ------------------------------------------------------------

async def ejecutar_estudio(
    consulta_id: str,
    cedula: str,
    actor: dict,
    empresa: dict,
    forzar: bool,
    auditoria: dict,
    registrar_evento: Callable[..., None],
) -> dict:
    """Ejecuta fuentes en paralelo, calcula estado, persiste y devuelve el doc.

    El doc EN_PROGRESO ya fue creado por el endpoint ANTES de llamar esto.
    """
    inicio = time.monotonic()
    habilitadas = list((empresa.get("config") or {}).get("fuentes_habilitadas") or FUENTES)
    _id = col_estudios.find_one({"consulta_id": consulta_id}, {"_id": 1})["_id"]

    async with _SEMAFORO_ESTUDIOS:
        resultados = await asyncio.gather(
            *[
                _ejecutar_fuente(nombre, cedula, actor, forzar)
                if nombre in habilitadas
                else _fuente_deshabilitada(nombre)
                for nombre in FUENTES
            ]
        )

    fuentes = dict(zip(FUENTES, resultados))

    # Anexo Procuraduría: subir el certificado oficial a GCS (privado) si llegó.
    anexo = None
    pdf_bytes_proc = fuentes.get("procuraduria", {}).pop("_pdf_bytes", None)
    if pdf_bytes_proc:
        try:
            from Funciones import storage_seguridad

            ruta = storage_seguridad.ruta_blob(actor["empresa_id"], _utcnow().year, consulta_id, "_procuraduria")
            subido = storage_seguridad.subir_pdf(pdf_bytes_proc, ruta, cedula)
            anexo = subido
        except Exception as exc:
            logger.error("Anexo procuraduría no se pudo subir a GCS: %s", exc)
            fuentes["procuraduria"]["anexo_error"] = str(exc)[:200]

    estado_global = calcular_estado_global(fuentes)
    finalizado = _utcnow()
    duracion = round(time.monotonic() - inicio, 2)

    nombre_consultado = (fuentes.get("procuraduria") or {}).get("nombre_certificado") or ""

    col_estudios.update_one(
        {"_id": _id},
        {
            "$set": {
                "estado": estado_global,
                "finalizado_en": finalizado,
                "duracion_s": duracion,
                "fuentes": {k: _limpiar_seccion(v) for k, v in fuentes.items()},
                "anexo_procuraduria": anexo,
                "nombre_consultado": nombre_consultado,
            }
        },
    )
    for nombre, fuente in fuentes.items():
        if fuente.get("estado") in {"NO_DISPONIBLE", "ERROR"}:
            registrar_evento(
                "fuente_error",
                actor=actor,
                consulta_id=consulta_id,
                fuente=nombre,
                detalle=f"Estado {fuente['estado']}: {(fuente.get('error') or {}).get('mensaje', '')[:150]}",
            )
    registrar_evento(
        "estudio_creado",
        actor=actor,
        consulta_id=consulta_id,
        detalle=f"Cédula {enmascarar_cedula(cedula)}, estado {estado_global}",
    )

    doc = col_estudios.find_one({"_id": _id})
    doc.pop("_id", None)
    return doc


def _fuente_deshabilitada(nombre: str) -> dict:
    return {"estado": "DESHABILITADA", "origen": None, "intentos": 0, "duraciones_s": [], "error": None, "consultado_en": _utcnow()}


def _limpiar_seccion(seccion: dict) -> dict:
    """Solo campos persistibles (fuera _pdf_bytes y otros volátiles)."""
    return {k: v for k, v in seccion.items() if not k.startswith("_")}


# --- Doc inicial del estudio ------------------------------------------------------

def crear_documento_estudio(consulta_id: str, cedula: str, actor: dict, empresa: dict, forzar: bool, auditoria: dict) -> str:
    """Inserta el doc EN_PROGRESO y retorna el consulta_id. Se llama ANTES de
    ejecutar fuentes: la consulta queda trazada aunque todo falle después."""
    ahora = _utcnow()
    retencion = int((empresa.get("config") or {}).get("retencion_dias") or RETENCION_DIAS)
    col_estudios.insert_one(
        {
            "consulta_id": consulta_id,
            "codigo_verificacion": codigo_verificacion(consulta_id),
            "empresa_id": actor["empresa_id"],
            "empresa_nombre": empresa.get("nombre", ""),
            "usuario_id": actor["usuario_id"],
            "usuario": actor["usuario"],
            "usuario_nombre": actor["usuario_nombre"],
            "usuario_correo": actor["usuario_correo"],
            "cedula": cedula,
            "nombre_consultado": "",
            "estado": "EN_PROGRESO",
            "creado_en": ahora,
            "finalizado_en": None,
            "duracion_s": None,
            "forzado": bool(forzar),
            "fuentes": {},
            "pdf": None,
            "anexo_procuraduria": None,
            "retencion_expira_en": ahora + timedelta(days=retencion),
            "auditoria": auditoria,
        }
    )
    return consulta_id
