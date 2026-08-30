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

from bson import ObjectId

from bd.bd_cliente import bd_cliente
from Funciones.bot_policia import (
    BotPoliciaCaptchaFallido,
    BotPoliciaSinCaptchaKey,
    BotPoliciaSinResultado,
    consultar_antecedentes_policia_sync,
)
from Funciones.bot_procuraduria import (
    BotProcuraduriaError,
    BotProcuraduriaSinResultado,
    consultar_antecedentes_sync,
)
from Funciones.bot_rndc2 import BotRNDC2Error, consultar_historial_viajes_sync
from Funciones.bot_runt import (
    BotRuntCaptchaFallido,
    BotRuntSinCaptchaKey,
    BotRuntSinResultado,
    consultar_vehiculo_runt_sync,
)
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

FUENTES = ("manifiestos_rndc", "procuraduria", "policia", "runt")

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
    """Ventana fija de DIAS_VENTANA días en el formato AAAA/MM/DD del portal.

    La fecha tope es HOY en hora Colombia (UTC−5: RESTAR 5 horas). El código
    viejo SUMABA 5 (hora UTC+5, Estambul): entre las 19:00 y las 24:00 de
    Colombia la fecha caía en MAÑANA y el portal RNDC responde con la página
    sin módulo de consulta (sin tabla ni "Consulta realizada") — respuesta
    incompleta → fuente NO_DISPONIBLE. Nunca pedir una fecha futura.
    """
    hasta = (datetime.now(timezone.utc) - timedelta(hours=5)).replace(tzinfo=None)  # hora Colombia
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

    Un resultado RNDC 'vacío sin confirmación del portal' (respuesta Ajax
    incompleta) también consume intento y se reintenta (fix 2026-08-29): no es
    una excepción, pero tampoco es un resultado válido.
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
            if _resultado_vacio_sin_confirmar(nombre, resultado):
                ultima = BotRNDC2Incompleto("El portal no confirmó la consulta (respuesta incompleta)")
                logger.warning(
                    "Fuente %s respondió incompleta (intento %s/%s, %.1fs) — reintentando",
                    nombre, intentos, INTENTOS_FUENTE, duraciones[-1],
                )
                queda_tiempo = time.monotonic() + BACKOFF_MS / 1000 < presupuesto
                if intentos >= INTENTOS_FUENTE or not queda_tiempo:
                    break
                await asyncio.sleep(BACKOFF_MS / 1000)
                continue
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


class BotRNDC2Incompleto(Exception):
    """El portal RNDC respondió sin tabla ni confirmación (Ajax incompleto)."""


def _resultado_vacio_sin_confirmar(nombre: str, resultado: dict | None) -> bool:
    """True si el resultado de RNDC llegó sin datos NI confirmación del portal."""
    if nombre != "manifiestos_rndc" or not isinstance(resultado, dict):
        return False
    if resultado.get("viajes"):
        return False
    return "consulta realizada" not in (resultado.get("mensaje_portal") or "").lower()


def _clasificar_error(exc: Exception) -> tuple[str, dict]:
    """(estado de la fuente, error {tipo, mensaje}) — NO_DISPONIBLE vs ERROR."""
    if isinstance(exc, asyncio.TimeoutError):
        return "NO_DISPONIBLE", {"tipo": "TimeoutError", "mensaje": f"La fuente no respondió en {TIMEOUT_FUENTE_S:.0f} s"}
    if isinstance(exc, BotRNDC2Incompleto):
        return "NO_DISPONIBLE", {"tipo": "portal_inconsistente", "mensaje": str(exc)[:MAX_MENSAJE]}
    if isinstance(exc, BotProcuraduriaSinResultado):
        # Postback de la PGN sin respuesta (lento/caído): NO_DISPONIBLE para no
        # disparar reembolso en cascada por pura lentitud, y el reintento del
        # _llamar_con_reintento ya corrió (fix 2026-08-30).
        return "NO_DISPONIBLE", {"tipo": "portal_inconsistente", "mensaje": str(exc)[:MAX_MENSAJE]}
    if isinstance(exc, BotPoliciaSinCaptchaKey):
        # Falta de configuración (no del portal): NO_DISPONIBLE para que una
        # causa pura de config no dispare la cadena "todas fallidas → ERROR
        # → reembolso". El mensaje dice exactamente qué hacer.
        return "NO_DISPONIBLE", {"tipo": "configuracion_faltante", "mensaje": str(exc)[:MAX_MENSAJE]}
    if isinstance(exc, BotPoliciaSinResultado):
        return "NO_DISPONIBLE", {"tipo": "portal_inconsistente", "mensaje": str(exc)[:MAX_MENSAJE]}
    if isinstance(exc, BotPoliciaCaptchaFallido):
        return "ERROR", {"tipo": "captcha", "mensaje": str(exc)[:MAX_MENSAJE]}
    if isinstance(exc, BotRuntSinCaptchaKey):
        return "NO_DISPONIBLE", {"tipo": "configuracion_faltante", "mensaje": str(exc)[:MAX_MENSAJE]}
    if isinstance(exc, BotRuntSinResultado):
        return "NO_DISPONIBLE", {"tipo": "portal_inconsistente", "mensaje": str(exc)[:MAX_MENSAJE]}
    if isinstance(exc, BotRuntCaptchaFallido):
        return "ERROR", {"tipo": "captcha", "mensaje": str(exc)[:MAX_MENSAJE]}
    tipo = type(exc).__name__
    mensaje = str(exc)[:MAX_MENSAJE]
    return "ERROR", {"tipo": tipo, "mensaje": mensaje}


def _estado_runt(seccion: dict) -> str:
    """Estado de la fuente runt a partir de su sección.

    - SOAT vencido → ADVERTENCIA (el vehículo NO está asegurado: el estudio no
      puede afirmar que todo está en orden, aunque los datos sí llegaron).
    - "No propietario activo" (no_registra False) → EXITO: el portal dio una
      respuesta determinante y negativa (no es un fallo de la fuente).
    - Sin info de SOAT (portal sin tabla o placa sin pólizas) → EXITO con lo
      que haya (no se inventa una advertencia que el portal no reportó).
    """
    soat = seccion.get("soat") or {}
    if soat and soat.get("vigente") is False:
        return "ADVERTENCIA"
    return "EXITO"


async def _ejecutar_fuente(
    nombre: str, cedula: str, actor: dict, forzar: bool, *, placa: str | None = None,
    cedula_propietario: str | None = None,
) -> dict:
    """Ejecuta una fuente (caché → portal con reintento) y devuelve su sección
    lista para el doc del estudio. NUNCA lanza: una fuente caída queda
    registrada como NO_DISPONIBLE/ERROR. `placa`/`cedula_propietario` solo los
    usa la fuente runt (consulta de vehículo por placa + cédula del PROPIETARIO
    ACTIVO, que puede ser distinta de la persona evaluada)."""
    if nombre == "runt":
        # El RUNT valida la cédula contra el PROPIETARIO ACTIVO de la placa:
        # cuando el conductor evaluado no es el dueño, la consulta (y la caché,
        # y el bot) van con la cédula del propietario. Sin `cedula_propietario`
        # se asume que el evaluado es el propietario (comportamiento previo).
        cedula = cedula_propietario or cedula
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
    #    runt discrimina además por placa (una cédula puede tener varios
    #    vehículos: sin placa en el filtro habría cross-contaminación).
    cache = _buscar_cache(nombre, cedula, forzar, placa=placa)
    if cache:
        seccion.update({"estado": "EXITO", "origen": "cache", "intentos": 0, "cache_id": str(cache["_id"])})
        if nombre == "manifiestos_rndc":
            viajes = cache.get("viajes", [])[:MAX_VIAJES_DOC]
            seccion.update({
                "desde": cache.get("desde"), "hasta": cache.get("hasta"),
                "viajes": viajes, "columnas": cache.get("columnas", []),
                "total": cache.get("total", len(viajes)),
            })
        elif nombre == "runt":
            # El estado puede DEGRADAR desde que se cacheó: un SOAT vigente
            # ayer puede estar vencido hoy → recalcular el semáforo en cada hit
            # contra la fecha de vencimiento (no confiar en el flag cacheado).
            from Funciones.bot_runt import _soat_vigente

            soat = dict(cache.get("soat") or {})
            if soat.get("fecha_fin_vigencia"):
                soat["vigente"] = _soat_vigente(soat["fecha_fin_vigencia"])
            seccion.update({
                "no_registra": cache.get("no_registra"),
                "mensaje": (cache.get("mensaje") or "")[:MAX_MENSAJE],
                "datos_vehiculo": cache.get("datos_vehiculo") or {},
                "soat": soat,
                "polizas": (cache.get("polizas") or [])[:10],
                "placa": cache.get("placa", ""),
            })
            seccion["estado"] = _estado_runt(seccion)
        else:
            seccion.update({
                "no_registra": cache.get("no_registra"),
                "mensaje": (cache.get("mensaje") or "")[:MAX_MENSAJE],
                "nombre_certificado": cache.get("nombre_certificado", ""),
                "pdf_tamano": cache.get("pdf_tamano", 0),
            })
            if nombre == "policia":
                seccion["nombre_consultado"] = cache.get("nombre_consultado", "")
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
    elif nombre == "policia":

        async def invocar() -> dict:
            return await asyncio.to_thread(consultar_antecedentes_policia_sync, cedula)
    elif nombre == "runt":

        async def invocar() -> dict:
            # runt consulta por placa + cédula del PROPIETARIO (sin placa no hay
            # consulta posible; el endpoint ya lo validó antes de llegar aquí).
            return await asyncio.to_thread(consultar_vehiculo_runt_sync, placa or "", cedula)
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
        # Cero viajes solo es EXITO si el portal CONFIRMÓ la consulta (mensaje
        # "Consulta realizada"): un vacío sin confirmación es una respuesta
        # Ajax tardía/incompleta — tratarla como éxito envenenaría la caché
        # 24 h con datos que no son (fix 2026-08-29).
        mensaje_portal = resultado.get("mensaje_portal") or ""
        confirmado = "consulta realizada" in mensaje_portal.lower()
        if total == 0 and not confirmado:
            seccion.update({
                "estado": "NO_DISPONIBLE",
                "error": {
                    "tipo": "portal_inconsistente",
                    "mensaje": "El portal RNDC no confirmó la consulta (sin tabla ni mensaje). Intente de nuevo.",
                },
                "viajes": [], "columnas": resultado.get("columnas", []),
                "total": 0, "desde": fecha_inicio, "hasta": fecha_fin,
            })
            logger.warning(
                "RNDC sin confirmación para %s (sin cachear): %s",
                enmascarar_cedula(cedula), mensaje_portal[:150] or "(sin mensaje)",
            )
            return seccion
        doc_cache = {
            "tipo": nombre, "cedula": cedula,
            "desde": fecha_inicio, "hasta": fecha_fin,
            "viajes": viajes, "columnas": resultado.get("columnas", []), "total": total,
            "usuario": actor["usuario"], "perfil": actor.get("perfil", ""),
            "empresa_id": actor.get("empresa_id"), "usuario_id": actor.get("usuario_id"),
            "consultado_en": ahora, "expira_en": expira, "forzado": bool(forzar),
            "mensaje_portal": mensaje_portal[:MAX_MENSAJE],
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
    elif nombre == "policia":
        # Mismo shape que procuraduría (veredicto tri-estado), pero el portal
        # no genera PDF: mensaje = leyenda oficial (SU-458) y el nombre del
        # consultado viene de la línea "Apellidos y Nombres".
        pdf_bytes = resultado.get("pdf_bytes") or b""
        no_registra = resultado.get("no_registra")
        mensaje = (resultado.get("mensaje") or "").strip()
        nombre_consultado = (resultado.get("nombre_consultado") or "").strip()
        # Anti-envenenamiento (segunda barrera: el bot ya lanza
        # BotPoliciaSinResultado; esto cubre resultados vacíos que lleguen
        # como dict): sin leyenda, sin nombre y sin PDF no hay consulta válida.
        if no_registra is None and not nombre_consultado and not pdf_bytes:
            seccion.update({
                "estado": "NO_DISPONIBLE",
                "error": {
                    "tipo": "portal_inconsistente",
                    "mensaje": "El portal de la Policía no entregó veredicto ni datos. Intente de nuevo.",
                },
            })
            logger.warning(
                "Policía sin resultado legible para %s (sin cachear)",
                enmascarar_cedula(cedula),
            )
            return seccion
        doc_cache = {
            "tipo": nombre, "cedula": cedula,
            "no_registra": no_registra,
            "mensaje": mensaje[:MAX_MENSAJE],
            "nombre_consultado": nombre_consultado,
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
            logger.error("Caché policía %s no se pudo auditar: %s", enmascarar_cedula(cedula), exc)
        # ADVERTENCIA si el veredicto no fue legible (no_registra None): el
        # estudio no puede afirmar "limpio" (análogo a procuraduría).
        estado = "EXITO" if no_registra is not None else "ADVERTENCIA"
        seccion.update({
            "estado": estado,
            "no_registra": no_registra,
            "mensaje": mensaje[:MAX_MENSAJE],
            "nombre_consultado": nombre_consultado,
            "pdf_sha256": doc_cache["pdf_sha256"],
            "pdf_tamano": len(pdf_bytes),
        })
        # Si el portal llegara a entregar PDF (hoy no lo hace), queda listo el
        # canal de anexo: se sube a GCS y se descarta.
        seccion["_pdf_bytes"] = pdf_bytes  # volátil: se sube a GCS y se descarta
    elif nombre == "runt":
        # Consulta de vehículo (placa + cédula del propietario). Sin PDF
        # consolidado del portal: el informe lo genera Integra (como policía).
        datos_vehiculo = resultado.get("datos_vehiculo") or {}
        soat = resultado.get("soat")
        polizas = (resultado.get("polizas") or [])[:10]
        no_registra = resultado.get("no_registra")
        mensaje = (resultado.get("mensaje") or "").strip()
        # Anti-envenenamiento (segunda barrera: el bot ya lanza BotRuntSinResultado;
        # esto cubre dicts vacíos que lleguen igual): sin datos, sin pólizas y sin
        # mensaje determinante NO es una consulta válida.
        if not datos_vehiculo and not polizas and no_registra is None:
            seccion.update({
                "estado": "NO_DISPONIBLE",
                "error": {
                    "tipo": "portal_inconsistente",
                    "mensaje": "El portal del RUNT no entregó datos del vehículo. Intente de nuevo.",
                },
            })
            logger.warning("RUNT sin resultado legible para %s (sin cachear)", enmascarar_cedula(cedula))
            return seccion
        doc_cache = {
            "tipo": nombre, "cedula": cedula, "placa": resultado.get("placa") or placa or "",
            "no_registra": no_registra,
            "mensaje": mensaje[:MAX_MENSAJE],
            "datos_vehiculo": datos_vehiculo,
            "soat": soat,
            "polizas": polizas,
            "usuario": actor["usuario"], "perfil": actor.get("perfil", ""),
            "empresa_id": actor.get("empresa_id"), "usuario_id": actor.get("usuario_id"),
            "consultado_en": ahora, "expira_en": expira, "forzado": bool(forzar),
        }
        try:
            col_consultas.insert_one(doc_cache)
            seccion["cache_id"] = str(doc_cache["_id"])
        except Exception as exc:
            logger.error("Caché runt %s no se pudo auditar: %s", enmascarar_cedula(cedula), exc)
        seccion.update({
            "no_registra": no_registra,
            "mensaje": mensaje[:MAX_MENSAJE],
            "datos_vehiculo": datos_vehiculo,
            "soat": soat,
            "polizas": polizas,
            "placa": doc_cache["placa"],
        })
        # El semáforo de SOAT decide el estado: vencido = ADVERTENCIA (decisión
        # de negocio 2026-08-30: el estudio no puede afirmar "todo en orden").
        seccion["estado"] = _estado_runt(seccion)
    else:
        pdf_bytes = resultado.get("pdf_bytes") or b""
        nombre_cert = _nombre_del_certificado(resultado.get("texto_pdf", "") or "")
        no_registra = resultado.get("no_registra")
        # Anti-envenenamiento (fix 2026-08-30, análogo RNDC/Policía/RUNT): sin
        # veredicto Y sin PDF no hay consulta válida — el portal quedó en el
        # formulario o falló la descarga. Antes esto era ADVERTENCIA con
        # mensaje "ver PDF" (¡sin PDF!) y quedaba CACHÉ 24 h: la cédula salía
        # "no concluyente" todo el día aunque el portal respondiera bien luego.
        if no_registra is None and not pdf_bytes:
            seccion.update({
                "estado": "NO_DISPONIBLE",
                "error": {
                    "tipo": "portal_inconsistente",
                    "mensaje": "El portal de la Procuraduría no entregó certificado ni veredicto. Intente de nuevo.",
                },
            })
            logger.warning(
                "Procuraduría sin veredicto ni PDF para %s (sin cachear): %s",
                enmascarar_cedula(cedula), (resultado.get("texto_resultado") or "")[:150] or "(sin texto)",
            )
            return seccion
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
    fuentes: list[str] | None = None,
    *,
    placa: str | None = None,
    cedula_propietario: str | None = None,
) -> dict:
    """Ejecuta fuentes en paralelo, calcula estado, persiste y devuelve el doc.

    El doc EN_PROGRESO ya fue creado por el endpoint ANTES de llamar esto.
    `fuentes` (opcional) es la lista AUTORITATIVA de fuentes a correr — el
    endpoint ya la calculó (config ∩ planes vigentes por fuente); si no llega,
    se usa config.fuentes_habilitadas como antes. `placa`/`cedula_propietario`
    solo los usa runt (vehículo del propietario, que puede ≠ persona evaluada).
    """
    inicio = time.monotonic()
    habilitadas = list(fuentes) if fuentes is not None else list(
        (empresa.get("config") or {}).get("fuentes_habilitadas") or FUENTES
    )
    _id = col_estudios.find_one({"consulta_id": consulta_id}, {"_id": 1})["_id"]

    async def _deshabilitada(nombre: str) -> dict:
        # gather solo acepta awaitables: envolver el resultado sincrónico.
        return _fuente_deshabilitada(nombre)

    async with _SEMAFORO_ESTUDIOS:
        resultados = await asyncio.gather(
            *[
                _ejecutar_fuente(
                    nombre, cedula, actor, forzar,
                    placa=placa, cedula_propietario=cedula_propietario,
                )
                if nombre in habilitadas
                else _deshabilitada(nombre)
                for nombre in FUENTES
            ]
        )

    fuentes = dict(zip(FUENTES, resultados))

    # Anexos con certificado oficial (GCS privado) si llegaron. Hoy solo
    # procuraduría genera PDF; policía/runt mantienen el canal listo por si
    # el portal cambia.
    anexos: dict[str, dict] = {}
    for nombre_fuente in ("procuraduria", "policia", "runt"):
        bytes_anexo = (fuentes.get(nombre_fuente) or {}).pop("_pdf_bytes", None)
        if not bytes_anexo:
            continue
        try:
            from Funciones import storage_seguridad

            ruta = storage_seguridad.ruta_blob(actor["empresa_id"], _utcnow().year, consulta_id, f"_{nombre_fuente}")
            anexos[nombre_fuente] = storage_seguridad.subir_pdf(bytes_anexo, ruta, cedula)
        except Exception as exc:
            logger.error("Anexo %s no se pudo subir a GCS: %s", nombre_fuente, exc)
            fuentes[nombre_fuente]["anexo_error"] = str(exc)[:200]

    estado_global = calcular_estado_global(fuentes)
    finalizado = _utcnow()
    duracion = round(time.monotonic() - inicio, 2)

    # Nombre del consultado en cascada: la PGN es la confiable (regex sobre el
    # certificado); policía lo trae de la línea "Apellidos y Nombres". RUNT no
    # aporta nombre (la vista ciudadana no lo expone).
    nombre_consultado = (
        (fuentes.get("procuraduria") or {}).get("nombre_certificado")
        or (fuentes.get("policia") or {}).get("nombre_consultado")
        or ""
    )

    col_estudios.update_one(
        {"_id": _id},
        {
            "$set": {
                "estado": estado_global,
                "finalizado_en": finalizado,
                "duracion_s": duracion,
                "fuentes": {k: _limpiar_seccion(v) for k, v in fuentes.items()},
                "anexo_procuraduria": anexos.get("procuraduria"),
                "anexo_policia": anexos.get("policia"),
                "anexo_runt": anexos.get("runt"),
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

def crear_documento_estudio(
    consulta_id: str, cedula: str, actor: dict, empresa: dict, forzar: bool, auditoria: dict,
    *, placa: str | None = None, cedula_propietario: str | None = None,
) -> str:
    """Inserta el doc EN_PROGRESO y retorna el consulta_id. Se llama ANTES de
    ejecutar fuentes: la consulta queda trazada aunque todo falle después.
    `placa`/`vehiculos` se persisten solo cuando la consulta incluye runt; el
    array `vehiculos` queda listo para soportar varios por estudio (hoy 1)."""
    ahora = _utcnow()
    retencion = int((empresa.get("config") or {}).get("retencion_dias") or RETENCION_DIAS)
    ced_prop = cedula_propietario or cedula  # resuelta: dueño asumido = evaluado
    col_estudios.insert_one(
        {
            "consulta_id": consulta_id,
            "codigo_verificacion": codigo_verificacion(consulta_id),
            # ObjectId nativo: los filtros de aislamiento comparan contra ObjectId.
            "empresa_id": ObjectId(actor["empresa_id"]),
            "empresa_nombre": empresa.get("nombre", ""),
            # API keys no tienen usuario humano (canal "api"); el doc queda
            # atribuido a la integración ("API: SILO") sin usuario_id.
            "usuario_id": ObjectId(actor["usuario_id"]) if actor.get("usuario_id") else None,
            "usuario": actor["usuario"],
            "usuario_nombre": actor["usuario_nombre"],
            "usuario_correo": actor["usuario_correo"],
            # Origen de la consulta: "portal" (humano) | "api" (integración).
            "canal": actor.get("canal") or "portal",
            "api_key": (
                {"id": actor["api_key_id"], "nombre": actor["api_key_nombre"]}
                if actor.get("canal") == "api" else None
            ),
            "cedula": cedula,
            "placa": placa,
            # Vehículos validados por runt: cada uno con SU propietario (puede
            # ser distinto de la persona evaluada). `placa` top-level queda
            # como espejo de vehiculos[0] (compatibilidad con docs previos).
            "vehiculos": (
                [{
                    "placa": placa,
                    "cedula_propietario": ced_prop,
                    "propietario_es_evaluado": ced_prop == cedula,
                }] if placa else []
            ),
            "nombre_consultado": "",
            "estado": "EN_PROGRESO",
            "creado_en": ahora,
            "finalizado_en": None,
            "duracion_s": None,
            "forzado": bool(forzar),
            "fuentes": {},
            "pdf": None,
            "anexo_procuraduria": None,
            "anexo_policia": None,
            "anexo_runt": None,
            "retencion_expira_en": ahora + timedelta(days=retencion),
            "auditoria": auditoria,
        }
    )
    return consulta_id
