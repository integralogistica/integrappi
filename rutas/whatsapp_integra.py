# rutas/whatsapp_integra.py
import os
import re
import asyncio
import traceback
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, Request
from fastapi.responses import PlainTextResponse, JSONResponse
from Funciones.chat_state_integra import (
    get_state, set_state, reset_state,
    set_auth_session, get_auth_session, is_authenticated, invalidate_auth_session
)
from Funciones.whatsapp_utils_integra import (
    enviar_texto,
    verificar_credenciales_transportador,
    solicitar_recuperacion_clave,
    crear_usuario_transportador,
    verificar_codigo_confirmacion,
)
from Funciones.whatsapp_logs_integra import log_whatsapp_event
from Funciones.whatsapp_certificado_integra import generar_y_enviar_certificado_por_cedula
from Funciones.vulcano_whatsapp_format import (
    agrupar_por_estado,
    formatear_resumen_tenedor,
    formatear_pagos_saldo,
    formatear_manifiestos_estado,
    formatear_detalle_manifiesto,
    PAGE_SIZE_DETALLE,
)
import httpx
import logging
from fastapi import BackgroundTasks

logger = logging.getLogger("integra")
logging.basicConfig(level=logging.INFO)

from rutas.vulcano import consultar_manifiestos_detallado

VERIFY_TOKEN = os.getenv("WHATSAPP_VERIFY_TOKEN", "integra_verify_2026")
PAGOS_ENDPOINT = os.getenv(
    "PAGOS_TENEDOR_URL",
    "https://integrappi-dvmh.onrender.com/manifiestos/tenedor/{cedula}",
)

# Regex
CEDULA_REGEX = re.compile(r"^\d{5,15}$")
GUIA_REGEX = re.compile(r"^[A-Za-z0-9\-]+$")
YEAR_REGEX = re.compile(r"^(19|20)\d{2}$")
MANIFIESTO_REGEX = re.compile(r"^[A-Za-z0-9]{5,20}$")

# TTL estado
STATE_TTL_MINUTES = 60

# Dedup
PROCESSED_IDS_MAX = 30

# Defaults Transportador
TRANSP_DEFAULT_YEAR = "2025"

ruta_whatsapp_integra = APIRouter(
    prefix="/whatsapp",
    tags=["whatsapp-integra"],
)

# -------------------------
# Textos
# -------------------------
def texto_inicio() -> str:
    return (
        "Bienvenido a *Integra Soluciones Logísticas*.\n\n"
        "Selecciona una opción (responde con un número):\n"
        "1️⃣ 🚚 Soy transportador\n"
        "2️⃣ 🧑‍💼 Soy empleado\n"
        "3️⃣ 🧾 Soy cliente\n\n"
        "Para volver a este menú escribe *menu*."
    )


def texto_menu_empleado() -> str:
    return (
        "🧑‍💼 *Menú Empleado*\n\n"
        "Responde con un número:\n"
        "1️⃣ 📄 Certificado laboral (sin salario)\n"
        "2️⃣ 📄 Certificado laboral (con salario)\n\n"
        "Para volver escribe *menu*."
    )


def texto_pedir_cedula() -> str:
    return (
        "📄 *Certificado laboral*\n\n"
        "Por favor escribe tu *cédula* (solo números).\n"
        "Ejemplo: 1020304050"
    )


def texto_menu_cliente() -> str:
    return (
        "🧾 *Menú Cliente*\n\n"
        "Responde con un número:\n"
        "1️⃣ 🔎 Consultar guía\n"
        "2️⃣ ↩️ Volver al menú principal\n\n"
        "Para volver en cualquier momento escribe *menu*."
    )


def texto_pedir_guia() -> str:
    return (
        "🔎 *Consultar guía*\n\n"
        "Escribe el número de la *guía*\n"
        "Ejemplo: 801203424"
    )


# -------------------------
# Transportador (Vulcano)
# -------------------------
def texto_menu_transportador() -> str:
    return (
        "🚚 *Menú Transportador*\n\n"
        "Responde con un número:\n"
        "1️⃣ 🔎 Consultar manifiestos por *cédula del tenedor*\n"
        "2️⃣ ↩️ Volver al menú principal\n\n"
        "Para volver en cualquier momento escribe *menu*."
    )


def texto_pedir_cedula_tenedor() -> str:
    return (
        "🔎 *Consultar manifiestos*\n\n"
        "Escribe la *cédula del tenedor* (solo números).\n"
        "Ejemplo: 1012455147"
    )


# -------------------------
# Transportador — helpers async
# -------------------------
async def _obtener_pagos_mongodb(cedula: str, timeout: float = 15.0) -> List[Dict[str, Any]]:
    """Consulta el endpoint de pagos de saldo programados para un tenedor."""
    url = PAGOS_ENDPOINT.replace("{cedula}", cedula)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            r = await client.get(url)
            r.raise_for_status()
            data = r.json()
            return data if isinstance(data, list) else []
    except Exception as e:
        logger.warning("Pagos MongoDB error cedula=%s: %s", cedula, str(e))
        return []


async def _consultar_datos_tenedor(
    cedula: str, year: str
) -> Tuple[Dict[str, List], List[Dict], Dict[str, Dict]]:
    """
    Consulta en paralelo:
      - Vulcano rpt_id=26 (manifiestos detallados, sin ANULADOS)
      - MongoDB endpoint (pagos de saldo programados)
    Retorna (grupos, pagos_enriquecidos, dict_pagos).
    """
    filas_task = asyncio.to_thread(
        consultar_manifiestos_detallado,
        cedula_tenedor=cedula,
        year=year,
        page_size=500,
        page=1,
    )
    pagos_task = _obtener_pagos_mongodb(cedula)

    filas, pagos_raw = await asyncio.gather(filas_task, pagos_task)

    grupos = agrupar_por_estado(filas)

    # Índice Vulcano por Manif_numero para enriquecer los pagos
    dict_vulcano: Dict[str, Dict] = {
        str(f.get("Manif_numero", "")): f
        for f in filas
        if f.get("Manif_numero")
    }

    # Solo pagos cuyo manifiesto esté en estado LIQUIDADO en Vulcano
    mfts_liquidados = {
        str(f.get("Manif_numero", ""))
        for f in filas
        if str(f.get("Estado_mft") or "").upper() == "LIQUIDADO" and f.get("Manif_numero")
    }

    # Enriquecer pagos con Origen/Destino de Vulcano (solo LIQUIDADOS)
    pagos_enriquecidos: List[Dict] = []
    for p in pagos_raw:
        mft = str(p.get("Manifiesto") or "")
        if not mft or mft not in mfts_liquidados:
            continue
        pago = dict(p)
        if mft in dict_vulcano:
            v = dict_vulcano[mft]
            pago["Origen"] = v.get("Origen", "-")
            pago["Destino"] = v.get("Destino", "-")
            pago["Fecha"] = v.get("Fecha", "")   # fecha de despacho
        pagos_enriquecidos.append(pago)

    # dict_pagos para cruce en formateo (solo LIQUIDADOS)
    dict_pagos: Dict[str, Dict] = {
        str(p.get("Manifiesto", "")): p
        for p in pagos_enriquecidos
        if p.get("Manifiesto")
    }

    return grupos, pagos_enriquecidos, dict_pagos


# -------------------------
# Helpers
# -------------------------
def extraer_mensaje(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Extrae el primer mensaje entrante del webhook (texto o interactive).
    Devuelve dict con: from, type, text, id
    """
    try:
        value = data["entry"][0]["changes"][0]["value"]
        mensajes = value.get("messages", [])
        if not mensajes:
            return None

        m = mensajes[0]
        msg_type = m.get("type")
        texto = ""

        if msg_type == "text":
            texto = ((m.get("text") or {}).get("body") or "").strip()

        elif msg_type == "interactive":
            inter = m.get("interactive") or {}
            itype = inter.get("type")

            if itype == "button_reply":
                br = inter.get("button_reply") or {}
                texto = (br.get("id") or br.get("title") or "").strip()

            elif itype == "list_reply":
                lr = inter.get("list_reply") or {}
                texto = (lr.get("id") or lr.get("title") or "").strip()

        return {"from": m.get("from"), "type": msg_type, "text": texto, "id": m.get("id")}
    except Exception as e:
        print(f"❌ extraer_mensaje error: {e}")
        return None


def _es_menu(t: str) -> bool:
    t = (t or "").strip().lower()
    return t in ["menu", "menú", "inicio", "volver", "reiniciar", "cancelar", "reset"]


def _limpiar_numero(t: str) -> str:
    return (t or "").replace(".", "").replace(" ", "").strip()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_dt_iso(dt_str: str) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        if dt_str.endswith("Z"):
            dt_str = dt_str.replace("Z", "+00:00")
        d = datetime.fromisoformat(dt_str)
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d
    except Exception:
        return None


def set_state_with_ts(phone: str, state: str, context: Dict[str, Any]):
    # set_state debe aceptar updated_at (tu chat_state_integra ya lo tiene)
    set_state(phone, state, context or {}, updated_at=_utc_now_iso())


def _get_state_normalizado(phone: str) -> Dict[str, Any]:
    estado = get_state(phone)
    if not estado:
        return {"state": "START", "context": {}, "updated_at": None}

    state = estado.get("state") if isinstance(estado, dict) else None
    context = estado.get("context") if isinstance(estado, dict) else None
    updated_at = estado.get("updated_at") if isinstance(estado, dict) else None

    return {
        "state": state or "START",
        "context": context or {},
        "updated_at": updated_at,
    }


def _estado_expirado(updated_at: Optional[str]) -> bool:
    d = _parse_dt_iso(updated_at or "")
    if not d:
        return False
    return (datetime.now(timezone.utc) - d) > timedelta(minutes=STATE_TTL_MINUTES)


def _ctx_get_processed_ids(context: Dict[str, Any]) -> List[str]:
    ids = (context or {}).get("processed_msg_ids") or []
    if not isinstance(ids, list):
        return []
    # filtra solo strings
    return [x for x in ids if isinstance(x, str)]


def _ctx_has_processed_id(context: Dict[str, Any], msg_id: Optional[str]) -> bool:
    if not msg_id:
        return False
    return msg_id in _ctx_get_processed_ids(context)


def _ctx_add_processed_id(context: Dict[str, Any], msg_id: Optional[str]) -> Dict[str, Any]:
    ctx = dict(context or {})
    if not msg_id:
        return ctx

    ids = _ctx_get_processed_ids(ctx)
    if msg_id in ids:
        ctx["processed_msg_ids"] = ids
        return ctx

    ids.append(msg_id)
    if len(ids) > PROCESSED_IDS_MAX:
        ids = ids[-PROCESSED_IDS_MAX:]
    ctx["processed_msg_ids"] = ids
    return ctx


def _ctx_only_processed_ids(context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Cuando cambias de estado y no quieres arrastrar context viejo,
    pero sí quieres conservar la lista de dedup.
    """
    ids = _ctx_get_processed_ids(context or {})
    return {"processed_msg_ids": ids} if ids else {}


def _safe_int(v: Any, default: int) -> int:
    try:
        return int(v)
    except Exception:
        return default


async def _procesar_siscore_y_responder(numero: str, guia: str):
    from Funciones.siscore_ws_tracking import consultar_guia_ws
    from Funciones.siscore_ws_format import formatear_respuesta_guia

    try:
        payload = await consultar_guia_ws(guia, timeout_seconds=60.0)
        texto_respuesta = formatear_respuesta_guia(payload)
        await enviar_texto(numero, texto_respuesta)

        log_whatsapp_event(
            phone=numero,
            direction="OUT",
            event="MESSAGE_SENT",
            text="respuesta siscore soap enviada (bg)",
            state="CLIENTE_POST",
            context={"guia": guia, "ok": bool(payload.get("ok")), "exists": payload.get("exists")},
        )
    except Exception as e:
        tb = traceback.format_exc()
        logger.error("Siscore SOAP ERROR (bg): %s", str(e))
        logger.error("Traceback Siscore (bg):\n%s", tb)

        log_whatsapp_event(
            phone=numero,
            direction="SYSTEM",
            event="ERROR",
            state="CLIENTE_PROCESSING",
            context={"guia": guia},
            meta={"error_type": type(e).__name__, "error": str(e), "traceback": tb[:3000]},
        )

        await enviar_texto(
            numero,
            "❗ No pude consultar Siscore en este momento. Intenta de nuevo en unos minutos.\n\n"
            "Responde:\n1️⃣ Consultar otra guía\n2️⃣ Volver al menú principal"
        )



# -------------------------
# GET verificación (sin slash y con slash) para evitar 307
# -------------------------
@ruta_whatsapp_integra.get("")
async def verify_no_slash(request: Request):
    return await verify_webhook(request)


@ruta_whatsapp_integra.get("/")
async def verify_webhook(request: Request):
    hub_challenge = request.query_params.get("hub.challenge")
    hub_verify_token = request.query_params.get("hub.verify_token")

    if hub_verify_token == VERIFY_TOKEN:
        return PlainTextResponse(str(hub_challenge))
    return PlainTextResponse("Error de verificación", status_code=403)


# -------------------------
# POST mensajes (sin slash y con slash) para evitar 307
# -------------------------
@ruta_whatsapp_integra.post("")
async def webhook_no_slash(request: Request, background_tasks: BackgroundTasks):
    return await webhook(request, background_tasks)


@ruta_whatsapp_integra.post("/")
async def webhook(request: Request, background_tasks: BackgroundTasks):
    # 1) Lee JSON (si falla, responde 200 para que Meta NO reintente)
    try:
        data = await request.json()
    except Exception as e:
        print(f"❌ No pude leer JSON: {e}")
        return JSONResponse({"status": "ok"})

    # 2) Wrapper general: evita 500 (si hay 500, Meta reintenta y spamea)
    try:
        msg = extraer_mensaje(data)
        if not msg:
            return JSONResponse({"status": "ok"})

        numero = msg.get("from")
        texto = (msg.get("text") or "").strip()
        texto_lower = texto.lower().strip()
        msg_id = msg.get("id")

        # LOG entrada
        log_whatsapp_event(
            phone=numero,
            direction="IN",
            event="MESSAGE_RECEIVED",
            text=texto,
            meta={"msg_id": msg_id, "type": msg.get("type")},
        )

        # Si llega vacío, ignora
        if not texto:
            return JSONResponse({"status": "ok"})

        # Estado actual + TTL
        estado = _get_state_normalizado(numero)
        state = estado.get("state") or "START"
        context = estado.get("context") or {}

        # -------------------------
        # DEDUP robusto: si Meta reintenta un msg_id antiguo, no respondas otra vez
        # -------------------------
        if _ctx_has_processed_id(context, msg_id):
            return JSONResponse({"status": "ok"})

        # Marca este msg_id como procesado ANTES de seguir (idempotencia)
        context = _ctx_add_processed_id(context, msg_id)
        set_state_with_ts(numero, state, context)

        # TTL (si expiró, resetea y manda menú)
        if _estado_expirado(estado.get("updated_at")):
            reset_state(numero)
            ctx = _ctx_add_processed_id({}, msg_id)
            set_state_with_ts(numero, "START", ctx)
            log_whatsapp_event(
                phone=numero,
                direction="SYSTEM",
                event="STATE_EXPIRED",
                state="START",
                context={},
                meta={"ttl_minutes": STATE_TTL_MINUTES},
            )
            await enviar_texto(numero, "⏳ Tu sesión expiró por inactividad.\n\n" + texto_inicio())
            log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="sesion expirada -> menu", state="START")
            return JSONResponse({"status": "ok"})

        # Atajo menú
        if _es_menu(texto_lower):
            # Invalidar sesión autenticada si existe
            if is_authenticated(numero):
                invalidate_auth_session(numero)
                log_whatsapp_event(phone=numero, direction="SYSTEM", event="SESSION_INVALIDATED", state="START", context={})
            
            reset_state(numero)
            ctx = _ctx_add_processed_id({}, msg_id)
            set_state_with_ts(numero, "START", ctx)
            log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="START", context={})
            await enviar_texto(numero, texto_inicio())
            log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="(menu inicio)", state="START")
            return JSONResponse({"status": "ok"})

        # -------------------------
        # START
        # -------------------------
        if state == "START":
            if texto_lower in ["1", "2", "3"]:
                base_ctx = _ctx_only_processed_ids(context)
                base_ctx = _ctx_add_processed_id(base_ctx, msg_id)

                if texto_lower == "1":
                    # Verificar si ya está autenticado
                    if is_authenticated(numero):
                        auth_session = get_auth_session(numero)
                        cedula = auth_session.get("cedula", "")
                        set_state_with_ts(numero, "TRANSPORTADOR_MENU", base_ctx)
                        log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="TRANSPORTADOR_MENU", context={"cedula": cedula})
                        await enviar_texto(numero, texto_menu_transportador())
                        log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="menu transportador (autenticado)", state="TRANSPORTADOR_MENU")
                    else:
                        set_state_with_ts(numero, "TRANSPORTADOR_AUTH_CEDULA", base_ctx)
                        log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="TRANSPORTADOR_AUTH_CEDULA", context={})
                        await enviar_texto(numero, "🔐 *Autenticación Transportador*\n\n" + "Por favor escribe tu *cédula* (solo números).\n" + "Ejemplo: 1012455147")
                        log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="pedir cedula autenticacion", state="TRANSPORTADOR_AUTH_CEDULA")
                    return JSONResponse({"status": "ok"})

                if texto_lower == "2":
                    set_state_with_ts(numero, "EMPLOYEE_MENU", base_ctx)
                    log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="EMPLOYEE_MENU", context={})
                    await enviar_texto(numero, texto_menu_empleado())
                    log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="menu empleado", state="EMPLOYEE_MENU")
                    return JSONResponse({"status": "ok"})

                if texto_lower == "3":
                    set_state_with_ts(numero, "CLIENTE_MENU", base_ctx)
                    log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="CLIENTE_MENU", context={})
                    await enviar_texto(numero, texto_menu_cliente())
                    log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="menu cliente", state="CLIENTE_MENU")
                    return JSONResponse({"status": "ok"})

            await enviar_texto(numero, texto_inicio())
            log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="menu inicio", state="START")
            # refresca estado START conservando dedup
            ctx = _ctx_only_processed_ids(context)
            ctx = _ctx_add_processed_id(ctx, msg_id)
            set_state_with_ts(numero, "START", ctx)
            return JSONResponse({"status": "ok"})

        # -------------------------
        # TRANSPORTADOR_MENU
        # -------------------------
        if state == "TRANSPORTADOR_MENU":
            if texto_lower == "1":
                # Si está autenticado, usa su cédula de sesión
                auth_session = get_auth_session(numero)
                cedula = auth_session.get("cedula", "") if auth_session else ""
                
                ctx = _ctx_only_processed_ids(context)
                ctx.update({"cedula_tenedor": cedula, "year": TRANSP_DEFAULT_YEAR})
                ctx = _ctx_add_processed_id(ctx, msg_id)
                set_state_with_ts(numero, "TRANSPORTADOR_ASK_CEDULA", ctx)
                log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="TRANSPORTADOR_ASK_CEDULA", context={"cedula": cedula})
                await enviar_texto(numero, texto_pedir_cedula_tenedor())
                return JSONResponse({"status": "ok"})

            if texto_lower == "2":
                reset_state(numero)
                ctx = _ctx_add_processed_id({}, msg_id)
                set_state_with_ts(numero, "START", ctx)
                await enviar_texto(numero, texto_inicio())
                return JSONResponse({"status": "ok"})

            await enviar_texto(numero, "Opción no válida.\n\n" + texto_menu_transportador())
            ctx = _ctx_add_processed_id(_ctx_only_processed_ids(context), msg_id)
            set_state_with_ts(numero, "TRANSPORTADOR_MENU", ctx)
            return JSONResponse({"status": "ok"})

        # -------------------------
        # TRANSPORTADOR_AUTH_CEDULA
        # -------------------------
        if state == "TRANSPORTADOR_AUTH_CEDULA":
            cedula = _limpiar_numero(texto)

            if not CEDULA_REGEX.match(cedula):
                await enviar_texto(numero, "La cédula debe contener solo números (5-15 dígitos).\n\n" + "Por favor escribe tu *cédula* (solo números).\n" + "Ejemplo: 1012455147")
                ctx = _ctx_add_processed_id(_ctx_only_processed_ids(context), msg_id)
                set_state_with_ts(numero, "TRANSPORTADOR_AUTH_CEDULA", ctx)
                return JSONResponse({"status": "ok"})

            # Verificar que el usuario existe
            from Funciones.whatsapp_utils_integra import buscar_usuario_por_cedula
            usuario = buscar_usuario_por_cedula(cedula)
            
            if not usuario:
                # Usuario no encontrado, preguntar si quiere registrarse
                ctx = _ctx_only_processed_ids(context)
                ctx.update({"registro_cedula": cedula})
                ctx = _ctx_add_processed_id(ctx, msg_id)
                set_state_with_ts(numero, "TRANSPORTADOR_AUTH_PREGUNTAR_REGISTRO", ctx)
                await enviar_texto(numero, 
                    f"No se encontró un transportador registrado con la cédula {cedula}.\n\n"
                    "¿Quieres registrarte como nuevo usuario?\n\n"
                    "1️⃣ Sí, quiero registrarme\n"
                    "2️⃣ No, volver al inicio"
                )
                log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="TRANSPORTADOR_AUTH_PREGUNTAR_REGISTRO", context={"cedula": cedula})
                return JSONResponse({"status": "ok"})

            ctx = _ctx_only_processed_ids(context)
            ctx.update({"auth_cedula": cedula})
            ctx = _ctx_add_processed_id(ctx, msg_id)
            set_state_with_ts(numero, "TRANSPORTADOR_AUTH_CLAVE", ctx)
            log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="TRANSPORTADOR_AUTH_CLAVE", context={"cedula": cedula})
            await enviar_texto(numero, "🔐 *Autenticación Transportador*\n\n" + "Por favor escribe tu *clave*.")
            return JSONResponse({"status": "ok"})

        # -------------------------
        # TRANSPORTADOR_AUTH_CLAVE
        # -------------------------
        if state == "TRANSPORTADOR_AUTH_CLAVE":
            cedula = (context or {}).get("auth_cedula", "")
            clave = texto

            if not clave or len(clave) < 1:
                await enviar_texto(numero, "Por favor escribe tu clave.\n\n" + "O escribe *RECUPERAR* si olvidaste tu clave.")
                ctx = _ctx_add_processed_id(dict(context or {}), msg_id)
                set_state_with_ts(numero, "TRANSPORTADOR_AUTH_CLAVE", ctx)
                return JSONResponse({"status": "ok"})

            # Verificar credenciales
            es_valido, usuario = verificar_credenciales_transportador(cedula, clave)

            if es_valido:
                # Guardar sesión autenticada
                set_auth_session(numero, cedula)
                
                # Ir al menú de transportador
                ctx = _ctx_only_processed_ids(context)
                ctx = _ctx_add_processed_id(ctx, msg_id)
                set_state_with_ts(numero, "TRANSPORTADOR_MENU", ctx)
                log_whatsapp_event(phone=numero, direction="SYSTEM", event="AUTH_SUCCESS", state="TRANSPORTADOR_MENU", context={"cedula": cedula})
                await enviar_texto(numero, "✅ ¡Autenticación exitosa!\n\n" + texto_menu_transportador())
                return JSONResponse({"status": "ok"})
            else:
                # Credenciales incorrectas
                if texto_lower in ["recuperar", "2"]:
                    # Flujo de recuperación
                    ctx = _ctx_only_processed_ids(context)
                    ctx = _ctx_add_processed_id(ctx, msg_id)
                    set_state_with_ts(numero, "TRANSPORTADOR_AUTH_RECUPERAR", ctx)
                    log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="TRANSPORTADOR_AUTH_RECUPERAR", context={"cedula": cedula})
                    
                    # Solicitar recuperación
                    exito, mensaje, email = solicitar_recuperacion_clave(cedula)
                    await enviar_texto(numero, mensaje)
                    log_whatsapp_event(phone=numero, direction="OUT", event="AUTH_RECOVERY_REQUEST", state="TRANSPORTADOR_AUTH_RECUPERAR", context={"cedula": cedula, "email": email})
                    
                    # Volver a pedir cédula
                    ctx = _ctx_only_processed_ids(context)
                    ctx = _ctx_add_processed_id(ctx, msg_id)
                    set_state_with_ts(numero, "TRANSPORTADOR_AUTH_CEDULA", ctx)
                    return JSONResponse({"status": "ok"})
                
                await enviar_texto(
                    numero,
                    "❌ Clave incorrecta.\n\n"
                    "1️⃣ Intentar de nuevo\n"
                    "2️⃣ Recuperar clave (escribe RECUPERAR)\n"
                    "3️⃣ Volver al inicio (escribe menu)\n\n"
                    "O escribe tu clave nuevamente."
                )
                ctx = _ctx_add_processed_id(dict(context or {}), msg_id)
                set_state_with_ts(numero, "TRANSPORTADOR_AUTH_CLAVE", ctx)
                return JSONResponse({"status": "ok"})

        # -------------------------
        # TRANSPORTADOR_AUTH_PREGUNTAR_REGISTRO
        # -------------------------
        if state == "TRANSPORTADOR_AUTH_PREGUNTAR_REGISTRO":
            if texto_lower == "1":
                # Usuario quiere registrarse
                ctx = _ctx_only_processed_ids(context)
                ctx = _ctx_add_processed_id(ctx, msg_id)
                set_state_with_ts(numero, "TRANSPORTADOR_REGISTRO_NOMBRE", ctx)
                await enviar_texto(numero, 
                    "📝 *Registro de Transportador*\n\n"
                    "Vamos a crear tu cuenta. Por favor proporciona la siguiente información:\n\n"
                    "1️⃣ Escribe tu *nombre completo*"
                )
                log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="TRANSPORTADOR_REGISTRO_NOMBRE", context={})
                return JSONResponse({"status": "ok"})
            
            if texto_lower == "2":
                # Usuario no quiere registrarse, volver al inicio
                reset_state(numero)
                ctx = _ctx_add_processed_id({}, msg_id)
                set_state_with_ts(numero, "START", ctx)
                await enviar_texto(numero, texto_inicio())
                return JSONResponse({"status": "ok"})
            
            await enviar_texto(numero, "Opción no válida. Responde 1️⃣ o 2️⃣ (o escribe *menu*).")
            ctx = _ctx_add_processed_id(_ctx_only_processed_ids(context), msg_id)
            set_state_with_ts(numero, "TRANSPORTADOR_AUTH_PREGUNTAR_REGISTRO", ctx)
            return JSONResponse({"status": "ok"})

        # -------------------------
        # TRANSPORTADOR_REGISTRO_NOMBRE
        # -------------------------
        if state == "TRANSPORTADOR_REGISTRO_NOMBRE":
            nombre = texto.strip()
            
            if len(nombre) < 3:
                await enviar_texto(numero, "El nombre debe tener al menos 3 caracteres.\n\n" + "Por favor escribe tu *nombre completo*.")
                ctx = _ctx_add_processed_id(_ctx_only_processed_ids(context), msg_id)
                set_state_with_ts(numero, "TRANSPORTADOR_REGISTRO_NOMBRE", ctx)
                return JSONResponse({"status": "ok"})
            
            cedula = (context or {}).get("registro_cedula", "")
            ctx = _ctx_only_processed_ids(context)
            ctx.update({"registro_nombre": nombre})
            ctx = _ctx_add_processed_id(ctx, msg_id)
            set_state_with_ts(numero, "TRANSPORTADOR_REGISTRO_CEDULA", ctx)
            await enviar_texto(numero, 
                f"✅ Nombre: {nombre}\n\n"
                f"Tu cédula: {cedula}\n\n"
                "2️⃣ Escribe tu *correo electrónico*"
            )
            log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="TRANSPORTADOR_REGISTRO_EMAIL", context={"nombre": nombre})
            return JSONResponse({"status": "ok"})

        # -------------------------
        # TRANSPORTADOR_REGISTRO_EMAIL
        # -------------------------
        if state == "TRANSPORTADOR_REGISTRO_EMAIL":
            email = texto.strip().lower()
            
            from Funciones.whatsapp_utils_integra import _validar_email
            es_valido, mensaje = _validar_email(email)
            
            if not es_valido:
                await enviar_texto(numero, f"{mensaje}\n\n" + "Por favor escribe tu *correo electrónico*.")
                ctx = _ctx_add_processed_id(_ctx_only_processed_ids(context), msg_id)
                set_state_with_ts(numero, "TRANSPORTADOR_REGISTRO_EMAIL", ctx)
                return JSONResponse({"status": "ok"})
            
            ctx = _ctx_only_processed_ids(context)
            ctx.update({"registro_email": email})
            ctx = _ctx_add_processed_id(ctx, msg_id)
            set_state_with_ts(numero, "TRANSPORTADOR_REGISTRO_TELEFONO", ctx)
            await enviar_texto(numero, 
                "✅ Correo: " + email + "\n\n"
                "3️⃣ Escribe tu *teléfono* (10 dígitos)"
            )
            log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="TRANSPORTADOR_REGISTRO_TELEFONO", context={"email": email})
            return JSONResponse({"status": "ok"})

        # -------------------------
        # TRANSPORTADOR_REGISTRO_TELEFONO
        # -------------------------
        if state == "TRANSPORTADOR_REGISTRO_TELEFONO":
            telefono = texto.strip().replace(" ", "").replace("-", "")
            
            from Funciones.whatsapp_utils_integra import _validar_telefono
            es_valido = _validar_telefono(telefono)
            
            if not es_valido:
                await enviar_texto(numero, "El teléfono debe tener 10 dígitos.\n\n" + "Por favor escribe tu *teléfono* (10 dígitos).")
                ctx = _ctx_add_processed_id(_ctx_only_processed_ids(context), msg_id)
                set_state_with_ts(numero, "TRANSPORTADOR_REGISTRO_TELEFONO", ctx)
                return JSONResponse({"status": "ok"})
            
            ctx = _ctx_only_processed_ids(context)
            ctx.update({"registro_telefono": telefono})
            ctx = _ctx_add_processed_id(ctx, msg_id)
            set_state_with_ts(numero, "TRANSPORTADOR_REGISTRO_CLAVE", ctx)
            await enviar_texto(numero, 
                "✅ Teléfono: " + telefono + "\n\n"
                "4️⃣ Escribe tu *clave* (mínimo 6 caracteres)"
            )
            log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="TRANSPORTADOR_REGISTRO_CLAVE", context={"telefono": telefono})
            return JSONResponse({"status": "ok"})

        # -------------------------
        # TRANSPORTADOR_REGISTRO_CLAVE
        # -------------------------
        if state == "TRANSPORTADOR_REGISTRO_CLAVE":
            clave = texto.strip()
            
            from Funciones.whatsapp_utils_integra import _validar_clave
            es_valido, mensaje = _validar_clave(clave)
            
            if not es_valido:
                await enviar_texto(numero, f"{mensaje}\n\n" + "Por favor escribe tu *clave* (mínimo 6 caracteres).")
                ctx = _ctx_add_processed_id(_ctx_only_processed_ids(context), msg_id)
                set_state_with_ts(numero, "TRANSPORTADOR_REGISTRO_CLAVE", ctx)
                return JSONResponse({"status": "ok"})
            
            ctx = _ctx_only_processed_ids(context)
            ctx.update({"registro_clave": clave})
            ctx = _ctx_add_processed_id(ctx, msg_id)
            set_state_with_ts(numero, "TRANSPORTADOR_REGISTRO_CONFIRMAR_CLAVE", ctx)
            await enviar_texto(numero, 
                "✅ Clave guardada\n\n"
                "5️⃣ Confirma tu *clave* (escribe la misma clave nuevamente)"
            )
            log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="TRANSPORTADOR_REGISTRO_CONFIRMAR_CLAVE", context={})
            return JSONResponse({"status": "ok"})

        # -------------------------
        # TRANSPORTADOR_REGISTRO_CONFIRMAR_CLAVE
        # -------------------------
        if state == "TRANSPORTADOR_REGISTRO_CONFIRMAR_CLAVE":
            clave_confirmar = texto.strip()
            clave_original = (context or {}).get("registro_clave", "")
            
            if clave_confirmar != clave_original:
                await enviar_texto(numero, 
                    "❌ Las claves no coinciden.\n\n"
                    "Por favor confirma tu *clave* (escribe la misma clave nuevamente)."
                )
                ctx = _ctx_add_processed_id(_ctx_only_processed_ids(context), msg_id)
                set_state_with_ts(numero, "TRANSPORTADOR_REGISTRO_CONFIRMAR_CLAVE", ctx)
                return JSONResponse({"status": "ok"})
            
            # Crear usuario
            nombre = (context or {}).get("registro_nombre", "")
            cedula = (context or {}).get("registro_cedula", "")
            email = (context or {}).get("registro_email", "")
            telefono = (context or {}).get("registro_telefono", "")
            
            from Funciones.whatsapp_utils_integra import crear_usuario_transportador, _guardar_codigo_confirmacion, _generar_codigo_confirmacion, _enviar_correo_confirmacion
            
            exito, mensaje, usuario_id = crear_usuario_transportador(cedula, nombre, email, telefono, clave_confirmar)
            
            if not exito:
                await enviar_texto(numero, f"❌ {mensaje}\n\n" + "Escribe *menu* para volver al inicio.")
                reset_state(numero)
                ctx = _ctx_add_processed_id({}, msg_id)
                set_state_with_ts(numero, "START", ctx)
                return JSONResponse({"status": "ok"})
            
            # Generar y enviar código de confirmación
            codigo = _generar_codigo_confirmacion()
            _guardar_codigo_confirmacion(usuario_id, email, codigo)
            _enviar_correo_confirmacion(email, codigo)
            
            ctx = _ctx_only_processed_ids(context)
            ctx.update({"registro_usuario_id": usuario_id})
            ctx = _ctx_add_processed_id(ctx, msg_id)
            set_state_with_ts(numero, "TRANSPORTADOR_REGISTRO_VERIFICAR", ctx)
            
            log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="TRANSPORTADOR_REGISTRO_VERIFICAR", context={"usuario_id": usuario_id})
            
            await enviar_texto(numero, 
                "📧 Hemos enviado un código de confirmación a tu correo: *" + email + "*\n\n"
                "El código expira en 1 hora.\n\n"
                "6️⃣ Escribe el código de 6 dígitos que recibiste"
            )
            return JSONResponse({"status": "ok"})

        # -------------------------
        # TRANSPORTADOR_REGISTRO_VERIFICAR
        # -------------------------
        if state == "TRANSPORTADOR_REGISTRO_VERIFICAR":
            codigo = texto.strip().upper()
            usuario_id = (context or {}).get("registro_usuario_id", "")
            
            if not codigo or len(codigo) != 6:
                await enviar_texto(numero, "El código debe tener 6 dígitos.\n\n" + "Por favor escribe el código de 6 dígitos que recibiste en tu correo.")
                ctx = _ctx_add_processed_id(_ctx_only_processed_ids(context), msg_id)
                set_state_with_ts(numero, "TRANSPORTADOR_REGISTRO_VERIFICAR", ctx)
                return JSONResponse({"status": "ok"})
            
            from Funciones.whatsapp_utils_integra import verificar_codigo_confirmacion
            exito, mensaje, usuario = verificar_codigo_confirmacion(usuario_id, codigo)
            
            if not exito:
                await enviar_texto(numero, f"❌ {mensaje}\n\n" + "Escribe *menu* para volver al inicio.")
                reset_state(numero)
                ctx = _ctx_add_processed_id({}, msg_id)
                set_state_with_ts(numero, "START", ctx)
                return JSONResponse({"status": "ok"})
            
            # Usuario confirmado, autenticar automáticamente
            cedula = usuario.get("tenedor", "")
            set_auth_session(numero, cedula)
            
            ctx = _ctx_only_processed_ids(context)
            ctx = _ctx_add_processed_id(ctx, msg_id)
            set_state_with_ts(numero, "TRANSPORTADOR_REGISTRO_EXITO", ctx)
            
            log_whatsapp_event(phone=numero, direction="SYSTEM", event="REGISTRATION_COMPLETE", state="TRANSPORTADOR_REGISTRO_EXITO", context={"cedula": cedula})
            
            await enviar_texto(numero, 
                "🎉 ¡Cuenta creada y confirmada exitosamente!\n\n"
                "Ya estás autenticado y puedes consultar tu información.\n\n" 
                + texto_menu_transportador()
            )
            return JSONResponse({"status": "ok"})

        # -------------------------
        # TRANSPORTADOR_REGISTRO_EXITO
        # -------------------------
        if state == "TRANSPORTADOR_REGISTRO_EXITO":
            # Ir al menú de transportador
            ctx = _ctx_only_processed_ids(context)
            ctx = _ctx_add_processed_id(ctx, msg_id)
            set_state_with_ts(numero, "TRANSPORTADOR_MENU", ctx)
            await enviar_texto(numero, texto_menu_transportador())
            return JSONResponse({"status": "ok"})

        # -------------------------
        # TRANSPORTADOR_AUTH_RECUPERAR
        # -------------------------
        if state == "TRANSPORTADOR_AUTH_RECUPERAR":
            # Este estado es solo informativo, la recuperación ya se envió
            ctx = _ctx_only_processed_ids(context)
            ctx = _ctx_add_processed_id(ctx, msg_id)
            set_state_with_ts(numero, "TRANSPORTADOR_AUTH_CEDULA", ctx)
            await enviar_texto(numero, "🔐 *Autenticación Transportador*\n\n" + "Por favor escribe tu *cédula* (solo números).\n" + "Ejemplo: 1012455147")
            return JSONResponse({"status": "ok"})

        # -------------------------
        # TRANSPORTADOR_ASK_CEDULA
        # -------------------------
        if state == "TRANSPORTADOR_ASK_CEDULA":
            cedula = _limpiar_numero(texto)

            if not CEDULA_REGEX.match(cedula):
                await enviar_texto(numero, "La cédula debe contener solo números.\n\n" + texto_pedir_cedula_tenedor())
                ctx = _ctx_add_processed_id(_ctx_only_processed_ids(context), msg_id)
                set_state_with_ts(numero, "TRANSPORTADOR_ASK_CEDULA", ctx)
                return JSONResponse({"status": "ok"})

            # Verificar si el usuario está autenticado y si la cédula coincide
            if is_authenticated(numero):
                auth_session = get_auth_session(numero)
                cedula_autenticada = auth_session.get("cedula", "")
                
                if cedula != cedula_autenticada:
                    await enviar_texto(numero,
                        "⚠️ Solo puedes consultar información de tu cuenta autenticada.\n\n"
                        f"Tu cédula registrada: *{cedula_autenticada}*\n\n"
                        "Si quieres consultar otra cédula, primero cierra sesión escribiendo *menu*."
                    )
                    ctx = _ctx_add_processed_id(_ctx_only_processed_ids(context), msg_id)
                    set_state_with_ts(numero, "TRANSPORTADOR_MENU", ctx)
                    return JSONResponse({"status": "ok"})

            year = str((context or {}).get("year") or TRANSP_DEFAULT_YEAR)

            ctxp = _ctx_only_processed_ids(context)
            ctxp.update({"cedula_tenedor": cedula, "year": year})
            ctxp = _ctx_add_processed_id(ctxp, msg_id)
            set_state_with_ts(numero, "TRANSPORTADOR_PROCESSING", ctxp)

            await enviar_texto(numero, "🔎 Consultando manifiestos y pagos, un momento…")
            log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="consultando vulcano+pagos", state="TRANSPORTADOR_PROCESSING")

            try:
                grupos, pagos, dict_pagos = await _consultar_datos_tenedor(cedula, year)
            except Exception as e:
                tb = traceback.format_exc()
                logger.error("Vulcano detallado ERROR: %s\n%s", str(e), tb)
                log_whatsapp_event(
                    phone=numero, direction="SYSTEM", event="ERROR",
                    state="TRANSPORTADOR_PROCESSING", context=ctxp,
                    meta={"error_type": type(e).__name__, "error": str(e), "traceback": tb[:3000]},
                )
                ctx_back = _ctx_add_processed_id(_ctx_only_processed_ids(context), msg_id)
                set_state_with_ts(numero, "TRANSPORTADOR_MENU", ctx_back)
                err_txt = str(e).lower()
                msg_user = "❗ No pude consultar en este momento."
                if "timeout" in err_txt:
                    msg_user += " El servicio tardó demasiado (timeout)."
                elif "401" in err_txt or "403" in err_txt or "unauthorized" in err_txt:
                    msg_user += " Problema de autenticación con Vulcano."
                msg_user += "\nIntenta de nuevo en unos minutos.\n\n" + texto_menu_transportador()
                await enviar_texto(numero, msg_user)
                return JSONResponse({"status": "ok"})

            texto_resumen, opcion_map = formatear_resumen_tenedor(cedula, year, grupos, pagos)

            ctx_res = _ctx_only_processed_ids(ctxp)
            ctx_res.update({
                "cedula_tenedor": cedula,
                "year": year,
                "grupos": grupos,
                "pagos": pagos,
                "dict_pagos": dict_pagos,
                "opcion_map": opcion_map,
            })
            ctx_res = _ctx_add_processed_id(ctx_res, msg_id)
            set_state_with_ts(numero, "TRANSPORTADOR_RESUMEN", ctx_res)
            log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="TRANSPORTADOR_RESUMEN", context={"cedula_tenedor": cedula})

            await enviar_texto(numero, texto_resumen)
            log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="resumen transportador", state="TRANSPORTADOR_RESUMEN")
            return JSONResponse({"status": "ok"})

        # -------------------------
        # TRANSPORTADOR_RESUMEN
        # -------------------------
        if state == "TRANSPORTADOR_RESUMEN":
            opcion_map = (context or {}).get("opcion_map") or {}
            accion = opcion_map.get(texto_lower, "")

            cedula = str((context or {}).get("cedula_tenedor") or "")
            year   = str((context or {}).get("year") or TRANSP_DEFAULT_YEAR)
            grupos = (context or {}).get("grupos") or {}
            pagos  = (context or {}).get("pagos") or []
            dict_pagos = (context or {}).get("dict_pagos") or {}

            if accion == "pagos":
                ctx = dict(context or {})
                ctx = _ctx_add_processed_id(ctx, msg_id)
                set_state_with_ts(numero, "TRANSPORTADOR_PAGOS", ctx)
                await enviar_texto(numero, formatear_pagos_saldo(pagos))
                return JSONResponse({"status": "ok"})

            if accion == "consultar_manifiesto":
                ctx = dict(context or {})
                ctx = _ctx_add_processed_id(ctx, msg_id)
                set_state_with_ts(numero, "TRANSPORTADOR_ASK_MANIFIESTO", ctx)
                await enviar_texto(
                    numero,
                    "🔍 *Consultar manifiesto*\n\n"
                    "Escribe el código del manifiesto.\n"
                    "Ejemplo: 00134165 o U000006314\n\n"
                    "Para volver escribe *menu*."
                )
                return JSONResponse({"status": "ok"})

            if accion == "ver_historico_web":
                await enviar_texto(
                    numero,
                    "🌐 *Historial en web*\n\n"
                    "Puedes ver tu historial completo en:\n\n"
                    "https://integralogistica.com/integrapp/loginpropietarios\n\n"
                    "Responde:\n"
                    "1️⃣ Volver al resumen\n"
                    "2️⃣ Menú principal\n\n"
                    "O escribe *menu*."
                )
                ctx = dict(context or {})
                ctx = _ctx_add_processed_id(ctx, msg_id)
                set_state_with_ts(numero, "TRANSPORTADOR_HISTORICO_WEB", ctx)
                return JSONResponse({"status": "ok"})

            if accion.startswith("estado_"):
                estado = accion[len("estado_"):]
                ctx = dict(context or {})
                ctx["estado_actual"] = estado
                ctx["page_estado"] = 1
                ctx = _ctx_add_processed_id(ctx, msg_id)
                set_state_with_ts(numero, "TRANSPORTADOR_ESTADO_DETALLE", ctx)
                filas_e = grupos.get(estado, [])
                await enviar_texto(numero, formatear_manifiestos_estado(filas_e, estado, page=1, dict_pagos=dict_pagos))
                return JSONResponse({"status": "ok"})

            if accion == "otra_cedula":
                ctx = _ctx_only_processed_ids(context or {})
                ctx["year"] = year
                ctx = _ctx_add_processed_id(ctx, msg_id)
                set_state_with_ts(numero, "TRANSPORTADOR_ASK_CEDULA", ctx)
                await enviar_texto(numero, texto_pedir_cedula_tenedor())
                return JSONResponse({"status": "ok"})

            if accion == "menu_principal":
                reset_state(numero)
                ctx = _ctx_add_processed_id({}, msg_id)
                set_state_with_ts(numero, "START", ctx)
                await enviar_texto(numero, texto_inicio())
                return JSONResponse({"status": "ok"})

            # Opción no válida: re-muestra el resumen
            texto_resumen, opcion_map_nuevo = formatear_resumen_tenedor(cedula, year, grupos, pagos)
            ctx = dict(context or {})
            ctx["opcion_map"] = opcion_map_nuevo
            ctx = _ctx_add_processed_id(ctx, msg_id)
            set_state_with_ts(numero, "TRANSPORTADOR_RESUMEN", ctx)
            await enviar_texto(numero, "Opción no válida.\n\n" + texto_resumen)
            return JSONResponse({"status": "ok"})

        # -------------------------
        # TRANSPORTADOR_HISTORICO_WEB
        # -------------------------
        if state == "TRANSPORTADOR_HISTORICO_WEB":
            cedula = str((context or {}).get("cedula_tenedor") or "")
            year   = str((context or {}).get("year") or TRANSP_DEFAULT_YEAR)
            grupos = (context or {}).get("grupos") or {}
            pagos  = (context or {}).get("pagos") or []

            if texto_lower == "1":
                texto_res, opcion_map_nuevo = formatear_resumen_tenedor(cedula, year, grupos, pagos)
                ctx = dict(context or {})
                ctx["opcion_map"] = opcion_map_nuevo
                ctx = _ctx_add_processed_id(ctx, msg_id)
                set_state_with_ts(numero, "TRANSPORTADOR_RESUMEN", ctx)
                await enviar_texto(numero, texto_res)
                return JSONResponse({"status": "ok"})

            if texto_lower == "2":
                reset_state(numero)
                ctx = _ctx_add_processed_id({}, msg_id)
                set_state_with_ts(numero, "START", ctx)
                await enviar_texto(numero, texto_inicio())
                return JSONResponse({"status": "ok"})

            await enviar_texto(numero, "Opción no válida. Responde 1️⃣ o 2️⃣ (o escribe *menu*).")
            ctx = _ctx_add_processed_id(dict(context or {}), msg_id)
            set_state_with_ts(numero, "TRANSPORTADOR_HISTORICO_WEB", ctx)
            return JSONResponse({"status": "ok"})

        # -------------------------
        # TRANSPORTADOR_PAGOS (sin paginación)
        # -------------------------
        if state == "TRANSPORTADOR_PAGOS":
            pagos  = (context or {}).get("pagos") or []
            grupos = (context or {}).get("grupos") or {}

            if texto_lower == "1":
                cedula = str((context or {}).get("cedula_tenedor") or "")
                year   = str((context or {}).get("year") or TRANSP_DEFAULT_YEAR)
                texto_res, opcion_map_nuevo = formatear_resumen_tenedor(cedula, year, grupos, pagos)
                ctx = dict(context or {})
                ctx["opcion_map"] = opcion_map_nuevo
                ctx = _ctx_add_processed_id(ctx, msg_id)
                set_state_with_ts(numero, "TRANSPORTADOR_RESUMEN", ctx)
                await enviar_texto(numero, texto_res)
                return JSONResponse({"status": "ok"})

            if texto_lower == "2":
                reset_state(numero)
                ctx = _ctx_add_processed_id({}, msg_id)
                set_state_with_ts(numero, "START", ctx)
                await enviar_texto(numero, texto_inicio())
                return JSONResponse({"status": "ok"})

            await enviar_texto(numero, "Opción no válida. Responde 1️⃣ o 2️⃣ (o escribe *menu*).")
            ctx = _ctx_add_processed_id(dict(context or {}), msg_id)
            set_state_with_ts(numero, "TRANSPORTADOR_PAGOS", ctx)
            return JSONResponse({"status": "ok"})

        # -------------------------
        # TRANSPORTADOR_ASK_MANIFIESTO
        # -------------------------
        if state == "TRANSPORTADOR_ASK_MANIFIESTO":
            cod = texto.strip().upper()

            if not MANIFIESTO_REGEX.match(cod):
                await enviar_texto(
                    numero,
                    "Código no válido. Debe tener entre 5 y 20 caracteres alfanuméricos.\n\n"
                    "Escribe el código del manifiesto o *menu* para salir."
                )
                ctx = _ctx_add_processed_id(dict(context or {}), msg_id)
                set_state_with_ts(numero, "TRANSPORTADOR_ASK_MANIFIESTO", ctx)
                return JSONResponse({"status": "ok"})

            # Buscar en todos los grupos
            grupos    = (context or {}).get("grupos") or {}
            dict_pagos = (context or {}).get("dict_pagos") or {}
            fila_encontrada = None
            for filas_e in grupos.values():
                for f in filas_e:
                    if str(f.get("Manif_numero") or "").upper() == cod:
                        fila_encontrada = f
                        break
                if fila_encontrada:
                    break

            pago_info = dict_pagos.get(cod) or dict_pagos.get(cod.lower())
            texto_det = formatear_detalle_manifiesto(cod, fila_encontrada, pago_info)

            ctx = dict(context or {})
            ctx["ultimo_manifiesto"] = cod
            ctx = _ctx_add_processed_id(ctx, msg_id)
            set_state_with_ts(numero, "TRANSPORTADOR_MANIFIESTO_DETALLE", ctx)
            await enviar_texto(numero, texto_det)
            return JSONResponse({"status": "ok"})

        # -------------------------
        # TRANSPORTADOR_MANIFIESTO_DETALLE
        # -------------------------
        if state == "TRANSPORTADOR_MANIFIESTO_DETALLE":
            if texto_lower == "1":
                ctx = dict(context or {})
                ctx = _ctx_add_processed_id(ctx, msg_id)
                set_state_with_ts(numero, "TRANSPORTADOR_ASK_MANIFIESTO", ctx)
                await enviar_texto(
                    numero,
                    "🔍 *Consultar manifiesto*\n\n"
                    "Escribe el código del manifiesto.\n"
                    "Ejemplo: 00134165 o U000006314\n\n"
                    "Para volver escribe *menu*."
                )
                return JSONResponse({"status": "ok"})

            if texto_lower == "2":
                cedula = str((context or {}).get("cedula_tenedor") or "")
                year   = str((context or {}).get("year") or TRANSP_DEFAULT_YEAR)
                grupos = (context or {}).get("grupos") or {}
                pagos  = (context or {}).get("pagos") or []
                texto_res, opcion_map_nuevo = formatear_resumen_tenedor(cedula, year, grupos, pagos)
                ctx = dict(context or {})
                ctx["opcion_map"] = opcion_map_nuevo
                ctx = _ctx_add_processed_id(ctx, msg_id)
                set_state_with_ts(numero, "TRANSPORTADOR_RESUMEN", ctx)
                await enviar_texto(numero, texto_res)
                return JSONResponse({"status": "ok"})

            if texto_lower == "3":
                reset_state(numero)
                ctx = _ctx_add_processed_id({}, msg_id)
                set_state_with_ts(numero, "START", ctx)
                await enviar_texto(numero, texto_inicio())
                return JSONResponse({"status": "ok"})

            await enviar_texto(numero, "Opción no válida. Responde 1️⃣, 2️⃣ o 3️⃣ (o escribe *menu*).")
            ctx = _ctx_add_processed_id(dict(context or {}), msg_id)
            set_state_with_ts(numero, "TRANSPORTADOR_MANIFIESTO_DETALLE", ctx)
            return JSONResponse({"status": "ok"})

        # -------------------------
        # TRANSPORTADOR_ESTADO_DETALLE
        # -------------------------
        if state == "TRANSPORTADOR_ESTADO_DETALLE":
            estado     = str((context or {}).get("estado_actual") or "LIQUIDADO")
            grupos     = (context or {}).get("grupos") or {}
            dict_pagos = (context or {}).get("dict_pagos") or {}
            filas_e    = grupos.get(estado, [])
            page       = _safe_int((context or {}).get("page_estado"), 1)
            hay_mas    = page * PAGE_SIZE_DETALLE < len(filas_e)
            op_mas     = "1" if hay_mas else None
            op_resume  = "2" if hay_mas else "1"
            op_menu    = "3" if hay_mas else "2"

            if op_mas and texto_lower == op_mas:
                nueva_page = page + 1
                ctx = dict(context or {})
                ctx["page_estado"] = nueva_page
                ctx = _ctx_add_processed_id(ctx, msg_id)
                set_state_with_ts(numero, "TRANSPORTADOR_ESTADO_DETALLE", ctx)
                await enviar_texto(numero, formatear_manifiestos_estado(filas_e, estado, page=nueva_page, dict_pagos=dict_pagos))
                return JSONResponse({"status": "ok"})

            if texto_lower == op_resume:
                cedula = str((context or {}).get("cedula_tenedor") or "")
                year   = str((context or {}).get("year") or TRANSP_DEFAULT_YEAR)
                pagos  = (context or {}).get("pagos") or []
                texto_res, opcion_map_nuevo = formatear_resumen_tenedor(cedula, year, grupos, pagos)
                ctx = dict(context or {})
                ctx["opcion_map"] = opcion_map_nuevo
                ctx = _ctx_add_processed_id(ctx, msg_id)
                set_state_with_ts(numero, "TRANSPORTADOR_RESUMEN", ctx)
                await enviar_texto(numero, texto_res)
                return JSONResponse({"status": "ok"})

            if texto_lower == op_menu:
                reset_state(numero)
                ctx = _ctx_add_processed_id({}, msg_id)
                set_state_with_ts(numero, "START", ctx)
                await enviar_texto(numero, texto_inicio())
                return JSONResponse({"status": "ok"})

            ops = "/".join(filter(None, [op_mas, op_resume, op_menu]))
            await enviar_texto(numero, f"Opción no válida. Responde {ops} (o escribe *menu*).")
            ctx = _ctx_add_processed_id(dict(context or {}), msg_id)
            set_state_with_ts(numero, "TRANSPORTADOR_ESTADO_DETALLE", ctx)
            return JSONResponse({"status": "ok"})

        # -------------------------
        # EMPLOYEE_MENU
        # -------------------------
        if state == "EMPLOYEE_MENU":
            if texto_lower == "1":
                ctx = _ctx_only_processed_ids(context)
                ctx.update({"incluir_salario": False})
                ctx = _ctx_add_processed_id(ctx, msg_id)
                set_state_with_ts(numero, "EMPLOYEE_CERT_ASK_CEDULA", ctx)
                log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="EMPLOYEE_CERT_ASK_CEDULA", context={"incluir_salario": False})
                await enviar_texto(numero, texto_pedir_cedula())
                log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="pedir cedula sin salario", state="EMPLOYEE_CERT_ASK_CEDULA")
                return JSONResponse({"status": "ok"})

            if texto_lower == "2":
                ctx = _ctx_only_processed_ids(context)
                ctx.update({"incluir_salario": True})
                ctx = _ctx_add_processed_id(ctx, msg_id)
                set_state_with_ts(numero, "EMPLOYEE_CERT_CONFIRM_SALARIO", ctx)
                log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="EMPLOYEE_CERT_CONFIRM_SALARIO", context={"incluir_salario": True})
                await enviar_texto(
                    numero,
                    "⚠️ Este certificado incluye información salarial.\n\n"
                    "Responde:\n"
                    "1️⃣ ✅ Confirmo\n"
                    "2️⃣ ❌ Cancelar\n"
                )
                log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="confirmar certificado con salario", state="EMPLOYEE_CERT_CONFIRM_SALARIO")
                return JSONResponse({"status": "ok"})

            await enviar_texto(numero, "Opción no válida.\n\n" + texto_menu_empleado())
            log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="opcion invalida menu empleado", state="EMPLOYEE_MENU")
            ctx = _ctx_only_processed_ids(context)
            ctx = _ctx_add_processed_id(ctx, msg_id)
            set_state_with_ts(numero, "EMPLOYEE_MENU", ctx)
            return JSONResponse({"status": "ok"})

        # -------------------------
        # EMPLOYEE_CERT_CONFIRM_SALARIO
        # -------------------------
        if state == "EMPLOYEE_CERT_CONFIRM_SALARIO":
            incluir_salario = bool((context or {}).get("incluir_salario", True))

            if texto_lower == "1":
                ctx = _ctx_only_processed_ids(context)
                ctx.update({"incluir_salario": True})
                ctx = _ctx_add_processed_id(ctx, msg_id)
                set_state_with_ts(numero, "EMPLOYEE_CERT_ASK_CEDULA", ctx)
                log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="EMPLOYEE_CERT_ASK_CEDULA", context={"incluir_salario": True})
                await enviar_texto(numero, texto_pedir_cedula())
                log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="pedir cedula con salario", state="EMPLOYEE_CERT_ASK_CEDULA")
                return JSONResponse({"status": "ok"})

            if texto_lower == "2":
                ctx = _ctx_only_processed_ids(context)
                ctx = _ctx_add_processed_id(ctx, msg_id)
                set_state_with_ts(numero, "EMPLOYEE_MENU", ctx)
                log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="EMPLOYEE_MENU", context={})
                await enviar_texto(numero, "Listo. No se enviará certificado.\n\n" + texto_menu_empleado())
                log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="cancelar certificado con salario", state="EMPLOYEE_MENU")
                return JSONResponse({"status": "ok"})

            await enviar_texto(numero, "Opción no válida.\n\nResponde 1️⃣ Confirmo o 2️⃣ Cancelar.")
            log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="opcion invalida confirm salario", state="EMPLOYEE_CERT_CONFIRM_SALARIO")
            ctx = _ctx_only_processed_ids(context)
            ctx.update({"incluir_salario": incluir_salario})
            ctx = _ctx_add_processed_id(ctx, msg_id)
            set_state_with_ts(numero, "EMPLOYEE_CERT_CONFIRM_SALARIO", ctx)
            return JSONResponse({"status": "ok"})

        # -------------------------
        # EMPLOYEE_CERT_ASK_CEDULA
        # -------------------------
        if state == "EMPLOYEE_CERT_ASK_CEDULA":
            cedula = _limpiar_numero(texto)

            if not CEDULA_REGEX.match(cedula):
                await enviar_texto(numero, "La cédula debe contener solo números.\n\n" + texto_pedir_cedula())
                log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="cedula invalida", state="EMPLOYEE_CERT_ASK_CEDULA")
                ctx = _ctx_only_processed_ids(context)
                ctx.update({"incluir_salario": bool((context or {}).get("incluir_salario", False))})
                ctx = _ctx_add_processed_id(ctx, msg_id)
                set_state_with_ts(numero, "EMPLOYEE_CERT_ASK_CEDULA", ctx)
                return JSONResponse({"status": "ok"})

            incluir_salario = bool((context or {}).get("incluir_salario", False))
            ctx_proc = _ctx_only_processed_ids(context)
            ctx_proc.update({"cedula": cedula, "accion": "certificado_laboral", "incluir_salario": incluir_salario})
            ctx_proc = _ctx_add_processed_id(ctx_proc, msg_id)

            set_state_with_ts(numero, "EMPLOYEE_CERT_PROCESSING", ctx_proc)
            log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="EMPLOYEE_CERT_PROCESSING", context=ctx_proc)

            await enviar_texto(numero, "Procesando tu solicitud…")
            log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="procesando", state="EMPLOYEE_CERT_PROCESSING", context=ctx_proc)

            try:
                ok, mensaje, correo = generar_y_enviar_certificado_por_cedula(cedula, incluir_salario=incluir_salario)
            except Exception as e:
                log_whatsapp_event(phone=numero, direction="SYSTEM", event="ERROR", state="EMPLOYEE_CERT_PROCESSING", context=ctx_proc, meta={"error": str(e)})
                reset_state(numero)
                ctx = _ctx_add_processed_id({}, msg_id)
                set_state_with_ts(numero, "START", ctx)
                await enviar_texto(numero, "Ocurrió un error generando el certificado. Por favor intenta de nuevo.\n\n" + texto_inicio())
                log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="error certificado", state="START")
                return JSONResponse({"status": "ok"})

            if not ok:
                ctx = _ctx_add_processed_id(_ctx_only_processed_ids(context), msg_id)
                set_state_with_ts(numero, "EMPLOYEE_MENU", ctx)
                log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="EMPLOYEE_MENU", context={})
                await enviar_texto(numero, f"❗ {mensaje}\n\n" + texto_menu_empleado())
                log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text=f"fallo: {mensaje}", state="EMPLOYEE_MENU", context={"cedula": cedula})
                return JSONResponse({"status": "ok"})

            ctx = _ctx_add_processed_id(_ctx_only_processed_ids(context), msg_id)
            set_state_with_ts(numero, "EMPLOYEE_MENU", ctx)
            log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="EMPLOYEE_MENU", context={})

            await enviar_texto(
                numero,
                "✅ Solicitud completada.\n\n"
                f"Tu certificado laboral fue enviado al correo:\n*{correo}*\n\n"
                "Escribe *menu* para volver al inicio."
            )
            log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text=f"cert enviado a {correo}", state="EMPLOYEE_MENU", context={"cedula": cedula, "correo": correo})
            return JSONResponse({"status": "ok"})
        
        # -------------------------
        # CLIENTE_MENU
        # -------------------------
        if state == "CLIENTE_MENU":
            if texto_lower == "1":
                ctx = _ctx_add_processed_id(_ctx_only_processed_ids(context), msg_id)
                set_state_with_ts(numero, "CLIENTE_GUIA_ASK", ctx)
                log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="CLIENTE_GUIA_ASK", context={})
                await enviar_texto(numero, texto_pedir_guia())
                log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="pedir guia", state="CLIENTE_GUIA_ASK")
                return JSONResponse({"status": "ok"})

            if texto_lower == "2":
                reset_state(numero)
                ctx = _ctx_add_processed_id({}, msg_id)
                set_state_with_ts(numero, "START", ctx)
                log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="START", context={})
                await enviar_texto(numero, texto_inicio())
                log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="volver menu inicio", state="START")
                return JSONResponse({"status": "ok"})

            await enviar_texto(numero, "Opción no válida.\n\n" + texto_menu_cliente())
            log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="opcion invalida menu cliente", state="CLIENTE_MENU")
            ctx = _ctx_add_processed_id(_ctx_only_processed_ids(context), msg_id)
            set_state_with_ts(numero, "CLIENTE_MENU", ctx)
            return JSONResponse({"status": "ok"})

                
        # -------------------------
        # CLIENTE_GUIA_ASK
        # -------------------------
        if state == "CLIENTE_GUIA_ASK":
            guia = _limpiar_numero(texto).upper()

            if not GUIA_REGEX.match(guia):
                await enviar_texto(numero, "La guía solo puede contener letras, números o guiones.\n\n" + texto_pedir_guia())
                log_whatsapp_event(
                    phone=numero,
                    direction="OUT",
                    event="MESSAGE_SENT",
                    text="guia invalida",
                    state="CLIENTE_GUIA_ASK",
                )
                ctx = _ctx_add_processed_id(_ctx_only_processed_ids(context), msg_id)
                set_state_with_ts(numero, "CLIENTE_GUIA_ASK", ctx)
                return JSONResponse({"status": "ok"})

            # Conserva dedup + guarda guía
            ctxp = _ctx_only_processed_ids(context)
            ctxp.update({"guia": guia})
            ctxp = _ctx_add_processed_id(ctxp, msg_id)

            # Estado processing (útil para logs/seguimiento)
            set_state_with_ts(numero, "CLIENTE_PROCESSING", ctxp)

            # Respuesta inmediata (NO bloquea webhook)
            await enviar_texto(numero, "🔎 Consultando Siscore, un momento…")
            log_whatsapp_event(
                phone=numero,
                direction="OUT",
                event="MESSAGE_SENT",
                text="consultando siscore soap (bg)",
                state="CLIENTE_PROCESSING",
                context={"guia": guia},
            )

            # Deja el flujo listo para manejar 1/2 después
            ctx_post = _ctx_only_processed_ids(ctxp)
            ctx_post.update({"guia": guia, "last_payload_ok": None})
            ctx_post = _ctx_add_processed_id(ctx_post, msg_id)
            set_state_with_ts(numero, "CLIENTE_POST", ctx_post)

            # ✅ Ejecuta SOAP en background (evita timeouts/restarts en Render)
            background_tasks.add_task(_procesar_siscore_y_responder, numero, guia)

            return JSONResponse({"status": "ok"})


        # -------------------------
        # CLIENTE_POST
        # -------------------------
        if state == "CLIENTE_POST":
            if texto_lower == "1":
                ctx = _ctx_add_processed_id(_ctx_only_processed_ids(context), msg_id)
                set_state_with_ts(numero, "CLIENTE_GUIA_ASK", ctx)
                log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="CLIENTE_GUIA_ASK", context={})
                await enviar_texto(numero, texto_pedir_guia())
                log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="consultar otra guia", state="CLIENTE_GUIA_ASK")
                return JSONResponse({"status": "ok"})

            if texto_lower == "2":
                reset_state(numero)
                ctx = _ctx_add_processed_id({}, msg_id)
                set_state_with_ts(numero, "START", ctx)
                log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="START", context={})
                await enviar_texto(numero, texto_inicio())
                log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="volver menu inicio", state="START")
                return JSONResponse({"status": "ok"})

            await enviar_texto(
                numero,
                "Opción no válida.\n\n"
                "Responde:\n"
                "1️⃣ Consultar otra guía\n"
                "2️⃣ Volver al menú principal\n\n"
                "O escribe *menu*."
            )

            log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="opcion invalida cliente_post", state="CLIENTE_POST")

            # refresca CLIENTE_POST, conserva url/guia + dedup
            ctx = dict(context or {})
            ctx = _ctx_add_processed_id(ctx, msg_id)
            set_state_with_ts(numero, "CLIENTE_POST", ctx)
            return JSONResponse({"status": "ok"})

        # -------------------------
        # Fallback general
        # -------------------------
        reset_state(numero)
        ctx = _ctx_add_processed_id({}, msg_id)
        set_state_with_ts(numero, "START", ctx)
        log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="START", context={})
        await enviar_texto(numero, texto_inicio())
        log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="fallback menu inicio", state="START")
        return JSONResponse({"status": "ok"})

    except Exception as e:
        tb = traceback.format_exc()
        logger.error("❌ ERROR webhook general: %s", str(e))
        logger.error("Traceback webhook:\n%s", tb)
        return JSONResponse({"status": "ok"})

