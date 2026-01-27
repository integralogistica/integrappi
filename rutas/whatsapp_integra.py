# rutas/whatsapp_integra.py
import os
import re
from typing import Optional, Dict, Any
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, Request
from fastapi.responses import PlainTextResponse, JSONResponse

from Funciones.chat_state_integra import get_state, set_state, reset_state
from Funciones.whatsapp_utils_integra import enviar_texto
from Funciones.whatsapp_logs_integra import log_whatsapp_event
from Funciones.whatsapp_certificado_integra import generar_y_enviar_certificado_por_cedula

VERIFY_TOKEN = os.getenv("WHATSAPP_VERIFY_TOKEN", "integra_verify_2026")

# Regex
CEDULA_REGEX = re.compile(r"^\d{5,15}$")
GUIA_REGEX = re.compile(r"^\d{5,20}$")  # ajusta si tus guías son más largas

# URL base Siscore
SISCORE_PUBLIC_URL = "https://integra.appsiscore.com/app/app-cliente/cons_publica.php"

# TTL estado
STATE_TTL_MINUTES = 60

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
        "Escribe el número de la *guía* (solo números).\n"
        "Ejemplo: 801203424"
    )


def texto_post_guia(url: str) -> str:
    return (
        "🔎 Aquí puedes consultar tu guía:\n"
        "(Ten presente que si la guía no existe te aparecerá un mensaje indicando: "
        "*No se encontraron resultados*.)\n\n"
        f"{url}\n\n"
        "¿Qué deseas hacer ahora?\n"
        "1️⃣ Consultar otra guía\n"
        "2️⃣ Volver al menú principal"
    )


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


def _url_guia(guia: str) -> str:
    return f"{SISCORE_PUBLIC_URL}?GUIA={guia}"


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
    # ✅ Firma correcta: set_state(phone, state, context, updated_at=?)
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


def _ctx_set_last_msg_id(context: Dict[str, Any], msg_id: Optional[str]) -> Dict[str, Any]:
    ctx = dict(context or {})
    if msg_id:
        ctx["last_msg_id"] = msg_id
    return ctx


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
async def webhook_no_slash(request: Request):
    return await webhook(request)


@ruta_whatsapp_integra.post("/")
async def webhook(request: Request):
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

        # -------------------------
        # DEDUP: si Meta reintenta el MISMO msg_id, no respondas de nuevo
        # -------------------------
        last_id = (estado.get("context") or {}).get("last_msg_id")
        if msg_id and last_id == msg_id:
            return JSONResponse({"status": "ok"})

        state = estado.get("state") or "START"
        context = estado.get("context") or {}

        # Marca este msg_id como procesado ANTES de seguir (idempotencia)
        context = _ctx_set_last_msg_id(context, msg_id)
        set_state_with_ts(numero, state, context)

        # TTL
        if _estado_expirado(estado.get("updated_at")):
            reset_state(numero)
            set_state_with_ts(numero, "START", _ctx_set_last_msg_id({}, msg_id))
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
            reset_state(numero)
            set_state_with_ts(numero, "START", _ctx_set_last_msg_id({}, msg_id))
            log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="START", context={})
            await enviar_texto(numero, texto_inicio())
            log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="(menu inicio)", state="START")
            return JSONResponse({"status": "ok"})

        # -------------------------
        # START
        # -------------------------
        if state == "START":
            if texto_lower in ["1", "2", "3"]:
                if texto_lower == "1":
                    set_state_with_ts(numero, "TRANSPORTADOR_MENU", _ctx_set_last_msg_id({}, msg_id))
                    log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="TRANSPORTADOR_MENU", context={})
                    await enviar_texto(numero, "🚚 Módulo transportador: *en construcción*.\n\nEscribe *menu* para volver.")
                    log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="transportador en construcción", state="TRANSPORTADOR_MENU")
                    return JSONResponse({"status": "ok"})

                if texto_lower == "2":
                    set_state_with_ts(numero, "EMPLOYEE_MENU", _ctx_set_last_msg_id({}, msg_id))
                    log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="EMPLOYEE_MENU", context={})
                    await enviar_texto(numero, texto_menu_empleado())
                    log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="menu empleado", state="EMPLOYEE_MENU")
                    return JSONResponse({"status": "ok"})

                if texto_lower == "3":
                    set_state_with_ts(numero, "CLIENTE_MENU", _ctx_set_last_msg_id({}, msg_id))
                    log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="CLIENTE_MENU", context={})
                    await enviar_texto(numero, texto_menu_cliente())
                    log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="menu cliente", state="CLIENTE_MENU")
                    return JSONResponse({"status": "ok"})

            await enviar_texto(numero, texto_inicio())
            log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="menu inicio", state="START")
            set_state_with_ts(numero, "START", _ctx_set_last_msg_id({}, msg_id))
            return JSONResponse({"status": "ok"})

        # -------------------------
        # EMPLOYEE_MENU
        # -------------------------
        if state == "EMPLOYEE_MENU":
            if texto_lower == "1":
                set_state_with_ts(numero, "EMPLOYEE_CERT_ASK_CEDULA", _ctx_set_last_msg_id({"incluir_salario": False}, msg_id))
                log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="EMPLOYEE_CERT_ASK_CEDULA", context={"incluir_salario": False})
                await enviar_texto(numero, texto_pedir_cedula())
                log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="pedir cedula sin salario", state="EMPLOYEE_CERT_ASK_CEDULA")
                return JSONResponse({"status": "ok"})

            if texto_lower == "2":
                set_state_with_ts(numero, "EMPLOYEE_CERT_CONFIRM_SALARIO", _ctx_set_last_msg_id({"incluir_salario": True}, msg_id))
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
            set_state_with_ts(numero, "EMPLOYEE_MENU", _ctx_set_last_msg_id({}, msg_id))
            return JSONResponse({"status": "ok"})

        # -------------------------
        # EMPLOYEE_CERT_CONFIRM_SALARIO
        # -------------------------
        if state == "EMPLOYEE_CERT_CONFIRM_SALARIO":
            if texto_lower == "1":
                set_state_with_ts(numero, "EMPLOYEE_CERT_ASK_CEDULA", _ctx_set_last_msg_id({"incluir_salario": True}, msg_id))
                log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="EMPLOYEE_CERT_ASK_CEDULA", context={"incluir_salario": True})
                await enviar_texto(numero, texto_pedir_cedula())
                log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="pedir cedula con salario", state="EMPLOYEE_CERT_ASK_CEDULA")
                return JSONResponse({"status": "ok"})

            if texto_lower == "2":
                set_state_with_ts(numero, "EMPLOYEE_MENU", _ctx_set_last_msg_id({}, msg_id))
                log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="EMPLOYEE_MENU", context={})
                await enviar_texto(numero, "Listo. No se enviará certificado.\n\n" + texto_menu_empleado())
                log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="cancelar certificado con salario", state="EMPLOYEE_MENU")
                return JSONResponse({"status": "ok"})

            await enviar_texto(numero, "Opción no válida.\n\nResponde 1️⃣ Confirmo o 2️⃣ Cancelar.")
            log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="opcion invalida confirm salario", state="EMPLOYEE_CERT_CONFIRM_SALARIO")
            set_state_with_ts(numero, "EMPLOYEE_CERT_CONFIRM_SALARIO", _ctx_set_last_msg_id({"incluir_salario": True}, msg_id))
            return JSONResponse({"status": "ok"})

        # -------------------------
        # EMPLOYEE_CERT_ASK_CEDULA
        # -------------------------
        if state == "EMPLOYEE_CERT_ASK_CEDULA":
            cedula = _limpiar_numero(texto)

            if not CEDULA_REGEX.match(cedula):
                await enviar_texto(numero, "La cédula debe contener solo números.\n\n" + texto_pedir_cedula())
                log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="cedula invalida", state="EMPLOYEE_CERT_ASK_CEDULA")
                set_state_with_ts(numero, "EMPLOYEE_CERT_ASK_CEDULA", _ctx_set_last_msg_id(context if isinstance(context, dict) else {}, msg_id))
                return JSONResponse({"status": "ok"})

            incluir_salario = bool((context or {}).get("incluir_salario", False))
            context_proc = _ctx_set_last_msg_id(
                {"cedula": cedula, "accion": "certificado_laboral", "incluir_salario": incluir_salario},
                msg_id,
            )
            set_state_with_ts(numero, "EMPLOYEE_CERT_PROCESSING", context_proc)
            log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="EMPLOYEE_CERT_PROCESSING", context=context_proc)

            await enviar_texto(numero, "Procesando tu solicitud…")
            log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="procesando", state="EMPLOYEE_CERT_PROCESSING", context=context_proc)

            try:
                ok, mensaje, correo = generar_y_enviar_certificado_por_cedula(cedula, incluir_salario=incluir_salario)
            except Exception as e:
                log_whatsapp_event(phone=numero, direction="SYSTEM", event="ERROR", state="EMPLOYEE_CERT_PROCESSING", context=context_proc, meta={"error": str(e)})
                reset_state(numero)
                set_state_with_ts(numero, "START", _ctx_set_last_msg_id({}, msg_id))
                await enviar_texto(numero, "Ocurrió un error generando el certificado. Por favor intenta de nuevo.\n\n" + texto_inicio())
                log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="error certificado", state="START")
                return JSONResponse({"status": "ok"})

            if not ok:
                set_state_with_ts(numero, "EMPLOYEE_MENU", _ctx_set_last_msg_id({}, msg_id))
                log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="EMPLOYEE_MENU", context={})
                await enviar_texto(numero, f"❗ {mensaje}\n\n" + texto_menu_empleado())
                log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text=f"fallo: {mensaje}", state="EMPLOYEE_MENU", context={"cedula": cedula})
                return JSONResponse({"status": "ok"})

            set_state_with_ts(numero, "EMPLOYEE_MENU", _ctx_set_last_msg_id({}, msg_id))
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
                set_state_with_ts(numero, "CLIENTE_GUIA_ASK", _ctx_set_last_msg_id({}, msg_id))
                log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="CLIENTE_GUIA_ASK", context={})
                await enviar_texto(numero, texto_pedir_guia())
                log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="pedir guia", state="CLIENTE_GUIA_ASK")
                return JSONResponse({"status": "ok"})

            if texto_lower == "2":
                reset_state(numero)
                set_state_with_ts(numero, "START", _ctx_set_last_msg_id({}, msg_id))
                log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="START", context={})
                await enviar_texto(numero, texto_inicio())
                log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="volver menu inicio", state="START")
                return JSONResponse({"status": "ok"})

            await enviar_texto(numero, "Opción no válida.\n\n" + texto_menu_cliente())
            log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="opcion invalida menu cliente", state="CLIENTE_MENU")
            set_state_with_ts(numero, "CLIENTE_MENU", _ctx_set_last_msg_id({}, msg_id))
            return JSONResponse({"status": "ok"})

        # -------------------------
        # CLIENTE_GUIA_ASK
        # -------------------------
        if state == "CLIENTE_GUIA_ASK":
            guia = _limpiar_numero(texto)

            if not GUIA_REGEX.match(guia):
                await enviar_texto(numero, "La guía debe contener solo números.\n\n" + texto_pedir_guia())
                log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="guia invalida", state="CLIENTE_GUIA_ASK")
                set_state_with_ts(numero, "CLIENTE_GUIA_ASK", _ctx_set_last_msg_id({}, msg_id))
                return JSONResponse({"status": "ok"})

            url = _url_guia(guia)
            set_state_with_ts(numero, "CLIENTE_POST", _ctx_set_last_msg_id({"guia": guia, "url": url}, msg_id))
            log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="CLIENTE_POST", context={"guia": guia, "url": url})

            await enviar_texto(numero, texto_post_guia(url))
            log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="enviar link guia (sin validar)", state="CLIENTE_POST", context={"guia": guia})
            return JSONResponse({"status": "ok"})

        # -------------------------
        # CLIENTE_POST
        # -------------------------
        if state == "CLIENTE_POST":
            if texto_lower == "1":
                set_state_with_ts(numero, "CLIENTE_GUIA_ASK", _ctx_set_last_msg_id({}, msg_id))
                log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="CLIENTE_GUIA_ASK", context={})
                await enviar_texto(numero, texto_pedir_guia())
                log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="consultar otra guia", state="CLIENTE_GUIA_ASK")
                return JSONResponse({"status": "ok"})

            if texto_lower == "2":
                reset_state(numero)
                set_state_with_ts(numero, "START", _ctx_set_last_msg_id({}, msg_id))
                log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="START", context={})
                await enviar_texto(numero, texto_inicio())
                log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="volver menu inicio", state="START")
                return JSONResponse({"status": "ok"})

            url = (context or {}).get("url") or ""
            await enviar_texto(
                numero,
                "Opción no válida.\n\n"
                + (texto_post_guia(url) if url else "Responde 1️⃣ o 2️⃣. Escribe *menu* para volver.")
            )
            log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="opcion invalida cliente_post", state="CLIENTE_POST")
            set_state_with_ts(numero, "CLIENTE_POST", _ctx_set_last_msg_id(context if isinstance(context, dict) else {}, msg_id))
            return JSONResponse({"status": "ok"})

        # -------------------------
        # Fallback general
        # -------------------------
        reset_state(numero)
        set_state_with_ts(numero, "START", _ctx_set_last_msg_id({}, msg_id))
        log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="START", context={})
        await enviar_texto(numero, texto_inicio())
        log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="fallback menu inicio", state="START")
        return JSONResponse({"status": "ok"})

    except Exception as e:
        print(f"❌ ERROR webhook general: {e}")
        # Importante: responder 200 para que Meta NO reintente
        return JSONResponse({"status": "ok"})
