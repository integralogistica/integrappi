# rutas/whatsapp_integra.py
import os
import re
from datetime import datetime
from typing import Optional, Dict, Any

from fastapi import APIRouter, Request
from fastapi.responses import PlainTextResponse, JSONResponse

from Funciones.chat_state_integra import get_state, set_state, reset_state
from Funciones.whatsapp_utils_integra import enviar_texto
from Funciones.whatsapp_logs_integra import log_whatsapp_event
from Funciones.whatsapp_certificado_integra import generar_y_enviar_certificado_por_cedula

VERIFY_TOKEN = os.getenv("WHATSAPP_VERIFY_TOKEN", "integra_verify_2026")

ruta_whatsapp_integra = APIRouter(
    prefix="/whatsapp",
    tags=["whatsapp-integra"],
)

CEDULA_REGEX = re.compile(r"^\d{5,15}$")  # ajustable

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

# -------------------------
# Extraer mensaje (text / interactive)
# -------------------------
def extraer_mensaje(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
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

# -------------------------
# GET verificación
# -------------------------
@ruta_whatsapp_integra.get("/")
async def verify_webhook(request: Request):
    hub_challenge = request.query_params.get("hub.challenge")
    hub_verify_token = request.query_params.get("hub.verify_token")

    if hub_verify_token == VERIFY_TOKEN:
        return PlainTextResponse(str(hub_challenge))
    return PlainTextResponse("Error de verificación", status_code=403)

# -------------------------
# POST mensajes
# -------------------------
@ruta_whatsapp_integra.post("/")
async def webhook(request: Request):
    try:
        data = await request.json()
    except Exception as e:
        print(f"❌ No pude leer JSON: {e}")
        return JSONResponse({"status": "ok"})

    msg = extraer_mensaje(data)
    if not msg:
        return JSONResponse({"status": "ok"})

    numero = msg.get("from")
    texto = (msg.get("text") or "").strip()
    texto_lower = texto.lower().strip()

    # LOG entrada
    log_whatsapp_event(
        phone=numero,
        direction="IN",
        event="MESSAGE_RECEIVED",
        text=texto,
        meta={"msg_id": msg.get("id"), "type": msg.get("type")},
    )

    # Atajo menú
    if _es_menu(texto_lower):
        reset_state(numero)
        set_state(numero, "START", {})
        log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="START", context={})
        await enviar_texto(numero, texto_inicio())
        log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="(menu inicio)", state="START")
        return JSONResponse({"status": "ok"})

    # Estado actual
    estado = get_state(numero) or {"state": "START", "context": {}}
    state = estado.get("state") or "START"
    context = estado.get("context") or {}

    # -------------------------
    # START -> menú principal
    # -------------------------
    if state == "START":
        if texto_lower in ["1", "2", "3"]:
            if texto_lower == "1":
                set_state(numero, "TRANSPORTADOR_MENU", {})
                log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="TRANSPORTADOR_MENU", context={})
                await enviar_texto(numero, "🚚 Módulo transportador: *en construcción*.\n\nEscribe *menu* para volver.")
                log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="transportador en construcción", state="TRANSPORTADOR_MENU")
                return JSONResponse({"status": "ok"})

            if texto_lower == "2":
                set_state(numero, "EMPLOYEE_MENU", {})
                log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="EMPLOYEE_MENU", context={})
                await enviar_texto(numero, texto_menu_empleado())
                log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="menu empleado", state="EMPLOYEE_MENU")
                return JSONResponse({"status": "ok"})

            if texto_lower == "3":
                set_state(numero, "CLIENTE_MENU", {})
                log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="CLIENTE_MENU", context={})
                await enviar_texto(numero, "🧾 Módulo cliente: *en construcción*.\n\nEscribe *menu* para volver.")
                log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="cliente en construcción", state="CLIENTE_MENU")
                return JSONResponse({"status": "ok"})

        # si escribe cualquier cosa, le mostramos menú
        await enviar_texto(numero, texto_inicio())
        log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="menu inicio", state="START")
        return JSONResponse({"status": "ok"})

    # -------------------------
    # EMPLOYEE_MENU
    # -------------------------
    if state == "EMPLOYEE_MENU":
        if texto_lower == "1":
            set_state(numero, "EMPLOYEE_CERT_ASK_CEDULA", {"incluir_salario": False})
            log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="EMPLOYEE_CERT_ASK_CEDULA", context={"incluir_salario": False})
            await enviar_texto(numero, texto_pedir_cedula())
            log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="pedir cedula sin salario", state="EMPLOYEE_CERT_ASK_CEDULA")
            return JSONResponse({"status": "ok"})

        if texto_lower == "2":
            set_state(numero, "EMPLOYEE_CERT_CONFIRM_SALARIO", {"incluir_salario": True})
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
        return JSONResponse({"status": "ok"})

    if state == "EMPLOYEE_CERT_CONFIRM_SALARIO":
        if texto_lower == "1":
            # confirmado
            set_state(numero, "EMPLOYEE_CERT_ASK_CEDULA", {"incluir_salario": True})
            log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="EMPLOYEE_CERT_ASK_CEDULA", context={"incluir_salario": True})
            await enviar_texto(numero, texto_pedir_cedula())
            log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="pedir cedula con salario", state="EMPLOYEE_CERT_ASK_CEDULA")
            return JSONResponse({"status": "ok"})

        if texto_lower == "2":
            set_state(numero, "EMPLOYEE_MENU", {})
            log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="EMPLOYEE_MENU", context={})
            await enviar_texto(numero, "Listo. No se enviará certificado.\n\n" + texto_menu_empleado())
            log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="cancelar certificado con salario", state="EMPLOYEE_MENU")
            return JSONResponse({"status": "ok"})

        await enviar_texto(numero, "Opción no válida.\n\nResponde 1️⃣ Confirmo o 2️⃣ Cancelar.")
        log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="opcion invalida confirm salario", state="EMPLOYEE_CERT_CONFIRM_SALARIO")
        return JSONResponse({"status": "ok"})

    # -------------------------
    # EMPLOYEE_CERT_ASK_CEDULA
    # -------------------------
    if state == "EMPLOYEE_CERT_ASK_CEDULA":
        cedula = texto.replace(".", "").replace(" ", "").strip()

        if not CEDULA_REGEX.match(cedula):
            await enviar_texto(numero, "La cédula debe contener solo números.\n\n" + texto_pedir_cedula())
            log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="cedula invalida", state="EMPLOYEE_CERT_ASK_CEDULA")
            return JSONResponse({"status": "ok"})

        # Guardar contexto
        incluir_salario = bool(context.get("incluir_salario", False))
        context = {"cedula": cedula, "accion": "certificado_laboral", "incluir_salario": incluir_salario}
        set_state(numero, "EMPLOYEE_CERT_PROCESSING", context)
        log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="EMPLOYEE_CERT_PROCESSING", context=context)

        await enviar_texto(numero, "Procesando tu solicitud…")
        log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="procesando", state="EMPLOYEE_CERT_PROCESSING", context=context)

        try:
            ok, mensaje, correo = generar_y_enviar_certificado_por_cedula(cedula, incluir_salario=incluir_salario)
        except Exception as e:
            log_whatsapp_event(phone=numero, direction="SYSTEM", event="ERROR", state="EMPLOYEE_CERT_PROCESSING", context=context, meta={"error": str(e)})
            reset_state(numero)
            set_state(numero, "START", {})
            await enviar_texto(numero, "Ocurrió un error generando el certificado. Por favor intenta de nuevo.\n\n" + texto_inicio())
            log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="error certificado", state="START")
            return JSONResponse({"status": "ok"})

        if not ok:
            # vuelve al menú empleado
            set_state(numero, "EMPLOYEE_MENU", {})
            log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="EMPLOYEE_MENU", context={})
            await enviar_texto(numero, f"❗ {mensaje}\n\n" + texto_menu_empleado())
            log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text=f"fallo: {mensaje}", state="EMPLOYEE_MENU", context={"cedula": cedula})
            return JSONResponse({"status": "ok"})

        # éxito
        set_state(numero, "EMPLOYEE_MENU", {})
        log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="EMPLOYEE_MENU", context={})

        await enviar_texto(
            numero,
            "✅ Solicitud completada.\n\n"
            f"Tu certificado laboral fue enviado al correo:\n*{correo}*\n\n"
            "Escribe *menu* para volver al inicio."
        )
        log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text=f"cert enviado a {correo}", state="EMPLOYEE_MENU", context={"cedula": cedula, "correo": correo})
        return JSONResponse({"status": "ok"})

    # Fallback general
    reset_state(numero)
    set_state(numero, "START", {})
    log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="START", context={})
    await enviar_texto(numero, texto_inicio())
    log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="fallback menu inicio", state="START")
    return JSONResponse({"status": "ok"})



