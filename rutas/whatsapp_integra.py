import os
from fastapi import APIRouter, Request
from fastapi.responses import PlainTextResponse, JSONResponse
from typing import Any, Dict, Optional

from Funciones.chat_state_integra import get_state, set_state, reset_state
from Funciones.whatsapp_utils_integra import enviar_texto

VERIFY_TOKEN = os.getenv("WHATSAPP_VERIFY_TOKEN", "integra_verify")

ruta_whatsapp_integra = APIRouter(prefix="/whatsapp", tags=["whatsapp-integra"])


# --------- 1) Verificación webhook (GET) ----------
@ruta_whatsapp_integra.get("")
async def verify_webhook(request: Request):
    hub_challenge = request.query_params.get("hub.challenge")
    hub_verify_token = request.query_params.get("hub.verify_token")

    if hub_verify_token == VERIFY_TOKEN:
        return PlainTextResponse(str(hub_challenge))
    return PlainTextResponse("Error de verificación", status_code=403)


# --------- 2) Extraer mensaje entrante (POST) ----------
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
        print(f"❌ extraer_mensaje Integra error: {e}")
        return None


# --------- 3) Lógica del bot (muy básica, la ampliamos luego) ----------
def _es_menu(t: str) -> bool:
    return t in ["menu", "menú", "inicio", "reset", "reiniciar", "cancelar"]

@ruta_whatsapp_integra.post("")
async def webhook(request: Request):
    try:
        data = await request.json()
    except Exception as e:
        print(f"❌ No pude leer JSON: {e}")
        return JSONResponse({"status": "ok"})

    msg = extraer_mensaje(data)
    if not msg:
        return JSONResponse({"status": "ok"})

    phone = msg["from"]
    texto = (msg.get("text") or "").strip()
    t = texto.lower().strip()

    if _es_menu(t):
        reset_state(phone)
        set_state(phone, "START", {})
        await enviar_texto(
            phone,
            "Hola 👋 Soy el asistente de *Integra Soluciones Logísticas*.\n\n"
            "Escribe el número de lo que necesitas:\n"
            "1) Cotizar servicio\n"
            "2) Estado de solicitud / radicado\n"
            "3) Hablar con un asesor\n"
        )
        return JSONResponse({"status": "ok"})

    estado = get_state(phone)
    state = estado["state"]
    ctx = estado["context"]

    if state == "START":
        if t in ["1", "cotizar", "cotización", "cotizacion"]:
            set_state(phone, "ASK_CITY", {})
            await enviar_texto(phone, "Perfecto. ¿Desde qué ciudad se envía? (ej: Medellín)")
            return JSONResponse({"status": "ok"})

        if t in ["2", "estado", "seguimiento"]:
            set_state(phone, "ASK_TICKET", {})
            await enviar_texto(phone, "Escribe tu número de radicado / ticket (solo texto).")
            return JSONResponse({"status": "ok"})

        if t in ["3", "asesor", "humano", "agente"]:
            await enviar_texto(phone, "Listo ✅ Un asesor te contactará. Si quieres reiniciar escribe *menu*.")
            set_state(phone, "HANDOFF", ctx)
            return JSONResponse({"status": "ok"})

        await enviar_texto(
            phone,
            "No entendí 😅\n\n"
            "Responde con:\n"
            "1) Cotizar\n"
            "2) Estado\n"
            "3) Asesor\n\n"
            "O escribe *menu*."
        )
        return JSONResponse({"status": "ok"})

    if state == "ASK_CITY":
        ctx["city"] = texto
        set_state(phone, "ASK_DETAILS", ctx)
        await enviar_texto(phone, "¿Qué vas a enviar? (peso, #cajas, destino)")
        return JSONResponse({"status": "ok"})

    if state == "ASK_DETAILS":
        ctx["details"] = texto
        set_state(phone, "DONE", ctx)
        await enviar_texto(
            phone,
            "Gracias ✅\n\n"
            f"📍 Origen: {ctx.get('city')}\n"
            f"📦 Detalles: {ctx.get('details')}\n\n"
            "En breve un asesor te responde con la cotización.\n"
            "Si quieres reiniciar escribe *menu*."
        )
        return JSONResponse({"status": "ok"})

    if state == "ASK_TICKET":
        ctx["ticket"] = texto
        set_state(phone, "DONE", ctx)
        # Aquí luego conectas a tu DB / API de Integra para consultar estado real.
        await enviar_texto(
            phone,
            f"Recibido ✅ Ticket: *{ctx['ticket']}*\n\n"
            "Estoy consultando el estado. Si quieres reiniciar escribe *menu*."
        )
        return JSONResponse({"status": "ok"})

    # fallback
    reset_state(phone)
    set_state(phone, "START", {})
    await enviar_texto(phone, "Reinicié el chat. Escribe *menu* para ver opciones.")
    return JSONResponse({"status": "ok"})
