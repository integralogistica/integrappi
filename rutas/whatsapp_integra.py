# rutas/whatsapp_integra.py
import os
import re
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, Request
from fastapi.responses import PlainTextResponse, JSONResponse

from Funciones.chat_state_integra import get_state, set_state, reset_state
from Funciones.whatsapp_utils_integra import enviar_texto
from Funciones.whatsapp_logs_integra import log_whatsapp_event
from Funciones.whatsapp_certificado_integra import generar_y_enviar_certificado_por_cedula


# ✅ Vulcano (consulta por cédula del tenedor)
# Ajusta el import si tu ruta es diferente (por ejemplo: from rutas.vulcano import consultar_por_tenedor)
from rutas.vulcano import consultar_por_tenedor

VERIFY_TOKEN = os.getenv("WHATSAPP_VERIFY_TOKEN", "integra_verify_2026")

# Regex
CEDULA_REGEX = re.compile(r"^\d{5,15}$")
GUIA_REGEX = re.compile(r"^\d{5,20}$")  # ajusta si tus guías son más largas
YEAR_REGEX = re.compile(r"^(19|20)\d{2}$")

# TTL estado
STATE_TTL_MINUTES = 60

# Dedup: cuántos msg_id guardamos por usuario
PROCESSED_IDS_MAX = 30

# Defaults Transportador (para no pedir 1000 de una)
TRANSP_DEFAULT_YEAR = "2024"
TRANSP_DEFAULT_PAGO_SALDO = "No Aplicado"
TRANSP_DEFAULT_PAGE_SIZE = 200  # recomendado
TRANSP_DEFAULT_PAGE = 1

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
        "🔎 *Consultar manifiestos (Vulcano)*\n\n"
        "Escribe la *cédula del tenedor* (solo números).\n"
        "Ejemplo: 1012455147"
    )


def texto_post_transportador(resumen: str) -> str:
    return (
        f"{resumen}\n\n"
        "¿Qué deseas hacer ahora?\n"
        "1️⃣ Ver más resultados\n"
        "2️⃣ Cambiar año\n"
        "3️⃣ Consultar otra cédula\n"
        "4️⃣ Volver al menú principal"
    )


def _texto_pedir_year_transportador() -> str:
    return (
        "📅 Escribe el *año* (4 dígitos).\n"
        "Ejemplo: 2024\n\n"
        "Para volver escribe *menu*."
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


def _vulcano_consultar_compat(
    cedula_tenedor: str,
    year: str,
    pago_saldo: str,
    page_size: int,
    page: int,
) -> List[Dict[str, Any]]:
    """
    Compatibilidad: si tu consultar_por_tenedor NO acepta page/page_size, lo llama sin ellos.
    """
    try:
        return consultar_por_tenedor(
            cedula_tenedor=cedula_tenedor,
            year=year,
            pago_saldo=pago_saldo,
            page_size=page_size,
            page=page,
        )
    except TypeError:
        # versión antigua sin paginación
        return consultar_por_tenedor(
            cedula_tenedor=cedula_tenedor,
            year=year,
            pago_saldo=pago_saldo,
        )


def _resumen_transportador(
    cedula: str,
    year: str,
    pago_saldo: str,
    filas: List[Dict[str, Any]],
    page: int,
    page_size: int,
) -> str:
    total = len(filas)
    manifiestos = [f.get("Manifiesto") for f in filas if isinstance(f, dict) and f.get("Manifiesto")]
    fechas = [f.get("Fecha") for f in filas if isinstance(f, dict) and f.get("Fecha")]

    top_m = manifiestos[:10]
    top_f = fechas[:10]

    bloque_m = "\n".join([f"• {m}" for m in top_m]) if top_m else "• (sin manifiestos)"
    bloque_f = "\n".join([f"• {x}" for x in top_f]) if top_f else "• (sin fechas)"

    return (
        "✅ *Consulta Vulcano*\n\n"
        f"👤 Tenedor: *{cedula}*\n"
        f"📅 Año: *{year}*\n"
        f"💳 Pago saldo: *{pago_saldo}*\n"
        f"📄 Página: *{page}* (tamaño {page_size})\n"
        f"🔢 Registros en esta página: *{total}*\n\n"
        f"📦 *Manifiestos (primeros {len(top_m)}):*\n{bloque_m}\n\n"
        f"🗓️ *Fechas (primeras {len(top_f)}):*\n{bloque_f}"
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
                    set_state_with_ts(numero, "TRANSPORTADOR_MENU", base_ctx)
                    log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="TRANSPORTADOR_MENU", context={})
                    await enviar_texto(numero, texto_menu_transportador())
                    log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="menu transportador", state="TRANSPORTADOR_MENU")
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
                ctx = _ctx_add_processed_id(_ctx_only_processed_ids(context), msg_id)
                set_state_with_ts(numero, "TRANSPORTADOR_ASK_CEDULA", ctx)
                log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="TRANSPORTADOR_ASK_CEDULA", context={})
                await enviar_texto(numero, texto_pedir_cedula_tenedor())
                log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="pedir cedula tenedor", state="TRANSPORTADOR_ASK_CEDULA")
                return JSONResponse({"status": "ok"})

            if texto_lower == "2":
                reset_state(numero)
                ctx = _ctx_add_processed_id({}, msg_id)
                set_state_with_ts(numero, "START", ctx)
                log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="START", context={})
                await enviar_texto(numero, texto_inicio())
                log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="volver menu inicio", state="START")
                return JSONResponse({"status": "ok"})

            await enviar_texto(numero, "Opción no válida.\n\n" + texto_menu_transportador())
            ctx = _ctx_add_processed_id(_ctx_only_processed_ids(context), msg_id)
            set_state_with_ts(numero, "TRANSPORTADOR_MENU", ctx)
            return JSONResponse({"status": "ok"})

        # -------------------------
        # TRANSPORTADOR_ASK_CEDULA
        # -------------------------
        if state == "TRANSPORTADOR_ASK_CEDULA":
            cedula = _limpiar_numero(texto)

            if not CEDULA_REGEX.match(cedula):
                await enviar_texto(numero, "La cédula debe contener solo números.\n\n" + texto_pedir_cedula_tenedor())
                log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="cedula tenedor invalida", state="TRANSPORTADOR_ASK_CEDULA")
                ctx = _ctx_add_processed_id(_ctx_only_processed_ids(context), msg_id)
                set_state_with_ts(numero, "TRANSPORTADOR_ASK_CEDULA", ctx)
                return JSONResponse({"status": "ok"})

            year = str((context or {}).get("year") or TRANSP_DEFAULT_YEAR)
            pago_saldo = str((context or {}).get("pago_saldo") or TRANSP_DEFAULT_PAGO_SALDO)
            page_size = _safe_int((context or {}).get("page_size"), TRANSP_DEFAULT_PAGE_SIZE)
            page = TRANSP_DEFAULT_PAGE

            ctxp = _ctx_only_processed_ids(context)
            ctxp.update(
                {
                    "cedula_tenedor": cedula,
                    "year": year,
                    "pago_saldo": pago_saldo,
                    "page_size": page_size,
                    "page": page,
                }
            )
            ctxp = _ctx_add_processed_id(ctxp, msg_id)

            set_state_with_ts(numero, "TRANSPORTADOR_PROCESSING", ctxp)
            log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="TRANSPORTADOR_PROCESSING", context=ctxp)

            await enviar_texto(numero, "🔎 Consultando Vulcano, un momento…")
            log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="consultando vulcano", state="TRANSPORTADOR_PROCESSING")

            try:
                filas = _vulcano_consultar_compat(
                    cedula_tenedor=cedula,
                    year=year,
                    pago_saldo=pago_saldo,
                    page_size=page_size,
                    page=page,
                )
            except Exception as e:
                log_whatsapp_event(phone=numero, direction="SYSTEM", event="ERROR", state="TRANSPORTADOR_PROCESSING", context=ctxp, meta={"error": str(e)})
                ctx_back = _ctx_add_processed_id(_ctx_only_processed_ids(context), msg_id)
                set_state_with_ts(numero, "TRANSPORTADOR_MENU", ctx_back)
                await enviar_texto(
                    numero,
                    "❗ No pude consultar en este momento (timeout o error del servicio).\n"
                    "Intenta de nuevo en unos minutos.\n\n"
                    + texto_menu_transportador()
                )
                return JSONResponse({"status": "ok"})

            resumen = _resumen_transportador(cedula, year, pago_saldo, filas, page=page, page_size=page_size)

            ctx_post = dict(ctxp)
            ctx_post.update(
                {
                    "last_count": len(filas),
                }
            )
            set_state_with_ts(numero, "TRANSPORTADOR_POST", ctx_post)
            log_whatsapp_event(phone=numero, direction="SYSTEM", event="STATE_CHANGED", state="TRANSPORTADOR_POST", context={"cedula_tenedor": cedula, "year": year})

            await enviar_texto(numero, texto_post_transportador(resumen))
            log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="respuesta vulcano", state="TRANSPORTADOR_POST")
            return JSONResponse({"status": "ok"})

        # -------------------------
        # TRANSPORTADOR_ASK_YEAR
        # -------------------------
        if state == "TRANSPORTADOR_ASK_YEAR":
            year = _limpiar_numero(texto)

            if not YEAR_REGEX.match(year):
                await enviar_texto(numero, "Año inválido.\n\n" + _texto_pedir_year_transportador())
                log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="year invalido", state="TRANSPORTADOR_ASK_YEAR")
                ctx = _ctx_add_processed_id(_ctx_only_processed_ids(context), msg_id)
                set_state_with_ts(numero, "TRANSPORTADOR_ASK_YEAR", ctx)
                return JSONResponse({"status": "ok"})

            # Conserva dedup y vuelve a pedir cédula (más claro para el usuario)
            ctx = _ctx_only_processed_ids(context)
            ctx.update({"year": year})
            ctx = _ctx_add_processed_id(ctx, msg_id)
            set_state_with_ts(numero, "TRANSPORTADOR_ASK_CEDULA", ctx)

            await enviar_texto(numero, f"Listo ✅ Año configurado en *{year}*.\n\n" + texto_pedir_cedula_tenedor())
            log_whatsapp_event(phone=numero, direction="OUT", event="MESSAGE_SENT", text="year actualizado", state="TRANSPORTADOR_ASK_CEDULA")
            return JSONResponse({"status": "ok"})

        # -------------------------
        # TRANSPORTADOR_POST
        # -------------------------
        if state == "TRANSPORTADOR_POST":
            if texto_lower == "1":
                cedula = str((context or {}).get("cedula_tenedor") or "")
                year = str((context or {}).get("year") or TRANSP_DEFAULT_YEAR)
                pago_saldo = str((context or {}).get("pago_saldo") or TRANSP_DEFAULT_PAGO_SALDO)
                page_size = _safe_int((context or {}).get("page_size"), TRANSP_DEFAULT_PAGE_SIZE)
                page = _safe_int((context or {}).get("page"), TRANSP_DEFAULT_PAGE) + 1

                if not cedula:
                    ctx = _ctx_add_processed_id(_ctx_only_processed_ids(context), msg_id)
                    set_state_with_ts(numero, "TRANSPORTADOR_MENU", ctx)
                    await enviar_texto(numero, "No tengo la cédula en memoria.\n\n" + texto_menu_transportador())
                    return JSONResponse({"status": "ok"})

                set_state_with_ts(numero, "TRANSPORTADOR_PROCESSING", {**(context or {}), "page": page})
                await enviar_texto(numero, f"🔎 Consultando página {page}…")

                try:
                    filas = _vulcano_consultar_compat(
                        cedula_tenedor=cedula,
                        year=year,
                        pago_saldo=pago_saldo,
                        page_size=page_size,
                        page=page,
                    )
                except Exception as e:
                    log_whatsapp_event(phone=numero, direction="SYSTEM", event="ERROR", state="TRANSPORTADOR_PROCESSING", context=context, meta={"error": str(e)})
                    set_state_with_ts(numero, "TRANSPORTADOR_POST", context)
                    await enviar_texto(numero, "❗ No pude traer más resultados ahora.\n\nResponde 2️⃣, 3️⃣ o 4️⃣ (o escribe *menu*).")
                    return JSONResponse({"status": "ok"})

                if not filas:
                    set_state_with_ts(numero, "TRANSPORTADOR_POST", {**(context or {}), "page": page})
                    await enviar_texto(
                        numero,
                        "No hay más resultados.\n\n"
                        "2️⃣ Cambiar año\n"
                        "3️⃣ Consultar otra cédula\n"
                        "4️⃣ Volver al menú principal"
                    )
                    return JSONResponse({"status": "ok"})

                resumen = _resumen_transportador(cedula, year, pago_saldo, filas, page=page, page_size=page_size)
                set_state_with_ts(numero, "TRANSPORTADOR_POST", {**(context or {}), "page": page})
                await enviar_texto(numero, texto_post_transportador(resumen))
                return JSONResponse({"status": "ok"})

            if texto_lower == "2":
                ctx = _ctx_add_processed_id(_ctx_only_processed_ids(context), msg_id)
                set_state_with_ts(numero, "TRANSPORTADOR_ASK_YEAR", ctx)
                await enviar_texto(numero, _texto_pedir_year_transportador())
                return JSONResponse({"status": "ok"})

            if texto_lower == "3":
                ctx = _ctx_add_processed_id(_ctx_only_processed_ids(context), msg_id)
                set_state_with_ts(numero, "TRANSPORTADOR_ASK_CEDULA", ctx)
                await enviar_texto(numero, texto_pedir_cedula_tenedor())
                return JSONResponse({"status": "ok"})

            if texto_lower == "4":
                reset_state(numero)
                ctx = _ctx_add_processed_id({}, msg_id)
                set_state_with_ts(numero, "START", ctx)
                await enviar_texto(numero, texto_inicio())
                return JSONResponse({"status": "ok"})

            await enviar_texto(numero, "Opción no válida.\n\nResponde 1️⃣, 2️⃣, 3️⃣ o 4️⃣. (o escribe *menu*)")
            ctx = _ctx_add_processed_id(dict(context or {}), msg_id)
            set_state_with_ts(numero, "TRANSPORTADOR_POST", ctx)
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
            guia = _limpiar_numero(texto)

            if not GUIA_REGEX.match(guia):
                await enviar_texto(numero, "La guía debe contener solo números.\n\n" + texto_pedir_guia())
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

            # Guarda guía en contexto (sin estado intermedio, porque ya no consultamos SOAP)
            ctx = _ctx_only_processed_ids(context)
            ctx.update({"guia": guia})
            ctx = _ctx_add_processed_id(ctx, msg_id)

            # Construye URL pública de Siscore
            url = f"https://integra.appsiscore.com/app/app-cliente/cons_publica.php?GUIA={guia}"

            texto_respuesta = (
                "🔎 *Consulta de guía (Siscore)*\n\n"
                f"📦 Guía: *{guia}*\n"
                f"🔗 Abre este enlace para ver la trazabilidad:\n{url}\n\n"
                "⚠️ *Importante:* Si al abrir el enlace la página aparece vacía o sin información, "
                "es porque la guía no existe.\n\n"
                "¿Qué deseas hacer ahora?\n"
                "1️⃣ Consultar otra guía\n"
                "2️⃣ Volver al menú principal"
            )

            # Pasa a CLIENTE_POST para manejar 1/2 como ya lo tienes
            ctx_post = _ctx_only_processed_ids(ctx)
            ctx_post.update({"guia": guia, "tracking_url": url})
            ctx_post = _ctx_add_processed_id(ctx_post, msg_id)
            set_state_with_ts(numero, "CLIENTE_POST", ctx_post)

            await enviar_texto(numero, texto_respuesta)
            log_whatsapp_event(
                phone=numero,
                direction="OUT",
                event="MESSAGE_SENT",
                text="link siscore enviado",
                state="CLIENTE_POST",
                context={"guia": guia, "tracking_url": url},
            )
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
        print(f"❌ ERROR webhook general: {e}")
        # Importante: responder 200 para que Meta NO reintente
        return JSONResponse({"status": "ok"})
