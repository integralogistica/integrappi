# Funciones/siscore_ws_format.py
from typing import Dict, Any, List, Optional
from datetime import datetime


def _txt(v: Any, fallback: str = "-") -> str:
    s = str(v or "").strip()
    return s if s else fallback


def _fmt_fecha_ddmmyyyy(v: Any) -> str:
    """
    Convierte a dd-mm-aaaa.
    Soporta:
    - 2026-01-20
    - 2026-01-20 20:03:19
    - 2026-01-20T20:03:19
    Si no puede parsear, devuelve original.
    """
    s = str(v or "").strip()
    if not s:
        return "-"

    candidatos = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    ]

    for f in candidatos:
        try:
            dt = datetime.strptime(s, f)
            return dt.strftime("%d-%m-%Y")
        except Exception:
            pass

    # Si viene con zona (2026-01-20T20:03:19-05:00), intentamos recorte simple
    try:
        base = s[:19].replace("T", " ")
        dt = datetime.strptime(base, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%d-%m-%Y")
    except Exception:
        return s


def _fmt_fecha_hora_ddmmyyyy(v: Any) -> str:
    """
    Convierte a dd-mm-aaaa HH:MM:SS cuando hay hora.
    """
    s = str(v or "").strip()
    if not s:
        return "-"

    candidatos = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    ]
    for f in candidatos:
        try:
            dt = datetime.strptime(s, f)
            return dt.strftime("%d-%m-%Y %H:%M:%S")
        except Exception:
            pass

    # si es solo fecha
    try:
        dt = datetime.strptime(s, "%Y-%m-%d")
        return dt.strftime("%d-%m-%Y")
    except Exception:
        return s


def formatear_respuesta_guia(payload: Dict[str, Any]) -> str:
    if not payload or not payload.get("ok"):
        msg = _txt(payload.get("error") if isinstance(payload, dict) else "", "No se pudo consultar la guía.")
        return (
            f"❗ {msg}\n\n"
            "🧾 *Menú Cliente*\n"
            "Responde con un número:\n"
            "1️⃣ 🔎 Consultar guía\n"
            "2️⃣ ↩️ Volver al menú principal"
        )

    guia = _txt(payload.get("guia"))
    exists = bool(payload.get("exists"))
    not_found = bool(payload.get("not_found"))

    if not exists or not_found:
        return (
            f"❌ La guía *{guia}* no existe.\n\n"
            "¿Qué deseas hacer ahora?\n"
            "1️⃣ Consultar otra guía\n"
            "2️⃣ Volver al menú principal"
        )

    data = payload.get("data") or {}
    tracking_url = _txt(payload.get("tracking_url"), "")

    cliente = _txt(data.get("Cliente"), "(sin cliente)")
    envio = _fmt_fecha_ddmmyyyy(data.get("Envio"))
    estado = _txt(data.get("Estado"), "(sin estado)")
    fecha_estado = _fmt_fecha_ddmmyyyy(data.get("FechaEstado"))

    movimientos: List[Dict[str, Any]] = data.get("Movimientos") or []

    lineas_mov = []
    for m in movimientos[:6]:
        nom = _txt(m.get("NomMov"), "(sin movimiento)")
        fec = _fmt_fecha_hora_ddmmyyyy(m.get("FecMov"))
        lineas_mov.append(f"· {nom}\n{fec}")

    bloque_mov = "\n".join(lineas_mov) if lineas_mov else "· (sin movimientos)"

    bloque_url = f"\n\n🔗 *Seguimiento:* \n{tracking_url}" if tracking_url else ""

    return (
        "📦 *Trazabilidad de guía*\n\n"
        f"🔢 *Guía:* {guia}\n"
        f"🏢 *Cliente:* {cliente}\n"
        f"📅 *Envío:* {envio}\n"
        f"✅ *Estado:* {estado}\n"
        f"🗓️ *Fecha estado:* {fecha_estado}\n\n"
        f"🧾 *Últimos movimientos:*\n{bloque_mov}"
        f"{bloque_url}\n\n"
        "¿Qué deseas hacer ahora?\n"
        "1️⃣ Consultar otra guía\n"
        "2️⃣ Volver al menú principal"
    )
