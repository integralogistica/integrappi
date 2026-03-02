# Funciones/siscore_ws_format.py
from typing import Dict, Any, List, Optional
from datetime import datetime

MESES_ES = {
    1: "ene", 2: "feb", 3: "mar", 4: "abr",
    5: "may", 6: "jun", 7: "jul", 8: "ago",
    9: "sep", 10: "oct", 11: "nov", 12: "dic",
}


def _txt(v: Any, fallback: str = "-") -> str:
    s = str(v or "").strip()
    return s if s else fallback


def _fmt_fecha_amigable(v: Any) -> str:
    """
    Convierte a formato legible: 04 feb 2026.
    Soporta: 2026-01-20 / 2026-01-20 20:03:19 / 2026-01-20T20:03:19
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
            return f"{dt.day:02d} {MESES_ES[dt.month]} {dt.year}"
        except Exception:
            pass

    # Zona horaria (2026-01-20T20:03:19-05:00): recorte simple
    try:
        base = s[:19].replace("T", " ")
        dt = datetime.strptime(base, "%Y-%m-%d %H:%M:%S")
        return f"{dt.day:02d} {MESES_ES[dt.month]} {dt.year}"
    except Exception:
        return s


def _fmt_fecha_hora_amigable(v: Any) -> str:
    """
    Convierte a formato legible con hora: 04 feb 2026 19:17.
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
            return f"{dt.day:02d} {MESES_ES[dt.month]} {dt.year} {dt.hour:02d}:{dt.minute:02d}"
        except Exception:
            pass

    # Solo fecha sin hora
    try:
        dt = datetime.strptime(s, "%Y-%m-%d")
        return f"{dt.day:02d} {MESES_ES[dt.month]} {dt.year}"
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

    cliente_raw = str(data.get("Cliente") or "").strip()
    envio = _fmt_fecha_amigable(data.get("Envio"))
    estado = _txt(data.get("Estado"), "-")
    fecha_estado = _fmt_fecha_amigable(data.get("FechaEstado"))

    movimientos: List[Dict[str, Any]] = data.get("Movimientos") or []

    lineas_mov = []
    for m in movimientos[:6]:
        nom = str(m.get("NomMov") or "").strip()
        fec = _fmt_fecha_hora_amigable(m.get("FecMov"))
        if nom:
            lineas_mov.append(f"· {nom} — {fec}")
        else:
            lineas_mov.append(f"· 📍 {fec}")

    bloque_mov = "\n".join(lineas_mov) if lineas_mov else "· Sin movimientos registrados"

    bloque_url = f"\n\n🔗 *Seguimiento:*\n{tracking_url}" if tracking_url else ""

    # Línea de cliente: solo si tiene valor
    linea_cliente = f"🏢 *Cliente:* {cliente_raw}\n" if cliente_raw else ""

    return (
        "📦 *Trazabilidad de guía*\n\n"
        f"🔢 *Guía:* {guia}\n"
        f"{linea_cliente}"
        f"📅 *Envío:* {envio}\n"
        f"✅ *Estado:* {estado}\n"
        f"🗓️ *Fecha estado:* {fecha_estado}\n\n"
        f"🧾 *Últimos movimientos:*\n{bloque_mov}"
        f"{bloque_url}\n\n"
        "¿Qué deseas hacer ahora?\n"
        "1️⃣ Consultar otra guía\n"
        "2️⃣ Volver al menú principal"
    )