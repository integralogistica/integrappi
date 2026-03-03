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
    Soporta doble espacio entre fecha y hora (ej: 2023-04-21  19:25).
    """
    s = str(v or "").strip()
    if not s:
        return "-"

    # Normaliza doble espacio entre fecha y hora
    s_norm = " ".join(s.split())

    candidatos = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    ]
    for f in candidatos:
        try:
            dt = datetime.strptime(s_norm, f)
            return f"{dt.day:02d} {MESES_ES[dt.month]} {dt.year} {dt.hour:02d}:{dt.minute:02d}"
        except Exception:
            pass

    # Solo fecha sin hora
    try:
        dt = datetime.strptime(s_norm, "%Y-%m-%d")
        return f"{dt.day:02d} {MESES_ES[dt.month]} {dt.year}"
    except Exception:
        return s


def _val(v: Any) -> Optional[str]:
    """Retorna el valor limpio o None si está vacío/inválido."""
    s = str(v or "").strip()
    if not s or s.lower() in {"-", "null", "none", "n/a"}:
        return None
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

    # --- Campos principales (nombres correctos según el WS) ---
    cliente    = _val(data.get("Nombre_cliente"))
    envio      = _fmt_fecha_amigable(data.get("FecEnv"))
    estado     = _txt(data.get("Estado"), "-")
    fecha_est  = _fmt_fecha_amigable(data.get("FecEst"))
    servicio   = _val(data.get("Servicio"))
    num_piezas = _val(data.get("NumPie"))

    # --- Origen ---
    nom_rem = _val(data.get("NomRem"))
    ciu_rem = _val(data.get("CiuRem"))

    # --- Destino ---
    nom_des      = _val(data.get("NomDes"))
    ciu_des      = _val(data.get("CiuDes"))
    quien_recibe = _val(data.get("quienrecibe"))

    # --- Movimientos (DetalleMov + Tipo_Movimiento como contexto) ---
    movimientos: List[Dict[str, Any]] = data.get("Movimientos") or []

    lineas_mov = []
    for m in movimientos[:8]:
        detalle = str(m.get("DetalleMov") or "").strip()
        tipo    = str(m.get("Tipo_Movimiento") or "").strip()
        fec     = _fmt_fecha_hora_amigable(m.get("FecMov"))
        nom     = detalle or tipo
        if nom:
            lineas_mov.append(f"· {nom} — {fec}")
        else:
            lineas_mov.append(f"· 📍 {fec}")

    bloque_mov = "\n".join(lineas_mov) if lineas_mov else "· Sin movimientos registrados"

    # --- Construcción del mensaje ---
    lineas: List[str] = ["📦 *Trazabilidad de guía*\n"]

    lineas.append(f"🔢 *Guía:* {guia}")
    if cliente:
        lineas.append(f"🏢 *Cliente:* {cliente}")
    if servicio:
        lineas.append(f"🚚 *Servicio:* {servicio}")
    if num_piezas:
        lineas.append(f"📦 *Piezas:* {num_piezas}")
    lineas.append(f"📅 *Fecha envío:* {envio}")
    lineas.append(f"✅ *Estado:* {estado}")
    lineas.append(f"🗓️ *Fecha estado:* {fecha_est}")

    # Bloque origen
    if nom_rem or ciu_rem:
        origen = nom_rem or ""
        if ciu_rem:
            origen = f"{origen} ({ciu_rem})" if origen else ciu_rem
        lineas.append(f"\n📤 *Origen:* {origen}")

    # Bloque destino
    if nom_des or ciu_des:
        destino = nom_des or ""
        if ciu_des:
            destino = f"{destino} — {ciu_des}" if destino else ciu_des
        lineas.append(f"📥 *Destino:* {destino}")

    if quien_recibe:
        lineas.append(f"🖊️ *Recibió:* {quien_recibe}")

    lineas.append(f"\n🧾 *Movimientos:*\n{bloque_mov}")

    if tracking_url:
        lineas.append(f"\n🔗 *Seguimiento en línea:*\n{tracking_url}")

    lineas.append("\n¿Qué deseas hacer ahora?\n1️⃣ Consultar otra guía\n2️⃣ Volver al menú principal")

    return "\n".join(lineas)