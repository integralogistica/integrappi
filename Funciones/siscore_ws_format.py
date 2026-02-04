# Funciones/siscore_ws_format.py
from typing import Dict, Any, List


def formatear_respuesta_guia(payload: Dict[str, Any], max_movs: int = 6) -> str:
    """
    Recibe lo que retorna consultar_guia_ws y arma texto para WhatsApp.
    """
    if not payload.get("ok"):
        return "❗ No pude consultar la guía en este momento. Intenta nuevamente."

    d = (payload.get("data") or {})
    estado = d.get("Estado") or d.get("EstAct") or "(sin estado)"
    cliente = d.get("Nombre_cliente") or "(sin cliente)"
    fec_env = d.get("FecEnv") or ""
    fec_est = d.get("FecEst") or ""

    movs: List[Dict[str, str]] = d.get("Movimientos") or []
    # últimos movimientos al final normalmente; mostramos los más recientes
    ultimos = movs[-max_movs:] if len(movs) > max_movs else movs

    bloque_movs = ""
    if ultimos:
        lines = []
        for m in ultimos:
            tipo = m.get("Tipo_Movimiento", "").strip()
            det = m.get("DetalleMov", "").strip()
            fec = m.get("FecMov", "").strip()
            lines.append(f"• [{tipo}] {det}\n  {fec}".strip())
        bloque_movs = "\n".join(lines)
    else:
        bloque_movs = "• (sin movimientos)"

    return (
        "📦 *Trazabilidad de guía*\n\n"
        f"🔢 Guía: *{payload.get('guia')}*\n"
        f"🏢 Cliente: *{cliente}*\n"
        f"📅 Envío: *{fec_env or '-'}*\n"
        f"✅ Estado: *{estado}*\n"
        f"🗓️ Fecha estado: *{fec_est or '-'}*\n\n"
        f"🧾 *Últimos movimientos:*\n{bloque_movs}\n\n"
        "¿Qué deseas hacer ahora?\n"
        "1️⃣ Consultar otra guía\n"
        "2️⃣ Volver al menú principal"
    )
