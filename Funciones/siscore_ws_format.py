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


# ─────────────────────────────────────────────────────────────────────────────
# (2026-11-13) Trazabilidad ENRIQUECIDA — mismo payload del portal
# (Funciones/trazabilidad_guia.enriquecer_consulta_guia): agrega veredicto
# OT, cita de entrega, ubicación del vehículo (Monitor), novedad justificada,
# otras guías del vehículo y link de evidencia digitalizada.
# ─────────────────────────────────────────────────────────────────────────────

# WhatsApp corta los mensajes a 4096 caracteres: los bloques variables van
# acotados para que el mensaje completo siempre quepa en un solo envío.
_MAX_MOV_ENRIQUECIDO = 6
_MAX_COMPANERAS = 6
_MAX_JUSTIFICACION = 120


def _bloque_ot_wa(payload: Dict[str, Any]) -> List[str]:
    """Veredicto On Time — MISMA semántica de chips de la Trazabilidad del
    portal (index.tsx): anulada > devolución total > con novedad (incluida la
    justificada que rescata) > pendiente/vencida > cumplió/no cumplió."""
    ot = payload.get("ot")
    if not ot:
        return []
    just = payload.get("justificada") or {}
    con_novedad = ot.get("cumple_novedad") or just
    por_cita = ot.get("origen_promesa") == "CITA"
    fecha_ref = (ot.get("fecha_cita_usada") or ot.get("fecha_promesa")) if por_cita else ot.get("fecha_promesa")
    lbl = "cita" if por_cita else "promesa"
    fmt_ref = _fmt_fecha_amigable(fecha_ref)
    f_entrega = _fmt_fecha_amigable(ot.get("fecha_entrega"))

    if ot.get("anulada"):
        chip = "⊘ Guía anulada — cuenta como cumplida"
    elif ot.get("devolucion_total"):
        chip = "↩ Devolución total — cuenta como cumplida"
    elif just and ot.get("ot") == 0:
        chip = f"✔ Justificada — {just.get('causal') or 'novedad'}"
    elif con_novedad:
        chip = "✔ Con novedad — cuenta como cumplida"
    elif ot.get("pendiente"):
        vencida = ot.get("pendiente_vencida")
        chip = "🕒 Pendiente (vencida)" if vencida else "🕒 Pendiente"
        if fecha_ref:
            chip += f" — {lbl} {fmt_ref}" + (" ya venció" if vencida else " aún no vence")
    elif ot.get("ot") == 1:
        chip = f"✔ Cumplió — entrega {f_entrega} ≤ {lbl} {fmt_ref}"
    elif ot.get("ot") == 0:
        chip = f"✘ No cumplió — entrega {f_entrega} > {lbl} {fmt_ref}"
    elif ot.get("entregada"):
        chip = "— Sin evaluar (entregada sin promesa evaluable)"
    else:
        chip = "— Sin evaluar (en curso sin promesa evaluable)"
    return [f"🎯 *On Time:* {chip}"]


def _bloque_vehiculo_wa(payload: Dict[str, Any]) -> List[str]:
    """Ubicación del vehículo en curso (API Monitor). Mismo criterio del
    portal: nada si Monitor falló, la placa no existe o el manifiesto está
    Finalizado (la posición sería de un viaje que ya terminó). El bot es de
    cara a CLIENTES → igual que el perfil CLIENTE del portal, con
    ``coincide_guia=false`` NO se muestra NADA de Monitor (el manifiesto
    vigente de la placa es otro viaje y la posición no es de esta guía; el
    aviso «carro sin planillar» es solo para perfiles internos del portal)."""
    veh = payload.get("vehiculo") or {}
    v = veh.get("vehiculo") if (veh.get("ok") and veh.get("existe")) else None
    if not v:
        return []
    if str(v.get("estado_viaje") or "").strip().lower() == "finalizado":
        return []
    if v.get("coincide_guia") is False:
        return []
    lineas = ["\n🚛 *Vehículo en curso*"]
    if v.get("ubicacion"):
        lineas.append(f"📍 {v['ubicacion']}")
    extras = []
    if v.get("velocidad") is not None:
        extras.append(f"{v['velocidad']} km/h")
    if v.get("conductor"):
        extras.append(str(v["conductor"]))
    if extras:
        lineas.append(" · ".join(extras))
    gps = _fmt_fecha_hora_amigable(v.get("fecha_reporte_gps"))
    if v.get("fecha_reporte_gps") and gps != "-":
        lineas.append(f"🕒 Reporte GPS: {gps}")
    if v.get("eta_fecha"):
        eta = _fmt_fecha_hora_amigable(v["eta_fecha"])
        km = f" ({v['eta_distancia_km']} km)" if v.get("eta_distancia_km") is not None else ""
        lineas.append(f"⏱ Llegada estimada: {eta}{km}")
    if v.get("lat") is not None and v.get("lng") is not None:
        lineas.append(f"🗺️ https://maps.google.com/?q={v['lat']},{v['lng']}")
    return lineas


def formatear_trazabilidad_guia(payload: Dict[str, Any]) -> str:
    """Mensaje de WhatsApp para la consulta de guía ENRIQUECIDA (helper
    trazabilidad_guia). Degrada sola: si el payload no pasó por el
    enriquecimiento (o la guía no existe / falló), produce exactamente lo
    mismo que ``formatear_respuesta_guia``."""
    enriquecido = bool(payload) and payload.get("ok") and payload.get("exists") \
        and not payload.get("not_found") and (
            payload.get("ot") or payload.get("placa") or payload.get("vehiculo")
            or payload.get("cita_entrega") or payload.get("justificada")
            or payload.get("misma_placa") or payload.get("evidencia_url")
        )
    if not enriquecido:
        return formatear_respuesta_guia(payload)

    data = payload.get("data") or {}
    guia = _txt(payload.get("guia"))
    tracking_url = _txt(payload.get("tracking_url"), "")

    cliente = _val(data.get("Nombre_cliente"))
    servicio = _val(data.get("Servicio"))
    num_piezas = _val(data.get("NumPie"))
    envio = _fmt_fecha_amigable(data.get("FecEnv"))
    estado = _txt(data.get("Estado"), "-")
    fecha_est = _fmt_fecha_amigable(data.get("FecEst"))

    nom_rem, ciu_rem = _val(data.get("NomRem")), _val(data.get("CiuRem"))
    nom_des, ciu_des = _val(data.get("NomDes")), _val(data.get("CiuDes"))
    quien_recibe = _val(data.get("quienrecibe"))

    lineas: List[str] = ["📦 *Trazabilidad de guía*\n"]
    lineas.append(f"🔢 *Guía:* {guia}")
    if cliente:
        lineas.append(f"🏢 *Cliente:* {cliente}")
    if servicio:
        lineas.append(f"🚚 *Servicio:* {servicio}")
    if num_piezas:
        lineas.append(f"📦 *Piezas:* {num_piezas}")
    if _val(payload.get("placa")):
        lineas.append(f"🚛 *Placa:* {payload['placa']}")
    lineas.append(f"📅 *Fecha envío:* {envio}")
    lineas.append(f"✅ *Estado:* {estado}")
    lineas.append(f"🗓️ *Fecha estado:* {fecha_est}")

    # Veredicto OT (misma regla del informe/portal)
    lineas.extend(_bloque_ot_wa(payload))

    # Cita de entrega registrada a mano (/CitasEntrega) — la fecha con la que
    # se juega el OT cuando hay cita.
    cita = payload.get("cita_entrega") or {}
    if cita.get("fecha"):
        franja = ""
        if cita.get("hora_inicio"):
            franja = f" · {cita['hora_inicio']}–{cita.get('hora_fin') or ''}"
        reagendas = len(cita.get("anteriores") or [])
        sufijo = f" (re-agendada {reagendas} veces)" if reagendas > 1 else ""
        lineas.append(f"📅 *Cita de entrega:* {_fmt_fecha_amigable(cita['fecha'])}{franja}{sufijo}")

    # Origen / destino
    if nom_rem or ciu_rem:
        origen = nom_rem or ""
        if ciu_rem:
            origen = f"{origen} ({ciu_rem})" if origen else ciu_rem
        lineas.append(f"\n📤 *Origen:* {origen}")
    if nom_des or ciu_des:
        destino = nom_des or ""
        if ciu_des:
            destino = f"{destino} — {ciu_des}" if destino else ciu_des
        lineas.append(f"📥 *Destino:* {destino}")
    if quien_recibe:
        lineas.append(f"🖊️ *Recibió:* {quien_recibe}")

    # Novedad justificada (/NovedadesOT)
    just = payload.get("justificada") or {}
    if just.get("causal") or just.get("justificacion"):
        det = str(just.get("justificacion") or "").strip()
        if len(det) > _MAX_JUSTIFICACION:
            det = det[:_MAX_JUSTIFICACION - 1] + "…"
        linea = f"🚧 *Novedad:* {just.get('causal') or '-'}"
        if det:
            linea += f" — {det}"
        lineas.append(linea)

    # Ubicación del vehículo (Monitor) — solo guías en curso
    lineas.extend(_bloque_vehiculo_wa(payload))

    # Movimientos (últimos N para que el mensaje quepa entero)
    movimientos: List[Dict[str, Any]] = data.get("Movimientos") or []
    lineas_mov = []
    for m in movimientos[:_MAX_MOV_ENRIQUECIDO]:
        detalle = str(m.get("DetalleMov") or "").strip()
        tipo = str(m.get("Tipo_Movimiento") or "").strip()
        nom = detalle or tipo
        if nom:
            lineas_mov.append(f"· {nom} — {_fmt_fecha_hora_amigable(m.get('FecMov'))}")
    if len(movimientos) > _MAX_MOV_ENRIQUECIDO:
        lineas_mov.append(f"· …y {len(movimientos) - _MAX_MOV_ENRIQUECIDO} movimientos más")
    lineas.append(f"\n🧾 *Movimientos:*\n" + ("\n".join(lineas_mov) if lineas_mov else "· Sin movimientos registrados"))

    # Otras guías del mismo vehículo ese día
    companeras = payload.get("misma_placa") or []
    if companeras:
        textos = [f"{cp.get('guia')} ({_txt(cp.get('estado'), '-')})" for cp in companeras[:_MAX_COMPANERAS]]
        if len(companeras) > _MAX_COMPANERAS:
            textos.append(f"…y {len(companeras) - _MAX_COMPANERAS} más")
        lineas.append("\n🚛 *Otras guías del vehículo:* " + " · ".join(textos))

    # Evidencia digitalizada (reemplaza al tracking genérico, como en el
    # portal) o link de seguimiento normal.
    if payload.get("evidencia_url"):
        lineas.append(f"\n🖼️ *Evidencia de entrega digitalizada:*\n{payload['evidencia_url']}")
    elif tracking_url:
        lineas.append(f"\n🔗 *Seguimiento en línea:*\n{tracking_url}")

    lineas.append("\n¿Qué deseas hacer ahora?\n1️⃣ Consultar otra guía\n2️⃣ Volver al menú principal")
    return "\n".join(lineas)