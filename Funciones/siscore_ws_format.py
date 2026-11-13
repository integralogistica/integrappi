# Funciones/siscore_ws_format.py
import unicodedata
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta, timezone

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


# =============================================================================
# (2026-11-13) Trazabilidad ENRIQUECIDA — mensaje del bot de WhatsApp.
# Rediseño UX (misma fecha): estructura por secciones priorizando lo que el
# cliente necesita (estado → origen/destino → ubicación/ETA → cumplimiento →
# detalles → historial). El payload es el del helper
# Funciones/trazabilidad_guia.enriquecer_consulta_guia, pero degrada sola con
# un payload solo-WS.
# =============================================================================

# WhatsApp corta los mensajes a 4096 caracteres: los bloques variables van
# acotados para que el mensaje completo siempre quepa en un solo envío.
_MAX_MOV = 6
_MAX_COMPANERAS = 6
_MAX_JUSTIFICACION = 120
_MAX_UBICACION = 80


def _sin_tildes(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


def _norm_txt(v: Any) -> str:
    """Normaliza para comparar estados: SIN tildes, MAYÚSCULAS, espacios
    colapsados (regla del enlace de seguimiento: la comparación del estado
    debe tolerar mayúsculas/minúsculas/tildes/espacios)."""
    return " ".join(_sin_tildes(str(v or "")).upper().split())


def _ahora_colombia() -> datetime:
    """Ahora en hora de Colombia (UTC−5 fijo, sin horario de verano) — para
    saber si una ETA ya pasó (las fechas de Monitor son hora local CO).
    NAIVE a propósito: se compara contra los parses de _parse_dt (naive)."""
    return datetime.now(timezone(timedelta(hours=-5))).replace(tzinfo=None)


def _parse_dt(v: Any) -> Optional[datetime]:
    """Parsea 'YYYY-MM-DD HH:MM[:SS]' / ISO (formatos de Monitor). None si no."""
    s = " ".join(str(v or "").strip().split())
    for f in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, f)
        except Exception:
            pass
    return None


def _hora12(dt: datetime) -> str:
    h24, m = dt.hour, dt.minute
    if h24 < 12:
        suf, h = "a. m.", h24
    else:
        suf, h = "p. m.", h24 - 12 if h24 > 12 else 12
    if h == 0:
        h = 12
    return f"{h}:{m:02d} {suf}"


def _dt12(dt: datetime) -> str:
    """'13 nov · 10:03 a. m.' — fecha corta + hora 12h legible."""
    return f"{dt.day:02d} {MESES_ES[dt.month]} · {_hora12(dt)}"


def _estado_amigable(estado: Any):
    """(icono, texto, descripción) del estado actual — lenguaje de cliente.
    Match por SUBCADENA normalizada (los estados reales llegan 'ENTREGA
    DIGITALIZADA', 'En Ruta a Centro Logistico', 'Transito Nacional'...)."""
    n = _norm_txt(estado)
    if "ANULADA" in n:
        return "⊘", "Anulada", "El envío fue anulado."
    if "ENTREGA" in n or "DIGITALIZADA" in n:
        return "✅", "Entregado", "El envío fue entregado al destinatario."
    if "DISTRIBUCION" in n:
        return "🚚", "En distribución", "Tu envío está en ruta hacia el destinatario."
    if "TRANSITO" in n or "RUTA" in n:
        return "🚛", "En tránsito", "El envío está en tránsito hacia su destino."
    if "PENDIENTE" in n:
        return "🕒", "Pendiente", "El envío aún no ha sido despachado."
    if "NOVEDAD" in n:
        return "⚠️", "Con novedad", "El envío presenta una novedad en curso."
    if "ACONDICIONAMIENTO" in n or "STAGE" in n:
        return "📦", "En acondicionamiento", "El envío está en proceso de acondicionamiento."
    texto = str(estado or "").strip() or "Sin estado"
    return "ℹ️", texto.capitalize(), ""


def _guia_entregada_estado(payload: Dict[str, Any]) -> bool:
    """¿La guía está ENTREGADA según su estado ACTUAL? (regla del enlace de
    seguimiento: se valida contra el estado real, no el historial — y con
    normalización de tildes/mayúsculas/espacios)."""
    n = _norm_txt((payload.get("data") or {}).get("Estado"))
    return "ENTREGA" in n or "DIGITALIZADA" in n


def _acortar_ubicacion(texto: Any) -> str:
    """Ubicación corta y comprensible: las direcciones de Monitor son
    geocodificadas larguísimas ('Calle 51, Manchester, Bello, Valle de
    Aburrá, Antioquia, RAP del Agua y la Montaña, 051053, Colombia') — se
    toman los primeros componentes significativos, UNA sola línea."""
    partes = [p.strip() for p in str(texto or "").split(",") if p.strip()]
    corto = ", ".join(partes[:3])
    if len(corto) > _MAX_UBICACION:
        corto = corto[: _MAX_UBICACION - 1] + "…"
    return corto or "-"


def _icono_mov(txt: str) -> str:
    n = _norm_txt(txt)
    if "ENTREGA" in n or "DIGITALIZADA" in n:
        return "✅"
    if "DISTRIBUCION" in n:
        return "🚚"
    if "TRANSITO" in n or "RUTA" in n:
        return "🚛"
    if "CARG" in n or "DESCARG" in n or "PLANILL" in n:
        return "📦"
    return ""


def _bloque_cumplimiento_wa(payload: Dict[str, Any]) -> List[str]:
    """✅ Cumplimiento de entrega — indicador On Time en lenguaje de cliente.
    Mismos datos del veredicto OT del portal (``payload.ot``, misma
    ``_calcular_ot_filas``); NUNCA presenta como entrega confirmada un envío
    que solo está dentro del plazo."""
    ot = payload.get("ot") or {}
    just = payload.get("justificada") or {}
    lineas = ["✅ *Cumplimiento de entrega*"]
    if not ot:
        lineas.append("• Estado: Cumplimiento por confirmar")
        return lineas
    por_cita = ot.get("origen_promesa") == "CITA"
    compromiso = (ot.get("fecha_cita_usada") or ot.get("fecha_promesa")) if por_cita else ot.get("fecha_promesa")
    if compromiso:
        lineas.append(f"• Fecha compromiso: {_fmt_fecha_amigable(compromiso)}")
    entregado = bool(ot.get("fecha_entrega")) or bool(ot.get("entregada")) or _guia_entregada_estado(payload)
    con_novedad = bool(ot.get("cumple_novedad")) or bool(just)
    if ot.get("anulada"):
        estado = "Guía anulada — no cuenta como incumplimiento"
    elif ot.get("devolucion_total"):
        estado = "Devolución total — no cuenta como incumplimiento"
    elif entregado and ot.get("ot") == 1:
        estado = "Entregado a tiempo" + (" (con novedad)" if con_novedad else "")
    elif entregado and ot.get("ot") == 0:
        estado = "Entregado fuera de tiempo" + (" (justificada)" if just else "")
    elif entregado:
        # Entregada pero sin promesa evaluable → no se puede calcular.
        estado = "Cumplimiento por confirmar"
    elif ot.get("pendiente"):
        estado = "Plazo de entrega vencido" if ot.get("pendiente_vencida") else "Dentro del plazo de entrega"
    elif con_novedad:
        estado = "Con novedad — cuenta como cumplida"
    else:
        estado = "Cumplimiento por confirmar"
    lineas.append(f"• Estado: {estado}")
    if just.get("causal") or just.get("justificacion"):
        det = str(just.get("justificacion") or "").strip()
        if len(det) > _MAX_JUSTIFICACION:
            det = det[: _MAX_JUSTIFICACION - 1] + "…"
        linea = f"• Novedad: {just.get('causal') or '-'}"
        if det:
            linea += f" — {det}"
        lineas.append(linea)
    return lineas


def _bloque_gps_wa(payload: Dict[str, Any]) -> List[str]:
    """🕒 Ubicación y llegada estimada — solo guías en curso. Mismas reglas de
    visibilidad del portal: nada si Monitor falló, la placa no tiene
    despacho, el manifiesto está Finalizado, ``coincide_guia=false`` (bot de
    cara a clientes) o si NO hay NINGÚN dato de posición (despacho sin GPS).
    ETA ya pasada → 'pendiente de actualización' (no se promete una llegada
    futura que ya ocurrió); los 0 km/h se muestran tal cual (vehículo
    detenido, no una falla)."""
    veh = payload.get("vehiculo") or {}
    v = (veh.get("vehiculo") if (veh.get("ok") and veh.get("existe")) else None) or None
    if not v:
        return []
    if _norm_txt(v.get("estado_viaje")) == "FINALIZADO":
        return []
    if v.get("coincide_guia") is False:
        return []
    hay_posicion = bool(
        v.get("ubicacion") or v.get("velocidad") is not None
        or v.get("fecha_reporte_gps") or v.get("eta_fecha")
        or (v.get("lat") is not None and v.get("lng") is not None)
    )
    if not hay_posicion:
        return []
    lineas = ["🕒 *Ubicación y llegada estimada*"]
    if v.get("ubicacion"):
        ubi = _acortar_ubicacion(v["ubicacion"])
        if v.get("velocidad") is not None:
            ubi += f" · {v['velocidad']} km/h"
        lineas.append(f"• Ubicación: {ubi}")
    eta = _parse_dt(v.get("eta_fecha"))
    if eta is not None:
        if eta < _ahora_colombia():
            lineas.append("• Llegada estimada: pendiente de actualización")
        else:
            lineas.append(f"• Llegada estimada: {_dt12(eta)}")
        if v.get("eta_distancia_km") is not None:
            lineas.append(f"• Distancia restante: {v['eta_distancia_km']} km")
    gps = _parse_dt(v.get("fecha_reporte_gps"))
    if gps is not None:
        lineas.append(f"• Última actualización GPS: {_dt12(gps)}")
    if v.get("lat") is not None and v.get("lng") is not None:
        lineas.append(f"• https://maps.google.com/?q={v['lat']},{v['lng']}")
    return lineas


def formatear_trazabilidad_guia(payload: Dict[str, Any]) -> str:
    """Mensaje de WhatsApp para la consulta de guía ENRIQUECIDA (helper
    trazabilidad_guia). Estructura por secciones (2026-11-13): estado →
    origen/destino → ubicación/ETA → cumplimiento → detalles → historial →
    enlace (SOLO entregadas). Errores / guía inexistente → mismo formato de
    siempre (formatear_respuesta_guia)."""
    if not payload or not payload.get("ok") or not payload.get("exists") or payload.get("not_found"):
        return formatear_respuesta_guia(payload)

    data = payload.get("data") or {}
    guia = _txt(payload.get("guia"))
    icono_estado, texto_estado, desc_estado = _estado_amigable(data.get("Estado"))

    lineas: List[str] = ["📦 *Seguimiento de envío*", f"Guía #{guia}", ""]
    lineas.append(f"{icono_estado} *{texto_estado.upper()}*")
    if desc_estado:
        lineas.append(desc_estado)

    # ── Origen y destino ──
    nom_rem, ciu_rem = _val(data.get("NomRem")), _val(data.get("CiuRem"))
    nom_des, ciu_des = _val(data.get("NomDes")), _val(data.get("CiuDes"))
    if nom_rem or ciu_rem or nom_des or ciu_des:
        lineas.append("\n📍 *Origen y destino*")
        if nom_rem or ciu_rem:
            origen = f"{nom_rem}, {ciu_rem}" if (nom_rem and ciu_rem) else (nom_rem or ciu_rem)
            lineas.append(f"• Origen: {origen}")
        if nom_des or ciu_des:
            destino = f"{nom_des} — {ciu_des}" if (nom_des and ciu_des) else (nom_des or ciu_des)
            lineas.append(f"• Destino: {destino}")
        quien_recibe = _val(data.get("quienrecibe"))
        if quien_recibe:
            lineas.append(f"• Recibió: {quien_recibe}")

    # ── Ubicación y llegada estimada (solo en curso) ──
    gps = _bloque_gps_wa(payload)
    if gps:
        lineas.append("")
        lineas.extend(gps)

    # ── Cumplimiento de entrega (On Time) ──
    lineas.append("")
    lineas.extend(_bloque_cumplimiento_wa(payload))
    cita = payload.get("cita_entrega") or {}
    if cita.get("fecha"):
        franja = ""
        if cita.get("hora_inicio"):
            franja = f" · {cita['hora_inicio']}–{cita.get('hora_fin') or ''}"
        lineas.append(f"• Cita de entrega: {_fmt_fecha_amigable(cita['fecha'])}{franja}")

    # ── Detalles del envío ──
    detalles = []
    if _val(data.get("Nombre_cliente")):
        detalles.append(f"• Cliente: {data['Nombre_cliente']}")
    if _val(data.get("Servicio")):
        detalles.append(f"• Servicio: {data['Servicio']}")
    if _val(data.get("NumPie")):
        detalles.append(f"• Piezas: {data['NumPie']}")
    if _val(payload.get("placa")):
        detalles.append(f"• Vehículo: {payload['placa']}")
    veh = (payload.get("vehiculo") or {}).get("vehiculo") or {}
    if veh.get("conductor"):
        detalles.append(f"• Conductor: {veh['conductor']}")
    if _val(data.get("FecEnv")):
        detalles.append(f"• Fecha de envío: {_fmt_fecha_amigable(data['FecEnv'])}")
    ot = payload.get("ot") or {}
    if ot.get("fecha_entrega"):
        detalles.append(f"• Fecha de entrega: {_fmt_fecha_amigable(ot['fecha_entrega'])}")
    if detalles:
        lineas.append("\n📋 *Detalles del envío*")
        lineas.extend(detalles)

    # ── Historial de movimientos ──
    movimientos: List[Dict[str, Any]] = data.get("Movimientos") or []
    lineas.append("\n📜 *Historial de movimientos*")
    filas_mov = []
    for m in movimientos[:_MAX_MOV]:
        detalle = str(m.get("DetalleMov") or "").strip()
        tipo = str(m.get("Tipo_Movimiento") or "").strip()
        nombre = detalle or tipo
        if not nombre:
            continue
        dt = _parse_dt(m.get("FecMov"))
        cuando = _dt12(dt) if dt else _fmt_fecha_hora_amigable(m.get("FecMov"))
        icono = _icono_mov(nombre)
        filas_mov.append(f"- {icono} {cuando} — {nombre}" if icono else f"- {cuando} — {nombre}")
    if not filas_mov:
        filas_mov = ["- Sin movimientos registrados"]
    if len(movimientos) > _MAX_MOV:
        filas_mov.append(f"- …y {len(movimientos) - _MAX_MOV} movimientos más")
    lineas.extend(filas_mov)

    # ── Enlace de seguimiento: SOLO cuando el estado ACTUAL es ENTREGADO
    # (regla 2026-11-13: en distribución/tránsito/pendiente NO se muestra; se
    # compara el estado real normalizado, no el historial). La evidencia
    # digitalizada (que solo existe en entregadas) reemplaza al tracking
    # genérico, igual que en el portal. El mapa GPS es independiente. ──
    if _guia_entregada_estado(payload):
        if payload.get("evidencia_url"):
            lineas.append(f"\n🖼️ *Evidencia de entrega digitalizada:*\n{payload['evidencia_url']}")
        elif _txt(payload.get("tracking_url"), ""):
            lineas.append(f"\n🔗 *Seguimiento en línea:*\n{payload['tracking_url']}")

    # ── Otras guías del mismo vehículo ese día ──
    companeras = payload.get("misma_placa") or []
    if companeras:
        textos = [f"{cp.get('guia')} ({_txt(cp.get('estado'), '-')})" for cp in companeras[:_MAX_COMPANERAS]]
        if len(companeras) > _MAX_COMPANERAS:
            textos.append(f"…y {len(companeras) - _MAX_COMPANERAS} más")
        lineas.append("\n🚛 *Otras guías del vehículo:* " + " · ".join(textos))

    lineas.append("\n¿Qué deseas hacer ahora?\n1️⃣ Consultar otra guía\n2️⃣ Volver al menú principal")
    return "\n".join(lineas)
