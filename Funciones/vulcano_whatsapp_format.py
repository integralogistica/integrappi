# Funciones/vulcano_whatsapp_format.py
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

MESES_ES = {
    1: "ene", 2: "feb", 3: "mar", 4: "abr",
    5: "may", 6: "jun", 7: "jul", 8: "ago",
    9: "sep", 10: "oct", 11: "nov", 12: "dic",
}

PAGE_SIZE_DETALLE = 5

ORDEN_ESTADOS = ["LIQUIDADO", "CUMPLIDO", "TRANSITO"]

ESTADO_LABEL = {
    "LIQUIDADO": "LIQUIDADOS",
    "CUMPLIDO": "CUMPLIDOS",
    "TRANSITO": "EN TRÁNSITO",
}

ESTADO_EMOJI = {
    "LIQUIDADO": "🟢",
    "CUMPLIDO": "✅",
    "TRANSITO": "🚛",
}


# ==============================================================================
# Helpers internos
# ==============================================================================

def _fmt_moneda(v: Any) -> str:
    try:
        n = float(str(v or "0").replace(",", "."))
        return "${:,.0f}".format(int(n)).replace(",", ".")
    except Exception:
        return str(v or "-")


def _fmt_fecha(v: Any) -> str:
    """Convierte cualquier fecha ISO/datetime a '27 feb 2026'."""
    s = str(v or "").strip()
    if not s:
        return "-"
    try:
        dt = datetime.strptime(s[:10], "%Y-%m-%d")
        return f"{dt.day:02d} {MESES_ES[dt.month]} {dt.year}"
    except Exception:
        return s[:10]


def _txt(v: Any) -> str:
    s = str(v or "").strip()
    return s if s and s.lower() not in {"-", "null", "none", ""} else "-"


def _calcular_saldo_vulcano(fila: Dict[str, Any]) -> float:
    """Saldo = MontoTotal - ValorAnticipado - ReteFuente - ReteICA - ReteCREE - deducciones."""
    try:
        def _f(k: str) -> float:
            return float(str(fila.get(k) or "0").replace(",", "."))
        return _f("MontoTotal") - _f("ValorAnticipado") - _f("ReteFuente") - _f("ReteICA") - _f("ReteCREE") - _f("deducciones")
    except Exception:
        return 0.0


def _emoji_estado(estado: str) -> str:
    return ESTADO_EMOJI.get(estado.upper(), "📋")


def _label_estado(estado: str) -> str:
    return ESTADO_LABEL.get(estado.upper(), estado)


def _numeral_emoji(n: int) -> str:
    emojis = {1: "1️⃣", 2: "2️⃣", 3: "3️⃣", 4: "4️⃣", 5: "5️⃣", 6: "6️⃣", 7: "7️⃣", 8: "8️⃣", 9: "9️⃣"}
    return emojis.get(n, f"{n}.")


# ==============================================================================
# Agrupación
# ==============================================================================

def agrupar_por_estado(filas: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Agrupa las filas de Vulcano rpt_id=26 por Estado_mft (ya sin ANULADOS)."""
    grupos: Dict[str, List] = {}
    for f in filas:
        estado = str(f.get("Estado_mft") or "OTRO").upper().strip()
        grupos.setdefault(estado, []).append(f)
    return grupos


# ==============================================================================
# Resumen principal
# ==============================================================================

def formatear_resumen_tenedor(
    cedula: str,
    year: str,
    grupos: Dict[str, List[Dict[str, Any]]],
    pagos_mongodb: List[Dict[str, Any]],
) -> Tuple[str, Dict[str, str]]:
    """
    Devuelve (texto_whatsapp, opcion_map).
    opcion_map: {"1": "pagos", "2": "estado_LIQUIDADO", ...}
    """
    total = sum(len(v) for v in grupos.values())

    # --- Bloque de estados ---
    lineas_estado = []
    for estado in ORDEN_ESTADOS:
        filas_e = grupos.get(estado, [])
        if filas_e:
            lineas_estado.append(f"{_emoji_estado(estado)} {_label_estado(estado)}: {len(filas_e)}")
    for estado, filas_e in grupos.items():
        if estado not in ORDEN_ESTADOS and filas_e:
            lineas_estado.append(f"📋 {estado}: {len(filas_e)}")

    bloque_estados = "\n".join(lineas_estado) if lineas_estado else "Sin manifiestos registrados"

    # --- Bloque de pagos próximos con total ---
    bloque_pagos = ""
    if pagos_mongodb:
        n_pagos = len(pagos_mongodb)
        total_saldo = sum(float(p.get("Saldo") or 0) for p in pagos_mongodb)
        pagos_ord = sorted(pagos_mongodb, key=lambda p: str(p.get("Fecha_saldo") or "")[:10])
        proximo = pagos_ord[0]
        fecha_prox = _fmt_fecha(proximo.get("Fecha_saldo"))
        mft_prox = _txt(proximo.get("Manifiesto"))
        bloque_pagos = (
            f"\n\n💳 *Saldos pendientes de pago:*\n"
            f"{n_pagos} manifiesto(s) | 💰 Total: {_fmt_moneda(total_saldo)}\n"
            f"El más próximo: 📅 {fecha_prox} — Mft {mft_prox}"
        )

    # --- Opciones dinámicas ---
    opcion_map: Dict[str, str] = {}
    opciones: List[str] = []
    n = 1

    if pagos_mongodb:
        opcion_map[str(n)] = "pagos"
        opciones.append(f"{_numeral_emoji(n)} 💳 Ver saldos pendientes")
        n += 1

    for estado in ORDEN_ESTADOS:
        if grupos.get(estado):
            opcion_map[str(n)] = f"estado_{estado}"
            opciones.append(f"{_numeral_emoji(n)} {_emoji_estado(estado)} Ver {_label_estado(estado)}")
            n += 1

    opcion_map[str(n)] = "consultar_manifiesto"
    opciones.append(f"{_numeral_emoji(n)} 🔍 Consultar un manifiesto")
    n += 1

    opcion_map[str(n)] = "ver_historico_web"
    opciones.append(f"{_numeral_emoji(n)} 🌐 Ver el historico desde web")
    n += 1

    opcion_map[str(n)] = "menu_principal"
    opciones.append(f"{_numeral_emoji(n)} 🏠 Volver al menú principal")

    texto = (
        f"🚚 *Manifiestos — Tenedor {cedula}*\n"
        f"📅 Desde {int(year)+1} | Total: {total}\n\n"
        f"📊 *Estado de tus manifiestos:*\n"
        f"{bloque_estados}"
        f"{bloque_pagos}\n\n"
        f"¿Qué deseas ver?\n"
        + "\n".join(opciones)
    )

    return texto, opcion_map


# ==============================================================================
# Detalle pagos de saldo (MongoDB) — sin paginación
# ==============================================================================

def formatear_pagos_saldo(pagos: List[Dict[str, Any]]) -> str:
    """
    Muestra todos los saldos de una vez (sin paginar).
    Incluye fecha de despacho desde Vulcano y total al inicio.
    """
    if not pagos:
        return "No hay saldos pendientes de pago.\n\n1️⃣ Volver al resumen\n2️⃣ Menú principal"

    pagos_ord = sorted(pagos, key=lambda p: str(p.get("Fecha_saldo") or "")[:10])
    total_saldo = sum(float(p.get("Saldo") or 0) for p in pagos_ord)

    lineas = [
        f"💳 *Saldos pendientes de pago*\n"
        f"{len(pagos_ord)} manifiesto(s) | 💰 Total: {_fmt_moneda(total_saldo)}\n"
    ]

    for p in pagos_ord:
        mft            = _txt(p.get("Manifiesto"))
        saldo          = _fmt_moneda(p.get("Saldo"))
        fecha_pago     = _fmt_fecha(p.get("Fecha_saldo"))
        fecha_despacho = _fmt_fecha(p.get("Fecha"))        # fecha de despacho de Vulcano
        origen         = _txt(p.get("Origen"))
        destino        = _txt(p.get("Destino"))

        partes = [f"📌 *Mft {mft}*"]
        if fecha_despacho != "-":
            partes.append(f"   📦 Despacho: {fecha_despacho}")
        if origen != "-" or destino != "-":
            partes.append(f"   🗺️ {origen} → {destino}")
        partes.append(f"   💰 Saldo: {saldo}")
        partes.append(f"   📅 Fecha pago: {fecha_pago}")
        lineas.append("\n".join(partes))

    return "\n\n".join(lineas) + "\n\n1️⃣ Volver al resumen\n2️⃣ Menú principal"


# ==============================================================================
# Detalle manifiestos por estado (Vulcano) — con paginación
# ==============================================================================

def formatear_manifiestos_estado(
    filas: List[Dict[str, Any]],
    estado: str,
    page: int,
    dict_pagos: Dict[str, Dict[str, Any]],
    page_size: int = PAGE_SIZE_DETALLE,
) -> str:
    """Muestra manifiestos de un estado con info financiera y saldo cruzado con MongoDB."""
    filas_ord = sorted(filas, key=lambda f: str(f.get("Fecha") or ""), reverse=True)
    total = len(filas_ord)
    inicio = (page - 1) * page_size
    fin = min(inicio + page_size, total)
    pagina = filas_ord[inicio:fin]

    if not pagina:
        return "No hay más manifiestos.\n\n1️⃣ Volver al resumen\n2️⃣ Menú principal"

    emoji = _emoji_estado(estado)
    label = _label_estado(estado)
    lineas = [f"{emoji} *Manifiestos {label}*\nMostrando {inicio+1}–{fin} de {total}\n"]

    for f in pagina:
        mft_num     = _txt(f.get("Manif_numero"))
        fecha       = _fmt_fecha(f.get("Fecha"))
        fecha_cumpl = _fmt_fecha(f.get("Fecha cumpl."))
        origen      = _txt(f.get("Origen"))
        destino     = _txt(f.get("Destino"))
        placa       = _txt(f.get("Placa"))
        total_flete = _fmt_moneda(f.get("MontoTotal"))
        retefuente  = _fmt_moneda(f.get("ReteFuente"))
        reteica     = _fmt_moneda(f.get("ReteICA"))
        anticipo    = _fmt_moneda(f.get("ValorAnticipado"))

        pago_info = dict_pagos.get(mft_num)
        saldo_line = ""
        if pago_info:
            saldo_txt  = _fmt_moneda(pago_info.get("Saldo"))
            fecha_pago = _fmt_fecha(pago_info.get("Fecha_saldo"))
            saldo_line = f"\n   💳 Saldo: {saldo_txt} (📅 pago: {fecha_pago})"

        cumpl_line = f"\n   ✅ Cumplido: {fecha_cumpl}" if estado.upper() in {"CUMPLIDO", "LIQUIDADO"} and fecha_cumpl != "-" else ""

        lineas.append(
            f"📌 *Mft {mft_num}* | {fecha}{cumpl_line}\n"
            f"   🗺️ {origen} → {destino}  🚗 {placa}\n"
            f"   💵 Total: {total_flete}  🔻 ReteFte: {retefuente}  🔻 ReteICA: {reteica}  ➕ Anticipo: {anticipo}"
            f"{saldo_line}"
        )

    hay_mas = fin < total
    opciones = _opciones_navegacion(hay_mas, fin, min(fin + page_size, total))
    return "\n\n".join(lineas) + "\n\n" + opciones


# ==============================================================================
# Detalle de un manifiesto puntual
# ==============================================================================

def formatear_detalle_manifiesto(
    mft_num: str,
    fila: Optional[Dict[str, Any]],
    pago_info: Optional[Dict[str, Any]],
) -> str:
    """Detalle completo de un manifiesto consultado por código."""
    if not fila:
        return (
            f"❌ El manifiesto *{mft_num}* no se encontró en el período consultado.\n\n"
            "1️⃣ Consultar otro manifiesto\n"
            "2️⃣ Volver al resumen\n"
            "3️⃣ Menú principal"
        )

    estado      = _txt(fila.get("Estado_mft"))
    fecha       = _fmt_fecha(fila.get("Fecha"))
    fecha_cumpl = _fmt_fecha(fila.get("Fecha cumpl."))
    origen      = _txt(fila.get("Origen"))
    destino     = _txt(fila.get("Destino"))
    placa       = _txt(fila.get("Placa"))
    total_flete = _fmt_moneda(fila.get("MontoTotal"))
    retefuente  = _fmt_moneda(fila.get("ReteFuente"))
    reteica     = _fmt_moneda(fila.get("ReteICA"))
    anticipo    = _fmt_moneda(fila.get("ValorAnticipado"))
    emoji       = _emoji_estado(estado)

    lineas = [f"📋 *Manifiesto {mft_num}*\n"]
    lineas.append(f"{emoji} *Estado:* {estado}")
    if fecha != "-":
        lineas.append(f"📦 *Despacho:* {fecha}")
    if fecha_cumpl != "-":
        lineas.append(f"✅ *Cumplido:* {fecha_cumpl}")
    if origen != "-" or destino != "-":
        lineas.append(f"🗺️ *Ruta:* {origen} → {destino}")
    if placa != "-":
        lineas.append(f"🚗 *Placa:* {placa}")
    lineas.append(f"💵 *Total flete:* {total_flete}")
    lineas.append(f"🔻 *ReteFuente:* {retefuente}")
    lineas.append(f"🔻 *ReteICA:* {reteica}")
    lineas.append(f"➕ *Anticipo:* {anticipo}")
    if pago_info:
        saldo_txt  = _fmt_moneda(pago_info.get("Saldo"))
        fecha_pago = _fmt_fecha(pago_info.get("Fecha_saldo"))
        lineas.append(f"💳 *Saldo:* {saldo_txt}")
        lineas.append(f"📅 *Fecha pago:* {fecha_pago}")

    return "\n".join(lineas) + "\n\n1️⃣ Consultar otro manifiesto\n2️⃣ Volver al resumen\n3️⃣ Menú principal"


# ==============================================================================
# Helper opciones de navegación
# ==============================================================================

def _opciones_navegacion(hay_mas: bool, fin: int, prox_fin: int) -> str:
    if hay_mas:
        return (
            f"1️⃣ Ver más ({fin+1}–{prox_fin})\n"
            "2️⃣ Volver al resumen\n"
            "3️⃣ Menú principal"
        )
    return (
        "1️⃣ Volver al resumen\n"
        "2️⃣ Menú principal"
    )
