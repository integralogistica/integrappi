# archivo: rutas/indicadores_cliente.py
"""
Indicadores por cliente (/indicadores/clientes/<id>).

Cada cliente del panel tiene SU fuente de datos (ver clientes.ts del frontend):
- 'postgres' → informe_guias_tms
- 'mongo'    → colecciones de Costo de Operación

Por ahora solo existe el gráfico "Cantidad de cajas" (media milla:
``pedidos_completados``). El endpoint está parametrizado por ``cliente_id``
pero validado contra el registro CLIENTES de abajo — el mismo criterio con el
que Costo de Operación identifica clientes (NIT en media milla, nombre
normalizado en las demás colecciones).
"""

from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from datetime import date, datetime, timedelta
import logging
import re

# Reutiliza helpers/pipelines de Costo de Operación (misma BD, mismas reglas):
# - _filtro_media_milla / _anios_media_milla → filtro y años de pedidos_completados
# - _num, NIT_FRESENIUS                      → detección de cliente
from rutas.indicadores_costo_operacion import (
    col_completados,
    _filtro_media_milla,
    _anios_media_milla,
    _num,
    NIT_FRESENIUS,
)
from rutas.fletes import coleccion_fletes
from bd.bd_postgres import consultar_guias

router = APIRouter(
    prefix="/indicadores-cliente",
    tags=["Indicadores Cliente"],
)
logger = logging.getLogger(__name__)

# Registro backend de clientes (espejo del clientes.ts del frontend; mantener
# sincronizado). El filtro de media milla se expresa como $match directo
# (forma query, no forma expr): por NIT para Kabi, igual que /costo-por-caja.
CLIENTES = {
    "fresenius-kabi": {
        "nombre": "Fresenius Kabi",
        "fuente": "mongo",
        "match_media_milla": lambda: {"nit_cliente": NIT_FRESENIUS},
    },
    # 'fresenius-medical-care' usará las colecciones de última milla/otros
    # cuando tenga gráficos.
}


@router.get("/{cliente_id}/cajas")
def get_cajas_cliente(
    cliente_id: str,
    anio: Optional[List[int]] = Query(None),
    mes: Optional[List[int]] = Query(None),
):
    """Cantidad de CAJAS por período (mensual + diaria) para UN cliente,
    desde la media milla (``pedidos_completados``).

    Misma lógica que la etapa media milla de /indicadores-costo-operacion/resumen:
    filtro por ``fecha_creacion`` (string local), dedup por ``consecutivo_vehiculo``
    (los totales del vehículo vienen duplicados en cada doc de pedido) y suma de
    ``total_cajas_vehiculo`` por bucket. Aquí el $match de cliente (por NIT para
    Kabi) se aplica ANTES del dedup para no arrastrar vehículos ajenos.
    """
    cliente = CLIENTES.get(cliente_id)
    if not cliente:
        raise HTTPException(status_code=404, detail=f"Cliente no registrado: {cliente_id}")

    try:
        pipeline = [
            {"$match": _filtro_media_milla(anio or [], mes or [])},
            {"$match": cliente["match_media_milla"]()},
            # DEDUP por vehículo: total_cajas_vehiculo está duplicado en cada
            # doc del vehículo; se toma uno solo antes de agrupar por período.
            {"$group": {
                "_id": "$consecutivo_vehiculo",
                "cajas": {"$first": _num("total_cajas_vehiculo")},
                "fecha_creacion": {"$first": "$fecha_creacion"},
            }},
            {"$facet": {
                "mensual": [
                    {"$group": {
                        "_id": {"$substrCP": ["$fecha_creacion", 0, 7]},
                        "cajas": {"$sum": "$cajas"},
                        "vehiculos": {"$sum": 1},
                    }},
                    {"$sort": {"_id": 1}},
                    {"$project": {"_id": 0, "periodo": "$_id", "cajas": 1, "vehiculos": 1}},
                ],
                "diario": [
                    {"$group": {
                        "_id": {"$substrCP": ["$fecha_creacion", 0, 10]},
                        "cajas": {"$sum": "$cajas"},
                        "vehiculos": {"$sum": 1},
                    }},
                    {"$sort": {"_id": 1}},
                    {"$project": {"_id": 0, "periodo": "$_id", "cajas": 1, "vehiculos": 1}},
                ],
            }},
        ]
        res = next(col_completados.aggregate(pipeline, allowDiskUse=True), {})
        return {
            "success": True,
            "data": {
                "cliente": cliente["nombre"],
                "mensual": res.get("mensual", []),
                "diario": res.get("diario", []),
                "anios": _anios_media_milla(),
            },
        }
    except Exception as e:
        logger.exception(f"[indicadores-cliente] Error en cajas de {cliente_id}")
        raise HTTPException(status_code=500, detail=str(e))


# ── Informe de guías TMS ─────────────────────────────────────────────────────
# planilla_siscore (Mongo) == guia (PostgreSQL informe_guias_tms). Puede traer
# VARIAS guías separadas por coma → se explota en una fila por guía.

# Válvulas de seguridad del informe (un año completo son miles de guías).
MAX_VEHICULOS = 8000
MAX_FILAS = 5000

# Estados reales de informe_guias_tms (verificados 2026-08-20): ENTREGADO,
# PENDIENTE, "En distribucion", "CON NOVEDAD", "Transito Nacional" + basura
# ('', '0000-00-00', 'planilla normal'). Solo ENTREGADO cuenta como entregada.
ESTADO_ENTREGADO = "ENTREGADO"


def _split_planillas(valor) -> List[str]:
    """'801195758, 801195771' → ['801195758', '801195771'] (trim, sin vacíos)."""
    if not valor:
        return []
    if isinstance(valor, float) and valor.is_integer():
        valor = int(valor)
    return [p.strip() for p in str(valor).split(",") if p.strip()]


def _fecha_iso(valor, largo: int = 10) -> Optional[str]:
    """Casteo defensivo a 'YYYY-MM-DD' (date/timestamp de PG, str o None)."""
    if valor is None:
        return None
    if isinstance(valor, (datetime, date)):
        return valor.isoformat()[:largo]
    texto = str(valor).strip()
    return texto[:largo] or None


# ── On Time (OT) ─────────────────────────────────────────────────────────────
# Días hábiles entre fecha inicial y entrega (sin sáb/dom/festivos Colombia),
# comparados contra la fecha promesa: fecha_cita si es una fecha servible; si
# no, fecha_inicial + promesa_entrega_dias (días hábiles) del destino en las
# tarifas FUNZA. ot=1 cumplió, 0 no cumplió, None no evaluable.

_PATRON_FECHA = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _fecha_servible(texto: Optional[str]) -> Optional[date]:
    """'2026-08-14' → date. None para basura del TMS ('BOGOTA', 'Z_CIU', '')."""
    if not texto:
        return None
    t = str(texto).strip()[:10]
    if not _PATRON_FECHA.fullmatch(t):
        return None
    try:
        return date.fromisoformat(t)
    except ValueError:  # 2026-02-30 y similares
        return None


def _pascua(anio: int) -> date:
    """Domingo de Pascua (algoritmo de Butcher/Meeus) — base de los festivos."""
    a, b, c = anio % 19, anio // 100, anio % 100
    d, e = b // 4, b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = c // 4, c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    mes = (h + l - 7 * m + 114) // 31
    dia = ((h + l - 7 * m + 114) % 31) + 1
    return date(anio, mes, dia)


def _festivos_colombia(anio: int) -> set:
    """Los 18 festivos oficiales de Colombia (Ley 51 de 1983).

    FIJOS (no se trasladan): Año Nuevo, Trabajo, Independencia (20 jul),
    Boyacá (7 ago), Inmaculada (8 dic), Navidad + Jueves y Viernes Santo.
    TRASLADABLES (a lunes si no caen lunes): Reyes, San José, San Pedro y
    San Pablo, Asunción, Raza, Todos los Santos, Cartagena + Ascensión,
    Corpus y Sagrado Corazón (estos tres por Pascua).
    """
    p = _pascua(anio)

    def trasladar_lunes(d: date) -> date:
        return d if d.weekday() == 0 else d + timedelta(days=(7 - d.weekday()))

    fijos = [
        date(anio, 1, 1),                                    # Año Nuevo
        date(anio, 5, 1),                                    # Día del Trabajo
        date(anio, 7, 20),                                   # Independencia
        date(anio, 8, 7),                                    # Batalla de Boyacá
        date(anio, 12, 8),                                   # Inmaculada
        date(anio, 12, 25),                                  # Navidad
    ]
    trasladables = [
        trasladar_lunes(date(anio, 1, 6)),                   # Reyes
        trasladar_lunes(date(anio, 3, 19)),                  # San José
        trasladar_lunes(date(anio, 6, 29)),                  # San Pedro y San Pablo
        trasladar_lunes(date(anio, 8, 15)),                  # Asunción
        trasladar_lunes(date(anio, 10, 12)),                 # Día de la Raza
        trasladar_lunes(date(anio, 11, 1)),                  # Todos los Santos
        trasladar_lunes(date(anio, 11, 11)),                 # Independencia Cartagena
    ]
    pascuales = [
        p - timedelta(days=3),                               # Jueves Santo
        p - timedelta(days=2),                               # Viernes Santo
        trasladar_lunes(p + timedelta(days=39)),             # Ascensión (+43 si se cuenta el lunes)
        trasladar_lunes(p + timedelta(days=60)),             # Corpus Christi
        trasladar_lunes(p + timedelta(days=68)),             # Sagrado Corazón
    ]
    return set(fijos + trasladables + pascuales)


_CACHE_FESTIVOS: dict = {}


def _es_habil(d: date) -> bool:
    """L-V que no sea festivo colombiano (cacheado por año)."""
    if d.weekday() >= 5:
        return False
    anio = d.year
    if anio not in _CACHE_FESTIVOS:
        _CACHE_FESTIVOS[anio] = _festivos_colombia(anio)
    return d not in _CACHE_FESTIVOS[anio]


def _dias_habiles_entre(inicio: date, fin: date) -> int:
    """Días hábiles del rango (inicio, fin] — el día inicial no se cuenta."""
    if fin <= inicio:
        return 0
    n, d = 0, inicio + timedelta(days=1)
    while d <= fin:
        if _es_habil(d):
            n += 1
        d += timedelta(days=1)
    return n


def _sumar_dias_habiles(inicio: date, dias: int) -> date:
    """Avanza N días hábiles desde inicio (el resultado cae en día hábil)."""
    d, restantes = inicio, dias
    while restantes > 0:
        d += timedelta(days=1)
        if _es_habil(d):
            restantes -= 1
    return d


def _mapa_promesa_destinos() -> dict:
    """{DESTINO_NORMALIZADO: promesa_entrega_dias} desde tarifas origen FUNZA."""
    out = {}
    for t in coleccion_fletes.find({"origen": "FUNZA"}, {"destino": 1, "promesa_entrega_dias": 1}):
        dest = (t.get("destino") or "").strip().upper()
        dias = t.get("promesa_entrega_dias") or 0
        if dest and dias > 0 and dest not in out:
            out[dest] = int(dias)
    return out


@router.get("/{cliente_id}/guias")
def get_guias_cliente(
    cliente_id: str,
    anio: Optional[List[int]] = Query(None),
    mes: Optional[List[int]] = Query(None),
):
    """Informe de guías por cliente: contexto del vehículo (Mongo media milla)
    + estado/fecha_entrega/fecha_digitalizacion (PostgreSQL informe_guias_tms).

    Misma base que /cajas (mismo filtro de fecha, NIT antes del dedup por
    ``consecutivo_vehiculo``), pero en vez de agregar cajas expone por vehículo
    la ``planilla_siscore`` — que puede traer varias guías separadas por coma —
    y hace JOIN en Python contra Postgres por ``guia``. Guías sin match en
    Postgres se devuelven con estado/fechas en None (el frontend pinta «—»).
    """
    cliente = CLIENTES.get(cliente_id)
    if not cliente:
        raise HTTPException(status_code=404, detail=f"Cliente no registrado: {cliente_id}")

    try:
        pipeline = [
            {"$match": _filtro_media_milla(anio or [], mes or [])},
            {"$match": cliente["match_media_milla"]()},
            # Orden por fecha desc ANTES del $group: hace determinista el $first
            # de planilla_siscore (toma el doc más reciente del vehículo).
            {"$sort": {"fecha_creacion": -1}},
            {"$group": {
                "_id": "$consecutivo_vehiculo",
                "fecha_creacion": {"$first": "$fecha_creacion"},
                "cajas": {"$first": _num("total_cajas_vehiculo")},
                "planilla": {"$first": {"$ifNull": ["$planilla_siscore", ""]}},
                # destino crudo; se normaliza (trim/upper) en Python — esta
                # versión de Atlas no acepta $ifNull dentro de $trim.
                "destino": {"$first": "$destino"},
            }},
            {"$sort": {"fecha_creacion": -1}},
            {"$limit": MAX_VEHICULOS},
        ]
        vehiculos = list(col_completados.aggregate(pipeline, allowDiskUse=True))

        # Explotar planilla_siscore (multi-guía por coma) → una fila por guía,
        # con dedup global: si una guía apareciera en dos vehículos, gana la
        # primera aparición (los vehículos ya vienen ordenados fecha desc).
        filas = []
        vistas = set()
        for v in vehiculos:
            for guia in _split_planillas(v.get("planilla")):
                if guia in vistas:
                    continue
                vistas.add(guia)
                filas.append({
                    "guia": guia,
                    "consecutivo_vehiculo": v["_id"],
                    "fecha_creacion": _fecha_iso(v.get("fecha_creacion")),
                    "cajas_vehiculo": v.get("cajas") or 0,
                    "destino": str(v.get("destino") or "").strip().upper(),
                })

        # Orden estable compuesto: fecha desc, guía asc.
        filas.sort(key=lambda f: f["guia"])
        filas.sort(key=lambda f: f["fecha_creacion"] or "", reverse=True)

        truncada = len(filas) > MAX_FILAS
        filas = filas[:MAX_FILAS]

        # JOIN con Postgres (degradación elegante: {} → filas sin estado).
        info = consultar_guias([f["guia"] for f in filas]) if filas else {}
        advertencia = None
        if filas and not info:
            advertencia = "Estado de guías no disponible en este momento (TMS)"

        entregadas = en_proceso = sin_info = 0
        ot_cumplen = ot_no_cumplen = ot_no_evaluables = 0
        por_estado = {}
        promesas = _mapa_promesa_destinos()  # {DESTINO: dias} desde tarifas FUNZA
        for f in filas:
            dato = info.get(f["guia"])
            estado = (dato or {}).get("estado")
            if estado:
                f["estado"] = estado
                f["fecha_entrega"] = _fecha_iso(dato.get("fecha_entrega"))
                f["fecha_digitalizacion"] = _fecha_iso(dato.get("fecha_digitalizacion"))
                # fecha_cita viaja cruda (TEXT en PG: aún puede traer basura).
                f["fecha_cita"] = dato.get("fecha_cita")
                f["destinatario"] = dato.get("destinatario")
                f["fecha_emision"] = _fecha_iso(dato.get("fecha_emision"))
                por_estado[estado] = por_estado.get(estado, 0) + 1
                if estado.upper() == ESTADO_ENTREGADO:
                    entregadas += 1
                else:
                    en_proceso += 1
            else:
                f["estado"] = None
                f["fecha_entrega"] = None
                f["fecha_digitalizacion"] = None
                f["fecha_cita"] = None
                f["destinatario"] = None
                f["fecha_emision"] = None
                sin_info += 1

            # ── On Time ──
            # Solo guías ENTREGADO con fecha de entrega son evaluables.
            f_ot = None
            f_fecha_promesa = None
            origen_ot = None
            f_dias_habiles = None
            if estado and estado.upper() == ESTADO_ENTREGADO and f["fecha_entrega"]:
                f_inicial = (_fecha_servible(f.get("fecha_emision"))
                             or _fecha_servible(f.get("fecha_creacion")))
                entrega = _fecha_servible(f["fecha_entrega"])
                cita = _fecha_servible(f.get("fecha_cita"))
                if f_inicial and entrega:
                    # Fecha promesa: la CITA manda si es servible; si no,
                    # inicial + promesa del destino (días hábiles).
                    if cita:
                        f_fecha_promesa = cita.isoformat()
                        origen_ot = "CITA"
                    else:
                        dias_promesa = promesas.get((f.get("destino") or "").strip().upper())
                        if dias_promesa:
                            f_fecha_promesa = _sumar_dias_habiles(
                                f_inicial, dias_promesa
                            ).isoformat()
                            origen_ot = "PROMESA"
                    if f_fecha_promesa:
                        f_dias_habiles = _dias_habiles_entre(f_inicial, entrega)
                        f_ot = 1 if entrega <= _fecha_servible(f_fecha_promesa) else 0

            f["fecha_promesa"] = f_fecha_promesa
            f["origen_promesa"] = origen_ot  # 'CITA' | 'PROMESA' | None
            f["dias_habiles"] = f_dias_habiles
            f["ot"] = f_ot
            if f_ot == 1:
                ot_cumplen += 1
            elif f_ot == 0:
                ot_no_cumplen += 1
            else:
                ot_no_evaluables += 1

        ot_evaluables = ot_cumplen + ot_no_cumplen
        resumen = {
            "total_vehiculos": len({f["consecutivo_vehiculo"] for f in filas}),
            "total_guias": len(filas),
            "entregadas": entregadas,
            "en_proceso": en_proceso,
            "sin_info": sin_info,
            "por_estado": dict(sorted(por_estado.items(), key=lambda kv: -kv[1])),
            "ot_cumplen": ot_cumplen,
            "ot_no_cumplen": ot_no_cumplen,
            "ot_no_evaluables": ot_no_evaluables,
            "ot_pct": round(ot_cumplen / ot_evaluables * 100, 1) if ot_evaluables else None,
            "truncada": truncada,
        }

        return {
            "success": True,
            "data": {
                "cliente": cliente["nombre"],
                "filas": filas,
                "resumen": resumen,
                "advertencia": advertencia,
            },
        }
    except Exception as e:
        logger.exception(f"[indicadores-cliente] Error en guías de {cliente_id}")
        raise HTTPException(status_code=500, detail=str(e))
