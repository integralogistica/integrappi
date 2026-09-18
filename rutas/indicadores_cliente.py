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

from fastapi import APIRouter, HTTPException, Query, UploadFile, File
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
from bd.bd_cliente import bd_cliente
from bd.bd_postgres import consultar_guias

import pandas as pd
from io import BytesIO
from pymongo import UpdateOne

router = APIRouter(
    prefix="/indicadores-cliente",
    tags=["Indicadores Cliente"],
)
logger = logging.getLogger(__name__)

# Citas "plan B" (colección citas_kabi): cuando fecha_cita del TMS (Postgres)
# viene vacía o con basura, la fecha de cita digitada acá manda. Un doc por
# guía: {guia, fecha_cita (string 'YYYY-MM-DD'), actualizado_el}.
col_citas = bd_cliente["integra"]["citas_kabi"]

# Perfiles autorizados a cargar el Excel de citas.
PERFILES_CARGA_CITAS = {"ADMIN", "ANALISTA", "COORDINADOR", "CONTROL"}

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
# Un año completo de Kabi son ~12-15k guías desde el fix de planillas
# acumuladas ($addToSet) — 20k cubre todo el histórico (2025-09 → hoy).
MAX_FILAS = 20000

# Estados reales de informe_guias_tms (verificados 2026-08-20): ENTREGADO,
# PENDIENTE, "En distribucion", "CON NOVEDAD", "Transito Nacional" + basura
# ('', '0000-00-00', 'planilla normal'). Solo ENTREGADO cuenta como entregada.
ESTADO_ENTREGADO = "ENTREGADO"

# Una guía sin registro en el TMS más antigua que esto se marca ANULADA
# (regla de negocio: planillas anuladas nunca generaron guía real); más
# reciente = aún no cargada por el bot diario.
DIAS_ANULADA = 7


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
    q: Optional[str] = Query(None, description="Trazabilidad: número de guía o consecutivo de vehículo"),
):
    """Informe de guías por cliente: contexto del vehículo (Mongo media milla)
    + estado/fecha_entrega/fecha_digitalizacion (PostgreSQL informe_guias_tms).

    Misma base que /cajas (mismo filtro de fecha, NIT antes del dedup por
    ``consecutivo_vehiculo``), pero en vez de agregar cajas expone por vehículo
    la ``planilla_siscore`` — que puede traer varias guías separadas por coma —
    y hace JOIN en Python contra Postgres por ``guia``. Guías sin match en
    Postgres se devuelven con estado/fechas en None (el frontend pinta «—»).

    Con ``q`` (trazabilidad): SIN filtro de fecha, trae los vehículos que
    coincidan — por consecutivo de vehículo (regex, case-insensitive) o cuya
    ``planilla_siscore`` contenga la guía buscada. Así se ve el vehículo en
    que viajó una guía y el estado de todas sus compañeras de planilla.
    """
    cliente = CLIENTES.get(cliente_id)
    if not cliente:
        raise HTTPException(status_code=404, detail=f"Cliente no registrado: {cliente_id}")

    try:
        # ── Modo trazabilidad: match por consecutivo o por guía ──
        consulta = (q if isinstance(q, str) else "").strip()
        if consulta:
            import re as _re
            rx = _re.escape(consulta)
            cond_regex = {"$regex": rx, "$options": "i"}
            match_traza = {
                "$and": [
                    cliente["match_media_milla"](),
                    {"$or": [
                        {"consecutivo_vehiculo": cond_regex},
                        {"planilla_siscore": cond_regex},
                    ]},
                ]
            }
            pipeline = [
                {"$match": match_traza},
                {"$sort": {"fecha_creacion": -1}},
                {"$group": {
                    "_id": "$consecutivo_vehiculo",
                    "fecha_creacion": {"$first": "$fecha_creacion"},
                    "cajas": {"$first": _num("total_cajas_vehiculo")},
                    "planillas": {"$addToSet": {"$ifNull": ["$planilla_siscore", ""]}},
                    "destino": {"$first": "$destino"},
                }},
                {"$sort": {"fecha_creacion": -1}},
                {"$limit": MAX_VEHICULOS},
            ]
        else:
            pipeline = [
                {"$match": _filtro_media_milla(anio or [], mes or [])},
                {"$match": cliente["match_media_milla"]()},
                # Orden por fecha desc ANTES del $group: hace determinista el
                # $first de fecha/cajas (doc más reciente del vehículo).
                {"$sort": {"fecha_creacion": -1}},
                {"$group": {
                    "_id": "$consecutivo_vehiculo",
                    "fecha_creacion": {"$first": "$fecha_creacion"},
                    "cajas": {"$first": _num("total_cajas_vehiculo")},
                    # ⚠️ UN vehículo puede tener VARIOS docs con planilla_siscore
                    # distintas (178/322 vehículos de enero 2026): con $first se
                    # perdían ~54 guías del mes. Se acumulan TODAS y se explotan
                    # abajo (dedup global por guía).
                    "planillas": {"$addToSet": {"$ifNull": ["$planilla_siscore", ""]}},
                    # destino crudo; se normaliza (trim/upper) en Python — esta
                    # versión de Atlas no acepta $ifNull dentro de $trim.
                    "destino": {"$first": "$destino"},
                }},
                {"$sort": {"fecha_creacion": -1}},
                {"$limit": MAX_VEHICULOS},
            ]
        vehiculos = list(col_completados.aggregate(pipeline, allowDiskUse=True))

        # Explotar planilla_siscore (multi-guía por coma y multi-planilla por
        # vehículo) → una fila por guía, con dedup global: si una guía
        # apareciera en dos vehículos, gana la primera aparición (los vehículos
        # ya vienen ordenados fecha desc).
        filas = []
        vistas = set()
        for v in vehiculos:
            for planilla in v.get("planillas") or [v.get("planilla") or ""]:
                for guia in _split_planillas(planilla):
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

        # Citas plan B (citas_kabi): para guías cuya fecha_cita del TMS no es
        # servible, se busca la digitada acá (mandan sobre el cálculo por
        # promesa del destino).
        citas_kb = {}
        if filas:
            for c in col_citas.find(
                {"guia": {"$in": [f["guia"] for f in filas]}},
                {"guia": 1, "fecha_cita": 1},
            ):
                if c.get("fecha_cita"):
                    citas_kb[c["guia"]] = c["fecha_cita"]
        advertencia = None
        if filas and not info:
            advertencia = "Estado de guías no disponible en este momento (TMS)"

        entregadas = en_proceso = sin_info = anuladas = 0
        ot_cumplen = ot_no_cumplen = ot_no_evaluables = 0
        por_estado = {}
        promesas = _mapa_promesa_destinos()  # {DESTINO: dias} desde tarifas FUNZA
        # Corte para clasificar "sin info": una guía sin registro en el TMS con
        # más de DIAS_ANULADA días de creada es ANULADA (nunca llegó a guía
        # real); más reciente que eso probablemente aún no ha sido cargada por
        # el bot diario.
        corte_anulada = (datetime.utcnow() - timedelta(days=DIAS_ANULADA)).date()
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
                # Regla de negocio: sin registro TMS y antigua → anulada.
                f_creacion = _fecha_servible(f.get("fecha_creacion"))
                if f_creacion and f_creacion < corte_anulada:
                    f["estado"] = "ANULADA"
                    anuladas += 1
                else:
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
                    # inicial + promesa del destino (días hábiles). La cita
                    # puede venir del TMS (PG) o del plan B (citas_kabi).
                    if not cita:
                        cita_kb = citas_kb.get(f["guia"])
                        if cita_kb:
                            # viene como string 'YYYY-MM-DD' desde citas_kabi
                            cita = cita_kb if isinstance(cita_kb, date) else _fecha_servible(str(cita_kb)[:10])
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
            "anuladas": anuladas,
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


# ── Uso de vehículos por tipo solicitado ─────────────────────────────────────
# % de uso de cada Veh Solicitado (tipo_vehiculo_sicetac) = kg REALES del
# vehículo (total_kilos_vehiculo) / tope de kg de la categoría solicitada.
# La tabla de categorías es la de la operación (CARRY ≤1.000 … TRACTOMULA
# >17.000); TRACTOMULA no tiene tope natural → 34.000 kg (máxima capacidad
# legal, configuración 6 ejes) como referencia. uso_pct puede superar 100:
# viajaron más kg de los que el tipo solicitado admite.

TOPES_TIPO_VEH = {
    "CARRY": 1000,
    "NHR": 2300,
    "TURBO": 4500,
    "NIES": 6100,
    "SENCILLO": 9000,
    "PATINETA": 17000,
    "TRACTOMULA": 34000,
}


def _tipo_solicitado(valor) -> str:
    """'SENCILLO_…' → 'SENCILLO' (split por '_', como en los Excel); vacío o
    desconocido → 'SIN TIPO' (sin tope → uso None)."""
    t = str(valor or "").strip().upper().split("_")[0]
    return t if t in TOPES_TIPO_VEH else "SIN TIPO"


def _categoria_por_kilos(kg) -> str:
    """Categoría que corresponde a un peso REAL según la tabla de la operación."""
    k = float(kg or 0)
    if k <= 1000:
        return "CARRY"
    if k <= 2300:
        return "NHR"
    if k <= 4500:
        return "TURBO"
    if k <= 6100:
        return "NIES"
    if k <= 9000:
        return "SENCILLO"
    if k <= 17000:
        return "PATINETA"
    return "TRACTOMULA"


@router.get("/{cliente_id}/uso-vehiculos")
def get_uso_vehiculos_cliente(
    cliente_id: str,
    anio: Optional[List[int]] = Query(None),
    mes: Optional[List[int]] = Query(None),
    q: Optional[str] = Query(None, description="Trazabilidad: número de pedido Vulcano (numero_pedido)"),
):
    """% de uso de los vehículos solicitados, UNA FILA POR VEHÍCULO (media milla).

    Misma base que /cajas (filtro por ``fecha_creacion`` + NIT del cliente ANTES
    del dedup por ``consecutivo_vehiculo``). El drill-down (tipo → período →
    destino → consecutivo) lo arma el frontend con estas filas — son pocos
    cientos de vehículos por período, igual patrón que el informe de guías.

    Con ``q`` (trazabilidad): SIN filtro de fecha, trae los vehículos con un
    pedido Vulcano (``numero_pedido`` — así se guarda en esta colección el
    número que llega del Excel Vulcano) que coincida (regex, case-insensitive).
    """
    cliente = CLIENTES.get(cliente_id)
    if not cliente:
        raise HTTPException(status_code=404, detail=f"Cliente no registrado: {cliente_id}")

    try:
        consulta = (q if isinstance(q, str) else "").strip()
        if consulta:
            import re as _re
            match_veh = {
                "$and": [
                    cliente["match_media_milla"](),
                    {"numero_pedido": {"$regex": _re.escape(consulta), "$options": "i"}},
                ]
            }
        else:
            match_veh = {"$and": [
                _filtro_media_milla(anio or [], mes or []),
                cliente["match_media_milla"](),
            ]}
        pipeline = [
            {"$match": match_veh},
            # Orden por fecha desc antes del $group: determina el $first.
            {"$sort": {"fecha_creacion": -1}},
            {"$group": {
                "_id": "$consecutivo_vehiculo",
                "fecha_creacion": {"$first": "$fecha_creacion"},
                "tipo_sic": {"$first": "$tipo_vehiculo_sicetac"},
                "tipo_sug": {"$first": "$tipo_vehiculo"},
                "kilos": {"$first": _num("total_kilos_vehiculo")},
                # Costo real del vehículo (Total Solicitado: flete+desvío+
                # puntos+cargue). En pedidos_completados el campo es
                # total_flete_vehiculo (costo_real_vehiculo no existe aquí);
                # $max cae al que tenga valor en docs de otros flujos.
                "costo": {"$first": {"$max": [_num("costo_real_vehiculo"), _num("total_flete_vehiculo")]}},
                "costo_teorico": {"$first": _num("costo_teorico_vehiculo")},
                # Sobrecosto = costo_real − costo_teorico (>0 sobrecosto,
                # <0 ahorro — mismo campo diferencia_flete de PedidosCompletados).
                "sobrecosto": {"$first": _num("diferencia_flete")},
                "destino": {"$first": "$destino"},
            }},
            {"$sort": {"fecha_creacion": -1}},
            {"$limit": MAX_VEHICULOS},
        ]
        vehiculos = list(col_completados.aggregate(pipeline, allowDiskUse=True))

        # Desglose por PEDIDO del vehículo (lo que muestra el "+" de
        # PedidosCompletados): destinatario (ubicacion_descargue), entrega
        # (planilla_siscore — puede traer varias guías por coma) y kilos por
        # pedido. Los docs son UNO POR PEDIDO y el $match de NIT (Kabi) solo
        # deja pasar los de Kabi — los pedidos de OTROS clientes en el mismo
        # vehículo se consultan aparte (mismo consecutivo, SIN filtro de NIT)
        # para que el detalle muestre TODO lo que llevaba el vehículo.
        pedidos_por_veh: dict = {}
        ids = [v["_id"] for v in vehiculos]
        if ids:
            pipeline_pedidos = [
                {"$match": {"consecutivo_vehiculo": {"$in": ids}}},
                {"$lookup": {
                    "from": "clientes",
                    "localField": "nit_cliente",
                    "foreignField": "nit",
                    "as": "cliente",
                }},
                {"$unwind": {"path": "$cliente", "preserveNullAndEmptyArrays": True}},
                {"$group": {
                    "_id": "$consecutivo_vehiculo",
                    "pedidos": {"$push": {
                        "pedido": {"$ifNull": ["$consecutivo_integrapp", ""]},
                        "pedido_vulcano": {"$ifNull": ["$numero_pedido", ""]},
                        "destinatario": {"$ifNull": ["$ubicacion_descargue", ""]},
                        "destino_real": {"$ifNull": ["$destino_real", ""]},
                        "cliente": {"$ifNull": ["$cliente.nombre", ""]},
                        "entrega": {"$ifNull": ["$planilla_siscore", ""]},
                        "kilos": {"$ifNull": [_num("num_kilos"), 0]},
                    }},
                }},
            ]
            for g in col_completados.aggregate(pipeline_pedidos, allowDiskUse=True):
                lst = []
                for p in g.get("pedidos") or []:
                    lst.append({
                        "pedido": str(p.get("pedido") or "").strip(),
                        "pedido_vulcano": str(p.get("pedido_vulcano") or "").strip(),
                        "destinatario": str(p.get("destinatario") or "").strip(),
                        "destino_real": str(p.get("destino_real") or "").strip(),
                        "cliente": str(p.get("cliente") or "").strip(),
                        "entrega": str(p.get("entrega") or "").strip(),
                        "kilos": round(float(p.get("kilos") or 0), 1),
                    })
                # Por peso desc: lo más pesado del vehículo primero.
                lst.sort(key=lambda x: -x["kilos"])
                pedidos_por_veh[g["_id"]] = lst

        filas = []
        for v in vehiculos:
            tipo = _tipo_solicitado(v.get("tipo_sic") or v.get("tipo_sug"))
            kg = float(v.get("kilos") or 0)
            tope = TOPES_TIPO_VEH.get(tipo)
            filas.append({
                "consecutivo_vehiculo": v["_id"],
                "fecha": _fecha_iso(v.get("fecha_creacion")),
                "tipo_solicitado": tipo,
                "kg_reales": round(kg, 1),
                "destino": str(v.get("destino") or "").strip().upper(),
                "categoria_real": _categoria_por_kilos(kg),
                "uso_pct": round(kg / tope * 100, 1) if tope else None,
                "costo_vehiculo": round(float(v.get("costo") or 0), 0),
                "costo_teorico": round(float(v.get("costo_teorico") or 0), 0),
                "sobrecosto": round(float(v.get("sobrecosto") or 0), 0),
                "pedidos": pedidos_por_veh.get(v["_id"], []),
            })

        return {
            "success": True,
            "data": {
                "cliente": cliente["nombre"],
                "filas": filas,
                "topes": TOPES_TIPO_VEH,
            },
        }
    except Exception as e:
        logger.exception(f"[indicadores-cliente] Error en uso de vehículos de {cliente_id}")
        raise HTTPException(status_code=500, detail=str(e))


# ── Carga de citas (plan B) ──────────────────────────────────────────────────

@router.post("/citas")
async def cargar_citas(archivo: UploadFile = File(...), perfil: str = Query("")):
    """Carga un Excel {GUIA, FECHA_CITA} a ``citas_kabi`` (upsert por guia).

    Plan B del OT: cuando la fecha_cita del TMS viene vacía/corrupta, la cita
    digitada acá es la que se usa como fecha promesa. Solo perfiles
    ADMIN/ANALISTA/COORDINADOR/CONTROL.
    """
    if (perfil or "").upper().strip() not in PERFILES_CARGA_CITAS:
        raise HTTPException(status_code=403, detail="Perfil no autorizado para cargar citas")

    try:
        contenido = await archivo.read()
        df = pd.read_excel(BytesIO(contenido))
        df.columns = [str(c).strip().upper().replace(" ", "_") for c in df.columns]

        if not {"GUIA", "FECHA_CITA"}.issubset(df.columns):
            raise HTTPException(
                status_code=400,
                detail="El Excel debe tener las columnas GUIA y FECHA_CITA",
            )

        ahora = datetime.utcnow()
        invalidas = 0
        errores = []
        operaciones = []
        for idx, row in df.iterrows():
            guia = str(row["GUIA"]).strip()
            fecha = _fecha_servible(_fecha_iso(row["FECHA_CITA"]))
            if not guia or guia.lower() in ("nan", "none"):
                continue
            if fecha is None:
                invalidas += 1
                if len(errores) < 10:
                    errores.append(f"Fila {idx + 2}: guia {guia or '?'} con fecha_cita inválida '{row['FECHA_CITA']}'")
                continue
            operaciones.append(UpdateOne(
                {"guia": guia},
                {"$set": {"fecha_cita": fecha.isoformat(), "actualizado_el": ahora}},
                upsert=True,
            ))

        # bulk_write por lotes: un update_one POR FILA por red a Atlas tarda
        # ~150ms → 4.000 guías = 10+ min. En lotes de 1.000 baja a segundos
        # (misma lección del execute_batch del Bot 001).
        cargadas = 0
        LOTE = 1000
        for i in range(0, len(operaciones), LOTE):
            lote = operaciones[i:i + LOTE]
            try:
                res = col_citas.bulk_write(lote, ordered=False)
                cargadas += res.upserted_count + res.modified_count
            except Exception as e:
                # BulkWriteError con ordered=False deja saber qué falló; el
                # resto del lote se aplicó.
                logger.warning(f"[indicadores-cliente] Lote de citas {i//LOTE + 1} con errores: {e}")
                cargadas += max(len(lote) - getattr(getattr(e, 'details', {}), 'get', lambda *_: 0)('nWriteErrors', 0), 0)

        return {
            "success": True,
            "data": {
                "cargadas": cargadas,
                "invalidas": invalidas,
                "errores": errores,
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("[indicadores-cliente] Error cargando citas")
        raise HTTPException(status_code=500, detail=str(e))
