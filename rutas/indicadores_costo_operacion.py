# archivo: rutas/indicadores_costo_operacion.py
"""
Indicadores de Costo de Operación.

Une el costo de las tres etapas del viaje en un solo tablero (todo en COP):

- **Media milla**  (`pedidos_completados`)     — salida desde Funza. Suma el costo
  real del vehículo ``total_flete_vehiculo`` (ya incluye flete + desvío + puntos +
  cargue/descargue; ver ``pedidos.py``). Hay varios docs por vehículo, así que se
  agrupa por ``consecutivo_vehiculo`` ANTES de sumar (si no, se sobreconta).
- **Última milla** (`pedidos_medical_historico`) — cross-docking en los CDI del país.
  Suma ``total_solicitado``.
- **Otros costos** (`historico_otros_costos`)    — costos que se conocen al cerrar la
  entrega. Suma ``valor_total``.

Las tres son piernas distintas del mismo viaje: un cliente puede aparecer en varias
y sus costos se SUMAN (no es doble conteo). No se excluye ningún cliente por fuente.

Fechas eje:
- Media milla: ``fecha_creacion`` es un string local ``YYYY-MM-DD HH:MM:SS`` (se guarda
  y filtra sin offset, ver ``pedidos.py``). Para bucket mensual/diario se extrae directo
  del prefijo del string (NO se resta 5 h).
- Última milla y Otros costos: datetime UTC; se resta 5 h antes de extraer día/mes
  (igual que el módulo Fletes).
"""

from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from datetime import datetime
import asyncio
import logging

# Reutiliza la conexión Mongo y el offset Colombia de siscore_consultas
# (main.py lo importa antes que este módulo, así que ya está cargado).
from rutas.siscore_consultas import coleccion_historico, _OFFSET_COLOMBIA
from rutas.indicadores_fletes import _expr_clientes_expandidos
from bd.bd_cliente import bd_cliente

_db = bd_cliente["integra"]
col_completados = _db["pedidos_completados"]            # media milla
col_otros = _db["historico_otros_costos"]               # otros costos
# coleccion_historico (importado) = pedidos_medical_historico  → última milla

router = APIRouter(
    prefix="/indicadores-costo-operacion",
    tags=["Indicadores Costo Operación"],
)
logger = logging.getLogger(__name__)

# 5 h en milisegundos (UTC-5 Colombia). Para restar a un Date en aggregation.
_MS_5H = 5 * 60 * 60 * 1000


def _num(field: str) -> dict:
    """Convierte un campo a double de forma segura (0 si nulo/no numérico)."""
    return {
        "$convert": {
            "input": "${}".format(field),
            "to": "double",
            "onError": 0,
            "onNull": 0,
        }
    }


# ── Filtros por fuente ──────────────────────────────────────────────────────

def _filtro_media_milla(anios: List[int], meses: List[int]) -> dict:
    """$match sobre ``fecha_creacion`` (string local) para AÑO y MES.
    El filtro de CLIENTE va aparte (necesita el $lookup a `clientes`)."""
    conds = [{"fecha_creacion": {"$exists": True, "$ne": None, "$type": "string"}}]

    if anios:
        rangos = []
        for a in anios:
            a = int(a)
            rangos.append({
                "fecha_creacion": {
                    "$gte": f"{a}-01-01 00:00:00",
                    "$lt": f"{a + 1}-01-01 00:00:00",
                }
            })
        conds.append({"$or": rangos} if len(rangos) > 1 else rangos[0])

    if meses:
        meses_int = [int(m) for m in meses]
        # Mes = caracteres 5..6 de "YYYY-MM-DD ..." → entero Colombia (string local).
        conds.append({"$expr": {"$in": [
            {"$convert": {
                "input": {"$substrCP": ["$fecha_creacion", 5, 2]},
                "to": "int", "onError": -1, "onNull": -1,
            }},
            meses_int,
        ]}})

    return {"$and": conds} if len(conds) > 1 else conds[0]


def _filtro_datetime_utc(
    campo: str,
    anios: List[int],
    meses: List[int],
    cliente_campo: Optional[str] = None,
    cliente_lista: Optional[List[str]] = None,
    cliente_tambien_en: Optional[str] = None,
) -> dict:
    """$match sobre un campo datetime UTC (resta 5 h para alinear a Colombia)."""
    conds = [{campo: {"$exists": True, "$ne": None}}]

    if anios:
        rangos = []
        for a in anios:
            a = int(a)
            inicio = datetime(a, 1, 1) + _OFFSET_COLOMBIA
            fin = datetime(a + 1, 1, 1) + _OFFSET_COLOMBIA
            rangos.append({campo: {"$gte": inicio, "$lt": fin}})
        conds.append({"$or": rangos} if len(rangos) > 1 else rangos[0])

    if meses:
        meses_int = [int(m) for m in meses]
        conds.append({"$expr": {"$in": [
            {"$month": {"$subtract": [f"${campo}", _MS_5H]}},
            meses_int,
        ]}})

    if cliente_lista and cliente_campo:
        ors = [{cliente_campo: {"$in": list(cliente_lista)}}]
        if cliente_tambien_en:
            ors.append({cliente_tambien_en: {"$in": list(cliente_lista)}})
        conds.append({"$or": ors} if len(ors) > 1 else ors[0])

    return {"$and": conds} if len(conds) > 1 else conds[0]


# ── Pipelines de series (mensual + diario) ──────────────────────────────────

def _pipeline_media_milla(anios: List[int], meses: List[int], clientes: Optional[List[str]]) -> list:
    """Media milla: lookup de nombre de cliente, dedup por vehículo, bucket por fecha_creacion."""
    pipeline = [
        {"$match": _filtro_media_milla(anios, meses)},
        # Traer el nombre del cliente por NIT (igual que listar-vehiculo-completados).
        {"$lookup": {
            "from": "clientes",
            "localField": "nit_cliente",
            "foreignField": "nit",
            "as": "_cli",
        }},
        {"$set": {"nombre_cliente": {"$ifNull": [
            {"$arrayElemAt": ["$_cli.nombre", 0]},
            {"$ifNull": ["$nombre_cliente", None]},
        ]}}},
        {"$project": {"_cli": 0}},
    ]
    if clientes:
        cl = list(clientes)
        pipeline.append({"$match": {"$or": [
            {"nombre_cliente": {"$in": cl}},
            {"nit_cliente": {"$in": cl}},
        ]}})
    # DEDUP por vehículo: el costo total_flete_vehiculo y la diferencia_flete están
    # duplicados en cada doc del vehículo; tomamos uno solo antes de sumar por período.
    pipeline += [
        {"$group": {
            "_id": "$consecutivo_vehiculo",
            "costo": {"$first": _num("total_flete_vehiculo")},
            "diferencia": {"$first": _num("diferencia_flete")},
            "fecha_creacion": {"$first": "$fecha_creacion"},
            "nombre_cliente": {"$first": {"$ifNull": ["$nombre_cliente", ""]}},
        }},
        {"$facet": {
            # Costo, sobrecosto (diferencia>0) y ahorro (diferencia<0, negativo) por mes.
            "mensual": [
                {"$group": {
                    "_id": {"$substrCP": ["$fecha_creacion", 0, 7]},
                    "media_milla": {"$sum": "$costo"},
                    "sobrecosto": {"$sum": {"$max": ["$diferencia", 0]}},
                    "ahorro": {"$sum": {"$min": ["$diferencia", 0]}},
                }},
                {"$sort": {"_id": 1}},
                {"$project": {"_id": 0, "periodo": "$_id", "media_milla": 1, "sobrecosto": 1, "ahorro": 1}},
            ],
            "diario": [
                {"$group": {
                    "_id": {"$substrCP": ["$fecha_creacion", 0, 10]},
                    "media_milla": {"$sum": "$costo"},
                    "sobrecosto": {"$sum": {"$max": ["$diferencia", 0]}},
                    "ahorro": {"$sum": {"$min": ["$diferencia", 0]}},
                }},
                {"$sort": {"_id": 1}},
                {"$project": {"_id": 0, "periodo": "$_id", "media_milla": 1, "sobrecosto": 1, "ahorro": 1}},
            ],
        }},
    ]
    return pipeline


def _facet_datetime_utc(campo: str, valor_field: str, out_key: str, dif_field: Optional[str] = None) -> dict:
    """$facet mensual+diario sobre un campo datetime UTC (resta 5 h).

    Si se pasa ``dif_field``, suma también el sobrecosto (diferencia>0) y el ahorro
    (diferencia<0, como número negativo) para el gráfico divergente."""
    def _gran(fmt: str) -> list:
        group = {
            "_id": {"$dateToString": {"format": fmt, "date": {"$subtract": [f"${campo}", _MS_5H]}}},
            out_key: {"$sum": _num(valor_field)},
        }
        project = {"_id": 0, "periodo": "$_id", out_key: 1}
        if dif_field:
            group["sobrecosto"] = {"$sum": {"$max": [_num(dif_field), 0]}}
            group["ahorro"] = {"$sum": {"$min": [_num(dif_field), 0]}}
            project["sobrecosto"] = 1
            project["ahorro"] = 1
        return [{"$group": group}, {"$sort": {"_id": 1}}, {"$project": project}]

    return {"mensual": _gran("%Y-%m"), "diario": _gran("%Y-%m-%d")}


# ── Listas para selectores ──────────────────────────────────────────────────

def _anios_media_milla() -> list:
    """Años con datos en pedidos_completados (año Colombia = prefijo del string)."""
    return [
        d["_id"] for d in col_completados.aggregate([
            {"$match": {"fecha_creacion": {"$exists": True, "$ne": None, "$type": "string"}}},
            {"$group": {"_id": {"$convert": {
                "input": {"$substrCP": ["$fecha_creacion", 0, 4]},
                "to": "int", "onError": -1, "onNull": -1,
            }}}},
        ]) if d.get("_id") and d["_id"] > 0
    ]


def _anios_datetime(col, campo: str) -> list:
    return [
        d["_id"] for d in col.aggregate([
            {"$match": {campo: {"$exists": True, "$ne": None}}},
            {"$group": {"_id": {"$year": {"$subtract": [f"${campo}", _MS_5H]}}}},
        ]) if d.get("_id")
    ]


def _clientes_media_milla(anios: List[int], meses: List[int]) -> list:
    """Nombres de cliente (vía lookup a `clientes`) en pedidos_completados, respetando año/mes."""
    docs = col_completados.aggregate([
        {"$match": _filtro_media_milla(anios, meses)},
        {"$lookup": {
            "from": "clientes", "localField": "nit_cliente",
            "foreignField": "nit", "as": "_cli",
        }},
        {"$set": {"nombre_cliente": {"$ifNull": [
            {"$arrayElemAt": ["$_cli.nombre", 0]},
            {"$ifNull": ["$nombre_cliente", None]},
        ]}}},
        {"$group": {"_id": "$nombre_cliente"}},
    ])
    out = []
    for d in docs:
        n = d.get("_id")
        if n and str(n).strip():
            out.append(str(n).strip())
    return out


def _clientes_ultima_milla(anios: List[int], meses: List[int]) -> list:
    """Clientes de la última milla expandiendo fusionadas (un nombre por cliente,
    sin comas), respetando año/mes. Reutiliza la expansión del módulo Fletes."""
    filtro = _filtro_datetime_utc("fecha_movimiento_historico", anios, meses)
    out = set()
    for d in coleccion_historico.aggregate([
        {"$match": filtro},
        {"$project": {"_exp": _expr_clientes_expandidos()}},
        {"$unwind": "$_exp"},
        {"$group": {"_id": "$_exp.cliente"}},
    ]):
        n = d.get("_id")
        if n and str(n).strip() and str(n).strip().upper() != "SIN CLIENTE":
            out.add(str(n).strip())
    return list(out)


def _clientes_otros(anios: List[int], meses: List[int]) -> list:
    filtro = _filtro_datetime_utc("created_at", anios, meses)
    out = []
    for d in col_otros.aggregate([
        {"$match": filtro},
        {"$group": {"_id": "$datos_servicio.cliente"}},
    ]):
        n = d.get("_id")
        if n and str(n).strip():
            out.append(str(n).strip())
    return out


# ── Merge de las 3 fuentes ──────────────────────────────────────────────────

def _merge_series(series: dict) -> tuple:
    """Combina las series mensual/diaria de las 3 fuentes en una lista por período.

    Cada fuente aporta sus medidas (media_milla, ultima_milla, otros_costos y, para
    media/última milla, sobrecosto y ahorro). Se suman todas por período: los costos
    por su clave propia y sobrecosto/ahorro combinan media + última milla.
    Devuelve (serieMensual, serieDiaria) ordenadas por período, con total."""
    MEDIDAS = ("media_milla", "ultima_milla", "otros_costos", "sobrecosto", "ahorro")

    def _merge(gran: str) -> list:
        merged: dict = {}
        for fuente in ("media_milla", "ultima_milla", "otros_costos"):
            for item in (series.get(fuente, {}).get(gran) or []):
                p = item.get("periodo")
                if not p:
                    continue
                bucket = merged.setdefault(p, {m: 0.0 for m in MEDIDAS})
                for m in MEDIDAS:
                    bucket[m] += float(item.get(m) or 0)
        out = []
        for p in sorted(merged):
            b = merged[p]
            for m in MEDIDAS:
                b[m] = round(b[m])
            b["periodo"] = p
            b["total"] = b["media_milla"] + b["ultima_milla"] + b["otros_costos"]
            out.append(b)
        return out

    return _merge("mensual"), _merge("diario")


async def _run_sync(func, *args):
    """Ejecuta una función bloqueante en un hilo y devuelve su resultado."""
    return await asyncio.to_thread(lambda: func(*args))


# ── Endpoint ────────────────────────────────────────────────────────────────

@router.get("/resumen")
async def get_resumen_costo_operacion(
    anio: Optional[List[int]] = Query(None),
    mes: Optional[List[int]] = Query(None),
    cliente: Optional[List[str]] = Query(None),
):
    """
    Costo total de la operación: combina media milla, última milla y otros costos.
    Devuelve serieMensual y serieDiaria (cada bucket con las 3 etapas + total) y las
    listas de años y clientes para los selectores.
    """
    try:
        anios = [int(a) for a in anio] if anio else []
        meses = [int(m) for m in mes] if mes else []
        clientes = [str(c) for c in cliente] if cliente else []

        # Filtros de última milla y otros costos.
        um_filtro = _filtro_datetime_utc(
            "fecha_movimiento_historico", anios, meses,
            "cliente_origen", clientes,
            "fusion_info.datos_originales.cliente_origen",
        )
        oc_filtro = _filtro_datetime_utc(
            "created_at", anios, meses,
            "datos_servicio.cliente", clientes,
        )

        # Las 3 series se calculan en hilos (pymongo es bloqueante) y en paralelo.
        async def _run(col, pipe):
            return await asyncio.to_thread(lambda: list(col.aggregate(pipe, allowDiskUse=True)))

        mm_res, um_res, oc_res = await asyncio.gather(
            _run(col_completados, _pipeline_media_milla(anios, meses, clientes)),
            _run(coleccion_historico, [
                {"$match": um_filtro},
                {"$facet": _facet_datetime_utc("fecha_movimiento_historico", "total_solicitado", "ultima_milla", "diferencia")},
            ]),
            _run(col_otros, [
                {"$match": oc_filtro},
                {"$facet": _facet_datetime_utc("created_at", "valor_total", "otros_costos")},
            ]),
        )

        def _facet_doc(res):
            r = res[0] if res else {}
            return {"mensual": r.get("mensual", []), "diario": r.get("diario", [])}

        serie_mensual, serie_diaria = _merge_series({
            "media_milla": _facet_doc(mm_res),
            "ultima_milla": _facet_doc(um_res),
            "otros_costos": _facet_doc(oc_res),
        })

        # Años y clientes (listas para selectores). Los años son globales (con datos);
        # los clientes respetan año/mes pero NO el filtro de cliente.
        anios_mm, anios_um, anios_oc, cli_mm, cli_um, cli_oc = await asyncio.gather(
            _run_sync(_anios_media_milla),
            _run_sync(_anios_datetime, coleccion_historico, "fecha_movimiento_historico"),
            _run_sync(_anios_datetime, col_otros, "created_at"),
            _run_sync(_clientes_media_milla, anios, meses),
            _run_sync(_clientes_ultima_milla, anios, meses),
            _run_sync(_clientes_otros, anios, meses),
        )

        anios_disponibles = sorted(
            {a for a in (anios_mm + anios_um + anios_oc) if a}, reverse=True,
        )
        clientes_disponibles = sorted(
            {c for c in (cli_mm + cli_um + cli_oc) if c and c.upper() != "SIN CLIENTE"},
        )

        return {
            "success": True,
            "data": {
                "serieMensual": serie_mensual,
                "serieDiaria": serie_diaria,
                "anios": anios_disponibles,
                "clientes": clientes_disponibles,
            },
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        logger.error(f"[COSTO_OPERACION] Error en /resumen: {e}")
        raise HTTPException(status_code=500, detail=str(e))
