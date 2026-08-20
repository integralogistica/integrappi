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
from datetime import date, datetime
import logging

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
        por_estado = {}
        for f in filas:
            dato = info.get(f["guia"])
            estado = (dato or {}).get("estado")
            if estado:
                f["estado"] = estado
                f["fecha_entrega"] = _fecha_iso(dato.get("fecha_entrega"))
                f["fecha_digitalizacion"] = _fecha_iso(dato.get("fecha_digitalizacion"))
                por_estado[estado] = por_estado.get(estado, 0) + 1
                if estado.upper() == ESTADO_ENTREGADO:
                    entregadas += 1
                else:
                    en_proceso += 1
            else:
                f["estado"] = None
                f["fecha_entrega"] = None
                f["fecha_digitalizacion"] = None
                sin_info += 1

        resumen = {
            "total_vehiculos": len({f["consecutivo_vehiculo"] for f in filas}),
            "total_guias": len(filas),
            "entregadas": entregadas,
            "en_proceso": en_proceso,
            "sin_info": sin_info,
            "por_estado": dict(sorted(por_estado.items(), key=lambda kv: -kv[1])),
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
