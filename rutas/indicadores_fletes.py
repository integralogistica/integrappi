# archivo: rutas/indicadores_fletes.py
"""
Indicadores de Fletes.

A diferencia de `indicadores_transporte.py` (que lee PostgreSQL), este módulo lee
MongoDB (`pedidos_medical_historico`) y se enfoca en el dinero: flete cobrado vs
teórico, diferencia, recargos y distribución por cliente/ruta/regional/tipo de veh.

Nota: aunque la colección se llame `pedidos_medical_historico`, allí se guarda la
información de TODOS los clientes (no solo Medical Care); por eso el módulo es
multi-cliente.

Fecha eje: `fecha_movimiento_historico` (fecha en que la planilla se completa y se
factura — pedido Vulcano asignado). Es la "fecha de realización del flete", está
garantizada en todos los docs del histórico y es la misma que usa /siscore/historico.
El servidor corre en UTC y Mongo guarda los datetime como instantes UTC; para alinear
al día/mes Colombia se resta 5 h antes de extraer partes de fecha (igual que el resto
del sistema suma 5 h a los límites de los filtros).
"""

from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from datetime import datetime
import logging

# Reutiliza la conexión Mongo y los helpers de regional ya definidos en siscore_consultas.
# (main.py importa siscore_consultas antes que este módulo, así que ya está cargado.)
from rutas.siscore_consultas import (
    coleccion_historico,
    _OFFSET_COLOMBIA,
    _aplicar_filtro_regional_dropdown,
    regional_a_origen_bodega,
)

router = APIRouter(
    prefix="/indicadores-fletes",
    tags=["Indicadores Fletes"],
)
logger = logging.getLogger(__name__)

# 5 h en milisegundos (UTC-5 Colombia). Para restar a un Date en aggregation.
_MS_5H = 5 * 60 * 60 * 1000


def _num(field: str) -> dict:
    """Expresión de aggregation que convierte un campo a double de forma segura
    (0 si es nulo o no numérico). Soporta docs legacy sin el campo o con strings."""
    return {
        "$convert": {
            "input": "${}".format(field),
            "to": "double",
            "onError": 0,
            "onNull": 0,
        }
    }


def _id_no_vacio(field: str, default: str = "Sin dato") -> dict:
    """Devuelve el valor del campo si existe y no es vacío; si no, `default`.
    Evita buckets nulos o '' en los group-by."""
    return {
        "$cond": [
            {"$in": [{"$ifNull": ["${}".format(field), ""]}, [None, "", " "]]},
            default,
            "${}".format(field),
        ]
    }


def _construir_filtro(
    anio: Optional[List[int]] = None,
    mes: Optional[List[int]] = None,
    dia: Optional[List[int]] = None,
    cliente: Optional[List[str]] = None,
    regional: Optional[str] = None,
) -> dict:
    """
    Construye el dict de $match para /resumen y /detalle.
    Año → rangos UTC alineados a día Colombia (igual que /siscore/historico).
    Mes → mes de la fecha Colombia (restando 5 h antes de $month).
    Día → día del mes de la fecha Colombia (restando 5 h antes de $dayOfMonth).
    Cliente → coincide exacto sobre cliente_origen.
    Regional → helper dropdown (cubre bodega / nombre regional / código CEDI).
    """
    conds = [{"fecha_movimiento_historico": {"$exists": True, "$ne": None}}]

    if anio:
        rangos = []
        for a in anio:
            a = int(a)
            inicio = datetime(a, 1, 1) + _OFFSET_COLOMBIA
            fin = datetime(a + 1, 1, 1) + _OFFSET_COLOMBIA
            rangos.append({"fecha_movimiento_historico": {"$gte": inicio, "$lt": fin}})
        conds.append({"$or": rangos} if len(rangos) > 1 else rangos[0])

    if mes:
        meses = [int(m) for m in mes]
        conds.append({
            "$expr": {
                "$in": [
                    {"$month": {"$subtract": ["$fecha_movimiento_historico", _MS_5H]}},
                    meses,
                ]
            }
        })

    if dia:
        dias = [int(d) for d in dia]
        conds.append({
            "$expr": {
                "$in": [
                    {"$dayOfMonth": {"$subtract": ["$fecha_movimiento_historico", _MS_5H]}},
                    dias,
                ]
            }
        })

    if cliente:
        # El cliente puede estar a nivel top-level O embebido en una planilla
        # fusionada (fusion_info.datos_originales[].cliente_origen). Hay que
        # atrapar ambos sitios para que el filtro funcione con fusionadas
        # multi-cliente; si no, una fusionada cuyo cliente solo vive en los
        # originales nunca se filtraría.
        clientes_in = list(cliente)
        conds.append({"$or": [
            {"cliente_origen": {"$in": clientes_in}},
            {"fusion_info.datos_originales.cliente_origen": {"$in": clientes_in}},
        ]})

    filtro = {"$and": conds} if len(conds) > 1 else conds[0]

    if regional:
        _aplicar_filtro_regional_dropdown(filtro, regional)

    return filtro


def _normalizar_por_regional(lista: list, valor_key: str = "flete") -> list:
    """Une variantes de regional (bodega YUMBO / nombre CALI / código CEDI) en una
    sola bucket por bodega de origen, sumando el valor (`valor_key`) y despachos."""
    out = {}
    for item in lista:
        nombre = regional_a_origen_bodega(item.get("regional")) or item.get("regional") or ""
        nombre = str(nombre).strip().upper() or "SIN REGIONAL"
        bucket = out.setdefault(nombre, {"regional": nombre, valor_key: 0.0, "despachos": 0})
        bucket[valor_key] += float(item.get(valor_key) or 0)
        bucket["despachos"] += int(item.get("despachos") or 0)
    return sorted(out.values(), key=lambda x: x.get(valor_key, 0), reverse=True)


def _expr_clientes_expandidos() -> dict:
    """
    Expresión de aggregation que, por cada documento del histórico, devuelve un
    ARRAY de sub-docs ``{cliente, flete, sobrecosto}`` para alimentar group-by
    por cliente tras expandir las planillas fusionadas:

    - **Doc fusionado** con ``fusion_info.datos_originales`` no vacío: un sub-doc
      por cada planilla original, con el ``cliente_origen`` del original y el
      ``total_solicitado`` (flete) y la ``diferencia`` (sobrecosto) del doc
      fusionado **repartidos proporcionalmente por piezas (cajas)**. Es la misma
      política con la que se facturan los Excel de aprobados/gastos
      (``_repartir_flete``); aquí sin residuo exacto porque a nivel indicador la
      pérdida de redondeo (unos COP) es despreciable frente a cifras en millones.
    - **Doc normal**: un único sub-doc con los valores top-level.

    Pensado para usarse como ``{"$project": {"_exp": _expr_clientes_expandidos()}}``
    seguido de ``$unwind`` + ``$group`` por ``$_exp.cliente``.
    """
    da = "$fusion_info.datos_originales"  # atajo de lectura
    piezas_o = {"$convert": {"input": "$$o.piezas", "to": "double", "onError": 0, "onNull": 0}}
    total_piezas = {
        # Suma de piezas de los originales (como double, tolerando strings).
        "$sum": {
            "$map": {
                "input": {"$ifNull": [da, []]},
                "as": "x",
                "in": {"$convert": {"input": "$$x.piezas", "to": "double", "onError": 0, "onNull": 0}},
            }
        }
    }
    # factor de reparto por piezas (protegido por $cond en su uso). Si total_piezas
    # es 0, se reparte equitativamente (1/n), igual que _repartir_flete del Excel.
    factor_piezas = {"$divide": [piezas_o, "$$tp"]}
    factor_eq = {"$divide": [1, "$$n"]}
    return {
        "$cond": [
            {  # ¿es fusionada con originales?
                "$and": [
                    {"$eq": [{"$ifNull": ["$fusion_info.es_fusionada", False]}, True]},
                    {"$gt": [{"$size": {"$ifNull": [da, []]}}, 0]},
                ]
            },
            {  # rama fusionada: repartir flete y sobrecosto por piezas
                "$let": {
                    "vars": {
                        "tp": total_piezas,
                        "n": {"$size": {"$ifNull": [da, []]}},
                    },
                    "in": {
                        "$map": {
                            "input": {"$ifNull": [da, []]},
                            "as": "o",
                            "in": {
                                "cliente": {"$ifNull": ["$$o.cliente_origen", "Sin cliente"]},
                                "flete": {
                                    "$cond": [
                                        {"$gt": ["$$tp", 0]},
                                        {"$multiply": [_num("total_solicitado"), factor_piezas]},
                                        {"$multiply": [_num("total_solicitado"), factor_eq]},
                                    ]
                                },
                                "sobrecosto": {
                                    "$cond": [
                                        {"$gt": ["$$tp", 0]},
                                        {"$multiply": [_num("diferencia"), factor_piezas]},
                                        {"$multiply": [_num("diferencia"), factor_eq]},
                                    ]
                                },
                            },
                        }
                    },
                }
            },
            {  # rama no fusionada: un sub-doc con los valores top-level
                "$cond": [
                    {"$in": [{"$ifNull": ["$cliente_origen", ""]}, [None, "", " "]]},
                    [{"cliente": "Sin cliente", "flete": _num("total_solicitado"), "sobrecosto": _num("diferencia")}],
                    [{"cliente": "$cliente_origen", "flete": _num("total_solicitado"), "sobrecosto": _num("diferencia")}],
                ]
            },
        ]
    }


@router.get("/resumen")
def get_resumen_fletes(
    anio: Optional[List[int]] = Query(None),
    mes: Optional[List[int]] = Query(None),
    dia: Optional[List[int]] = Query(None),
    cliente: Optional[List[str]] = Query(None),
    regional: Optional[str] = Query(None),
):
    """
    Agregación principal de Fletes. Un $match + $facet con todas las series y
    desgloses en una sola pasada.
    """
    try:
        filtro = _construir_filtro(anio, mes, dia, cliente, regional)

        # porCliente expande las fusionadas (un sub-doc por planilla original con
        # flete/sobrecosto repartidos por piezas). Si hay filtro de cliente, tras
        # expandir se descartan los compañeros de fusión NO elegidos, así el gráfico
        # "Flete por cliente" muestra solo el/los cliente(s) seleccionado(s) (con su
        # porción atribuida en fusionadas multi-cliente). Las series/KPIs siguen a
        # nivel planilla, así que su total puede diferir del de este gráfico cuando
        # hay fusionadas multi-cliente (decisión de producto 2026-07-06).
        por_cliente_stages = [
            {"$project": {"_exp": _expr_clientes_expandidos()}},
            {"$unwind": "$_exp"},
        ]
        if cliente:
            por_cliente_stages.append(
                {"$match": {"_exp.cliente": {"$in": [str(c) for c in cliente]}}}
            )
        por_cliente_stages += [
            {"$group": {
                "_id": "$_exp.cliente",
                "flete": {"$sum": "$_exp.flete"},
                "sobrecosto": {"$sum": "$_exp.sobrecosto"},
                "despachos": {"$sum": 1},
            }},
            {"$sort": {"sobrecosto": -1}},
            {"$limit": 12},
            {"$project": {"_id": 0, "cliente": "$_id", "flete": 1, "sobrecosto": 1, "despachos": 1}},
        ]

        pipeline = [
            {"$match": filtro},
            {"$facet": {
                # --- KPIs totales ---
                "kpi": [
                    {"$group": {
                        "_id": None,
                        "flete_cobrado": {"$sum": _num("total_solicitado")},
                        "flete_teorico": {"$sum": _num("tarifa_calculada")},
                        "diferencia": {"$sum": _num("diferencia")},
                        "toneladas": {"$sum": {"$divide": [_num("peso_real"), 1000]}},
                        "piezas": {"$sum": _num("piezas")},
                        "despachos": {"$sum": 1},
                        "con_diferencia_positiva": {
                            "$sum": {"$cond": [{"$gt": [_num("diferencia"), 0]}, 1, 0]}
                        },
                        "descargue": {"$sum": _num("requiere_descargue")},
                        "punto_adicional": {"$sum": _num("punto_adicional")},
                        "desvio": {"$sum": _num("desvio")},
                        "aforo": {"$sum": _num("aforo")},
                    }}
                ],
                # --- Serie mensual: flete cobrado vs teórico (mes Colombia) ---
                "serieMensual": [
                    {"$group": {
                        "_id": {
                            "$dateToString": {
                                "format": "%Y-%m",
                                "date": {"$subtract": ["$fecha_movimiento_historico", _MS_5H]},
                            }
                        },
                        "cobrado": {"$sum": _num("total_solicitado")},
                        "teorico": {"$sum": _num("tarifa_calculada")},
                        "despachos": {"$sum": 1},
                    }},
                    {"$sort": {"_id": 1}},
                    {"$project": {"_id": 0, "mes": "$_id", "cobrado": 1, "teorico": 1, "despachos": 1}},
                ],
                # --- Serie diaria: flete cobrado vs teórico (día Colombia) ---
                # Para el gráfico "Flete facturado" en vista diaria (sobrecosto/ahorro).
                "serieDiaria": [
                    {"$group": {
                        "_id": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": {"$subtract": ["$fecha_movimiento_historico", _MS_5H]},
                            }
                        },
                        "cobrado": {"$sum": _num("total_solicitado")},
                        "teorico": {"$sum": _num("tarifa_calculada")},
                        "despachos": {"$sum": 1},
                    }},
                    {"$sort": {"_id": 1}},
                    {"$project": {"_id": 0, "fecha": "$_id", "cobrado": 1, "teorico": 1, "despachos": 1}},
                ],
                # --- Flete y sobrecosto por cliente (top 12) ---
                # Stages construidos arriba (por_cliente_stages): expande fusionadas
                # repartiendo por piezas y, si hay filtro de cliente, deja solo los
                # clientes elegidos (descarta compañeros de fusión no seleccionados).
                "porCliente": por_cliente_stages,
                # --- Sobrecosto por ruta (top 10, solo planillas con diferencia > 0) ---
                "porRuta": [
                    {"$match": {"$expr": {"$gt": [_num("diferencia"), 0]}}},
                    {"$group": {
                        "_id": _id_no_vacio("ruta", "Sin ruta"),
                        "sobrecosto": {"$sum": _num("diferencia")},
                        "despachos": {"$sum": 1},
                    }},
                    {"$sort": {"sobrecosto": -1}},
                    {"$limit": 10},
                    {"$project": {"_id": 0, "ruta": "$_id", "sobrecosto": 1, "despachos": 1}},
                ],
                # --- Flete por tipo de vehículo (tipo_veh_sicetac con fallback tipo_vehiculo) ---
                "porTipoVeh": [
                    {"$group": {
                        "_id": _id_no_vacio_tipo_veh(),
                        "flete": {"$sum": _num("total_solicitado")},
                        "despachos": {"$sum": 1},
                        "toneladas": {"$sum": {"$divide": [_num("peso_real"), 1000]}},
                    }},
                    {"$sort": {"flete": -1}},
                    {"$project": {"_id": 0, "tipo_vehiculo": "$_id", "flete": 1, "despachos": 1, "toneladas": 1}},
                ],
                # --- Flete por regional (se normaliza en Python tras la consulta) ---
                "porRegionalRaw": [
                    {"$group": {
                        "_id": _id_no_vacio("regional", "Sin regional"),
                        "flete": {"$sum": _num("total_solicitado")},
                        "despachos": {"$sum": 1},
                    }},
                    {"$project": {"_id": 0, "regional": "$_id", "flete": 1, "despachos": 1}},
                ],
                # --- Sobrecosto por regional (solo planillas con diferencia > 0) ---
                "sobrecostoPorRegionalRaw": [
                    {"$match": {"$expr": {"$gt": [_num("diferencia"), 0]}}},
                    {"$group": {
                        "_id": _id_no_vacio("regional", "Sin regional"),
                        "sobrecosto": {"$sum": _num("diferencia")},
                        "despachos": {"$sum": 1},
                    }},
                    {"$project": {"_id": 0, "regional": "$_id", "sobrecosto": 1, "despachos": 1}},
                ],
                # --- Causales de sobrecosto: group-by del campo `causal` (texto que se
                # registra al derivar a Coordinador/Control) sobre planillas con diferencia>0
                # y causal no vacío. Sub-pipeline con su propio $match (válido en $facet).
                "causalesSobrecosto": [
                    {"$match": {"$and": [
                        {"$expr": {"$gt": [_num("diferencia"), 0]}},
                        {"causal": {"$exists": True, "$nin": [None, "", " "]}},
                    ]}},
                    {"$group": {
                        "_id": "$causal",
                        "cantidad": {"$sum": 1},
                        "sobrecosto": {"$sum": _num("diferencia")},
                    }},
                    {"$sort": {"sobrecosto": -1}},
                    {"$project": {"_id": 0, "causal": "$_id", "cantidad": 1, "sobrecosto": 1}},
                ],
            }},
        ]

        res = list(coleccion_historico.aggregate(pipeline))
        res = res[0] if res else {}

        kpi_doc = res.get("kpi", [None])
        kpi = kpi_doc[0] if kpi_doc else {}
        if not kpi:
            # Conjunto vacío: devolver ceros para que el frontend no rompa.
            kpi = {
                "flete_cobrado": 0, "flete_teorico": 0, "diferencia": 0, "toneladas": 0,
                "piezas": 0, "despachos": 0, "con_diferencia_positiva": 0,
                "descargue": 0, "punto_adicional": 0, "desvio": 0, "aforo": 0,
            }

        # Derivados
        despachos = kpi.get("despachos") or 0
        flete_cobrado = kpi.get("flete_cobrado") or 0
        flete_teorico = kpi.get("flete_teorico") or 0
        kpi["ticket_promedio"] = round(flete_cobrado / despachos) if despachos else 0
        kpi["pct_sobre_teorico"] = round((flete_cobrado / flete_teorico) * 100, 1) if flete_teorico else 0
        kpi["pct_con_diferencia_positiva"] = round((kpi.get("con_diferencia_positiva") / despachos) * 100, 1) if despachos else 0

        # Recargos como dict para el gráfico de composición
        recargos = {
            "Descargue": round(kpi.get("descargue") or 0),
            "Punto adicional": round(kpi.get("punto_adicional") or 0),
            "Desvío": round(kpi.get("desvio") or 0),
            "Aforo": round(kpi.get("aforo") or 0),
        }

        # Redondear moneda/totales a enteros (COP)
        for clave in ("flete_cobrado", "flete_teorico", "diferencia", "piezas"):
            kpi[clave] = round(kpi.get(clave) or 0)
        kpi["toneladas"] = round(kpi.get("toneladas") or 0, 2)

        # --- Listas para selectores (sin el filtro de su propio campo) ---
        # Años disponibles: sobre todo el histórico.
        anios_disponibles = sorted(
            (d["_id"] for d in coleccion_historico.aggregate([
                {"$match": {"fecha_movimiento_historico": {"$exists": True, "$ne": None}}},
                {"$group": {"_id": {"$year": {"$subtract": ["$fecha_movimiento_historico", _MS_5H]}}}},
            ]) if d.get("_id") is not None),
            reverse=True,
        )

        # Clientes disponibles: respeta año/mes/regional pero NO el filtro de
        # cliente. Se construye sobre la MISMA expansión de fusionadas para que
        # cada cliente sea único (sin comas) aun cuando varias planillas hayan
        # sido fusionadas bajo un mismo consecutivo.
        filtro_sin_cliente = _construir_filtro(anio, mes, dia, None, regional)
        clientes_doc = coleccion_historico.aggregate([
            {"$match": filtro_sin_cliente},
            {"$project": {"_exp": _expr_clientes_expandidos()}},
            {"$unwind": "$_exp"},
            {"$group": {"_id": "$_exp.cliente"}},
        ])
        _clientes = set()
        for d in clientes_doc:
            nombre = d.get("_id")
            if not nombre:
                continue
            nombre = str(nombre).strip()
            if nombre and nombre.upper() != "SIN CLIENTE":
                _clientes.add(nombre)
        clientes_disponibles = sorted(_clientes)

        # Normalizar regional (une variantes bodega/nombre/código)
        por_regional = _normalizar_por_regional(res.get("porRegionalRaw", []))

        # Sobrecosto por regional (mismo normalizado, sumando sobrecosto en vez de flete)
        sobrecosto_por_regional = _normalizar_por_regional(res.get("sobrecostoPorRegionalRaw", []), valor_key="sobrecosto")
        for r in sobrecosto_por_regional:
            r["sobrecosto"] = round(r.get("sobrecosto") or 0)

        # Causales de sobrecosto: agrupadas por el campo `causal` (motivo registrado al
        # derivar la planilla a Coordinador/Control). Solo planillas con diferencia>0.
        causales_sobrecosto = [
            {
                "causal": c.get("causal") or "Sin causal",
                "cantidad": int(c.get("cantidad") or 0),
                "sobrecosto": round(c.get("sobrecosto") or 0),
            }
            for c in res.get("causalesSobrecosto", [])
        ]

        # porCliente: redondear flete/sobrecosto a enteros COP (vienen repartidos
        # por piezas desde la agregación).
        por_cliente = [
            {
                "cliente": c.get("cliente") or "Sin cliente",
                "flete": round(c.get("flete") or 0),
                "sobrecosto": round(c.get("sobrecosto") or 0),
                "despachos": int(c.get("despachos") or 0),
            }
            for c in res.get("porCliente", [])
        ]

        return {
            "success": True,
            "data": {
                "kpis": kpi,
                "recargos": recargos,
                "serieMensual": res.get("serieMensual", []),
                "serieDiaria": res.get("serieDiaria", []),
                "porCliente": por_cliente,
                "porRuta": res.get("porRuta", []),
                "porTipoVeh": res.get("porTipoVeh", []),
                "porRegional": por_regional,
                "sobrecostoPorRegional": sobrecosto_por_regional,
                "causalesSobrecosto": causales_sobrecosto,
                "anios": anios_disponibles,
                "clientes": clientes_disponibles,
            },
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        logger.error(f"[FLETES] Error en /resumen: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _id_no_vacio_tipo_veh() -> dict:
    """tipo_veh_sicetac con fallback a tipo_vehiculo, y 'N/A' si ambos vacíos."""
    base = {"$ifNull": ["$tipo_veh_sicetac", "$tipo_vehiculo"]}
    return {
        "$cond": [
            {"$in": [{"$ifNull": [base, ""]}, [None, "", " "]]},
            "N/A",
            base,
        ]
    }


@router.get("/detalle")
def get_detalle_fletes(
    anio: Optional[List[int]] = Query(None),
    mes: Optional[List[int]] = Query(None),
    dia: Optional[List[int]] = Query(None),
    cliente: Optional[List[str]] = Query(None),
    regional: Optional[str] = Query(None),
):
    """
    Drill-down: planillas del período/filtros indicados. Alimenta el modal de
    detalle al hacer clic en un mes del gráfico 'teórico vs cobrado'.
    """
    try:
        filtro = _construir_filtro(anio, mes, dia, cliente, regional)

        docs = list(coleccion_historico.find(
            filtro,
            {
                "consecutivo": 1, "planilla": 1, "cliente_origen": 1, "ruta": 1,
                "regional": 1, "municipio_destino": 1, "tipo_veh_sicetac": 1,
                "tipo_vehiculo": 1, "peso_real": 1, "tarifa_calculada": 1,
                "total_solicitado": 1, "diferencia": 1,
                "fecha_movimiento_historico": 1, "_id": 0,
            },
        ).sort("fecha_movimiento_historico", -1).limit(2000))

        # Formatear fecha Colombia (UTC-5) y numéricos a float.
        for d in docs:
            f = d.get("fecha_movimiento_historico")
            if isinstance(f, datetime):
                d["fecha"] = (f - _OFFSET_COLOMBIA).strftime("%Y-%m-%d %H:%M")
            elif f:
                d["fecha"] = str(f).split("T")[0]
            else:
                d["fecha"] = ""
            d.pop("fecha_movimiento_historico", None)
            for k in ("peso_real", "tarifa_calculada", "total_solicitado", "diferencia"):
                v = d.get(k)
                d[k] = float(v) if isinstance(v, (int, float)) else 0
            if not d.get("tipo_veh_sicetac"):
                d["tipo_veh_sicetac"] = d.get("tipo_vehiculo") or "N/A"

        return {"success": True, "data": docs, "total": len(docs)}
    except Exception as e:
        import traceback
        traceback.print_exc()
        logger.error(f"[FLETES] Error en /detalle: {e}")
        raise HTTPException(status_code=500, detail=str(e))
