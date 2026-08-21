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
  (mismo alineamiento Colombia que usa el histórico en el resto del sistema).
"""

from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from datetime import datetime
import asyncio
import logging

# Reutiliza la conexión Mongo y el offset Colombia de siscore_consultas
# (main.py lo importa antes que este módulo, así que ya está cargado).
from rutas.siscore_consultas import coleccion_historico, _OFFSET_COLOMBIA
from bd.bd_cliente import bd_cliente

_db = bd_cliente["integra"]
col_completados = _db["pedidos_completados"]            # media milla
col_otros = _db["historico_otros_costos"]               # otros costos
col_usuarios = _db["baseusuarios"]                      # cruce de perfil (analistas)
# coleccion_historico (importado) = pedidos_medical_historico  → última milla

router = APIRouter(
    prefix="/indicadores-costo-operacion",
    tags=["Indicadores Costo Operación"],
)
logger = logging.getLogger(__name__)

# 5 h en milisegundos (UTC-5 Colombia). Para restar a un Date en aggregation.
_MS_5H = 5 * 60 * 60 * 1000

# Etiquetas (nombres visibles) de las 3 etapas del viaje. Las CLAVES (media_milla,
# ultima_milla, otros_costos) son técnicas y se usan en toda la API/agregaciones: NO
# se cambian (romperían el contrato backend↔frontend). Para renombrar lo que ve el
# usuario en el tablero, editar aquí los VALORES y reiniciar el backend (Render).
# El frontend las recibe en ``data.etiquetas`` de /resumen (con fallback local).
ETAPAS = {
    "media_milla": "Fresenius kabi",
    "ultima_milla": "Última milla general",
    "otros_costos": "Otros costos",
}


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

    (Migrada desde ``indicadores_fletes.py`` cuando ese módulo se eliminó.)
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


# NIT de Fresenius Kabi (mismo valor que pedidos.py y siscore_consultas.py). Única
# fuente de verdad para detectar Kabi en media milla (por NIT).
NIT_FRESENIUS = "900402080"

# Expr Mongo: es_kabi por NIT (media milla). Bool listo para usar en $cond.
_EXPR_KABI_NIT = {"$eq": [{"$ifNull": ["$nit_cliente", ""]}, NIT_FRESENIUS]}


def _expr_kabi_nombre(campo: str) -> dict:
    """Expr Mongo: True si ``campo`` (ruta tipo 'cliente_origen' o
    'datos_servicio.cliente') es FRESENIUS KABI (insensible a mayúsculas y espacios
    laterales). Reproduce lo esencial de _es_cliente_kabi (siscore_consultas.py)."""
    return {
        "$eq": [
            {"$trim": {"input": {"$toUpper": {"$ifNull": [f"${campo}", ""]}}}},
            "FRESENIUS KABI",
        ]
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
    # total_cajas_vehiculo también vive a nivel vehículo, así que se toma igual (first).
    pipeline += [
        {"$group": {
            "_id": "$consecutivo_vehiculo",
            "costo": {"$first": _num("total_flete_vehiculo")},
            "diferencia": {"$first": _num("diferencia_flete")},
            "cajas": {"$first": _num("total_cajas_vehiculo")},
            "fecha_creacion": {"$first": "$fecha_creacion"},
            "nombre_cliente": {"$first": {"$ifNull": ["$nombre_cliente", ""]}},
        }},
        {"$facet": {
            # Costo, sobrecosto (diferencia>0) y ahorro (diferencia<0, negativo) por mes.
            # cajas_media = total_cajas_vehiculo (cajas) sumado por período.
            "mensual": [
                {"$group": {
                    "_id": {"$substrCP": ["$fecha_creacion", 0, 7]},
                    "media_milla": {"$sum": "$costo"},
                    "cajas_media": {"$sum": "$cajas"},
                    "sobrecosto": {"$sum": {"$max": ["$diferencia", 0]}},
                    "ahorro": {"$sum": {"$min": ["$diferencia", 0]}},
                }},
                {"$sort": {"_id": 1}},
                {"$project": {"_id": 0, "periodo": "$_id", "media_milla": 1, "cajas_media": 1, "sobrecosto": 1, "ahorro": 1}},
            ],
            "diario": [
                {"$group": {
                    "_id": {"$substrCP": ["$fecha_creacion", 0, 10]},
                    "media_milla": {"$sum": "$costo"},
                    "cajas_media": {"$sum": "$cajas"},
                    "sobrecosto": {"$sum": {"$max": ["$diferencia", 0]}},
                    "ahorro": {"$sum": {"$min": ["$diferencia", 0]}},
                }},
                {"$sort": {"_id": 1}},
                {"$project": {"_id": 0, "periodo": "$_id", "media_milla": 1, "cajas_media": 1, "sobrecosto": 1, "ahorro": 1}},
            ],
        }},
    ]
    return pipeline


def _facet_datetime_utc(
    campo: str,
    valor_field: str,
    out_key: str,
    dif_field: Optional[str] = None,
    cajas_field: Optional[str] = None,
    cajas_key: Optional[str] = None,
) -> dict:
    """$facet mensual+diario sobre un campo datetime UTC (resta 5 h).

    Si se pasa ``dif_field``, suma también el sobrecosto (diferencia>0) y el ahorro
    (diferencia<0, como número negativo) para el gráfico divergente.
    Si se pasa ``cajas_field`` (+ ``cajas_key``), suma la cantidad de cajas/piezas
    como ``cajas_<etapa>`` para el gráfico de líneas de cajas."""
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
        if cajas_field and cajas_key:
            group[cajas_key] = {"$sum": _num(cajas_field)}
            project[cajas_key] = 1
        return [{"$group": group}, {"$sort": {"_id": 1}}, {"$project": project}]

    return {"mensual": _gran("%Y-%m"), "diario": _gran("%Y-%m-%d")}


def _expr_ultima_milla_expandido() -> dict:
    """Expr que expande la última milla por cliente, devolviendo un ARRAY de sub-docs
    ``{cliente, costo, piezas, diferencia}`` para repartir costo/cajas/sobrecosto de
    planillas fusionadas entre los clientes originales proporcionalmente a piezas.

    - **Fusionada** con ``fusion_info.datos_originales``: un sub-doc por original, con
      ``total_solicitado`` (costo), ``piezas`` y ``diferencia`` del doc fusionado
      repartidos por las piezas de cada original (equitativo 1/n si la suma es 0).
    - **Normal**: un sub-doc con los valores top-level."""
    da = "$fusion_info.datos_originales"
    piezas_o = {"$convert": {"input": "$$o.piezas", "to": "double", "onError": 0, "onNull": 0}}
    total_piezas = {
        "$sum": {
            "$map": {
                "input": {"$ifNull": [da, []]},
                "as": "x",
                "in": {"$convert": {"input": "$$x.piezas", "to": "double", "onError": 0, "onNull": 0}},
            }
        }
    }
    factor_piezas = {"$divide": [piezas_o, "$$tp"]}
    factor_eq = {"$divide": [1, "$$n"]}

    def repartir(campo):
        """Reparte el campo top-level del doc por el factor de piezas (o equitativo)."""
        return {"$cond": [
            {"$gt": ["$$tp", 0]},
            {"$multiply": [_num(campo), factor_piezas]},
            {"$multiply": [_num(campo), factor_eq]},
        ]}

    return {
        "$cond": [
            {  # ¿es fusionada con originales?
                "$and": [
                    {"$eq": [{"$ifNull": ["$fusion_info.es_fusionada", False]}, True]},
                    {"$gt": [{"$size": {"$ifNull": [da, []]}}, 0]},
                ]
            },
            {  # rama fusionada: repartir costo, piezas y diferencia por las piezas originales
                "$let": {
                    "vars": {"tp": total_piezas, "n": {"$size": {"$ifNull": [da, []]}}},
                    "in": {
                        "$map": {
                            "input": {"$ifNull": [da, []]},
                            "as": "o",
                            "in": {
                                "cliente": {"$ifNull": ["$$o.cliente_origen", "Sin cliente"]},
                                "costo": repartir("total_solicitado"),
                                "piezas": {"$cond": [
                                    {"$gt": ["$$tp", 0]}, piezas_o,
                                    {"$multiply": [_num("piezas"), factor_eq]},
                                ]},
                                "diferencia": repartir("diferencia"),
                            },
                        }
                    },
                }
            },
            {  # rama no fusionada (envuelta en $cond: la rama no admite lista directa)
                "$cond": [
                    {"$in": [{"$ifNull": ["$cliente_origen", ""]}, [None, "", " "]]},
                    [{"cliente": "Sin cliente", "costo": _num("total_solicitado"),
                      "piezas": _num("piezas"), "diferencia": _num("diferencia")}],
                    [{"cliente": {"$ifNull": ["$cliente_origen", "Sin cliente"]},
                      "costo": _num("total_solicitado"), "piezas": _num("piezas"),
                      "diferencia": _num("diferencia")}],
                ]
            },
        ]
    }


def _facet_ultima_milla_expandido(clientes: Optional[List[str]] = None) -> dict:
    """Facet mensual+diario de última milla que EXPANDO las planillas fusionadas y
    reparte costo (total_solicitado), piezas y diferencia por cliente original. Así,
    al filtrar un cliente, las fusionadas que también llevan a otros clientes aportan
    solo la porción de ese cliente (no la fusionada completa).

    Si ``clientes`` viene, aplica un $match POST-expansión sobre el cliente expandido:
    descarta las porciones cuyo cliente no es el filtrado (los 'compañeros de fusión').
    Salida por período: ``ultima_milla`` (Σ costo), ``cajas_ultima`` (Σ piezas),
    ``sobrecosto`` (Σ diferencias>0), ``ahorro`` (Σ diferencias<0)."""
    post_match = {"$match": {"_exp.cliente": {"$in": list(clientes)}}} if clientes else None

    def _gran(fmt: str) -> list:
        stages = [
            {"$set": {"_periodo": {"$dateToString": {
                "format": fmt,
                "date": {"$subtract": ["$fecha_movimiento_historico", _MS_5H]},
            }}}},
            {"$project": {"_periodo": 1, "_exp": _expr_ultima_milla_expandido()}},
            {"$unwind": "$_exp"},
        ]
        if post_match:
            stages.append(post_match)
        stages += [
            {"$group": {
                "_id": "$_periodo",
                "ultima_milla": {"$sum": "$_exp.costo"},
                "cajas_ultima": {"$sum": "$_exp.piezas"},
                "sobrecosto": {"$sum": {"$max": ["$_exp.diferencia", 0]}},
                "ahorro": {"$sum": {"$min": ["$_exp.diferencia", 0]}},
            }},
            {"$sort": {"_id": 1}},
            {"$project": {"_id": 0, "periodo": "$_id", "ultima_milla": 1, "cajas_ultima": 1,
                          "sobrecosto": 1, "ahorro": 1}},
        ]
        return stages

    return {"mensual": _gran("%Y-%m"), "diario": _gran("%Y-%m-%d")}


# ── Costo por caja (reglas por cliente) ─────────────────────────────────────
# costo_por_caja = (Σ costo_kabi + Σ costo_otros) / (Σ cajas_kabi + Σ cajas_otros)
#   - Kabi (NIT 900402080): cajas = media milla; costo = media + última + otros.
#   - Otros: cajas = última milla; costo = última + otros (sin media milla).
# Cada colección aporta solo a los acumuladores que le corresponden según la regla
# (ver _merge_costo_por_caja para la verificación de no doble conteo).

def _pipeline_costo_caja_media_milla(anios: List[int], meses: List[int], clientes: List[str]) -> list:
    """Media milla para costo por caja. Solo aporta al grupo Kabi (costo + cajas):
    la regla excluye la media milla del costo/cajas de 'otros clientes'. Los
    vehículos no-Kabi no contribuyen a ningún grupo."""
    pipeline = [
        {"$match": _filtro_media_milla(anios, meses)},
        {"$lookup": {
            "from": "clientes", "localField": "nit_cliente",
            "foreignField": "nit", "as": "_cli",
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
    pipeline += [
        # Dedup por vehículo: costo y cajas están a nivel vehículo.
        {"$group": {
            "_id": "$consecutivo_vehiculo",
            "costo": {"$first": _num("total_flete_vehiculo")},
            "cajas": {"$first": _num("total_cajas_vehiculo")},
            "fecha_creacion": {"$first": "$fecha_creacion"},
            "nit_cliente": {"$first": {"$ifNull": ["$nit_cliente", ""]}},
        }},
        {"$facet": {
            "mensual": [
                {"$group": {
                    "_id": {"$substrCP": ["$fecha_creacion", 0, 7]},
                    "costo_kabi": {"$sum": {"$cond": [_EXPR_KABI_NIT, "$costo", 0]}},
                    "costo_otros": {"$sum": 0},   # media milla nunca aporta a otros
                    "cajas_kabi": {"$sum": {"$cond": [_EXPR_KABI_NIT, "$cajas", 0]}},
                    "cajas_otros": {"$sum": 0},
                }},
                {"$sort": {"_id": 1}},
                {"$project": {"_id": 0, "periodo": "$_id", "costo_kabi": 1, "costo_otros": 1,
                              "cajas_kabi": 1, "cajas_otros": 1}},
            ],
            "diario": [
                {"$group": {
                    "_id": {"$substrCP": ["$fecha_creacion", 0, 10]},
                    "costo_kabi": {"$sum": {"$cond": [_EXPR_KABI_NIT, "$costo", 0]}},
                    "costo_otros": {"$sum": 0},
                    "cajas_kabi": {"$sum": {"$cond": [_EXPR_KABI_NIT, "$cajas", 0]}},
                    "cajas_otros": {"$sum": 0},
                }},
                {"$sort": {"_id": 1}},
                {"$project": {"_id": 0, "periodo": "$_id", "costo_kabi": 1, "costo_otros": 1,
                              "cajas_kabi": 1, "cajas_otros": 1}},
            ],
        }},
    ]
    return pipeline


def _expr_costo_caja_ultima_expandido() -> dict:
    """Expr de aggregation que expande la última milla por cliente, devolviendo un
    ARRAY de sub-docs ``{cliente, costo, piezas}`` para alimentar el costo por caja
    tras expandir planillas fusionadas.

    - **Doc fusionado** con ``fusion_info.datos_originales`` no vacío: un sub-doc
      por cada original, con su ``cliente_origen`` y el ``total_solicitado`` (costo)
      y ``piezas`` del doc fusionado **repartidos proporcionalmente por las piezas
      de cada original** (si la suma de piezas es 0, reparto equitativo 1/n).
    - **Doc normal**: un único sub-doc con los valores top-level.

    Igual política que ``_expr_clientes_expandidos`` (reparto por piezas),
    adaptada a costo por caja (sin ``diferencia``). Así una fusionada que
    mezcla Kabi con otro cliente reparte cada porción a su grupo correcto, evitando
    que caiga entera en 'otros' por el cliente_origen concatenado del top-level."""
    da = "$fusion_info.datos_originales"
    piezas_o = {"$convert": {"input": "$$o.piezas", "to": "double", "onError": 0, "onNull": 0}}
    total_piezas = {
        "$sum": {
            "$map": {
                "input": {"$ifNull": [da, []]},
                "as": "x",
                "in": {"$convert": {"input": "$$x.piezas", "to": "double", "onError": 0, "onNull": 0}},
            }
        }
    }
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
            {  # rama fusionada: repartir costo y piezas por las piezas de cada original
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
                                "costo": {
                                    "$cond": [
                                        {"$gt": ["$$tp", 0]},
                                        {"$multiply": [_num("total_solicitado"), factor_piezas]},
                                        {"$multiply": [_num("total_solicitado"), factor_eq]},
                                    ]
                                },
                                "piezas": {
                                    "$cond": [
                                        {"$gt": ["$$tp", 0]},
                                        piezas_o,
                                        {"$multiply": [_num("piezas"), factor_eq]},
                                    ]
                                },
                            },
                        }
                    },
                }
            },
            {  # rama no fusionada: un sub-doc con los valores top-level (envuelto en
                # $cond porque la rama de $cond no admite una lista directamente).
                "$cond": [
                    {"$in": [{"$ifNull": ["$cliente_origen", ""]}, [None, "", " "]]},
                    [{"cliente": "Sin cliente", "costo": _num("total_solicitado"), "piezas": _num("piezas")}],
                    [{"cliente": {"$ifNull": ["$cliente_origen", "Sin cliente"]},
                      "costo": _num("total_solicitado"), "piezas": _num("piezas")}],
                ]
            },
        ]
    }


def _facet_costo_caja_ultima_milla(clientes: Optional[List[str]] = None) -> dict:
    """Facet mensual+diario de última milla para costo por caja.
    - Costo: aporta a kabi o a otros según el cliente de cada porción.
    - Cajas: SOLO aporta a 'otros' (no-kabi); los Kabi usan cajas de media milla.

    EXPANDE las planillas fusionadas: reparte costo y piezas entre los clientes
    originales proporcionalmente a piezas (``_expr_costo_caja_ultima_expandido``),
    así una fusionada que mezcla Kabi con otro cliente reparte cada porción a su
    grupo correcto (no cae entera en 'otros' por el cliente_origen concatenado).

    Si ``clientes`` viene, aplica un $match POST-expansión sobre el cliente expandido:
    al filtrar un cliente, descarta las porciones de los 'compañeros de fusión' (así
    el costo por caja respeta estrictamente el filtro, igual que el resto del módulo)."""
    kabi = _expr_kabi_nombre("_exp.cliente")
    post_match = {"$match": {"_exp.cliente": {"$in": list(clientes)}}} if clientes else None

    def _gran(fmt: str) -> list:
        stages = [
            # Bucket por período ANTES de expandir (sobre el doc original).
            {"$set": {"_periodo": {"$dateToString": {
                "format": fmt,
                "date": {"$subtract": ["$fecha_movimiento_historico", _MS_5H]},
            }}}},
            {"$project": {"_periodo": 1, "_exp": _expr_costo_caja_ultima_expandido()}},
            {"$unwind": "$_exp"},
        ]
        if post_match:
            stages.append(post_match)
        stages += [
            {"$group": {
                "_id": "$_periodo",
                "costo_kabi": {"$sum": {"$cond": [kabi, "$_exp.costo", 0]}},
                "costo_otros": {"$sum": {"$cond": [kabi, 0, "$_exp.costo"]}},
                "cajas_kabi": {"$sum": 0},
                "cajas_otros": {"$sum": {"$cond": [kabi, 0, "$_exp.piezas"]}},
            }},
            {"$sort": {"_id": 1}},
            {"$project": {"_id": 0, "periodo": "$_id", "costo_kabi": 1, "costo_otros": 1,
                          "cajas_kabi": 1, "cajas_otros": 1}},
        ]
        return stages

    return {"mensual": _gran("%Y-%m"), "diario": _gran("%Y-%m-%d")}


def _facet_costo_caja_otros() -> dict:
    """Facet mensual+diario de otros costos para costo por caja. Aporta SOLO al
    COSTO (kabi u otros según datos_servicio.cliente); NO aporta cajas a ningún
    denominador (la regla excluye datos_servicio.piezas)."""
    kabi = _expr_kabi_nombre("datos_servicio.cliente")

    def _gran(fmt: str) -> list:
        return [
            {"$group": {
                "_id": {"$dateToString": {
                    "format": fmt,
                    "date": {"$subtract": ["$created_at", _MS_5H]},
                }},
                "costo_kabi": {"$sum": {"$cond": [kabi, _num("valor_total"), 0]}},
                "costo_otros": {"$sum": {"$cond": [kabi, 0, _num("valor_total")]}},
                "cajas_kabi": {"$sum": 0},
                "cajas_otros": {"$sum": 0},
            }},
            {"$sort": {"_id": 1}},
            {"$project": {"_id": 0, "periodo": "$_id", "costo_kabi": 1, "costo_otros": 1,
                          "cajas_kabi": 1, "cajas_otros": 1}},
        ]

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
    sin comas), respetando año/mes (expansión de ``_expr_clientes_expandidos``)."""
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
    media/última milla, sobrecosto y ahorro) más las cajas/piezas de cada etapa
    (cajas_media, cajas_ultima, cajas_otros). Se suman todas por período: los costos
    por su clave propia, sobrecosto/ahorro combinan media + última milla, y las cajas
    se acumulan por etapa y en un total_cajas.
    Devuelve (serieMensual, serieDiaria) ordenadas por período, con total y total_cajas."""
    MEDIDAS = (
        "media_milla", "ultima_milla", "otros_costos",
        "sobrecosto", "ahorro",
        "cajas_media", "cajas_ultima", "cajas_otros",
    )

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
            b["total_cajas"] = b["cajas_media"] + b["cajas_ultima"] + b["cajas_otros"]
            out.append(b)
        return out

    return _merge("mensual"), _merge("diario")


def _merge_costo_por_caja(series: dict) -> tuple:
    """Combina las series mensual/diaria de las 3 colecciones (cada una ya con los 4
    acumuladores costo_kabi/costo_otros/cajas_kabi/cajas_otros por período) en una
    lista por período con un único ``costo_por_caja``.

    costo_por_caja = (Σ costo_kabi + Σ costo_otros) / (Σ cajas_kabi + Σ cajas_otros)
    con división protegida (0 si denominador 0).

    Descomposición (sin doble conteo — cada dólar/caja va a un solo grupo):
      - cajas_kabi  = media milla cajas (kabi)            [última/otros = 0]
      - cajas_otros = última milla piezas (no-kabi)        [media/otros = 0]
      - costo_kabi  = media(kabi) + última(kabi) + otros(kabi)
      - costo_otros = última(no-kabi) + otros(no-kabi)     [media no aporta]"""
    CLAVES = ("costo_kabi", "costo_otros", "cajas_kabi", "cajas_otros")

    def _merge(gran: str) -> list:
        merged: dict = {}
        for fuente in ("media_milla", "ultima_milla", "otros_costos"):
            for item in (series.get(fuente, {}).get(gran) or []):
                p = item.get("periodo")
                if not p:
                    continue
                bucket = merged.setdefault(p, {k: 0.0 for k in CLAVES})
                for k in CLAVES:
                    bucket[k] += float(item.get(k) or 0)

        out = []
        for p in sorted(merged):
            b = merged[p]
            costo = b["costo_kabi"] + b["costo_otros"]
            cajas = b["cajas_kabi"] + b["cajas_otros"]
            ratio = (costo / cajas) if cajas else 0.0
            out.append({
                "periodo": p,
                "costo_por_caja": round(ratio),
                # Desglose para tooltip/auditoría del frontend.
                "costo_kabi": round(b["costo_kabi"]),
                "costo_otros": round(b["costo_otros"]),
                "cajas_kabi": round(b["cajas_kabi"]),
                "cajas_otros": round(b["cajas_otros"]),
            })
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
                {"$facet": _facet_ultima_milla_expandido(clientes or None)},
            ]),
            _run(col_otros, [
                {"$match": oc_filtro},
                {"$facet": _facet_datetime_utc(
                    "created_at", "valor_total", "otros_costos",
                    cajas_field="datos_servicio.piezas", cajas_key="cajas_otros",
                )},
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
                "etiquetas": dict(ETAPAS),
            },
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        logger.error(f"[COSTO_OPERACION] Error en /resumen: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/costo-por-caja")
async def get_costo_por_caja(
    anio: Optional[List[int]] = Query(None),
    mes: Optional[List[int]] = Query(None),
    cliente: Optional[List[str]] = Query(None),
):
    """
    Costo por caja: promedio ponderado por período (Σ costo / Σ cajas), con un solo
    número combinado. Reglas por cliente:
      - Fresenius Kabi (NIT 900402080): cajas = media milla; costo = media+última+otros.
      - Otros: cajas = última milla; costo = última+otros (sin media milla).
    Devuelve {mensual:[{periodo,costo_por_caja,...}], diario:[...]} con el desglose
    costo_kabi/costo_otros/cajas_kabi/cajas_otros para tooltip/auditoría.
    """
    try:
        anios = [int(a) for a in anio] if anio else []
        meses = [int(m) for m in mes] if mes else []
        clientes = [str(c) for c in cliente] if cliente else []

        um_filtro = _filtro_datetime_utc(
            "fecha_movimiento_historico", anios, meses,
            "cliente_origen", clientes,
            "fusion_info.datos_originales.cliente_origen",
        )
        oc_filtro = _filtro_datetime_utc(
            "created_at", anios, meses,
            "datos_servicio.cliente", clientes,
        )

        async def _run(col, pipe):
            return await asyncio.to_thread(lambda: list(col.aggregate(pipe, allowDiskUse=True)))

        mm_res, um_res, oc_res = await asyncio.gather(
            _run(col_completados, _pipeline_costo_caja_media_milla(anios, meses, clientes)),
            _run(coleccion_historico, [
                {"$match": um_filtro},
                {"$facet": _facet_costo_caja_ultima_milla(clientes or None)},
            ]),
            _run(col_otros, [
                {"$match": oc_filtro},
                {"$facet": _facet_costo_caja_otros()},
            ]),
        )

        def _facet_doc(res):
            r = res[0] if res else {}
            return {"mensual": r.get("mensual", []), "diario": r.get("diario", [])}

        mensual, diario = _merge_costo_por_caja({
            "media_milla": _facet_doc(mm_res),
            "ultima_milla": _facet_doc(um_res),
            "otros_costos": _facet_doc(oc_res),
        })

        return {"success": True, "data": {"mensual": mensual, "diario": diario}}
    except Exception as e:
        import traceback
        traceback.print_exc()
        logger.error(f"[COSTO_OPERACION] Error en /costo-por-caja: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── Pedidos atendidos por analista ───────────────────────────────────────────
# "El analista que atiende" = quien hace el trámite Vulcano de cada etapa:
#   - Media milla  (`pedidos_completados`): `pedido_actualizado_vulcano_por`
#     (quien sube el Excel de pedidos Vulcano, /cargar-masivo en pedidos.py).
#   - Última milla (`pedidos_medical_historico`): `usuario_pedido_vulcano`
#     (quien asigna el número de pedido Vulcano, _procesar_pedido_vulcano).
#   - Otros costos (`historico_otros_costos`): `tramite_vulcano_info.usuario`
#     (quien marca el trámite Vulcano, /marcar-tramite-vulcano en otros_costos.py).
# Se cuenta 1 por DOCUMENTO (pedido) y se agrega en 2 niveles: serie por período
# (mensual/diario) para el gráfico apilado por analista y un ranking total para
# la leyenda/CSV. En media milla NO se deduplica por consecutivo_vehiculo (a
# diferencia de los pipelines de costo): cada doc es un pedido atendido. Una
# planilla fusionada en última milla también cuenta 1, a nombre de quien asignó
# el ÚLTIMO original (quien completa la fusión).

# Clave del grupo "sin tramitador registrado" en las agregaciones. Empezada en
# "(" para que nunca colisione con un usuario real (vienen en MAYÚSCULAS sin
# paréntesis) y distinguible si alguien la ve cruda en un dump.
CLAVE_SIN_ASIGNAR = "(SIN ASIGNAR)"


def _expr_usuario_normalizado(campo: str) -> dict:
    """Expr Mongo: valor del campo de usuario normalizado a mayúsculas y sin
    espacios laterales (los campos de analista guardan casing según el origen:
    cookie en última milla, valor canónico de baseusuarios en las otras)."""
    return {"$toUpper": {"$trim": {"input": {"$ifNull": [f"${campo}", ""]}}}}


def _grupo_usuario_o_senal(campo: str) -> dict:
    """Expr Mongo: usuario normalizado, o la señal CLAVE_SIN_ASIGNAR si el campo
    está vacío/nulo (doc sin tramitador registrado). Distingue el bucket "Sin
    asignar" de un eventual usuario con nombre vacío-tras-normalizar."""
    return {"$cond": [
        {"$eq": [{"$trim": {"input": {"$ifNull": [f"${campo}", ""]}}}, ""]},
        CLAVE_SIN_ASIGNAR,
        _expr_usuario_normalizado(campo),
    ]}


def _pipeline_pedidos_analista_media_milla(anios: List[int], meses: List[int], clientes: Optional[List[str]]) -> list:
    """Conteo de pedidos por PERÍODO x analista en media milla
    (pedido_actualizado_vulcano_por). Período = día Colombia sacado del prefijo
    del string local fecha_creacion (mismo eje que el resto del módulo para
    media milla; el mensual se agrupa en Python por el prefijo YYYY-MM).
    El $lookup a `clientes` SOLO se agrega si hay filtro de cliente (en el conteo
    sin filtro no se necesita el nombre del cliente)."""
    pipeline = [{"$match": _filtro_media_milla(anios, meses)}]
    if clientes:
        cl = list(clientes)
        pipeline += [
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
            {"$match": {"$or": [
                {"nombre_cliente": {"$in": cl}},
                {"nit_cliente": {"$in": cl}},
            ]}},
        ]
    pipeline.append({"$group": {
        "_id": {
            "periodo": {"$substrCP": ["$fecha_creacion", 0, 10]},
            "usuario": _grupo_usuario_o_senal("pedido_actualizado_vulcano_por"),
        },
        "pedidos": {"$sum": 1},
    }})
    return pipeline


def _pipeline_pedidos_analista_ultima_milla(anios: List[int], meses: List[int], clientes: Optional[List[str]]) -> list:
    """Conteo de planillas por PERÍODO x analista en última milla
    (usuario_pedido_vulcano). Período = día Colombia de
    fecha_movimiento_historico (UTC − 5 h, mismo eje que el resto del módulo).
    El filtro de cliente es el mismo $match a nivel doc de /resumen (incluye los
    originales embebidos de fusiones); NO se expande: se cuentan docs, no dinero."""
    filtro = _filtro_datetime_utc(
        "fecha_movimiento_historico", anios, meses,
        "cliente_origen", clientes,
        "fusion_info.datos_originales.cliente_origen",
    )
    return [
        {"$match": filtro},
        {"$group": {
            "_id": {
                "periodo": {"$dateToString": {
                    "format": "%Y-%m-%d",
                    "date": {"$subtract": ["$fecha_movimiento_historico", _MS_5H]},
                }},
                "usuario": _grupo_usuario_o_senal("usuario_pedido_vulcano"),
            },
            "pedidos": {"$sum": 1},
        }},
    ]


def _pipeline_pedidos_analista_otros(anios: List[int], meses: List[int], clientes: Optional[List[str]]) -> list:
    """Conteo de solicitudes por PERÍODO x analista en otros costos
    (tramite_vulcano_info.usuario). Período = día Colombia de created_at (UTC − 5 h)."""
    filtro = _filtro_datetime_utc(
        "created_at", anios, meses,
        "datos_servicio.cliente", clientes,
    )
    return [
        {"$match": filtro},
        {"$group": {
            "_id": {
                "periodo": {"$dateToString": {
                    "format": "%Y-%m-%d",
                    "date": {"$subtract": ["$created_at", _MS_5H]},
                }},
                "usuario": _grupo_usuario_o_senal("tramite_vulcano_info.usuario"),
            },
            "pedidos": {"$sum": 1},
        }},
    ]


def _usuarios_analista_admin() -> dict:
    """Dict {USUARIO_NORMALIZADO: {nombre, perfil}} con los usuarios de
    baseusuarios cuyo perfil es ANALISTA o ADMIN (perfil con casing variable, se
    normaliza). No se filtra por `activo`: el trabajo pasado de un analista hoy
    inactivo sigue siendo trabajo atendido."""
    out = {}
    for u in col_usuarios.find({}, {"usuario": 1, "nombre": 1, "perfil": 1}):
        usuario = (u.get("usuario") or "").strip().upper()
        perfil = (u.get("perfil") or "").strip().upper()
        if usuario and perfil in {"ANALISTA", "ADMIN"}:
            out[usuario] = {"nombre": u.get("nombre") or usuario, "perfil": perfil}
    return out


def _merge_analistas_series(
    conteos_por_etapa: List[list],
    usuarios: dict,
) -> tuple:
    """Combina los conteos {periodo, usuario, pedidos} de las 3 etapas en:

    - ``series``: {periodo → {clave_usuario → n}} con las 3 etapas sumadas (un
      doc solo puede estar en una etapa, así que sumar es seguro).
    - ``ranking``: {clave_usuario → n} total del período filtrado.

    La clave es el USUARIO normalizado, o None para todo lo que no es un
    ANALISTA/ADMIN visible: docs sin tramitador (CLAVE_SIN_ASIGNAR), otros
    perfiles y usuarios que ya no existen en baseusuarios → "Sin asignar"."""
    series: dict = {}
    ranking: dict = {}

    def _destino(usuario_raw: str):
        """Clave de acumulación: el usuario si es ANALISTA/ADMIN, None si no."""
        if usuario_raw == CLAVE_SIN_ASIGNAR:
            return None
        if usuario_raw in usuarios:
            return usuario_raw
        return None  # otro perfil o usuario que ya no existe en baseusuarios

    for conteo in conteos_por_etapa:
        for d in conteo:
            _id = d.get("_id") or {}
            periodo = _id.get("periodo")
            usuario = (_id.get("usuario") or "").strip()
            n = int(d.get("pedidos") or 0)
            if not periodo or n <= 0:
                continue
            clave = _destino(usuario)
            bucket = series.setdefault(periodo, {})
            bucket[clave] = bucket.get(clave, 0) + n
            ranking[clave] = ranking.get(clave, 0) + n

    return series, ranking


@router.get("/pedidos-por-analista")
async def get_pedidos_por_analista(
    anio: Optional[List[int]] = Query(None),
    mes: Optional[List[int]] = Query(None),
    cliente: Optional[List[str]] = Query(None),
):
    """
    Pedidos atendidos por analista: cuántos pedidos/planillas/solicitudes tramitó
    cada usuario (perfil ANALISTA o ADMIN). Devuelve:
      - ``usuarios``: orden fijo de las series (total descendente; "Sin asignar"
        al final) — es el orden de columnas del gráfico y de la leyenda.
      - ``serieMensual``/``serieDiaria``: buckets {periodo, <usuario>: n, ...,
        total} para el gráfico apilado por analista (igual que el de costo total).
      - ``analistas``: ranking {usuario, nombre, perfil, pedidos} del período
        filtrado completo (CSV/auditoría).
    Los docs cuyo tramitador no es ANALISTA/ADMIN (o no se registró) van bajo la
    clave "Sin asignar". Respeta los filtros de año/mes/cliente.
    """
    try:
        anios = [int(a) for a in anio] if anio else []
        meses = [int(m) for m in mes] if mes else []
        clientes = [str(c) for c in cliente] if cliente else []

        async def _run(col, pipe):
            return await asyncio.to_thread(lambda: list(col.aggregate(pipe, allowDiskUse=True)))

        mm_res, um_res, oc_res, usuarios = await asyncio.gather(
            _run(col_completados, _pipeline_pedidos_analista_media_milla(anios, meses, clientes or None)),
            _run(coleccion_historico, _pipeline_pedidos_analista_ultima_milla(anios, meses, clientes or None)),
            _run(col_otros, _pipeline_pedidos_analista_otros(anios, meses, clientes or None)),
            _run_sync(_usuarios_analista_admin),
        )

        series, ranking = _merge_analistas_series([mm_res, um_res, oc_res], usuarios)

        # Orden de las series: total descendente; "Sin asignar" (None) al final.
        usuarios_orden = [u for u in sorted(ranking, key=lambda k: ranking[k], reverse=True) if u is not None]
        if None in ranking:
            usuarios_orden.append(None)

        def _nombre(u):
            return "Sin asignar" if u is None else usuarios[u]["nombre"]

        def _columna(u):
            """Clave EXACTA de la columna de este usuario en los buckets de las
            series (el código de usuario, o 'Sin asignar'). El frontend la usa
            como dataKey; `nombre` es solo el rótulo visible (leyenda/CSV)."""
            return u if u is not None else "Sin asignar"

        def _serie(fmt_mes: bool) -> list:
            """Buckets por período {periodo, <columna>: n..., total}. Los
            pipelines traen período DÍA; el mensual agrupa por el prefijo
            YYYY-MM antes de pivotear (así ambos salen de las mismas queries)."""
            acum: dict = {}
            for periodo in series:
                p = periodo[:7] if fmt_mes else periodo
                bucket = acum.setdefault(p, {})
                for u, n in series[periodo].items():
                    bucket[u] = bucket.get(u, 0) + n
            out = []
            for p in sorted(acum):
                fila = {"periodo": p}
                total = 0
                for u in usuarios_orden:
                    n = int(acum[p].get(u) or 0)
                    fila[_columna(u)] = n
                    total += n
                fila["total"] = total
                out.append(fila)
            return out

        return {
            "success": True,
            "data": {
                "usuarios": [
                    {"usuario": u, "nombre": _nombre(u), "columna": _columna(u),
                     "perfil": (usuarios[u]["perfil"] if u is not None else None)}
                    for u in usuarios_orden
                ],
                "serieMensual": _serie(True),
                "serieDiaria": _serie(False),
                "analistas": [
                    {"usuario": u, "nombre": _nombre(u),
                     "perfil": (usuarios[u]["perfil"] if u is not None else None),
                     "pedidos": ranking[u]}
                    for u in usuarios_orden if ranking.get(u, 0) > 0
                ],
                "etiquetas": dict(ETAPAS),
            },
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        logger.error(f"[COSTO_OPERACION] Error en /pedidos-por-analista: {e}")
        raise HTTPException(status_code=500, detail=str(e))
