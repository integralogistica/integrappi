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
import unicodedata

# Reutiliza helpers/pipelines de Costo de Operación (misma BD, mismas reglas):
# - _filtro_media_milla / _anios_media_milla → filtro y años de pedidos_completados
# - _num, NIT_FRESENIUS                      → detección de cliente
# - TOPES_TIPO_VEH/_tipo_solicitado/_categoria_por_kilos/_fecha_iso → uso de
#   vehículos (viven en costo_operación desde que el tablero general copió el
#   gráfico de kabi; una sola fuente de las reglas de tope/categoría)
from rutas.indicadores_costo_operacion import (
    col_completados,
    _filtro_media_milla,
    _anios_media_milla,
    _num,
    NIT_FRESENIUS,
    TOPES_TIPO_VEH,
    MAX_VEHICULOS,
    _tipo_solicitado,
    _categoria_por_kilos,
    _fecha_iso,
)
from bd.bd_cliente import bd_cliente
from bd.bd_postgres import (
    guias_minimas_cliente,
    pagina_guias_cliente,
    buscar_guias_cliente,
    guia_tms_placa,
    guias_placa_dia,
    buscar_destinatarios_cliente,
)

import time as _time

import pandas as pd
from io import BytesIO
from pymongo import UpdateOne

router = APIRouter(
    prefix="/indicadores-cliente",
    tags=["Indicadores Cliente"],
)

# (2026-11-03) Tipos de servicio del Excel real (fórmula BB) — valores que
# acepta el filtro ?tipo_servicio= de /guias y /guias/filas.
TIPOS_SERVICIO_OT = {
    "CARGA", "SERVICIO TECNICO", "RECOLECCION", "DEVOLUCION", "MUESTRA DE CALIDAD",
}
logger = logging.getLogger(__name__)

# Citas "plan B" (colección citas_kabi): cuando fecha_cita del TMS (Postgres)
# viene vacía o con basura, la fecha de cita digitada acá manda. Un doc por
# guía: {guia, fecha_cita (string 'YYYY-MM-DD'), actualizado_el}.
col_citas = bd_cliente["integra"]["citas_kabi"]

# (2026-10-29) Portal «Mis Indicadores» (perfil CLIENTE): usuarios y catálogo
# de clientes asignables (la asignación vive en baseusuarios.clientes_siscore).
col_usuarios = bd_cliente["integra"]["baseusuarios"]
col_clientes_siscore = bd_cliente["integra"]["clientes_siscore"]
# (2026-10-29) Novedades justificadas OT (gestión en /novedades-ot): una guía
# justificada sale de su estado TMS y pasa al segmento «justificadas».
col_novedades_ot = bd_cliente["integra"]["novedades_justificadas_ot"]

# Perfiles autorizados a cargar el Excel de citas.
PERFILES_CARGA_CITAS = {"ADMIN", "ANALISTA", "COORDINADOR", "CONTROL"}

# Registro backend de clientes (espejo del clientes.ts del frontend; mantener
# sincronizado). El filtro de media milla se expresa como $match directo
# (forma query, no forma expr): por NIT para Kabi, igual que /costo-por-caja.
# «nit» (2026-10-29): identidad del cliente para el portal de clientes
# externos (validación de asignación en /guias y /mis-indicadores).
CLIENTES = {
    "fresenius-kabi": {
        "nombre": "Fresenius Kabi",
        "fuente": "mongo",
        "nit": NIT_FRESENIUS,
        "match_media_milla": lambda: {"nit_cliente": NIT_FRESENIUS},
        # (2026-10-29) Informe de guías PG puro: valores EXACTOS de
        # nombre_cliente en informe_guias_tms (verificados con SELECT
        # DISTINCT; la igualdad directa en lista permite usar el índice
        # idx_guias_cliente_fecha). 2026-10-29 (tarde): el tablero UNE los
        # nombres con los que el TMS registra al grupo Kabi — servicio técnico
        # llega como 'FK SERVICIO TECNICO' y, en algunos registros viejos,
        # 'FRESENIUSKABISERVICIOTECNICO' concatenado.
        "pg_nombres": [
            "FRESENIUS KABI",
            "FK SERVICIO TECNICO",
            "FRESENIUSKABISERVICIOTECNICO",
        ],
        # (2026-10-29) Sub-grupos filtrables del informe de guías: el tablero
        # une los nombres del grupo, y este filtro deja ver uno solo. La
        # variante concatenada vieja viaja con servicio técnico (es su data).
        "pg_grupos": [
            {"etiqueta": "Fresenius Kabi", "nombres": ["FRESENIUS KABI"]},
            {"etiqueta": "FK Servicio Técnico", "nombres": ["FK SERVICIO TECNICO", "FRESENIUSKABISERVICIOTECNICO"]},
        ],
    },
    # 'fresenius-medical-care' usará las colecciones de última milla/otros
    # cuando tenga gráficos.
}


def _nombres_por_grupo(cliente: dict, grupo: Optional[str]) -> list:
    """(2026-10-29) Nombres de informe_guias_tms a consultar: acotados al
    sub-grupo pedido (``?grupo=``) o todos los ``pg_nombres``. Grupo
    desconocido → 422 (evita filtrar silenciosamente a nada)."""
    if not grupo:
        return cliente["pg_nombres"]
    for g in cliente.get("pg_grupos") or []:
        if g["etiqueta"] == grupo:
            return g["nombres"]
    validos = [g["etiqueta"] for g in cliente.get("pg_grupos") or []]
    raise HTTPException(status_code=422, detail=f"Grupo no válido: {grupo}. Válidos: {validos}")


def _resolver_usuario(usuario: str) -> dict:
    """Identidad desde baseusuarios (patrón _resolver_usuario de otros_costos:
    el backend NO confía en el perfil que envía el frontend).
    401 si no existe, 403 si está inactivo."""
    valor = str(usuario or "").strip().upper()
    doc = col_usuarios.find_one({"usuario": valor})
    if not doc:
        raise HTTPException(status_code=401, detail=f"Usuario no encontrado: {usuario}")
    if not doc.get("activo", True):
        raise HTTPException(status_code=403, detail="Usuario inactivo")
    return doc


def _verificar_acceso_cliente(usuario: str, cliente_id: str, cliente: dict):
    """(2026-10-29) Endurecimiento del tablero de clientes externos: si quien
    consulta tiene perfil CLIENTE, el NIT del cliente debe estar en su
    asignación ``clientes_siscore`` (gestionada en GestionUsuarios). Los
    perfiles internos pasan igual que siempre (comportamiento histórico)."""
    doc = _resolver_usuario(usuario)
    if str(doc.get("perfil", "")).upper() != "CLIENTE":
        return
    asignados = doc.get("clientes_siscore") or []
    nit = cliente.get("nit")
    if not nit or nit not in asignados:
        raise HTTPException(
            status_code=403,
            detail="No tienes acceso a los indicadores de este cliente",
        )


@router.get("/mis-indicadores")
def get_mis_indicadores(usuario: str = Query(..., description="Nombre de usuario (cookie usuarioPedidosCookie)")):
    """(2026-10-29) Clientes asignados al usuario para el panel «Mis
    Indicadores» (/indicadores/mis-clientes, perfil CLIENTE): cruza su
    asignación ``clientes_siscore`` (NITs) contra el catálogo clientes_siscore
    (nombre) y contra el registro CLIENTES (qué NIT tiene dashboard publicado).
    Un cliente sin dashboard llega con cliente_id=null → «Próximamente»."""
    doc = _resolver_usuario(usuario)
    perfil = str(doc.get("perfil", "")).upper()
    con_dashboard = {
        cfg.get("nit"): cid for cid, cfg in CLIENTES.items() if cfg.get("nit")
    }
    if perfil in ("ADMIN", "VISUALIZADOR"):
        # ADMIN siempre ve todo: TODOS los dashboards publicados (sin depender
        # de su asignación clientes_siscore). VISUALIZADOR (2026-11-05) igual:
        # es solo lectura interno y no maneja asignación clientes_siscore.
        clientes = [
            {
                "nit": nit or "",
                "entidad": CLIENTES[cid]["nombre"],
                "razon": CLIENTES[cid]["nombre"],
                "cliente_id": cid,
                "disponible": True,
            }
            for nit, cid in con_dashboard.items()
        ]
        return {"success": True, "data": {"perfil": perfil, "clientes": clientes}}

    nits = doc.get("clientes_siscore") or []
    catalogo = {
        c.get("nit"): c
        for c in col_clientes_siscore.find({"nit": {"$in": nits}}, {"entidad": 1, "razon": 1, "nit": 1})
    }
    clientes = []
    for nit in nits:
        cat = catalogo.get(nit) or {}
        cliente_id = con_dashboard.get(nit)
        clientes.append({
            "nit": nit,
            "entidad": cat.get("entidad") or "",
            "razon": cat.get("razon") or cat.get("entidad") or "",
            "cliente_id": cliente_id,
            "disponible": cliente_id is not None,
        })
    return {
        "success": True,
        "data": {
            "perfil": perfil,
            "clientes": clientes,
        },
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
# (2026-10-29) PostgreSQL puro: la lógica del informe vive más abajo, en el
# bloque "Informe de guías PostgreSQL puro" (helpers OT + /guias + /guias/filas).

# Estados reales de informe_guias_tms (verificados 2026-08-20): ENTREGADO,
# PENDIENTE, "En distribucion", "CON NOVEDAD", "Transito Nacional" + basura
# ('', '0000-00-00', 'planilla normal'). Solo ENTREGADO cuenta como entregada.
ESTADO_ENTREGADO = "ENTREGADO"


# (_fecha_iso viene importado de indicadores_costo_operacion.)

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


# ── (2026-10-31) Tablas de promesa del Excel REAL (calculo real ot.xlsx) ──
# Colecciones Mongo `tiempos_entrega` (CARGA, por municipio), `tiempos_entrega_st`
# (servicio técnico, envío directo por destino / recolección por origen) y
# `correcciones_nombres` (ciudades/dptos mal escritos en Siscore). Reemplazan
# a tarifas.promesa_entrega_dias como fuente de días del OT.

def _norm_nombre(texto) -> str:
    """Normalización de la importación: Ñ→N, sin acentos, espacios colapsados,
    upper — ' BOGOTA,  D.C. ' → 'BOGOTA, D.C.' (las correcciones ya traen la
    forma buena)."""
    t = unicodedata.normalize("NFD", str(texto or "").upper().replace("Ñ", "N"))
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", t).strip()


def _cache_tabla(nombre_cache: str, coleccion, builder):
    """Lee una colección de tablas con cache TTL 10 min (patrón _CACHE_PROMESAS)."""
    ahora = _time.monotonic()
    ts, val = _CACHE_TABLAS.get(nombre_cache, (0.0, None))
    if val is not None and ahora - ts < 600:
        return val
    val = builder(coleccion)
    _CACHE_TABLAS[nombre_cache] = (ahora, val)
    return val


_CACHE_TABLAS: dict = {}


def _mapa_correcciones() -> dict:
    """{nombre_mal (solo alfanumérico): nombre_bueno} — la clave alfanumérica
    absorbe 'BOGOTA, D C' ≡ 'BOGOTA, D.C.' ≡ 'BOGOTA D.C.'."""
    return _cache_tabla("correcciones", bd_cliente["integra"]["correcciones_nombres"], lambda c: {
        re.sub(r"[^A-Z0-9]", "", d["error"]): d["correccion"]
        for d in c.find({}, {"_id": 0, "error": 1, "correccion": 1})
    })


def _mapa_citas_agendadas() -> dict:
    """{guia: fecha_cita ISO} de CITASACT del Excel real (`citas_agendadas`) —
    la fuente EXACTA de la fórmula BN (VLOOKUP por guía, primera aparición).
    Es más rica que fecha_cita del TMS y que citas_kabi (tiene re-agendadas)."""
    return _cache_tabla("citas_agendadas", bd_cliente["integra"]["citas_agendadas"], lambda c: {
        d["guia"]: d["fecha_cita"] for d in c.find({}, {"_id": 0, "guia": 1, "fecha_cita": 1})
    })


def _mapa_novedades_siscore() -> dict:
    """{guia: MOTIVO} del reporte de novedades de Siscore (`novedades_siscore`,
    importado del Excel real — 856 guías). Es la fuente de la fórmula BP: VLOOKUP
    por guía → MOTIVO; «SIN NOVEDAD» si no está. ⚠️ NO sirve la columna `novedad`
    del informe (es el último evento: 'ENTREGA DIGITALIZADA' en casi todas)."""
    return _cache_tabla("novedades_siscore", bd_cliente["integra"]["novedades_siscore"], lambda c: {
        d["guia"]: d["motivo"] for d in c.find({}, {"_id": 0, "guia": 1, "motivo": 1})
    })


def _mapa_tiempos_carga() -> dict:
    """Tablas de CARGA: {'par': {(municipio, dpto): días 2026}, 'mun':
    {municipio: días cuando es único o todos los dptos coinciden}} — el índice
    por municipio cubre mientras el bot no cargue departamento_destino."""
    def builder(c):
        par, mun = {}, {}
        for d in c.find({}, {"_id": 0, "municipio": 1, "departamento": 1, "dias_2026": 1}):
            par[(d["municipio"], d["departamento"])] = d["dias_2026"]
            mun.setdefault(d["municipio"], set()).add(d["dias_2026"])
        return {"par": par, "mun": {m: next(iter(s)) for m, s in mun.items() if len(s) == 1}}
    return _cache_tabla("tiempos_carga", bd_cliente["integra"]["tiempos_entrega"], builder)


def _mapa_tiempos_st() -> dict:
    """Tablas ST: {'par': {(tipo, municipio, dpto): días}, 'mun': {(tipo,
    municipio): días único}}. La clave ENVIO DIRECTO se mira por el DESTINO y
    RECOLECCION por el ORIGEN (regla Trayecto del Excel); la importación ya
    las trae precalculadas."""
    def builder(c):
        par, mun = {}, {}
        for d in c.find({}, {"_id": 0, "tipo": 1, "municipio": 1, "departamento": 1,
                             "dias_envio": 1, "dias_recoleccion": 1}):
            dias = d["dias_envio"] if d["tipo"] == "ENVIO DIRECTO" else d["dias_recoleccion"]
            par[(d["tipo"], d["municipio"], d["departamento"])] = dias
            mun.setdefault((d["tipo"], d["municipio"]), set()).add(dias)
        return {"par": par, "mun": {k: next(iter(s)) for k, s in mun.items() if len(s) == 1}}
    return _cache_tabla("tiempos_st", bd_cliente["integra"]["tiempos_entrega_st"], builder)


def _dias_tabla(tabla: dict, clave_par: tuple, vista_mun: dict, municipio: str) -> Optional[int]:
    """Lookup con degradación: (municipio, dpto) exacto → municipio único →
    prefijo único de municipio ('BOGOTA'→'BOGOTA DC'). None si ambiguo.
    `vista_mun` es el índice por municipio YA filtrado por tipo (ST) o el
    completo (CARGA)."""
    d = tabla["par"].get(clave_par)
    if d is not None:
        return d
    if municipio in vista_mun:
        return vista_mun[municipio]
    # Prefijo único en cualquiera de los dos sentidos (≤4 chars de diferencia)
    cands = {v for k, v in vista_mun.items()
             if (k.startswith(municipio) or municipio.startswith(k))
             and 0 < abs(len(k) - len(municipio)) <= 4}
    return cands.pop() if len(cands) == 1 else None


def _ciudad_y_dpto(fila: dict, usar_origen: bool) -> tuple:
    """(municipio, dpto) reales de la fila, con la tabla de CORRECCIONES.
    Columnas explícitas si el bot ya las cargó; si no, se parte ciudad_destino
    'CIUDAD - DEPARTAMENTO' (formato que trae el informe). Las correcciones se
    casan por clave ALFANUMÉRICA ('BOGOTA, D C' ≡ 'BOGOTA, D.C.' → 'BOGOTA DC')."""
    if usar_origen:
        ciudad = fila.get("ciudad_origen")
        dpto = fila.get("departamento_origen")
    else:
        ciudad = fila.get("ciudad_destino")
        dpto = fila.get("departamento_destino")
    if not dpto and ciudad:
        partes = re.split(r"\s+-\s+", str(ciudad), maxsplit=1)
        if len(partes) == 2:
            ciudad, dpto = partes
    corr = _mapa_correcciones()

    def aplicar(x):
        n = _norm_nombre(x)
        alnum = re.sub(r"[^A-Z0-9]", "", n)
        return corr.get(alnum, n) if alnum else n

    return aplicar(ciudad), aplicar(dpto)


# ── Informe de guías PostgreSQL puro (2026-10-29) ─────────────────────────────
# Antes: agregación Mongo (media milla, planillas multi-guía) + JOIN por guía
# contra PG + TODAS las filas al frontend. Ahora: una pasada sobre columnas
# mínimas de informe_guias_tms → resumen + series OT + página de filas.

# Estados reales del TMS (verificados contra la tabla). Cualquier otro valor
# ('', '0000-00-00', 'planilla normal', …) → estado null + estado_crudo para
# diagnóstico/export (antes eran "sin_info" por no estar en el JOIN de Mongo).
_ESTADOS_TMS = {e.upper(): e for e in (
    "ENTREGADO", "CON NOVEDAD", "PENDIENTE", "En distribucion", "Transito Nacional",
)}


def _normalizar_estado(crudo: Optional[str]):
    """valor_tms → (estado_canonico_o_None, estado_crudo)."""
    t = str(crudo or "").strip()
    if not t:
        return None, None
    return _ESTADOS_TMS.get(t.upper()), t


_PATRON_TMS_DIA = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})$")


def _parse_fecha_tms(texto) -> Optional[date]:
    """Fecha del TMS en cualquiera de sus formatos: 'D/M/YYYY' (como carga el
    bot en fecha_cita TEXT) o ISO 'YYYY-MM-DD'. None para basura.

    ⚠️ Bug implícito corregido: _fecha_servible solo acepta ISO, así que las
    citas 'D/M/YYYY' del TMS JAMÁS servían y el OT caía siempre a citas_kabi
    o promesa-por-destino. Nunca usar to_date() en SQL: rueda fechas inválidas
    (31/02) sin error y corrompería promesas silenciosamente."""
    if not texto:
        return None
    t = str(texto).strip()
    m = _PATRON_TMS_DIA.match(t)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            return None
    return _fecha_servible(t)


def _calcular_ot_filas(filas: list) -> None:
    """(2026-10-31) REWORK TOTAL: réplica de la lógica del Excel operativo real
    (`calculo real ot.xlsx`, hoja BASE — fórmulas CUMPLIMIENTO OT/FECHA PROMESA/
    DIAS PROMESA/CITA). ÚNICA implementación del OT — la usan la pasada de
    agregación (series del gráfico), cada página de la tabla y la consulta de
    guía, así nunca divergen.

    Setea EN SITIO: tipo_servicio, tipo_st, ruta, fecha_promesa, origen_promesa
    (CITA|TARIFA|FALLBACK|null), dias_promesa, dias_habiles, cumple_novedad, ot.

    Diferencias clave vs la versión anterior:
    - Las guías SIN entregar SÍ se juzgan: la referencia pasa a ser HOY (una
      guía en curso cuenta como CUMPLE mientras su cita/promesa no venza).
    - Días de promesa desde las tablas del Excel (tiempos_entrega /
      tiempos_entrega_st), con fallback NACIONAL 12 / URBANO 8.
    - Cualquier novedad del TMS (≠ 'CITA RECHAZADA POR TIEMPOS DE TRANSITO')
      auto-cumple («Cumple con novedad»); CITA RECHAZADA compara la PROMESA.
    - Con cita se compara la CITA; sin cita la PROMESA; sin promesa evaluable
      (p. ej. estado CON NOVEDAD sin novedad en el texto) → ot null."""
    if not filas:
        return
    hoy = date.today()

    # (2026-10-31) CITASACT del Excel real (citas_agendadas): fuente EXACTA de
    # la fórmula BN — GANA sobre el TMS (trae re-agendadas que el TMS no refleja).
    citas_act = _mapa_citas_agendadas()

    tiempos_carga = _mapa_tiempos_carga()
    tiempos_st = _mapa_tiempos_st()
    novedades_siscore = _mapa_novedades_siscore()

    for f in filas:
        # ── 1. TIPO DE SERVICIO (fórmula BB del Excel) ──
        guia = str(f.get("guia") or "").strip().upper()
        nombre = str(f.get("nombre_cliente") or "").strip().upper()
        if nombre == "FK SERVICIO TECNICO":
            tipo = "SERVICIO TECNICO"
        elif guia.endswith("REC"):
            tipo = "RECOLECCION"
        elif guia.endswith("DEV"):
            tipo = "DEVOLUCION"
        elif guia.startswith("722"):
            tipo = "MUESTRA DE CALIDAD"
        else:
            tipo = "CARGA"

        # ── 2. TIPO DE SERVICIO ST (fórmula BC) ──
        ciudad_origen = str(f.get("ciudad_origen") or "").strip()
        origen_bogota = _norm_nombre(re.split(r"\s+-\s+|,", ciudad_origen)[0]) == "BOGOTA"
        if tipo == "SERVICIO TECNICO":
            dest_norm = _norm_nombre(f.get("destinatario"))
            if not ciudad_origen and dest_norm == "CENTRO DE SERVICIO TECNICO":
                # PROVISIONAL sin origen cargado: por fórmula, CST solo es
                # RECOLECCION con origen BOGOTA — el municipio de la clave es
                # el origen (BOGOTA), que sí conocemos.
                tipo_st = "RECOLECCION"
            elif not ciudad_origen:
                # PROVISIONAL: casi todo el ST sale de Bogotá; se asume ENVIO
                # DIRECTO (clave de días por DESTINO). Se corrige cuando el bot
                # cargue el origen.
                tipo_st = "ENVIO DIRECTO"
            elif origen_bogota and dest_norm == "CENTRO DE SERVICIO TECNICO":
                tipo_st = "RECOLECCION"
            elif origen_bogota:
                tipo_st = "ENVIO DIRECTO"
            else:
                tipo_st = "RECOLECCION"
        else:
            tipo_st = "NO ES ST"

        # ── 3-4. MUNICIPIO/DEPARTAMENTO REAL + RUTA (fórmulas BD/BE/BH) ──
        # ENVIO DIRECTO→destino; RECOLECCION→ORIGEN; CARGA→destino; DEV/MUESTRA→origen.
        usar_origen = tipo_st == "RECOLECCION" or tipo in ("DEVOLUCION", "MUESTRA DE CALIDAD")
        municipio, dpto = _ciudad_y_dpto(f, usar_origen)
        if usar_origen and not municipio and tipo == "SERVICIO TECNICO":
            # PROVISIONAL sin origen: la recolección CST sale de BOGOTA por
            # fórmula — clave de días por origen BOGOTA.
            municipio = "BOGOTA"
        ruta = "URBANO" if dpto in ("BOGOTA DC", "CUNDINAMARCA") else "NACIONAL"

        # ── 5. DIAS PROMESA (fórmula BI) ──
        # Novedad = MOTIVO del reporte de Siscore (fórmula BP); NO el texto
        # `novedad` del informe (último evento, casi siempre ENTREGA DIGITALIZADA).
        novedad_txt = novedades_siscore.get(guia) or ""
        f_dias = None
        if tipo == "SERVICIO TECNICO":
            vista_st = {k[1]: v for k, v in tiempos_st["mun"].items() if k[0] == tipo_st}
            f_dias = _dias_tabla(tiempos_st, (tipo_st, municipio, dpto), vista_st, municipio)
        elif f.get("estado") == "CON NOVEDAD":
            f_dias = None  # NO APLICA
        elif tipo == "CARGA":
            f_dias = _dias_tabla(tiempos_carga, (municipio, dpto), tiempos_carga["mun"], municipio)
        else:  # DEVOLUCION / MUESTRA DE CALIDAD → fallback calendario
            f_dias = 12 if ruta == "NACIONAL" else 8

        # ── 6. FECHA PROMESA (fórmula BJ: WORKDAY = días hábiles con festivos;
        #    fallback 12/8 en CALENDARIO) ──
        inicial = _fecha_servible(f.get("fecha_emision"))
        f_promesa = None
        origen = None
        if inicial:
            if tipo in ("CARGA", "SERVICIO TECNICO"):
                base = inicial
                if tipo == "CARGA" and novedad_txt == "EN ACONDICIONAMIENTO":
                    f_carga = _fecha_servible(f.get("fecha_carga"))
                    if f_carga:
                        base = f_carga
                if f_dias is not None:
                    f_promesa = _sumar_dias_habiles(base, f_dias)
                    origen = "TARIFA"
            else:  # DEV/MUESTRA: emisión + 12/8 calendario (fórmula BJ)
                f_promesa = inicial + timedelta(days=f_dias or 0)
                origen = "FALLBACK"

        # ── 7. CITA — SOLO citas_agendadas (CITASACT del Excel, fórmula BN).
        #     Ni el TMS fecha_cita ni citas_kabi: traen valores distintos
        #     (re-agendadas) que hacen divergir el veredicto del Excel real.
        #     Guías nuevas sin entrada → sin cita → se compara la PROMESA. ──
        cita = _fecha_servible(citas_act.get(guia))

        # ── 8. CUMPLIMIENTO OT (fórmula BK): referencia = entrega ó HOY ──
        entrega = _fecha_servible(f.get("fecha_entrega"))
        referencia = entrega or hoy
        f_cumple_nov = False
        f_ot = None
        if novedad_txt == "CITA RECHAZADA POR TIEMPOS DE TRANSITO":
            f_ot = (1 if f_promesa >= referencia else 0) if f_promesa else None
        elif novedad_txt:
            f_ot, f_cumple_nov = 1, True  # «Cumple con novedad»
        elif f_promesa is None:
            f_ot = None  # NO APLICA
        elif cita:
            f_ot = 1 if cita >= referencia else 0
            origen = "CITA"  # se juzgó la cita, no la promesa
        else:
            f_ot = 1 if f_promesa >= referencia else 0

        f["tipo_servicio"] = tipo
        f["tipo_st"] = tipo_st
        f["ruta"] = ruta
        f["dias_promesa"] = f_dias
        f["fecha_promesa"] = f_promesa.isoformat() if f_promesa else None
        f["origen_promesa"] = origen
        f["dias_habiles"] = _dias_habiles_entre(inicial, entrega) if inicial and entrega else None
        f["cumple_novedad"] = f_cumple_nov
        f["ot"] = f_ot


def _ot_por_campo(filas: list, campo: str, top: int | None = None) -> list:
    """(2026-10-30) OT agregado por un campo de la fila (ciudad_destino,
    destinatario) para las barras horizontales del portal de clientes.
    (2026-10-31) Mismo criterio del % OT nuevo: «con novedad» (TMS o manual)
    cuentan como cumplidas (numerador y denominador). Devuelve TODOS los
    grupos con evaluables (sin tope — el usuario hace scroll), ordenados del
    MÁS cumplido al menos (desempate por volumen)."""
    grupos: dict = {}
    for f in filas:
        clave = (f.get(campo) or "").strip() if isinstance(f.get(campo), str) else ""
        if not clave:
            continue
        g = grupos.setdefault(clave, {"c": 0, "n": 0, "j": 0, "t": 0})
        g["t"] += 1
        if f.get("novedad") or f.get("cumple_novedad"):
            g["j"] += 1
        elif f.get("ot") == 1:
            g["c"] += 1
        elif f.get("ot") == 0:
            g["n"] += 1
    out = []
    for clave, g in grupos.items():
        evaluables = g["c"] + g["n"] + g["j"]
        if evaluables <= 0:
            continue
        out.append({
            "clave": clave,
            "cumplieron": g["c"],
            "no_cumplieron": g["n"],
            "con_novedad": g["j"],
            "evaluables": evaluables,
            "total": g["t"],
            "pct": round((g["c"] + g["j"]) / evaluables * 100, 1),
        })
    out.sort(key=lambda x: (-x["pct"], -x["evaluables"], x["clave"]))
    return out[:top] if top else out


def _mapa_novedades_ot() -> dict:
    """(2026-10-29) Guía → justificación activa (novedades_justificadas_ot).
    La colección es pequeña (una por guía justificada): se lee completa."""
    mapa: dict = {}
    for d in col_novedades_ot.find({}, {"guia": 1, "causal": 1, "justificacion": 1, "creado_por": 1, "actualizado_por": 1, "updated_at": 1}):
        por = d.get("actualizado_por") or d.get("creado_por") or {}
        mapa[d["guia"]] = {
            "causal": d.get("causal"),
            "justificacion": d.get("justificacion"),
            "usuario": por.get("usuario"),
            "nombre": por.get("nombre"),
            "fecha": por.get("fecha").isoformat() if hasattr(por.get("fecha"), "isoformat") else por.get("fecha"),
        }
    return mapa


def _adjuntar_novedades(filas: list) -> None:
    """Marca in situ las filas que tienen justificación activa (f['novedad']).
    Se llama ANTES de _serie_ot y de la respuesta para que gráfico y tabla
    vean lo mismo."""
    if not filas:
        return
    mapa = _mapa_novedades_ot()
    if not mapa:
        return
    for f in filas:
        nov = mapa.get(f.get("guia"))
        if nov:
            f["novedad"] = nov


def _serie_ot(filas: list, largo: int) -> list:
    """(2026-10-31) Buckets del gráfico por FECHA DE EMISIÓN con los 4
    segmentos de VEREDICTO del Excel real: Cumplieron / No cumplieron (incluye
    guías en curso ya VENCIDAS: el veredicto compara contra HOY) / Con novedad
    (auto-cumplen: novedad del TMS o justificada manual de /NovedadesOT) /
    No aplica (sin promesa evaluable). El % OT = (cumplieron + conNovedad) /
    (cumplieron + noCumplieron + conNovedad)."""
    buckets: dict = {}
    for f in filas:
        clave = (f.get("fecha_emision") or "")[:largo]
        if not clave:
            continue
        b = buckets.setdefault(clave, {
            "cumplieron": 0, "noCumplieron": 0, "conNovedad": 0, "noAplica": 0,
        })
        if f.get("novedad") or f.get("cumple_novedad"):
            b["conNovedad"] += 1
        elif f.get("ot") == 1:
            b["cumplieron"] += 1
        elif f.get("ot") == 0:
            b["noCumplieron"] += 1
        else:
            b["noAplica"] += 1
    out = []
    for clave in sorted(buckets):
        b = buckets[clave]
        out.append({
            "periodo": clave,
            "cumplieron": b["cumplieron"],
            "noCumplieron": b["noCumplieron"],
            "conNovedad": b["conNovedad"],
            "noAplica": b["noAplica"],
            "evaluables": b["cumplieron"] + b["noCumplieron"] + b["conNovedad"],
            "total": sum(b.values()),
        })
    return out


# Cache del payload de /guias (la data cambia 1×/día con el bot; TTL corto
# para absorber el refresco). POST /citas lo limpia (las citas mueven el OT).
_CACHE_GUIAS: dict = {}
_CACHE_TTL = 300
LIMITE_PAGINA = 200
LIMITE_EXPORT = 30000

# (2026-10-30) Pestaña «Operación» del portal de clientes: consulta de guía
# en Siscore por SOAP — MISMO servicio del bot de WhatsApp
# (Funciones/siscore_ws_tracking.consultar_guia_ws). Devuelve el payload
# crudo {ok, exists, data{...}, tracking_url}; el frontend lo pinta como
# tarjeta web (el formato de texto del bot es solo para WA).
import re as _re_guias
_GUIA_PATTERN = _re_guias.compile(r"^[A-Za-z0-9\-]{4,30}$")


@router.get("/{cliente_id}/consulta-guia")
async def get_consulta_guia(
    cliente_id: str,
    guia: str = Query(..., description="Número de guía a consultar en Siscore"),
    usuario: Optional[str] = Query(None, description="Opcional: valida asignación si el consultante es perfil CLIENTE"),
):
    cliente = CLIENTES.get(cliente_id)
    if not cliente:
        raise HTTPException(status_code=404, detail=f"Cliente no registrado: {cliente_id}")
    if usuario:
        _verificar_acceso_cliente(usuario, cliente_id, cliente)
    g = (guia or "").strip()
    if not _GUIA_PATTERN.match(g):
        raise HTTPException(status_code=422, detail="Número de guía inválido.")

    from Funciones.siscore_ws_tracking import consultar_guia_ws
    try:
        payload = await consultar_guia_ws(g, timeout_seconds=30.0)
    except Exception as e:
        logger.exception(f"[indicadores-cliente] Error consultando guía {g} en Siscore")
        # Al portal entran clientes externos: mensaje genérico (el detalle
        # técnico queda en el log del servidor).
        raise HTTPException(status_code=503, detail="No se pudo consultar la guía en este momento. Intenta de nuevo en unos minutos.")
    if not payload.get("ok"):
        logger.warning(f"[indicadores-cliente] Siscore rechazó la consulta de {g}: {payload.get('error')}")
        raise HTTPException(status_code=502, detail="No se pudo consultar la guía en este momento. Intenta de nuevo en unos minutos.")
    # (2026-10-30) Placa asociada a la guía según el TMS (informe_guias_tms);
    # ciudad destino del TMS como refuerzo de la que trae el WS.
    tms = guia_tms_placa(g) or {}
    if tms.get("placa"):
        payload["placa"] = tms["placa"]
    if tms.get("ciudad_destino"):
        payload["ciudad_destino_tms"] = tms["ciudad_destino"]
    # (2026-10-31) ¿Cumplió la guía el OT? MISMA función del informe
    # (_calcular_ot_filas) sobre la fila del TMS — el veredicto no puede
    # divergir del que ven en el gráfico/tabla. Viaja aparte de
    # `justificada` (Mongo); el frontend prioriza la justificación si
    # rescata una no cumplida.
    if tms.get("fecha_emision"):
        fila_ot = {
            "guia": g,
            "estado": tms.get("estado"),
            "nombre_cliente": tms.get("nombre_cliente"),
            "destinatario": tms.get("destinatario"),
            "ciudad_origen": tms.get("ciudad_origen"),
            "departamento_origen": tms.get("departamento_origen"),
            "departamento_destino": tms.get("departamento_destino"),
            "fecha_carga": tms.get("fecha_carga"),
            "novedad_tms": tms.get("novedad_tms"),
            "fecha_emision": tms.get("fecha_emision"),
            "fecha_entrega": tms.get("fecha_entrega"),
            "fecha_cita": tms.get("fecha_cita"),
            "ciudad_destino": tms.get("ciudad_destino"),
        }
        fila_ot["estado"], _ = _normalizar_estado(fila_ot["estado"])
        _calcular_ot_filas([fila_ot])
        payload["ot"] = {
            "ot": fila_ot.get("ot"),
            "cumple_novedad": fila_ot.get("cumple_novedad"),
            "tipo_servicio": fila_ot.get("tipo_servicio"),
            "ruta": fila_ot.get("ruta"),
            "fecha_emision": fila_ot.get("fecha_emision"),
            "fecha_entrega": fila_ot.get("fecha_entrega"),
            "fecha_promesa": fila_ot.get("fecha_promesa"),
            "origen_promesa": fila_ot.get("origen_promesa"),
            "dias_habiles": fila_ot.get("dias_habiles"),
            "entregada": fila_ot.get("estado") == ESTADO_ENTREGADO,
        }
    # (2026-10-30) Otras guías del mismo vehículo emitidas el mismo día (la
    # remesa que viajó junta) — solo conocible desde el TMS.
    if tms.get("placa") and tms.get("fecha_emision"):
        companeras = guias_placa_dia(tms["placa"], tms["fecha_emision"], g)
        for cp in companeras:
            cp["estado"], _ = _normalizar_estado(cp.get("estado"))
        payload["misma_placa"] = companeras
    # (2026-10-30) ¿La guía tiene novedad JUSTIFICADA registrada?
    # (novedades_justificadas_ot — las que cargan en /NovedadesOT).
    nov = col_novedades_ot.find_one(
        {"guia": g},
        {"causal": 1, "justificacion": 1, "creado_por": 1, "actualizado_por": 1, "_id": 0},
    )
    if nov:
        por = nov.get("actualizado_por") or nov.get("creado_por") or {}
        fecha_por = por.get("fecha")
        payload["justificada"] = {
            "causal": nov.get("causal"),
            "justificacion": nov.get("justificacion"),
            "nombre": por.get("nombre") or por.get("usuario"),
            "fecha": fecha_por.isoformat()[:10] if hasattr(fecha_por, "isoformat") else fecha_por,
        }
    return {"success": True, "data": payload}


@router.get("/{cliente_id}/destinatarios")
def get_destinatarios_cliente(
    cliente_id: str,
    q: str = Query(..., min_length=3, description="Texto parcial del destinatario (ILIKE)"),
    usuario: Optional[str] = Query(None, description="Opcional: valida asignación si el consultante es perfil CLIENTE"),
):
    """(2026-10-30) Autocomplete de «cliente final» (destinatario del TMS)
    para el filtro del dashboard — DISTINCT sobre informe_guias_tms."""
    cliente = CLIENTES.get(cliente_id)
    if not cliente:
        raise HTTPException(status_code=404, detail=f"Cliente no registrado: {cliente_id}")
    if usuario:
        _verificar_acceso_cliente(usuario, cliente_id, cliente)
    nombres = _nombres_por_grupo(cliente, None)
    return {"success": True, "data": {"destinatarios": buscar_destinatarios_cliente(nombres, q)}}


@router.get("/{cliente_id}/guias")
def get_guias_cliente(
    cliente_id: str,
    anio: Optional[List[int]] = Query(None),
    mes: Optional[List[int]] = Query(None),
    grupo: Optional[str] = Query(None, description="(2026-10-29) Sub-grupo del cliente (p. ej. solo FK Servicio Técnico); vacío = todos"),
    usuario: Optional[str] = Query(None, description="(2026-10-29) Opcional: valida asignación si el consultante es perfil CLIENTE"),
    dia: Optional[str] = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$",
                               description="(2026-10-30) Zoom de día del portal: acota TODO el payload "
                                           "(resumen, series y OT por ciudad/destinatario) a las guías "
                                           "EMITIDAS ese día (fecha_emision)"),
    destinatario: Optional[List[str]] = Query(None,
                               description="(2026-10-30) Filtro por cliente final (destinatario del TMS); "
                                           "repetir el parámetro para varios"),
    tipo_servicio: Optional[List[str]] = Query(None,
                               description="(2026-11-03) Filtro por tipo de servicio (fórmula BB del Excel "
                                           "OT real): CARGA, SERVICIO TECNICO, RECOLECCION, DEVOLUCION, "
                                           "MUESTRA DE CALIDAD; repetir el parámetro para varios"),
    sin_filas: bool = Query(False,
                               description="(2026-11-05) No adjuntar la 1ª página de filas (~148KB): la usa "
                                           "el portal externo, que no renderiza la tabla del informe. "
                                           "filas=[] y paginacion.total = guías del filtro"),
):
    """Informe de guías PostgreSQL puro (2026-10-29): TODO sale de
    ``informe_guias_tms`` (antes Mongo media milla + JOIN por guía).

    UNA pasada de columnas mínimas (guias_minimas_cliente) → resumen + series
    OT mensual/diaria (misma función de OT que la tabla) + PRIMERA página de
    filas (200). Las siguientes páginas: ``/guias/filas``. Cache TTL 5 min.

    Población: TODAS las guías del cliente en el TMS del período (antes solo
    las que viajaron en vehículos registrados en Mongo, con cap 20k).
    ``grupo`` acota a un sub-conjunto de sus nombres TMS (``pg_grupos``) y el
    payload informa las etiquetas filtrables en ``grupos``. ``dia`` (2026-10-30)
    recorta la pasada en Python por fecha_emision — así el día del zoom del
    portal recalcula resumen/series/OT-por-campo con los mismos criterios.
    """
    cliente = CLIENTES.get(cliente_id)
    if not cliente:
        raise HTTPException(status_code=404, detail=f"Cliente no registrado: {cliente_id}")
    if usuario:
        _verificar_acceso_cliente(usuario, cliente_id, cliente)
    nombres = _nombres_por_grupo(cliente, grupo)
    if tipo_servicio:
        invalidos = {t for t in tipo_servicio if t not in TIPOS_SERVICIO_OT}
        if invalidos:
            raise HTTPException(status_code=422, detail=f"Tipo de servicio desconocido: {', '.join(sorted(invalidos))}")

    clave = (cliente_id, tuple(sorted(anio or [])), tuple(sorted(mes or [])), grupo or "", dia or "",
             tuple(sorted(destinatario or [])), tuple(sorted(tipo_servicio or [])), sin_filas)
    ahora = _time.monotonic()
    hit = _CACHE_GUIAS.get(clave)
    if hit and ahora - hit[0] < _CACHE_TTL:
        return {"success": True, "data": hit[1], "cache": True}

    t0 = _time.perf_counter()
    try:
        minimas = guias_minimas_cliente(nombres, anio or [], mes or [])
        if dia:
            minimas = [f for f in minimas if (f.get("fecha_emision") or "").startswith(dia)]
        if destinatario:
            sel_dest = set(destinatario)
            minimas = [f for f in minimas if f.get("destinatario") in sel_dest]
        for f in minimas:
            f["estado"], f["estado_crudo"] = _normalizar_estado(f.get("estado"))
        _calcular_ot_filas(minimas)
        _adjuntar_novedades(minimas)
        # (2026-11-03) Filtro por tipo de servicio: va DESPUÉS de calcular el
        # OT porque el tipo se deriva ahí (fórmula BB). Recalcula TODO el
        # payload (resumen, series, OT por ciudad/destinatario) sobre el subconjunto.
        if tipo_servicio:
            sel_tipos = set(tipo_servicio)
            minimas = [f for f in minimas if f.get("tipo_servicio") in sel_tipos]
        t_min = _time.perf_counter() - t0

        # Resumen (los ot_* salen de la MISMA pasada — consistencia garantizada).
        entregadas = sum(1 for f in minimas if f.get("estado") == ESTADO_ENTREGADO)
        en_proceso = sum(1 for f in minimas if f.get("estado") and f["estado"] != ESTADO_ENTREGADO)
        sin_info = sum(1 for f in minimas if not f.get("estado"))
        por_estado = {}
        for f in minimas:
            if f.get("estado"):
                por_estado[f["estado"]] = por_estado.get(f["estado"], 0) + 1
        # OT (2026-10-31, lógica del Excel real): «con novedad» (TMS o
        # justificada manual) auto-cumplen — numerador y denominador; las guías
        # en curso SÍ se juzgan (contra HOY), así que pueden ser cumplidas o
        # vencidas; No aplica = sin promesa evaluable (fuera del %).
        con_novedad = sum(1 for f in minimas if f.get("novedad") or f.get("cumple_novedad"))
        ot_cumplen = sum(1 for f in minimas if f.get("ot") == 1 and not (f.get("novedad") or f.get("cumple_novedad")))
        # Mismo criterio if/elif de _serie_ot: una guía con novedad va SOLO a
        # con_novedad — si no, entra también aquí y el denominador del
        # ot_pct del tacómetro se infla (97,0 vs 97,3 del gráfico vs meta).
        ot_no_cumplen = sum(1 for f in minimas if f.get("ot") == 0 and not (f.get("novedad") or f.get("cumple_novedad")))
        ot_evaluables = ot_cumplen + ot_no_cumplen + con_novedad
        ot_justificadas = con_novedad
        planillas = {f.get("planilla") for f in minimas if f.get("planilla")}
        # (2026-10-31) Cards de la pestaña Operación del portal: destinatarios
        # y ciudades ATENDIDOS = distintos sobre las guías ENTREGADAS (no sirve
        # len(ot_por_*): esos grupos excluyen a los sin evaluables).
        dest_atendidos = {
            f.get("destinatario") for f in minimas
            if f.get("estado") == ESTADO_ENTREGADO and f.get("destinatario")
        }
        ciudades_atendidas = {
            f.get("ciudad_destino") for f in minimas
            if f.get("estado") == ESTADO_ENTREGADO and f.get("ciudad_destino")
        }

        # (2026-11-05) sin_filas: sin la 1ª página (el portal externo no
        # renderiza la tabla — sus exports traen las filas por /guias/filas).
        # Ahorra ~148KB de payload Y el segundo cálculo de OT de la página.
        if sin_filas:
            pagina, total = [], len(minimas)
        else:
            pagina, total = pagina_guias_cliente(
                nombres, anio or [], mes or [], LIMITE_PAGINA, 0,
                tipos_servicio=tipo_servicio,
            )
            for f in pagina:
                f["estado"], f["estado_crudo"] = _normalizar_estado(f.get("estado"))
            _calcular_ot_filas(pagina)
            _adjuntar_novedades(pagina)

        payload = {
            "cliente": cliente["nombre"],
            "grupos": [g["etiqueta"] for g in cliente.get("pg_grupos") or []],
            "resumen": {
                "total_guias": len(minimas),
                "total_planillas": len(planillas),
                "entregadas": entregadas,
                "en_proceso": en_proceso,
                "sin_info": sin_info,
                "por_estado": dict(sorted(por_estado.items(), key=lambda kv: -kv[1])),
                "ot_cumplen": ot_cumplen,
                "ot_no_cumplen": ot_no_cumplen,
                "ot_justificadas": ot_justificadas,
                "ot_con_novedad": con_novedad,
                "ot_no_evaluables": len(minimas) - ot_evaluables,
                "destinatarios_atendidos": len(dest_atendidos),
                "ciudades_atendidas": len(ciudades_atendidas),
                "ot_pct": round(
                    (ot_cumplen + con_novedad) / ot_evaluables * 100, 1
                ) if ot_evaluables else None,
            },
            "serie_mensual": _serie_ot(minimas, 7),
            "serie_diaria": _serie_ot(minimas, 10),
            # (2026-10-30) OT por ciudad destino / destinatario (portal de
            # clientes): barras horizontales del período filtrado.
            # (2026-11-05) Destinatarios con TOP 100: el año Kabi trae ~1.350
            # (180KB — la mitad del payload) y nadie hace scroll más allá de
            # unos decenas; ciudades van completas (pocas, ~15KB).
            "ot_por_ciudad": _ot_por_campo(minimas, "ciudad_destino"),
            "ot_por_destinatario": _ot_por_campo(minimas, "destinatario", top=100),
            "filas": pagina,
            "paginacion": {
                "pagina": 1,
                "limite": LIMITE_PAGINA,
                "total": total,
                "hay_mas": total > LIMITE_PAGINA,
            },
        }
        _CACHE_GUIAS[clave] = (ahora, payload)
        logger.info(
            f"[indicadores-cliente] guías {cliente_id} n={len(minimas)} "
            f"agregación={t_min*1000:.0f}ms total={(_time.perf_counter()-t0)*1000:.0f}ms"
        )
        return {"success": True, "data": payload}
    except HTTPException:
        raise
    except Exception as e:
        # Sin fuente Mongo a la que degradar (la tabla PG ES la fuente): 503.
        logger.exception(f"[indicadores-cliente] Error en guías de {cliente_id}")
        raise HTTPException(status_code=503, detail=f"Informe de guías no disponible: {e}")


@router.get("/{cliente_id}/guias/filas")
def get_guias_filas_cliente(
    cliente_id: str,
    anio: Optional[List[int]] = Query(None),
    mes: Optional[List[int]] = Query(None),
    q: Optional[str] = Query(None, description="Trazabilidad: guía, planilla o destinatario (parcial, case-insensitive). Ignora año/mes"),
    pagina: int = Query(1, ge=1),
    limite: int = Query(LIMITE_PAGINA, ge=1, le=LIMITE_EXPORT),
    grupo: Optional[str] = Query(None, description="(2026-10-29) Sub-grupo del cliente; mismo contrato que /guias"),
    usuario: Optional[str] = Query(None, description="(2026-10-29) Opcional: valida asignación si el consultante es perfil CLIENTE"),
    destinatario: Optional[List[str]] = Query(None, description="(2026-10-30) Filtro por cliente final (destinatario del TMS)"),
    tipo_servicio: Optional[List[str]] = Query(None, description="(2026-11-03) Filtro por tipo de servicio (fórmula BB del Excel OT)"),
):
    """Páginas 2..N de la tabla del informe (la 1 viaja en /guias). Con ``q``:
    ILIKE parcial sobre guía/planilla/destinatario en TODO el histórico del
    cliente. Con ``limite=30000`` lo usa el Export Excel para traer el rango
    completo de una vez."""
    cliente = CLIENTES.get(cliente_id)
    if not cliente:
        raise HTTPException(status_code=404, detail=f"Cliente no registrado: {cliente_id}")
    if usuario:
        _verificar_acceso_cliente(usuario, cliente_id, cliente)
    nombres = _nombres_por_grupo(cliente, grupo)
    if tipo_servicio:
        invalidos = {t for t in tipo_servicio if t not in TIPOS_SERVICIO_OT}
        if invalidos:
            raise HTTPException(status_code=422, detail=f"Tipo de servicio desconocido: {', '.join(sorted(invalidos))}")

    offset = (pagina - 1) * limite
    t0 = _time.perf_counter()
    try:
        if q and q.strip():
            filas, total = buscar_guias_cliente(nombres, q, limite, offset)
        else:
            filas, total = pagina_guias_cliente(nombres, anio or [], mes or [], limite, offset,
                                                destinatarios=destinatario, tipos_servicio=tipo_servicio)
        for f in filas:
            f["estado"], f["estado_crudo"] = _normalizar_estado(f.get("estado"))
        _calcular_ot_filas(filas)
        _adjuntar_novedades(filas)
        logger.info(
            f"[indicadores-cliente] guías/filas {cliente_id} pagina={pagina} "
            f"n={len(filas)} total={total} {(_time.perf_counter()-t0)*1000:.0f}ms"
        )
        return {
            "success": True,
            "data": {
                "cliente": cliente["nombre"],
                "filas": filas,
                "paginacion": {
                    "pagina": pagina,
                    "limite": limite,
                    "total": total,
                    "hay_mas": offset + len(filas) < total,
                },
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"[indicadores-cliente] Error en guías/filas de {cliente_id}")
        raise HTTPException(status_code=503, detail=f"Informe de guías no disponible: {e}")


# ── Uso de vehículos por tipo solicitado ─────────────────────────────────────
# % de uso de cada Veh Solicitado (tipo_vehiculo_sicetac) = kg REALES del
# vehículo (total_kilos_vehiculo) / tope de kg de la categoría solicitada.
# Reglas (topes, tipo y categoría) importadas de indicadores_costo_operacion,
# donde vive la versión para TODAS las operaciones del tablero general.

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

        # Las citas mueven el OT del informe: fuera la cache de /guias.
        _CACHE_GUIAS.clear()

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
