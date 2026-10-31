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
from rutas.fletes import coleccion_fletes
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
    if perfil == "ADMIN":
        # ADMIN siempre ve todo: TODOS los dashboards publicados (sin depender
        # de su asignación clientes_siscore).
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


def _mapa_promesa_destinos() -> dict:
    """{DESTINO_NORMALIZADO: promesa_entrega_dias} desde tarifas origen FUNZA.

    (2026-10-29) Cache TTL 10 min — antes consultaba Mongo en cada request del
    informe. Las claves se NORMALIZAN (solo letras/dígitos) para absorber
    variantes catálogo↔TMS: 'BOGOTA D.C.' ↔ 'BOGOTA, D C' → 'BOGOTADC'."""
    global _CACHE_PROMESAS
    ahora = _time.monotonic()
    ts, mapa = _CACHE_PROMESAS
    if mapa is not None and ahora - ts < 600:
        return mapa
    out = {}
    for t in coleccion_fletes.find({"origen": "FUNZA"}, {"destino": 1, "promesa_entrega_dias": 1}):
        dest = (t.get("destino") or "").strip().upper()
        dias = t.get("promesa_entrega_dias") or 0
        if dest and dias > 0:
            out[_normalizar_ciudad(dest)] = int(dias)
    _CACHE_PROMESAS = (ahora, out)
    return out


_CACHE_PROMESAS = (0.0, None)


def _normalizar_ciudad(texto: str) -> str:
    """'BOGOTA, D C'/'BOGOTA D.C.' → 'BOGOTADC' (upper, solo alfanuméricos)."""
    return re.sub(r"[^A-Z0-9]", "", (texto or "").upper())


def _dias_promesa_destino(ciudad: Optional[str], mapa: dict) -> Optional[int]:
    """Días de promesa para una ciudad del TMS. Match exacto normalizado y, si
    no hay, prefijo ÚNICO de hasta 4 caracteres extra (cubre 'BOGOTA'→'BOGOTADC'
    o 'FLORIDABLANCA'→'FLORIDABLANCA'; ambiguos → None, igual que sin promesa)."""
    if not ciudad:
        return None
    n = _normalizar_ciudad(ciudad.split(",")[0])
    if not n:
        return None
    if n in mapa:
        return mapa[n]
    candidatos = [d for k, d in mapa.items() if k.startswith(n) and 0 < len(k) - len(n) <= 4]
    return candidatos[0] if len(candidatos) == 1 else None


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
    """Setea EN SITIO fecha_promesa/origen_promesa/dias_habiles/ot sobre filas
    dicts con {guia, estado, fecha_emision, fecha_entrega, ciudad_destino,
    fecha_cita}. ÚNICA implementación del OT — la usan la pasada de agregación
    (series del gráfico) y cada página de la tabla, así nunca divergen.

    Misma lógica del flujo Mongo original: promesa = cita TMS si es servible →
    cita plan B (citas_kabi) → emisión + promesa del destino (días hábiles).
    Solo evaluable: ENTREGADO con fecha de entrega."""
    if not filas:
        return

    # Plan B citas_kabi: solo para guías SIN cita TMS servible (lectura barata).
    sin_cita = [f["guia"] for f in filas if not _parse_fecha_tms(f.get("fecha_cita"))]
    citas_kb = {}
    if sin_cita:
        for c in col_citas.find(
            {"guia": {"$in": sin_cita}}, {"guia": 1, "fecha_cita": 1}
        ):
            if c.get("fecha_cita"):
                citas_kb[c["guia"]] = c["fecha_cita"]

    promesas = _mapa_promesa_destinos()

    for f in filas:
        f_ot = None
        f_promesa = None
        origen = None
        f_dias = None
        if f.get("estado") == ESTADO_ENTREGADO and f.get("fecha_entrega"):
            inicial = _fecha_servible(f.get("fecha_emision"))
            entrega = _fecha_servible(f["fecha_entrega"])
            cita = _parse_fecha_tms(f.get("fecha_cita"))
            if inicial and entrega:
                if not cita:
                    cita_kb = citas_kb.get(f["guia"])
                    if cita_kb:
                        cita = cita_kb if isinstance(cita_kb, date) else _fecha_servible(str(cita_kb)[:10])
                if cita:
                    f_promesa = cita.isoformat()
                    origen = "CITA"
                else:
                    dias = _dias_promesa_destino(f.get("ciudad_destino"), promesas)
                    if dias:
                        f_promesa = _sumar_dias_habiles(inicial, dias).isoformat()
                        origen = "PROMESA"
                if f_promesa:
                    f_dias = _dias_habiles_entre(inicial, entrega)
                    f_ot = 1 if entrega <= _fecha_servible(f_promesa) else 0

        f["fecha_promesa"] = f_promesa
        f["origen_promesa"] = origen
        f["dias_habiles"] = f_dias
        f["ot"] = f_ot


def _ot_por_campo(filas: list, campo: str, top: int | None = None) -> list:
    """(2026-10-30) OT agregado por un campo de la fila (ciudad_destino,
    destinatario) para las barras horizontales del portal de clientes.
    Mismo criterio del % OT: justificadas cuentan como cumplidas (numerador
    y denominador). Devuelve TODOS los grupos con evaluables (sin tope — el
    usuario hace scroll), ordenados del MÁS cumplido al menos (desempate por
    volumen)."""
    grupos: dict = {}
    for f in filas:
        clave = (f.get(campo) or "").strip() if isinstance(f.get(campo), str) else ""
        if not clave:
            continue
        g = grupos.setdefault(clave, {"c": 0, "n": 0, "j": 0, "t": 0})
        g["t"] += 1
        if f.get("novedad"):
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
            "justificadas": g["j"],
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
    """(2026-10-29, tarde) Buckets del gráfico por FECHA DE EMISIÓN (antes:
    entrega con fallback emisión — no cuadraba contra el Excel filtrado por
    F. Emisión). Segmentos = la división REAL de estados del TMS: ENTREGADO se
    parte en cumplieron/noCumplieron (ot 1/0) y cada estado en curso viaja
    solo (En distribucion, Transito Nacional, PENDIENTE, CON NOVEDAD); el
    resto (sin estado servible, o ENTREGADO sin fecha de entrega → no
    evaluable) cae en sin_info. Una guía con NOVEDAD justificada
    (f['novedad'], cargo _adjuntar_novedades) GANA a todo: segmento
    «justificadas» — viaja sola en la serie, pero el % OT que deriva el
    frontend la CUENTA COMO CUMPLIDA (numerador y denominador)."""
    buckets: dict = {}
    for f in filas:
        clave = (f.get("fecha_emision") or "")[:largo]
        if not clave:
            continue
        b = buckets.setdefault(clave, {
            "cumplieron": 0, "noCumplieron": 0, "justificadas": 0, "con_novedad": 0,
            "en_distribucion": 0, "transito": 0, "pendiente": 0, "sin_info": 0,
        })
        estado = f.get("estado")
        if f.get("novedad"):
            b["justificadas"] += 1
        elif f.get("ot") == 1:
            b["cumplieron"] += 1
        elif f.get("ot") == 0:
            b["noCumplieron"] += 1
        elif estado == "CON NOVEDAD":
            b["con_novedad"] += 1
        elif estado == "En distribucion":
            b["en_distribucion"] += 1
        elif estado == "Transito Nacional":
            b["transito"] += 1
        elif estado == "PENDIENTE":
            b["pendiente"] += 1
        else:
            b["sin_info"] += 1
    out = []
    for clave in sorted(buckets):
        b = buckets[clave]
        out.append({
            "periodo": clave,
            "cumplieron": b["cumplieron"],
            "noCumplieron": b["noCumplieron"],
            "justificadas": b["justificadas"],
            "con_novedad": b["con_novedad"],
            "en_distribucion": b["en_distribucion"],
            "transito": b["transito"],
            "pendiente": b["pendiente"],
            "sin_info": b["sin_info"],
            "evaluables": b["cumplieron"] + b["noCumplieron"],
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

    clave = (cliente_id, tuple(sorted(anio or [])), tuple(sorted(mes or [])), grupo or "", dia or "",
             tuple(sorted(destinatario or [])))
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
        t_min = _time.perf_counter() - t0

        # Resumen (los ot_* salen de la MISMA pasada — consistencia garantizada).
        entregadas = sum(1 for f in minimas if f.get("estado") == ESTADO_ENTREGADO)
        en_proceso = sum(1 for f in minimas if f.get("estado") and f["estado"] != ESTADO_ENTREGADO)
        sin_info = sum(1 for f in minimas if not f.get("estado"))
        por_estado = {}
        for f in minimas:
            if f.get("estado"):
                por_estado[f["estado"]] = por_estado.get(f["estado"], 0) + 1
        # OT (2026-10-30): las guías con novedad justificada CUENTAN COMO
        # CUMPLIDAS — van al numerador y al denominador (antes quedaban
        # excluidas del %). La novedad sigue ganando a su estado TMS, así una
        # entregada tarde pero justificada también suma como cumplimiento.
        ot_cumplen = sum(1 for f in minimas if f.get("ot") == 1 and not f.get("novedad"))
        ot_no_cumplen = sum(1 for f in minimas if f.get("ot") == 0 and not f.get("novedad"))
        ot_evaluables = ot_cumplen + ot_no_cumplen
        ot_justificadas = sum(1 for f in minimas if f.get("novedad"))
        planillas = {f.get("planilla") for f in minimas if f.get("planilla")}

        pagina, total = pagina_guias_cliente(
            nombres, anio or [], mes or [], LIMITE_PAGINA, 0
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
                "ot_no_evaluables": len(minimas) - ot_evaluables - ot_justificadas,
                "ot_pct": round(
                    (ot_cumplen + ot_justificadas) / (ot_evaluables + ot_justificadas) * 100, 1
                ) if (ot_evaluables + ot_justificadas) else None,
            },
            "serie_mensual": _serie_ot(minimas, 7),
            "serie_diaria": _serie_ot(minimas, 10),
            # (2026-10-30) OT por ciudad destino / destinatario (portal de
            # clientes): barras horizontales del período filtrado.
            "ot_por_ciudad": _ot_por_campo(minimas, "ciudad_destino"),
            "ot_por_destinatario": _ot_por_campo(minimas, "destinatario"),
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

    offset = (pagina - 1) * limite
    t0 = _time.perf_counter()
    try:
        if q and q.strip():
            filas, total = buscar_guias_cliente(nombres, q, limite, offset)
        else:
            filas, total = pagina_guias_cliente(nombres, anio or [], mes or [], limite, offset, destinatarios=destinatario)
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
