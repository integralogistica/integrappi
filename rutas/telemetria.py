# Telemetría de uso del portal de clientes (2026-11-19)
#
# Mide QUIÉN usa el portal de indicadores de clientes (p.ej. fresenius-kabi)
# y sus pestañas internas, de cualquier perfil, con frecuencia diaria — para
# reportar números de adopción al cliente.
#
# - POST /telemetria/evento : beacon fire-and-forget del frontend (NUNCA falla).
# - GET  /telemetria/resumen: agregados para la página interna solo-ADMIN.
#
# Colección `eventos_portal` (una colección = un propósito, regla de oro del
# README). El frontend envía `usuario` (cookie usuarioPedidosCookie) y el
# backend resuelve perfil/nombre en baseusuarios BEST-EFFORT: un usuario
# desconocido se registra crudo (perfil null), sin rechazar el beacon.
#
# TTL de 180 días sobre `fecha` (datetime BSON): reportería periódica con
# ventana default de 30 días; 180 da margen 6× para re-emitir informes. Si
# algún día se quiere comparativa anual, subir con collMod (nota abajo).

import logging
from collections import Counter
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

from bd.bd_cliente import bd_cliente
from rutas.otros_costos import (
    _resolver_usuario,
    _requiere,
    _ahora_utc,
    _hoy_colombia,
)

logger = logging.getLogger("telemetria")

router = APIRouter(prefix="/telemetria", tags=["Telemetría"])

db = bd_cliente["integra"]
col_eventos = db["eventos_portal"]

# (2026-11-25) "accion" = evento genérico de la app general (detalle.accion
# nombra la acción concreta: export_excel de Histórico, crear_oc, login…);
# "login" = entradas exitosas a la app / portal de conductores.
EVENTOS_VALIDOS = {
    "pageview",
    "pestana",
    "consulta_guia",
    "pendientes",
    "export_excel",
    "accion",
    "login",
}
MODOS_VALIDOS = {"externo", "interno"}
DIAS_TTL = 180

# Índices idempotentes al importar (mismo patrón que otros_costos.py).
# OJO: el TTL va sobre `fecha` (datetime) — Mongo NO aplica TTL sobre strings
# como fecha_col.
try:
    col_eventos.create_index(
        [("cliente_id", 1), ("fecha_col", 1)], name="idx_tl_cliente_fecha"
    )
except Exception as _e:
    logger.warning(f"[TELEMETRIA] No se pudo crear índice cliente/fecha: {_e}")
try:
    col_eventos.create_index(
        [("usuario", 1), ("fecha_col", 1)], name="idx_tl_usuario_fecha"
    )
except Exception as _e:
    logger.warning(f"[TELEMETRIA] No se pudo crear índice usuario/fecha: {_e}")
for _campo in ("evento", "fecha_col"):
    try:
        col_eventos.create_index(_campo)
    except Exception:
        pass
# (2026-11-25) Telemetría general de la app: índice por módulo y hora del día.
try:
    col_eventos.create_index(
        [("modulo", 1), ("fecha_col", 1)], name="idx_tl_modulo_fecha"
    )
except Exception as _e:
    logger.warning(f"[TELEMETRIA] No se pudo crear índice modulo/fecha: {_e}")
try:
    col_eventos.create_index("hora_col")
except Exception:
    pass
try:
    col_eventos.create_index(
        [("fecha", 1)], name="idx_tl_fecha_ttl", expireAfterSeconds=DIAS_TTL * 86400
    )
except Exception as _e:
    logger.warning(f"[TELEMETRIA] No se pudo crear índice TTL: {_e}")


# ── Beacon del frontend ───────────────────────────────────────────────────────
class EventoBeacon(BaseModel):
    usuario: str = Field(..., max_length=60)
    # (2026-11-25) Optional: los beacons de la app general (telemetriaApp.ts) no
    # llevan cliente_id — solo el módulo. El portal Kabi sigue enviándolo.
    cliente_id: Optional[str] = Field(None, max_length=60)
    # (2026-11-25) Módulo de la app (tabla ruta→módulo del frontend global).
    modulo: Optional[str] = Field(None, max_length=30)
    modo: str = Field("externo", max_length=10)
    evento: str = Field(..., max_length=30)
    detalle: dict = Field(default_factory=dict)
    sesion_id: str = Field(..., max_length=64)


def _sanear_detalle(detalle: dict) -> dict:
    """Claves/valores a string recortados, máx 8 claves (defensa en profundidad)."""
    saneado = {}
    for i, (k, v) in enumerate(detalle.items()):
        if i >= 8:
            break
        saneado[str(k)[:40]] = str(v)[:120]
    return saneado


@router.post("/evento")
async def registrar_evento(ev: EventoBeacon, request: Request):
    # Beacon: cualquier anomalía responde 200 rápido — jamás ruido en consola
    # del navegador ni bloquea la navegación.
    if ev.evento not in EVENTOS_VALIDOS or ev.modo not in MODOS_VALIDOS:
        return {"success": True, "guardado": False}

    try:
        info = _resolver_usuario(ev.usuario)
        usuario = info["usuario"]
        nombre = info.get("nombre", "")
        perfil = info.get("perfil")
        regional = info.get("regional", "")
    except HTTPException:
        # Usuario sin cookie válida / no existe en baseusuarios: se registra
        # crudo para no perder el evento (perfil null lo delata en reportes).
        usuario = str(ev.usuario).strip().upper()
        nombre = ""
        perfil = None
        regional = ""

    ahora_col = _hoy_colombia()
    doc = {
        "fecha": _ahora_utc(),
        "fecha_col": ahora_col.strftime("%Y-%m-%d"),
        "usuario": usuario,
        "nombre": nombre,
        "perfil": perfil,
        "regional": regional,
        "cliente_id": ev.cliente_id,
        # (2026-11-25) Telemetría general: módulo (mayúsculas; None para el
        # portal Kabi, que se clasifica por cliente_id) y hora del día en
        # Colombia computada en el BACKEND (no se confía en el reloj cliente)
        # para el análisis de horas pico. Eventos legacy: campo ausente.
        "modulo": (ev.modulo or "").strip().upper() or None,
        "hora_col": ahora_col.hour,
        "modo": ev.modo,
        "evento": ev.evento,
        "detalle": _sanear_detalle(ev.detalle),
        "sesion_id": ev.sesion_id,
        "user_agent": request.headers.get("user-agent", "")[:250],
        "ip": request.client.host if request.client else "",
    }
    try:
        col_eventos.insert_one(doc)
    except Exception as e:
        logger.warning(f"[TELEMETRIA] No se pudo guardar evento {ev.evento}: {e}")
        return {"success": True, "guardado": False}
    return {"success": True, "guardado": True}


# ── Resumen para la página interna (solo ADMIN) ───────────────────────────────
@router.get("/resumen")
async def resumen(
    usuario: str = Query(..., description="Usuario ADMIN que consulta (cookie)"),
    cliente_id: Optional[str] = Query(None, description="Filtro por portal (opcional)"),
    modulo: Optional[str] = Query(
        None, description="Filtro por módulo de la app (2026-11-25, telemetría general)"
    ),
    desde: Optional[str] = Query(None, description="YYYY-MM-DD (default: hace 29 días)"),
    hasta: Optional[str] = Query(None, description="YYYY-MM-DD (default: hoy Colombia)"),
    usuario_filtro: Optional[str] = Query(
        None, description="Limita TODO el resumen a un usuario (2026-11-20)"
    ),
    audiencia: str = Query(
        "todos", description="todos | clientes (perfil CLIENTE) | internos (2026-11-20)"
    ),
):
    info = _resolver_usuario(usuario)
    _requiere(info, {"ADMIN"}, "ver la telemetría del portal")

    hoy = _hoy_colombia().strftime("%Y-%m-%d")
    hasta_val = hasta or hoy
    desde_val = desde or (_hoy_colombia() - timedelta(days=29)).strftime("%Y-%m-%d")
    for etiqueta, valor in (("desde", desde_val), ("hasta", hasta_val)):
        try:
            datetime.strptime(valor, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(
                status_code=422, detail=f"Fecha '{etiqueta}' inválida: usar YYYY-MM-DD"
            )
    if desde_val > hasta_val:
        raise HTTPException(status_code=422, detail="'desde' no puede ser mayor que 'hasta'")
    if audiencia not in ("todos", "clientes", "internos"):
        raise HTTPException(
            status_code=422,
            detail="audiencia debe ser: todos | clientes | internos",
        )

    match: dict = {"fecha_col": {"$gte": desde_val, "$lte": hasta_val}}
    if cliente_id:
        match["cliente_id"] = cliente_id
    # (2026-11-25) Filtro por módulo de la app general (va en mayúsculas en el doc).
    if modulo:
        match["modulo"] = modulo.strip().upper()
    # (2026-11-20) Foco en un usuario: para la reunión con el cliente, ver SU
    # uso del portal (KPIs, días, pestañas — todo el resumen acotado).
    if usuario_filtro:
        match["usuario"] = usuario_filtro.strip().upper()
    # (2026-11-20) Audiencia: CLIENTE externo vs usuarios internos (los demás
    # perfiles, incl. perfil null = no resuelto en baseusuarios).
    if audiencia == "clientes":
        match["perfil"] = "CLIENTE"
    elif audiencia == "internos":
        match["perfil"] = {"$ne": "CLIENTE"}

    # Por día: eventos + usuarios únicos + DESGLOSE POR MÓDULO (2026-11-25,
    # 9.ª tanda: barras apiladas por módulo en el dashboard de uso). Se agrupa
    # por (fecha, módulo clasificado) y se re-ensambla en Python (usuarios de
    # la cubeta = unión de los usuarios de cada módulo). Las claves
    # fecha/eventos/usuarios no cambian → uso-kabi (UsoPortalKP) intacto.
    _clasif_modulo = {
        "$ifNull": [
            "$modulo",
            {"$concat": ["portal_", {"$ifNull": ["$cliente_id", "sin_clasificar"]}]},
        ]
    }
    acumulado_dia: dict = {}
    for g in col_eventos.aggregate(
        [
            {"$match": match},
            {
                "$group": {
                    "_id": {"fecha": "$fecha_col", "modulo": _clasif_modulo},
                    "eventos": {"$sum": 1},
                    "usr": {"$addToSet": "$usuario"},
                }
            },
        ]
    ):
        fecha = g["_id"]["fecha"]
        acc = acumulado_dia.setdefault(fecha, {"usr": set(), "mods": Counter()})
        acc["usr"].update(g["usr"])
        acc["mods"][g["_id"]["modulo"]] += g["eventos"]
    por_dia = [
        {
            "fecha": fecha,
            "eventos": sum(acc["mods"].values()),
            "usuarios": len(acc["usr"]),
            "por_modulo": dict(acc["mods"]),
        }
        for fecha, acc in sorted(acumulado_dia.items())
    ]

    # Por usuario: una sola pasada; el desglose por evento se cuenta en Python.
    # ⚠️ $first como ACUMULADOR solo vale en el nivel superior del $group —
    # dentro de un _id anidado Mongo lo lee como la EXPRESIÓN $first (espera
    # array) y revienta con Location28689 (bug productivo 2026-11-19).
    por_usuario = []
    for d in col_eventos.aggregate(
        [
            {"$match": match},
            {
                "$group": {
                    "_id": "$usuario",
                    "nombre": {"$first": "$nombre"},
                    "perfil": {"$first": "$perfil"},
                    "eventos": {"$sum": 1},
                    "dias": {"$addToSet": "$fecha_col"},
                    "ultima_conexion": {"$max": "$fecha"},
                    "evs": {"$push": "$evento"},
                }
            },
            {"$sort": {"eventos": -1}},
        ]
    ):
        por_usuario.append(
            {
                "usuario": d["_id"],
                "nombre": d.get("nombre") or "",
                "perfil": d.get("perfil"),
                "eventos": d["eventos"],
                "dias_activos": len(d["dias"]),
                "ultima_conexion": d["ultima_conexion"],
                "por_evento": dict(Counter(d["evs"])),
            }
        )

    por_evento = [
        {"evento": d["_id"], "total": d["total"]}
        for d in col_eventos.aggregate(
            [
                {"$match": match},
                {"$group": {"_id": "$evento", "total": {"$sum": 1}}},
                {"$sort": {"total": -1}},
            ]
        )
    ]

    # Pestañas: pageview lleva la pestaña inicial y `pestana` los cambios
    por_pestana = [
        {"pestana": d["_id"], "total": d["total"]}
        for d in col_eventos.aggregate(
            [
                {"$match": {**match, "evento": {"$in": ["pageview", "pestana"]}, "detalle.pestana": {"$exists": True}}},
                {"$group": {"_id": "$detalle.pestana", "total": {"$sum": 1}}},
                {"$sort": {"total": -1}},
            ]
        )
    ]

    por_cliente = sorted(
        (
            {"cliente_id": d["_id"], "eventos": d["eventos"], "usuarios": len(d["usr"])}
            for d in col_eventos.aggregate(
                [
                    {"$match": match},
                    {
                        "$group": {
                            "_id": "$cliente_id",
                            "eventos": {"$sum": 1},
                            "usr": {"$addToSet": "$usuario"},
                        }
                    },
                ]
            )
        ),
        key=lambda x: -x["eventos"],
    )

    # (2026-11-25) Por módulo de la app: los eventos del portal Kabi (legacy y
    # actuales) no llevan `modulo` — el $ifNull los clasifica como
    # portal_<cliente_id> para que no aparezcan como null.
    por_modulo = sorted(
        (
            {
                "modulo": d["_id"],
                "eventos": d["eventos"],
                "usuarios": len(d["usr"]),
            }
            for d in col_eventos.aggregate(
                [
                    {"$match": match},
                    {
                        "$group": {
                            "_id": {
                                "$ifNull": [
                                    "$modulo",
                                    {
                                        "$concat": [
                                            "portal_",
                                            {"$ifNull": ["$cliente_id", "sin_clasificar"]},
                                        ]
                                    },
                                ]
                            },
                            "eventos": {"$sum": 1},
                            "usr": {"$addToSet": "$usuario"},
                        }
                    },
                ]
            )
        ),
        key=lambda x: -x["eventos"],
    )

    # (2026-11-25) Por hora del día en Colombia (0-23): horas pico, con
    # desglose por módulo (9.ª tanda). Los eventos legacy (sin hora_col)
    # quedan fuera — el frontend rellena las 24 horas.
    acumulado_hora: dict = {}
    for g in col_eventos.aggregate(
        [
            {"$match": {**match, "hora_col": {"$type": "int"}}},
            {
                "$group": {
                    "_id": {"hora": "$hora_col", "modulo": _clasif_modulo},
                    "eventos": {"$sum": 1},
                    "usr": {"$addToSet": "$usuario"},
                }
            },
        ]
    ):
        hora = g["_id"]["hora"]
        acc = acumulado_hora.setdefault(hora, {"usr": set(), "mods": Counter()})
        acc["usr"].update(g["usr"])
        acc["mods"][g["_id"]["modulo"]] += g["eventos"]
    por_hora = [
        {
            "hora": hora,
            "eventos": sum(acc["mods"].values()),
            "usuarios": len(acc["usr"]),
            "por_modulo": dict(acc["mods"]),
        }
        # ⚠️ iterar .items(): usar solo `for hora in ...` reutilizaría la
        # variable `acc` que FUGA de la comprehension de por_dia (bug real
        # detectado por los tests: todas las horas mostraban el último día).
        for hora, acc in sorted(acumulado_hora.items())
    ]

    sesiones = list(
        col_eventos.aggregate(
            [
                {"$match": match},
                {"$group": {"_id": None, "s": {"$addToSet": "$sesion_id"}}},
            ]
        )
    )
    sesiones_unicas = len(sesiones[0]["s"]) if sesiones else 0

    kpi = {
        "eventos_total": sum(e["total"] for e in por_evento),
        "usuarios_unicos": len(por_usuario),
        "sesiones_unicas": sesiones_unicas,
        "dias_activos": len(por_dia),
    }

    return {
        "success": True,
        "data": {
            "kpi": kpi,
            "por_dia": por_dia,
            "por_usuario": por_usuario,
            "por_evento": por_evento,
            "por_pestana": por_pestana,
            "por_cliente": por_cliente,
            "por_modulo": por_modulo,
            "por_hora": por_hora,
        },
        "rango": {
            "desde": desde_val,
            "hasta": hasta_val,
            "cliente_id": cliente_id,
            "modulo": modulo,
            "usuario_filtro": usuario_filtro,
            "audiencia": audiencia,
        },
    }
