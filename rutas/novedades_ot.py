"""
Módulo «Novedades Justificadas OT» — Integrapp.

Gestión de justificaciones de guías para el indicador On Time: una guía
justificada (p. ej. por reagenda de cita, siniestro o derrumbe) sale de su
segmento de estado TMS en el gráfico OT del dashboard de clientes y pasa al
segmento «Justificadas», excluida de los evaluables del % OT (fue excusada).

Colecciones (base `integra`):
  - `causales_ot`: catálogo de causales permitidos (seed: «reagenda cita»,
    «siniestro», «derrumbe»). Ampliable por ADMIN.
  - `novedades_justificadas_ot`: UNA por guía (índice unique) con
    {guia, causal, justificacion, creado_por, actualizado_por, historial[]}.
  - `auditoria_novedades_ot`: bitácora append-only de CADA acción (carga,
    actualización, borrado) con usuario/nombre/perfil/fecha — para auditoría
    aunque el documento se haya borrado.

Perfiles: ADMIN, COORDINADOR, CONTROL y OPERATIVO (cargar, ver, borrar).
La auditoría la puede consultar ADMIN y CONTROL.
"""

import asyncio
import io
import logging
import re
import unicodedata
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import Response
from openpyxl import Workbook, load_workbook
from pydantic import BaseModel
from openpyxl.styles import Font, PatternFill

from bson import ObjectId
from pymongo import UpdateOne

from bd.bd_cliente import bd_cliente
from rutas.otros_costos import _ahora_utc, _requiere, _resolver_usuario

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/novedades-ot", tags=["Novedades Justificadas OT"])

PERFILES_NOVEDADES = {"ADMIN", "COORDINADOR", "CONTROL", "OPERATIVO"}
PERFILES_AUDITORIA = {"ADMIN", "CONTROL"}

db = bd_cliente["integra"]
col_causales = db["causales_ot"]
col_novedades = db["novedades_justificadas_ot"]
col_auditoria = db["auditoria_novedades_ot"]

# Seed del catálogo de causales (idempotente) + índices.
CAUSALES_SEED = ["reagenda cita", "siniestro", "derrumbe"]
try:
    if col_causales.count_documents({}) == 0:
        col_causales.insert_many([{"causal": c, "orden": i} for i, c in enumerate(CAUSALES_SEED)])
except Exception as _e:
    logger.warning(f"[NOVEDADES_OT] No se pudo sembrar causales_ot: {_e}")
try:
    col_causales.create_index("causal", unique=True, name="idx_causales_ot_causal")
    col_novedades.create_index("guia", unique=True, name="idx_nov_guia")
    col_novedades.create_index("causal")
    col_auditoria.create_index("fecha")
except Exception as _e:
    logger.warning(f"[NOVEDADES_OT] No se pudieron crear índices: {_e}")

MEDIA_TYPE_XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
CABECERAS = ["GUIA", "CAUSAL", "JUSTIFICACION"]


# ── Helpers ───────────────────────────────────────────────────────────────────


def _sin_acentos(s) -> str:
    return unicodedata.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode()


def _norm_col(s) -> str:
    """Cabecera Excel → clave normalizada (sin acentos, lower, espacios colapsados)."""
    return re.sub(r"\s+", " ", _sin_acentos(s).strip().lower())


def _celda_texto(v) -> str:
    """Celda Excel → texto limpio (cero decimal de openpyxl: 1.23e+08 → '123000000')."""
    if v is None:
        return ""
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return str(v).strip()


def _causales_validos() -> dict:
    """Mapa normalizado → causal canónico (la comparación es tolerante)."""
    return {
        _norm_col(c["causal"]): c["causal"]
        for c in col_causales.find({}, {"causal": 1})
    }


def _resolver_causal(valor: str, validos: dict) -> str:
    """Causal en cualquier escritura → canónico del catálogo, o 422."""
    v = _norm_col(valor)
    if v in validos:
        return validos[v]
    raise HTTPException(
        status_code=422,
        detail=f"Causal '{valor}' no permitido. Causales válidos: {', '.join(validos.values())}.",
    )


def _validar_guia(valor: str) -> str:
    g = str(valor or "").strip().upper()
    if not g:
        raise HTTPException(status_code=422, detail="La guía es obligatoria.")
    if len(g) > 30:
        raise HTTPException(status_code=422, detail=f"Guía '{valor}' demasiado larga.")
    return g


def _auditar(accion: str, info: dict, guia: str, detalle: dict, ahora: datetime) -> None:
    """Append en la bitácora (nunca bloquea la operación si falla)."""
    try:
        col_auditoria.insert_one({
            "accion": accion,  # creacion | actualizacion | borrado | causal_creado
            "guia": guia,
            "usuario": info["usuario"],
            "nombre": info["nombre"],
            "perfil": info["perfil"],
            "fecha": ahora,
            "detalle": detalle,
        })
    except Exception as e:
        logger.warning(f"[NOVEDADES_OT] Auditoría falló ({accion} {guia}): {e}")


def _limpiar_cache_guias() -> None:
    """Las justificaciones cambian el payload de /guias → cache fuera."""
    try:
        from rutas.indicadores_cliente import _CACHE_GUIAS
        _CACHE_GUIAS.clear()
    except Exception as e:
        logger.warning(f"[NOVEDADES_OT] No se pudo limpiar el cache de /guias: {e}")


def _serializar(doc: dict) -> dict:
    out = dict(doc)
    out["id"] = str(out.pop("_id", ""))
    for k, v in out.items():
        if isinstance(v, datetime):
            out[k] = v.isoformat()
        if k in ("creado_por", "actualizado_por") and isinstance(v, dict):
            out[k] = {kk: (vv.isoformat() if isinstance(vv, datetime) else vv) for kk, vv in v.items()}
    if isinstance(out.get("historial"), list):
        out["historial"] = [
            {kk: (vv.isoformat() if isinstance(vv, datetime) else vv) for kk, vv in h.items()}
            if isinstance(h, dict) else h
            for h in out["historial"]
        ]
    return out


# ── Causales ──────────────────────────────────────────────────────────────────


@router.get("/causales")
async def listar_causales(usuario: str = Query(...)):
    info = _resolver_usuario(usuario)
    _requiere(info, PERFILES_NOVEDADES, "consultar los causales OT")
    docs = list(col_causales.find({}, {"_id": 0}).sort("orden", 1))
    return {"causales": [d["causal"] for d in docs]}


@router.post("/causales")
async def crear_causal(usuario: str = Query(...), causal: str = Query(...)):
    """Ampliar el catálogo (solo ADMIN). El seed trae «reagenda cita»,
    «siniestro» y «derrumbe»."""
    info = _resolver_usuario(usuario)
    _requiere(info, {"ADMIN"}, "crear causales OT")
    c = str(causal or "").strip().lower()
    if not c:
        raise HTTPException(status_code=422, detail="El causal no puede estar vacío.")
    if col_causales.find_one({"causal": c}):
        raise HTTPException(status_code=422, detail=f"El causal '{c}' ya existe.")
    orden = col_causales.count_documents({})
    col_causales.insert_one({"causal": c, "orden": orden})
    _auditar("causal_creado", info, "", {"causal": c}, _ahora_utc())
    return {"mensaje": f"Causal '{c}' creado."}


# ── Listado / borrado / auditoría ─────────────────────────────────────────────


@router.get("/")
async def listar(usuario: str = Query(...), q: str = Query(""), pagina: int = Query(1, ge=1), limite: int = Query(50, ge=1, le=200)):
    """Justificaciones activas (una por guía), con quién registró/actualizó.
    `q` busca por guía, causal o justificación (parcial, case-insensitive)."""
    info = _resolver_usuario(usuario)
    _requiere(info, PERFILES_NOVEDADES, "ver las novedades justificadas OT")
    filtro: dict = {}
    if q.strip():
        patron = {"$regex": re.escape(q.strip()), "$options": "i"}
        filtro = {"$or": [{"guia": patron}, {"causal": patron}, {"justificacion": patron}]}
    total = col_novedades.count_documents(filtro)
    docs = (
        col_novedades.find(filtro)
        .sort("updated_at", -1)
        .skip((pagina - 1) * limite)
        .limit(limite)
    )
    return {"total": total, "pagina": pagina, "novedades": [_serializar(d) for d in docs]}


@router.delete("/eliminar")
async def eliminar(usuario: str = Query(...), id: str = Query(...), request: Request = None):
    """Borra la justificación de la guía (queda en auditoría quién borró y qué
    se borró — el detalle completo viaja en detalle.documento)."""
    info = _resolver_usuario(usuario)
    _requiere(info, PERFILES_NOVEDADES, "borrar novedades justificadas OT")
    try:
        doc = col_novedades.find_one({"_id": ObjectId(id)})
    except Exception:
        doc = None
    if not doc:
        raise HTTPException(status_code=404, detail="Novedad no encontrada.")
    ahora = _ahora_utc()
    col_novedades.delete_one({"_id": doc["_id"]})
    _auditar("borrado", info, doc["guia"], {"documento": _serializar(doc)}, ahora)
    _limpiar_cache_guias()
    return {"mensaje": f"Justificación de la guía {doc['guia']} eliminada (queda en auditoría)."}


@router.get("/auditoria")
async def auditoria(usuario: str = Query(...), limite: int = Query(200, ge=1, le=1000)):
    """Bitácora de acciones (carga/actualización/borrado) — ADMIN y CONTROL."""
    info = _resolver_usuario(usuario)
    _requiere(info, PERFILES_AUDITORIA, "ver la auditoría de novedades OT")
    docs = col_auditoria.find({}, {"detalle.documento.historial": 0}).sort("fecha", -1).limit(limite)
    return {"registros": [_serializar(d) for d in docs]}


# ── Registro manual (una guía, sin Excel) ─────────────────────────────────────


class NovedadIN(BaseModel):
    guia: str
    causal: str
    justificacion: str


@router.post("/guardar")
async def guardar(datos: NovedadIN, usuario: str = Query(...)):
    """Crea o actualiza UNA justificación a mano (2026-10-30). Misma semántica
    que importar-excel: upsert por guía (si la guía ya tenía justificación se
    actualiza y el valor previo queda en historial[]), causal contra el
    catálogo (tolerante), auditoría y limpieza del cache de /guias."""
    info = _resolver_usuario(usuario)
    _requiere(info, PERFILES_NOVEDADES, "registrar novedades justificadas OT")
    guia = _validar_guia(datos.guia)
    validos = _causales_validos()
    if not validos:
        raise HTTPException(status_code=503, detail="El catálogo causales_ot está vacío.")
    causal = _resolver_causal(datos.causal, validos)
    justificacion = datos.justificacion.strip()
    if not justificacion:
        raise HTTPException(status_code=422, detail="La justificación es obligatoria.")

    ahora = _ahora_utc()
    auditoria = {"usuario": info["usuario"], "nombre": info["nombre"], "perfil": info["perfil"], "fecha": ahora}
    previo = col_novedades.find_one({"guia": guia}, {"causal": 1, "justificacion": 1})
    cambio = {
        "guia": guia,
        "causal": causal,
        "justificacion": justificacion,
        "actualizado_por": auditoria,
        "updated_at": ahora,
    }
    if previo:
        cambio["historial"] = {
            "$each": [{
                "causal": previo.get("causal"),
                "justificacion": previo.get("justificacion"),
                "por": auditoria,
                "fecha": ahora,
            }],
            "$slice": -20,
        }
    col_novedades.update_one(
        {"guia": guia},
        {"$set": cambio, "$setOnInsert": {"creado_por": auditoria, "created_at": ahora}},
        upsert=True,
    )
    _auditar(
        "actualizacion" if previo else "creacion",
        info, guia,
        {"causal": causal, "justificacion": justificacion,
         **({"previo": {"causal": previo.get("causal"), "justificacion": previo.get("justificacion")}} if previo else {})},
        ahora,
    )
    _limpiar_cache_guias()
    accion = "actualizada" if previo else "creada"
    return {"mensaje": f"Justificación de la guía {guia} {accion}.", "accion": accion}


# ── Carga Excel ───────────────────────────────────────────────────────────────


@router.post("/importar-excel")
async def importar_excel(usuario: str = Query(...), archivo: UploadFile = File(...), request: Request = None):
    """Carga justificaciones desde un Excel con columnas GUIA, CAUSAL,
    JUSTIFICACION (ver /plantilla). Upsert por guía: si ya existía se
    actualiza causal/justificación y el valor previo queda en historial[]. Los
    errores se acumulan por fila sin abortar el resto."""
    info = _resolver_usuario(usuario)
    _requiere(info, PERFILES_NOVEDADES, "cargar novedades justificadas OT")

    contenido = await archivo.read()
    if not contenido:
        raise HTTPException(status_code=422, detail="El archivo está vacío.")
    try:
        wb = load_workbook(io.BytesIO(contenido), data_only=True, read_only=True)
    except Exception:
        raise HTTPException(status_code=422, detail="El archivo no es un Excel válido (.xlsx).")
    ws = wb.active
    filas = list(ws.iter_rows(values_only=True))
    wb.close()
    if not filas:
        raise HTTPException(status_code=422, detail="El archivo no tiene filas.")

    # Localizar columnas por cabecera (tolerante a mayúsculas/acentos/espacios).
    cabeceras = {_norm_col(c): i for i, c in enumerate(filas[0]) if c is not None}

    def col_de(*claves: str) -> Optional[int]:
        return next((cabeceras[k] for k in cabeceras if k in claves), None)

    col_guia = col_de("guia", "guía", "numero guia", "n guia")
    col_causal = col_de("causal", "causal ot", "novedad")
    col_just = col_de("justificacion", "justificación", "observacion", "observación")
    faltantes = [
        n for n, c in
        zip(["GUIA", "CAUSAL", "JUSTIFICACION"], [col_guia, col_causal, col_just])
        if c is None
    ]
    if faltantes:
        raise HTTPException(
            status_code=422,
            detail=f"El archivo debe tener las columnas: {', '.join(CABECERAS)}. Faltan: {', '.join(faltantes)}.",
        )

    validos = _causales_validos()
    if not validos:
        raise HTTPException(status_code=503, detail="El catálogo causales_ot está vacío.")

    def celda(fila, idx):
        return _celda_texto(fila[idx] if idx < len(fila) else "")

    # TODO-O-NADA (2026-10-29): se valida el archivo COMPLETO antes de escribir.
    # Una sola fila con causal no permitido (o guía/justificación vacía) rechaza
    # toda la carga con 422 — nada queda a medias.
    validas, errores = [], []
    for n, fila in enumerate(filas[1:], start=2):
        if fila is None or all(v is None or str(v).strip() == "" for v in fila):
            continue
        try:
            guia = _validar_guia(celda(fila, col_guia))
            causal = _resolver_causal(celda(fila, col_causal), validos)
            justificacion = celda(fila, col_just).strip()
            if not justificacion:
                raise HTTPException(status_code=422, detail="La justificación es obligatoria.")
        except HTTPException as e:
            errores.append({"fila": n, "guia": celda(fila, col_guia), "detalle": str(e.detail)})
            continue
        validas.append({"fila": n, "guia": guia, "causal": causal, "justificacion": justificacion})

    if errores:
        raise HTTPException(
            status_code=422,
            detail={
                "mensaje": f"La carga fue RECHAZADA: {len(errores)} fila(s) inválida(s) — no se guardó nada. "
                           f"Corrija y vuelva a subir el archivo.",
                "errores": errores,
            },
        )

    resultados = []
    ahora = _ahora_utc()
    auditoria = {"usuario": info["usuario"], "nombre": info["nombre"], "perfil": info["perfil"], "fecha": ahora}

    # Escritura EN LOTE (2026-10-30): antes find_one + update_one + insert_one de
    # auditoría POR FILA (3 viajes a Mongo × fila — 5.000 filas ≈ 15.000 round
    # trips secuenciales ≈ minutos, y pymongo bloqueante sobre el event loop
    # congelaba TODA la API durante la carga). Ahora: previos en UNA consulta
    # ($in), upserts con bulk_write y auditoría con insert_many (lotes de 1.000)
    # — mismas ~6-10 operaciones totales, misma semántica (historial, creado_por,
    # upsert por guía). Corre en hilo (to_thread) para no frenar el event loop.
    def _aplicar() -> list[dict]:
        # Previos de todas las guías del archivo en UNA consulta.
        previos = {
            d["guia"]: d
            for d in col_novedades.find(
                {"guia": {"$in": [v["guia"] for v in validas]}},
                {"guia": 1, "causal": 1, "justificacion": 1},
            )
        }
        ops, docs_auditoria, res = [], [], []
        for v in validas:
            guia, causal, justificacion = v["guia"], v["causal"], v["justificacion"]
            previo = previos.get(guia)
            cambio = {
                "guia": guia,
                "causal": causal,
                "justificacion": justificacion,
                "actualizado_por": auditoria,
                "updated_at": ahora,
            }
            if previo:
                cambio["historial"] = {
                    "$each": [{
                        "causal": previo.get("causal"),
                        "justificacion": previo.get("justificacion"),
                        "por": auditoria,
                        "fecha": ahora,
                    }],
                    "$slice": -20,  # cap: los últimos 20 cambios por guía
                }
            ops.append(UpdateOne(
                {"guia": guia},
                {"$set": cambio, "$setOnInsert": {"creado_por": auditoria, "created_at": ahora}},
                upsert=True,
            ))
            docs_auditoria.append({
                "accion": "actualizacion" if previo else "creacion",
                "guia": guia,
                "usuario": info["usuario"],
                "nombre": info["nombre"],
                "perfil": info["perfil"],
                "fecha": ahora,
                "detalle": {
                    "causal": causal,
                    "justificacion": justificacion,
                    **({"previo": {"causal": previo.get("causal"), "justificacion": previo.get("justificacion")}} if previo else {}),
                },
            })
            res.append({"fila": v["fila"], "guia": guia, "accion": "actualizada" if previo else "creada"})
        for i in range(0, len(ops), 1000):
            col_novedades.bulk_write(ops[i:i + 1000], ordered=True)
        for i in range(0, len(docs_auditoria), 1000):
            try:
                col_auditoria.insert_many(docs_auditoria[i:i + 1000], ordered=False)
            except Exception as e:
                logger.warning(f"[NOVEDADES_OT] Auditoría por lote falló ({len(docs_auditoria[i:i + 1000])} docs): {e}")
        return res

    if validas:
        resultados = await asyncio.to_thread(_aplicar)

    if resultados:
        _limpiar_cache_guias()
    return {
        "mensaje": f"{len(resultados)} procesadas, 0 con error",
        "procesadas": resultados,
        "errores": [],
    }


# ── Export Excel (toda la base) ───────────────────────────────────────────────


@router.get("/exportar")
async def exportar(usuario: str = Query(...)):
    """TODA la base de justificaciones en un Excel (.xlsx): Guía/Causal/
    Justificación + trazabilidad (quién registró/actualizó y cuándo). Un hoja,
    ordenada por updated_at desc (mismo orden de la lista)."""
    info = _resolver_usuario(usuario)
    _requiere(info, PERFILES_NOVEDADES, "exportar las novedades justificadas OT")

    def _generar() -> bytes:
        wb = Workbook()
        ws = wb.active
        ws.title = "Novedades OT"
        cabeceras = ["GUIA", "CAUSAL", "JUSTIFICACION", "REGISTRADA POR", "CREADA",
                     "ACTUALIZADA POR", "ACTUALIZADA"]
        fill = PatternFill(start_color="004d40", end_color="004d40", fill_type="solid")
        for i, col in enumerate(cabeceras, 1):
            celda = ws.cell(row=1, column=i, value=col)
            celda.font = Font(color="FFFFFF", bold=True)
            celda.fill = fill
        for i, ancho in enumerate([22, 34, 60, 28, 19, 28, 19], 1):
            ws.column_dimensions[chr(64 + i)].width = ancho

        def _quien(d: dict, campo: str) -> str:
            v = d.get(campo) or {}
            return f"{v.get('nombre') or v.get('usuario') or '—'}".strip()

        def _fecha(v) -> str:
            return v.strftime("%Y-%m-%d %H:%M") if isinstance(v, datetime) else "—"

        fila = 2
        for d in col_novedades.find({}, {"_id": 0}).sort("updated_at", -1):
            ws.cell(row=fila, column=1, value=d.get("guia", ""))
            ws.cell(row=fila, column=2, value=d.get("causal", ""))
            ws.cell(row=fila, column=3, value=d.get("justificacion", ""))
            ws.cell(row=fila, column=4, value=_quien(d, "creado_por"))
            ws.cell(row=fila, column=5, value=_fecha(d.get("created_at")))
            ws.cell(row=fila, column=6, value=_quien(d, "actualizado_por"))
            ws.cell(row=fila, column=7, value=_fecha(d.get("updated_at")))
            fila += 1
        salida = io.BytesIO()
        wb.save(salida)
        return salida.getvalue()

    contenido = await asyncio.to_thread(_generar)
    return Response(
        content=contenido,
        media_type=MEDIA_TYPE_XLSX,
        headers={"Content-Disposition": f"attachment; filename=novedades_ot_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"},
    )


# ── Plantilla ─────────────────────────────────────────────────────────────────


@router.get("/plantilla")
async def plantilla():
    """Excel de ejemplo con las columnas esperadas (y los causales vigentes)."""
    wb = Workbook()
    ws = wb.active
    ws.title = "Novedades OT"
    fill = PatternFill(start_color="004d40", end_color="004d40", fill_type="solid")
    for i, col in enumerate(CABECERAS, 1):
        celda = ws.cell(row=1, column=i, value=col)
        celda.font = Font(color="FFFFFF", bold=True)
        celda.fill = fill
        ws.column_dimensions[chr(64 + i)].width = 34
    # Fila de ejemplo con el primer causal vigente + nota con el listado.
    causales = [c["causal"] for c in col_causales.find({}, {"causal": 1}).sort("orden", 1)]
    ws.cell(row=2, column=1, value="EJEMPLO-001")
    if causales:
        ws.cell(row=2, column=2, value=causales[0])
    ws.cell(row=2, column=3, value=f"Ejemplo — reemplace esta fila. Causales válidos: {', '.join(causales)}")
    salida = io.BytesIO()
    wb.save(salida)
    return Response(
        content=salida.getvalue(),
        media_type=MEDIA_TYPE_XLSX,
        headers={"Content-Disposition": "attachment; filename=plantilla_novedades_ot.xlsx"},
    )
