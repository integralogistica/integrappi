"""
Módulo «Citas de Entrega» — Integrapp.

Registro MANUAL de la cita de entrega de una guía: fecha + franja horaria
(hora inicio/fin) en la que el cliente puede recibir. La FECHA registrada
GANA sobre `citas_agendadas` (CITASACT del Excel real) en el cálculo OT del
dashboard de clientes — ver `_mapa_citas_entrega` en indicadores_cliente.py.
La franja horaria es solo informativa: el OT compara dates, sin hora. Si se
borra el registro manual, el OT vuelve a `citas_agendadas` automáticamente.

Colecciones (base `integra`):
  - `citas_entrega`: UNA por guía (índice unique) con {guia, fecha
    'YYYY-MM-DD', hora_inicio 'HH:MM', hora_fin 'HH:MM', creado_por,
    actualizado_por, historial[]}.
  - `citas_eliminadas`: las citas BORRADAS con su trazabilidad completa
    (documento original + historial + `eliminada_por`/`eliminada_el`) —
    para auditoría: quién borró qué y cuándo (patrón anulados_otros_costos).
  - `auditoria_citas_entrega`: bitácora append-only de CADA acción (carga,
    actualización, borrado) con usuario/nombre/perfil/fecha.

Perfiles: ADMIN, COORDINADOR, CONTROL y OPERATIVO (cargar, ver, borrar).
La auditoría la puede consultar ADMIN y CONTROL.
"""

import asyncio
import io
import logging
import re
import unicodedata
from datetime import date, datetime, time, timedelta
from typing import Optional

from fastapi import APIRouter, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import Response
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill
from pydantic import BaseModel

from bson import ObjectId
from pymongo import UpdateOne

from bd.bd_cliente import bd_cliente
from rutas.otros_costos import _ahora_utc, _requiere, _resolver_usuario
from rutas.indicadores_cliente import _fecha_servible

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/citas-entrega", tags=["Citas de Entrega"])

PERFILES_CITAS = {"ADMIN", "COORDINADOR", "CONTROL", "OPERATIVO"}
PERFILES_AUDITORIA = {"ADMIN", "CONTROL"}

db = bd_cliente["integra"]
col_citas = db["citas_entrega"]
col_eliminadas = db["citas_eliminadas"]
col_auditoria = db["auditoria_citas_entrega"]

try:
    col_citas.create_index("guia", unique=True, name="idx_cita_entrega_guia")
    col_eliminadas.create_index("guia")
    col_eliminadas.create_index("eliminada_el")
    col_auditoria.create_index("fecha")
except Exception as _e:
    logger.warning(f"[CITAS_ENTREGA] No se pudieron crear índices: {_e}")

MEDIA_TYPE_XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
CABECERAS = ["GUIA", "FECHA", "HORA INICIO", "HORA FIN"]


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


def _validar_guia(valor: str) -> str:
    g = str(valor or "").strip().upper()
    if not g:
        raise HTTPException(status_code=422, detail="La guía es obligatoria.")
    if len(g) > 30:
        raise HTTPException(status_code=422, detail=f"Guía '{valor}' demasiado larga.")
    return g


_PATRON_HORA = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")


def _validar_fecha(valor) -> str:
    """Fecha dd/mm/aaaa (o ISO YYYY-MM-DD) → ISO normalizado, o 422."""
    iso = _celda_fecha(valor)
    if not iso:
        raise HTTPException(
            status_code=422,
            detail=f"Fecha '{valor}' inválida (use DD/MM/AAAA, p. ej. 10/11/2026).",
        )
    return iso


def _celda_fecha(v) -> str:
    """Celda/valor → fecha ISO. Acepta datetime/date de Excel, ISO y D/M/YYYY."""
    if isinstance(v, datetime):
        return v.date().isoformat()
    if isinstance(v, date):
        return v.isoformat()
    t = str(v or "").strip()
    if not t:
        return ""
    d = _fecha_servible(t)  # ISO 'YYYY-MM-DD'
    if d:
        return d.isoformat()
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(t, fmt).date().isoformat()
        except ValueError:
            continue
    return ""


def _validar_hora(valor, campo: str) -> str:
    """Hora 'HH:MM' (acepta 'H:MM' y time de Excel) → zero-padded, o 422."""
    if isinstance(valor, time):
        return valor.strftime("%H:%M")
    if isinstance(valor, datetime):
        return valor.strftime("%H:%M")
    h = str(valor or "").strip()
    if re.fullmatch(r"\d{1,2}:\d{2}", h):
        h = h.zfill(5)  # '8:00' → '08:00'
    if not _PATRON_HORA.fullmatch(h):
        raise HTTPException(status_code=422, detail=f"{campo} '{valor}' inválida (use HH:MM).")
    return h


def _validar_franja(hora_inicio: str, hora_fin: str) -> None:
    if hora_inicio > hora_fin:  # lexicográfico seguro con HH:MM zero-padded
        raise HTTPException(
            status_code=422,
            detail="La hora de inicio no puede ser mayor a la hora de fin.",
        )


def _historial_lista(h) -> list:
    """Historial SANEADO a lista. Los docs editados con el bug del `$each`
    dentro de `$set` (corregido 2026-11-09) guardaron el dict literal
    `{'$each': [...], '$slice': -20}` — se recuperan sus entradas; cualquier
    otra basura → lista vacía."""
    if isinstance(h, list):
        return h
    if isinstance(h, dict) and isinstance(h.get("$each"), list):
        return h["$each"]
    return []


def _auditar(accion: str, info: dict, guia: str, detalle: dict, ahora: datetime) -> None:
    """Append en la bitácora (nunca bloquea la operación si falla)."""
    try:
        col_auditoria.insert_one({
            "accion": accion,  # creacion | actualizacion | borrado | carga
            "guia": guia,
            "usuario": info["usuario"],
            "nombre": info["nombre"],
            "perfil": info["perfil"],
            "fecha": ahora,
            "detalle": detalle,
        })
    except Exception as e:
        logger.warning(f"[CITAS_ENTREGA] Auditoría falló ({accion} {guia}): {e}")


def _limpiar_caches_ot() -> None:
    """La cita manual cambia el veredicto OT: hay que invalidar el cache de
    /guias (_CACHE_GUIAS, TTL 300 s) Y el mapa de citas_entrega de
    _CACHE_TABLAS (TTL 600 s) — si no, el cambio tarda hasta 10 min en verse."""
    try:
        from rutas import indicadores_cliente as ic
        ic._CACHE_GUIAS.clear()
        ic._CACHE_TABLAS.pop("citas_entrega", None)
    except Exception as e:
        logger.warning(f"[CITAS_ENTREGA] No se pudieron limpiar los caches OT: {e}")


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


# ── Listado / borrado / auditoría ─────────────────────────────────────────────


@router.get("/")
async def listar(usuario: str = Query(...), q: str = Query(""), pagina: int = Query(1, ge=1), limite: int = Query(50, ge=1, le=200)):
    """Citas de entrega registradas (una por guía), con quién registró/actualizó.
    `q` busca por guía (parcial, case-insensitive)."""
    info = _resolver_usuario(usuario)
    _requiere(info, PERFILES_CITAS, "ver las citas de entrega")
    filtro: dict = {}
    if q.strip():
        filtro = {"guia": {"$regex": re.escape(q.strip()), "$options": "i"}}
    total = col_citas.count_documents(filtro)
    docs = (
        col_citas.find(filtro)
        .sort("updated_at", -1)
        .skip((pagina - 1) * limite)
        .limit(limite)
    )
    return {"total": total, "pagina": pagina, "citas": [_serializar(d) for d in docs]}


@router.delete("/eliminar")
async def eliminar(usuario: str = Query(...), id: str = Query(...), request: Request = None):
    """Borra la cita manual de la guía: el documento COMPLETO (con su
    historial de cambios) pasa a `citas_eliminadas` con quién lo eliminó y
    cuándo — para auditoría. Sin cita manual, el OT vuelve a
    `citas_agendadas` automáticamente."""
    info = _resolver_usuario(usuario)
    _requiere(info, PERFILES_CITAS, "borrar citas de entrega")
    try:
        doc = col_citas.find_one({"_id": ObjectId(id)})
    except Exception:
        doc = None
    if not doc:
        raise HTTPException(status_code=404, detail="Cita no encontrada.")
    ahora = _ahora_utc()
    # Trazabilidad: copia completa (sin _id, para que una misma guía pueda
    # eliminarse varias veces a lo largo de su vida) + quién/cuándo borró.
    eliminada = _serializar(doc)
    eliminada.pop("id", None)
    eliminada["eliminada_por"] = {"usuario": info["usuario"], "nombre": info["nombre"],
                                  "perfil": info["perfil"], "fecha": ahora}
    eliminada["eliminada_el"] = ahora
    try:
        col_eliminadas.insert_one(eliminada)
        col_citas.delete_one({"_id": doc["_id"]})
    except Exception as e:
        logger.error(f"[CITAS_ENTREGA] Error moviendo la cita de {doc['guia']} a citas_eliminadas: {e}")
        raise HTTPException(status_code=500, detail="No se pudo eliminar la cita (queda la original).")
    _auditar("borrado", info, doc["guia"], {"documento": _serializar(doc)}, ahora)
    _limpiar_caches_ot()
    return {"mensaje": f"Cita de la guía {doc['guia']} eliminada (queda en citas_eliminadas para auditoría; el OT vuelve a la cita agendada)."}


@router.get("/eliminadas")
async def listar_eliminadas(usuario: str = Query(...), q: str = Query(""), pagina: int = Query(1, ge=1), limite: int = Query(50, ge=1, le=200)):
    """Citas eliminadas con quién las borró — ADMIN y CONTROL (auditoría)."""
    info = _resolver_usuario(usuario)
    _requiere(info, PERFILES_AUDITORIA, "ver las citas de entrega eliminadas")
    filtro: dict = {}
    if q.strip():
        filtro = {"guia": {"$regex": re.escape(q.strip()), "$options": "i"}}
    total = col_eliminadas.count_documents(filtro)
    docs = (
        col_eliminadas.find(filtro)
        .sort("eliminada_el", -1)
        .skip((pagina - 1) * limite)
        .limit(limite)
    )
    return {"total": total, "pagina": pagina, "eliminadas": [_serializar(d) for d in docs]}


@router.get("/auditoria")
async def auditoria(usuario: str = Query(...), limite: int = Query(200, ge=1, le=1000)):
    """Bitácora de acciones (carga/actualización/borrado) — ADMIN y CONTROL."""
    info = _resolver_usuario(usuario)
    _requiere(info, PERFILES_AUDITORIA, "ver la auditoría de citas de entrega")
    docs = col_auditoria.find({}, {"detalle.documento.historial": 0}).sort("fecha", -1).limit(limite)
    return {"registros": [_serializar(d) for d in docs]}


# ── Registro manual (una guía, sin Excel) ─────────────────────────────────────


class CitaEntregaIN(BaseModel):
    guia: str
    fecha: str
    hora_inicio: str
    hora_fin: str


@router.post("/guardar")
async def guardar(datos: CitaEntregaIN, usuario: str = Query(...)):
    """Crea o actualiza UNA cita de entrega a mano. Misma semántica que
    importar-excel: upsert por guía (si la guía ya tenía cita se actualiza y el
    valor previo queda en historial[]), auditoría y limpieza de los caches OT."""
    info = _resolver_usuario(usuario)
    _requiere(info, PERFILES_CITAS, "registrar citas de entrega")
    guia = _validar_guia(datos.guia)
    fecha = _validar_fecha(datos.fecha)
    hora_inicio = _validar_hora(datos.hora_inicio, "La hora de inicio")
    hora_fin = _validar_hora(datos.hora_fin, "La hora de fin")
    _validar_franja(hora_inicio, hora_fin)

    ahora = _ahora_utc()
    auditoria = {"usuario": info["usuario"], "nombre": info["nombre"], "perfil": info["perfil"], "fecha": ahora}
    previo = col_citas.find_one({"guia": guia}, {"fecha": 1, "hora_inicio": 1, "hora_fin": 1, "historial": 1})
    cambio = {
        "guia": guia,
        "fecha": fecha,
        "hora_inicio": hora_inicio,
        "hora_fin": hora_fin,
        "actualizado_por": auditoria,
        "updated_at": ahora,
    }
    if previo:
        # El historial se reescribe COMPLETO (lista saneada + entrada nueva,
        # cap 20): no usa $push porque los docs editados con el bug del
        # `$each` en `$set` tienen el campo como dict y Mongo rechazaría el
        # push ("must be an array") — así cada guardado AUTO-REPARA el doc.
        # Cada entrada es el CAMBIO completo: previo → nuevo, quién y cuándo.
        cambio["historial"] = (_historial_lista(previo.get("historial")) + [{
            "previo": {k: previo.get(k) for k in ("fecha", "hora_inicio", "hora_fin")},
            "nuevo": {"fecha": fecha, "hora_inicio": hora_inicio, "hora_fin": hora_fin},
            "por": auditoria,
            "fecha_cambio": ahora,
        }])[-20:]
    col_citas.update_one(
        {"guia": guia},
        {"$set": cambio, "$setOnInsert": {"creado_por": auditoria, "created_at": ahora}},
        upsert=True,
    )
    _auditar(
        "actualizacion" if previo else "creacion",
        info, guia,
        {"fecha": fecha, "hora_inicio": hora_inicio, "hora_fin": hora_fin,
         **({"previo": {"fecha": previo.get("fecha"), "hora_inicio": previo.get("hora_inicio"),
                        "hora_fin": previo.get("hora_fin")}} if previo else {})},
        ahora,
    )
    _limpiar_caches_ot()
    accion = "actualizada" if previo else "creada"
    return {"mensaje": f"Cita de la guía {guia} {accion}.", "accion": accion}


# ── Carga Excel ───────────────────────────────────────────────────────────────


@router.post("/importar-excel")
async def importar_excel(usuario: str = Query(...), archivo: UploadFile = File(...), request: Request = None):
    """Carga citas desde un Excel con columnas GUIA, FECHA, HORA INICIO,
    HORA FIN (ver /plantilla). Upsert por guía: si ya existía se actualiza y el
    valor previo queda en historial[]. TODO-O-NADA: una fila inválida rechaza
    la carga completa (nada queda a medias)."""
    info = _resolver_usuario(usuario)
    _requiere(info, PERFILES_CITAS, "cargar citas de entrega")

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
    # "fecha (dd/mm/aaaa)" = cabecera de la PLANTILLA oficial (pide el formato
    # plano dd/mm/aaaa); ISO 'YYYY-MM-DD' sigue aceptado en archivos propios.
    col_fecha = col_de("fecha", "fecha (dd/mm/aaaa)", "fecha cita", "fecha de cita")
    col_hi = col_de("hora inicio", "hora inicial", "inicio")
    col_hf = col_de("hora fin", "hora final", "fin")
    faltantes = [
        n for n, c in
        zip(CABECERAS, [col_guia, col_fecha, col_hi, col_hf])
        if c is None
    ]
    if faltantes:
        raise HTTPException(
            status_code=422,
            detail=f"El archivo debe tener las columnas: {', '.join(CABECERAS)}. Faltan: {', '.join(faltantes)}.",
        )

    def celda(fila, idx):
        return fila[idx] if idx < len(fila) else ""

    # TODO-O-NADA: se valida el archivo COMPLETO antes de escribir (patrón
    # novedades_ot). Fecha inválida, hora inválida o franja invertida en UNA
    # fila rechaza toda la carga con 422.
    validas, errores = [], []
    for n, fila in enumerate(filas[1:], start=2):
        if fila is None or all(v is None or str(v).strip() == "" for v in fila):
            continue
        try:
            guia = _validar_guia(_celda_texto(celda(fila, col_guia)))
            fecha = _validar_fecha(celda(fila, col_fecha))
            hora_inicio = _validar_hora(celda(fila, col_hi), "La hora de inicio")
            hora_fin = _validar_hora(celda(fila, col_hf), "La hora de fin")
            _validar_franja(hora_inicio, hora_fin)
        except HTTPException as e:
            errores.append({"fila": n, "guia": _celda_texto(celda(fila, col_guia)), "detalle": str(e.detail)})
            continue
        validas.append({"fila": n, "guia": guia, "fecha": fecha,
                        "hora_inicio": hora_inicio, "hora_fin": hora_fin})

    # (2026-11-09) Una guía puede tener VARIAS fechas de cita (re-agendas):
    # la misma guía puede venir varias veces — en este archivo o en cargas
    # posteriores — y SIEMPRE LA ÚLTIMA es la vigente para el OT. Cada valor
    # anterior queda en historial[] (con quién lo cambió). No hay rechazo por
    # duplicados; el rechazo del archivo (todo-o-nada) es sólo por filas
    # inválidas (fecha/hora/guía).
    if errores:
        raise HTTPException(
            status_code=422,
            detail={
                "mensaje": f"La carga fue RECHAZADA: {len(errores)} fila(s) inválida(s) — no se guardó nada. "
                           f"Corrija y vuelva a subir el archivo.",
                "errores": errores,
            },
        )
    if not validas:
        raise HTTPException(status_code=422, detail="El archivo no tiene filas de datos.")

    resultados = []
    ahora = _ahora_utc()
    auditoria = {"usuario": info["usuario"], "nombre": info["nombre"], "perfil": info["perfil"], "fecha": ahora}

    # Escritura EN LOTE (patrón novedades_ot): previos en UNA consulta ($in),
    # upserts con bulk_write y auditoría con insert_many (lotes de 1.000), en
    # hilo para no frenar el event loop.
    def _aplicar() -> list[dict]:
        previos = {
            d["guia"]: d
            for d in col_citas.find(
                {"guia": {"$in": sorted({v["guia"] for v in validas})}},
                {"guia": 1, "fecha": 1, "hora_inicio": 1, "hora_fin": 1, "historial": 1},
            )
        }
        # Estado conocido por guía (arranca en el de BD): las filas se aplican
        # EN ORDEN — cada una deja el valor anterior en el historial y la
        # ÚLTIMA queda vigente. Filas idénticas al estado actual no escriben.
        # El historial se reescribe COMPLETO por $set (lista saneada con
        # _historial_lista + entradas nuevas, cap 20): no usa $push porque los
        # docs editados con el bug del `$each` en `$set` tienen el campo como
        # dict y Mongo rechazaría el push — así cada carga AUTO-REPARA el doc.
        estado = {
            g: {
                "fecha": d.get("fecha"),
                "hora_inicio": d.get("hora_inicio"),
                "hora_fin": d.get("hora_fin"),
                "historial": _historial_lista(d.get("historial")),
            }
            for g, d in previos.items()
        }
        ops, docs_auditoria, res = [], [], []
        for v in validas:
            guia = v["guia"]
            previo = estado.get(guia)
            nuevo = {"fecha": v["fecha"], "hora_inicio": v["hora_inicio"], "hora_fin": v["hora_fin"]}
            if previo and all(previo.get(k) == v_ for k, v_ in nuevo.items()):
                res.append({"fila": v["fila"], "guia": guia, "accion": "sin_cambio"})
                continue
            cambio = {
                "guia": guia,
                **nuevo,
                "actualizado_por": auditoria,
                "updated_at": ahora,
            }
            if previo:
                # Entrada = CAMBIO completo: previo → nuevo, quién y cuándo
                # (mismo shape que /guardar).
                cambio["historial"] = (previo["historial"] + [{
                    "previo": {k: previo.get(k) for k in ("fecha", "hora_inicio", "hora_fin")},
                    "nuevo": dict(nuevo),
                    "por": auditoria,
                    "fecha_cambio": ahora,
                }])[-20:]
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
                    **nuevo,
                    **({"previo": {k: previo.get(k) for k in ("fecha", "hora_inicio", "hora_fin")}} if previo else {}),
                },
            })
            res.append({"fila": v["fila"], "guia": guia, "accion": "actualizada" if previo else "creada"})
            estado[guia] = {**nuevo, "historial": cambio.get("historial", previo["historial"] if previo else [])}
        for i in range(0, len(ops), 1000):
            col_citas.bulk_write(ops[i:i + 1000], ordered=True)
        for i in range(0, len(docs_auditoria), 1000):
            try:
                col_auditoria.insert_many(docs_auditoria[i:i + 1000], ordered=False)
            except Exception as e:
                logger.warning(f"[CITAS_ENTREGA] Auditoría por lote falló ({len(docs_auditoria[i:i + 1000])} docs): {e}")
        return res

    resultados = await asyncio.to_thread(_aplicar)

    if resultados:
        _limpiar_caches_ot()
    return {
        "mensaje": f"{len(resultados)} procesadas, 0 con error",
        "procesadas": resultados,
        "errores": [],
    }


# ── Export Excel (toda la base) ───────────────────────────────────────────────


@router.get("/exportar")
async def exportar(usuario: str = Query(...)):
    """TODA la base de citas de entrega en un Excel (.xlsx): Guía/Fecha/Franja
    + trazabilidad (quién registró/actualizó y cuándo). Una hoja, ordenada por
    updated_at desc (mismo orden de la lista)."""
    info = _resolver_usuario(usuario)
    _requiere(info, PERFILES_CITAS, "exportar las citas de entrega")

    def _generar() -> bytes:
        wb = Workbook()
        ws = wb.active
        ws.title = "Citas de Entrega"
        cabeceras = ["GUIA", "FECHA", "HORA INICIO", "HORA FIN", "REGISTRADA POR",
                     "CREADA", "ACTUALIZADA POR", "ACTUALIZADA"]
        fill = PatternFill(start_color="004d40", end_color="004d40", fill_type="solid")
        for i, col in enumerate(cabeceras, 1):
            celda = ws.cell(row=1, column=i, value=col)
            celda.font = Font(color="FFFFFF", bold=True)
            celda.fill = fill
        for i, ancho in enumerate([22, 12, 12, 12, 28, 17, 28, 17], 1):
            ws.column_dimensions[chr(64 + i)].width = ancho

        def _quien(d: dict, campo: str) -> str:
            v = d.get(campo) or {}
            return f"{v.get('nombre') or v.get('usuario') or '—'}".strip()

        def _fecha(v) -> str:
            return v.strftime("%Y-%m-%d %H:%M") if isinstance(v, datetime) else "—"

        fila = 2
        for d in col_citas.find({}, {"_id": 0}).sort("updated_at", -1):
            ws.cell(row=fila, column=1, value=d.get("guia", ""))
            ws.cell(row=fila, column=2, value=d.get("fecha", ""))
            ws.cell(row=fila, column=3, value=d.get("hora_inicio", ""))
            ws.cell(row=fila, column=4, value=d.get("hora_fin", ""))
            ws.cell(row=fila, column=5, value=_quien(d, "creado_por"))
            ws.cell(row=fila, column=6, value=_fecha(d.get("created_at")))
            ws.cell(row=fila, column=7, value=_quien(d, "actualizado_por"))
            ws.cell(row=fila, column=8, value=_fecha(d.get("updated_at")))
            fila += 1
        salida = io.BytesIO()
        wb.save(salida)
        return salida.getvalue()

    contenido = await asyncio.to_thread(_generar)
    return Response(
        content=contenido,
        media_type=MEDIA_TYPE_XLSX,
        headers={"Content-Disposition": f"attachment; filename=citas_entrega_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"},
    )


# ── Plantilla ─────────────────────────────────────────────────────────────────


@router.get("/plantilla")
async def plantilla():
    """Excel de ejemplo con las columnas esperadas. La FECHA se pide en
    formato plano dd/mm/aaaa (la fila de ejemplo es una celda de fecha real
    con ese formato); el importador también acepta YYYY-MM-DD si alguien
    trae un archivo propio."""
    wb = Workbook()
    ws = wb.active
    ws.title = "Citas de Entrega"
    fill = PatternFill(start_color="004d40", end_color="004d40", fill_type="solid")
    cabeceras = ["GUIA", "FECHA (DD/MM/AAAA)", "HORA INICIO", "HORA FIN"]
    for i, col in enumerate(cabeceras, 1):
        celda = ws.cell(row=1, column=i, value=col)
        celda.font = Font(color="FFFFFF", bold=True)
        celda.fill = fill
        ws.column_dimensions[chr(64 + i)].width = 22
    ws.cell(row=2, column=1, value="EJEMPLO-001")
    # Celda de FECHA real con formato dd/mm/aaaa: Excel la muestra en el
    # formato pedido y al subir llega como datetime (aceptado por el parser).
    celda_fecha = ws.cell(row=2, column=2, value=date.today() + timedelta(days=1))
    celda_fecha.number_format = "DD/MM/YYYY"
    ws.cell(row=2, column=3, value="08:00")
    ws.cell(row=2, column=4, value="12:00")
    # (Sin fila de nota: una fila con texto y sin guía haría rechazar la
    # propia plantilla al subirla sin limpiar — el todo-o-nada la marcaría
    # inválida. El formato vive en la cabecera.)
    salida = io.BytesIO()
    wb.save(salida)
    return Response(
        content=salida.getvalue(),
        media_type=MEDIA_TYPE_XLSX,
        headers={"Content-Disposition": "attachment; filename=plantilla_citas_entrega.xlsx",
                 # Sin cache: la plantilla se descarga una vez tras cada carga.
                 "Cache-Control": "no-store"},
    )
