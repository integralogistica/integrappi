"""Contabilidad: facturas DIAN (carga Excel/ZIP-XML, trámite, radicación,
causación con movimientos contables, glosas).

Diseño nuevo sobre tablas normalizadas (en el externo esto vivía en el
documento JSON base_central con lógica en el navegador).
"""
import logging
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, Request, UploadFile, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from financiero.database import get_db_fin
from financiero.dependencies import get_usuario_fin, require_not_consulta, require_seccion
from financiero.models.contabilidad import EstadoFactura, FacturaDian, MovimientoContable
from financiero.models.usuario import RolUsuario, Usuario
from financiero.schemas import AccionTransicionRequest
from financiero.services import notificaciones
from financiero.services.audit_service import AuditService
from financiero.services.locking import cargar_con_bloqueo
from financiero.services.parser_dian import (
    analizar_filas, claves_duplicacion, extraer_zip_dian, leer_excel_dian,
)
from rutas.fin_adjuntos import subir_bytes_gcs
from rutas.fin_traslados import get_client_ip

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/fin/contabilidad", tags=["Financiero · Contabilidad DIAN"])

MAX_ARCHIVO_MB = 10

# Roles que operan contabilidad (el resto solo consulta lo que le dejen ver)
ROLES_CONTABILIDAD = {RolUsuario.TESORERIA, RolUsuario.CONTABILIDAD, RolUsuario.COMPRAS}


def _es_admin(u: Usuario) -> bool:
    return u.rol == RolUsuario.ADMIN or u.es_admin_adicional


def _evento(usuario: Usuario, accion: str, anterior, nuevo, obs: str = "") -> dict:
    return {"fecha": datetime.now(timezone.utc).isoformat(), "usuario": usuario.nombre,
            "username": usuario.username, "accion": accion,
            "estado_anterior": anterior.value if hasattr(anterior, "value") else anterior,
            "estado_nuevo": nuevo.value if hasattr(nuevo, "value") else nuevo, "observacion": obs}


def _factura_dict(f: FacturaDian) -> dict:
    return {
        "id": str(f.id), "cufe": f.cufe, "numero_factura": f.numero_factura,
        "nit_proveedor": f.nit_proveedor, "proveedor": f.proveedor,
        "fecha_emision": f.fecha_emision.isoformat() if f.fecha_emision else None,
        "tipo_documento": f.tipo_documento, "origen": f.origen,
        "iva": float(f.iva), "total": float(f.total), "estado": f.estado,
        "oc_vinculada": f.oc_vinculada,
        "fecha_radicacion": f.fecha_radicacion.isoformat() if f.fecha_radicacion else None,
        "fecha_causacion": f.fecha_causacion.isoformat() if f.fecha_causacion else None,
        "glosas": f.glosas or [], "detalles": f.detalles,
        "validada": f.validada, "area_aprobacion": f.area_aprobacion,
        "nota_validacion": f.nota_validacion,
        "motivo_rechazo": f.motivo_rechazo, "rechazado_por": f.rechazado_por,
        "fecha_rechazo": f.fecha_rechazo.isoformat() if f.fecha_rechazo else None,
        "cruzada_id": str(f.cruzada_id) if f.cruzada_id else None,
        "reemplaza_id": str(f.reemplaza_id) if f.reemplaza_id else None,
        "reemplaza_factura": f.reemplaza_factura,
        "fecha_cruce": f.fecha_cruce.isoformat() if f.fecha_cruce else None,
        "cruzado_por": f.cruzado_por,
        "fecha_envio_contab": f.fecha_envio_contab.isoformat() if f.fecha_envio_contab else None,
        "categoria_contable": f.categoria_contable, "num_orden_compra": f.num_orden_compra,
        "observacion_recepcion": f.observacion_recepcion,
        "estado_contable": f.estado_contable, "num_causacion": f.num_causacion,
        "fecha_contabilizacion": f.fecha_contabilizacion.isoformat() if f.fecha_contabilizacion else None,
        "es_cuenta_cobro": f.es_cuenta_cobro, "origen_doc": f.origen_doc,
        "caja_menor_sede": f.caja_menor_sede,
        "historial_cambios": f.historial_cambios or [], "version": f.version,
        "created_at": f.created_at.isoformat() if f.created_at else None,
    }


@router.get("/facturas")
def listar_facturas(
    estado: Optional[str] = None,
    proveedor: Optional[str] = None,
    desde: Optional[str] = None,
    hasta: Optional[str] = None,
    validada: Optional[bool] = None,
    categoria: Optional[str] = None,
    estado_contable: Optional[str] = None,
    caja_menor: Optional[str] = None,
    q: Optional[str] = None,
    db: Session = Depends(get_db_fin),
    _: Usuario = Depends(get_usuario_fin),
    __seccion=Depends(require_seccion("facturas")),
):
    stmt = select(FacturaDian)
    if estado:
        stmt = stmt.where(FacturaDian.estado == estado.upper())
    if proveedor:
        stmt = stmt.where(FacturaDian.proveedor.ilike(f"%{proveedor.strip()}%"))
    if desde:
        stmt = stmt.where(FacturaDian.fecha_emision >= desde)
    if hasta:
        stmt = stmt.where(FacturaDian.fecha_emision <= f"{hasta}T23:59:59")
    if validada is not None:
        stmt = stmt.where(FacturaDian.validada == validada)
    if categoria:
        stmt = stmt.where(FacturaDian.categoria_contable == categoria.upper())
    if estado_contable:
        stmt = stmt.where(FacturaDian.estado_contable == estado_contable)
    if caja_menor:
        stmt = stmt.where(FacturaDian.caja_menor_sede.ilike(f"%{caja_menor.strip()}%"))
    if q:
        stmt = stmt.where(FacturaDian.numero_factura.ilike(f"%{q.strip()}%"))
    stmt = stmt.order_by(FacturaDian.fecha_emision.desc()).limit(1000)
    return [_factura_dict(f) for f in db.scalars(stmt).all()]


@router.get("/facturas/{id}")
def obtener_factura(id: uuid.UUID, db: Session = Depends(get_db_fin), _: Usuario = Depends(get_usuario_fin),
                      __seccion=Depends(require_seccion("facturas"))):
    f = db.get(FacturaDian, id)
    if not f:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Factura no encontrada.")
    return _factura_dict(f)


def _claves_existentes(db: Session) -> set[str]:
    out: set[str] = set()
    filas = db.execute(select(FacturaDian.cufe, FacturaDian.nit_proveedor, FacturaDian.numero_factura)).all()
    for cufe, nit, numero in filas:
        faux = {"cufe": cufe, "nit": nit, "numero_factura": numero}
        out.update(claves_duplicacion(faux))
    return out


def _insertar(db: Session, fila: dict, usuario: Usuario) -> FacturaDian:
    try:
        fecha = datetime.fromisoformat(str(fila["fecha_emision"]))
    except ValueError:
        fecha = datetime.now(timezone.utc)
    nueva = FacturaDian(
        cufe=fila.get("cufe") or None,
        numero_factura=str(fila.get("numero_factura") or "S/N")[:50],
        nit_proveedor=str(fila.get("nit") or "SIN NIT")[:50],
        proveedor=str(fila.get("proveedor") or "PROVEEDOR SIN NOMBRE")[:250],
        fecha_emision=fecha,
        tipo_documento=str(fila.get("tipo_documento") or "Factura electrónica")[:80],
        origen=fila.get("origen", "excel")[:10],
        iva=Decimal(str(fila.get("iva") or 0)),
        total=Decimal(str(fila.get("total") or 0)),
        estado=EstadoFactura.PENDIENTE.value,
        detalles={k: v for k, v in fila.items() if k not in ("_pdf",)},
        historial_cambios=[_evento(usuario, "CARGA", None, EstadoFactura.PENDIENTE, fila.get("origen", "excel"))],
        created_by=usuario.username,
    )
    db.add(nueva)
    return nueva


@router.post("/facturas/cargar")
async def cargar_facturas(
    background: BackgroundTasks,
    request: Request,
    archivo: UploadFile = File(...),
    db: Session = Depends(get_db_fin),
    usuario: Usuario = Depends(require_not_consulta),
):
    """Carga el Excel exportado del portal DIAN o un ZIP con XMLs (+PDFs).
    Deduplica por CUFE y NIT+número; los PDF del ZIP quedan como adjuntos GCS."""
    contenido = await archivo.read()
    if len(contenido) > MAX_ARCHIVO_MB * 1024 * 1024:
        raise HTTPException(status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, f"El archivo supera {MAX_ARCHIVO_MB} MB.")
    nombre = (archivo.filename or "").lower()
    existentes = _claves_existentes(db)
    insertadas: list[FacturaDian] = []
    resumen = {"duplicadas": 0, "omitidas": 0, "propias": 0, "errores": []}

    try:
        if nombre.endswith(".zip"):
            r = extraer_zip_dian(contenido)
            resumen["errores"] = r["errores"][:20]
            vistas: set[str] = set()
            for fila in r["facturas"]:
                claves = claves_duplicacion(fila)
                if any(k in existentes or k in vistas for k in claves):
                    resumen["duplicadas"] += 1
                    continue
                vistas.update(claves)
                pdf = fila.pop("_pdf", None)
                f = _insertar(db, fila, usuario)
                db.flush()  # para tener f.id antes del adjunto
                insertadas.append(f)
                if pdf:
                    try:
                        subir_bytes_gcs(contenido=pdf[1], nombre=pdf[0], tipo_mime="application/pdf",
                                        modulo_origen="factura", referencia_id=str(f.id), db=db, usuario=usuario)
                    except Exception as exc:
                        logger.warning("[FIN] PDF del ZIP no se pudo adjuntar: %s", exc)
        elif nombre.endswith((".xlsx", ".xls")):
            filas = leer_excel_dian(contenido)
            r = analizar_filas(filas, existentes)
            if "error" in r:
                raise HTTPException(status.HTTP_400_BAD_REQUEST, r["error"])
            resumen["duplicadas"] = len(r["duplicadas"])
            resumen["omitidas"] = len(r["omitidas"])
            resumen["propias"] = len(r["propias"])
            for fila in r["nuevas"]:
                insertadas.append(_insertar(db, fila, usuario))
        else:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Sube el Excel del portal DIAN (.xlsx) o un ZIP con XMLs.")
    except HTTPException:
        raise
    except Exception as exc:
        db.rollback()
        logger.exception("[FIN] Error cargando facturas DIAN")
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"No se pudo procesar el archivo: {exc}")

    db.commit()
    for f in insertadas:
        db.refresh(f)
    # Registro histórico de la carga (ex db.uploads del original)
    from financiero.models.dian import DianUpload
    db.add(DianUpload(archivo=(archivo.filename or "?")[:200], usuario=usuario.nombre,
                      conteo_nuevas=len(insertadas), conteo_duplicadas=resumen.get("duplicadas", 0),
                      conteo_propias=resumen.get("propias", 0), conteo_transportadores=0,
                      conteo_otros=resumen.get("omitidas", 0), created_by=usuario.username))
    db.commit()
    AuditService.registrar(db=db, modulo="contabilidad", accion="CARGAR", usuario=usuario,
                           ip_cliente=get_client_ip(request),
                           detalles={"archivo": archivo.filename, "nuevas": len(insertadas), **{k: v for k, v in resumen.items() if k != "errores"}})
    if insertadas:
        notificaciones.avisar(background, db, [usuario.correo] if usuario.correo else [],
                              f"{len(insertadas)} facturas DIAN cargadas",
                              [f"Se cargaron <b>{len(insertadas)}</b> factura(s) de {archivo.filename}.",
                               f"Duplicadas: {resumen['duplicadas']} · Omitidas: {resumen['omitidas']} · Propias: {resumen['propias']}."])
    return {"nuevas": len(insertadas), **resumen,
            "facturas": [_factura_dict(f) for f in insertadas]}


@router.post("/facturas/{id}/transicion")
def transicion_factura(
    id: uuid.UUID,
    transicion: AccionTransicionRequest,
    request: Request,
    db: Session = Depends(get_db_fin),
    usuario: Usuario = Depends(require_not_consulta),
):
    """TRAMITAR (recepción/tesorería) · RADICAR (contabilidad, OC opcional) ·
    CAUSAR (contabilidad: genera movimientos) · GLOSAR (motivo obligatorio) ·
    ANULAR."""
    f = cargar_con_bloqueo(db, FacturaDian, id, transicion.version, "Factura")
    accion = transicion.accion.upper()
    ahora = datetime.now(timezone.utc)
    anterior = f.estado
    es_conta = usuario.rol in ROLES_CONTABILIDAD or _es_admin(usuario)

    if accion == "TRAMITAR":
        if f.estado != EstadoFactura.PENDIENTE.value:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Solo se tramitan facturas pendientes.")
        # Trámite enriquecido del original: fecha de envío a contabilidad,
        # categoría de gasto, nº OC y observación de recepción (prioriza a Contabilidad).
        datos = transicion.datos_adicionales or {}
        f.estado = EstadoFactura.TRAMITADA.value
        f.validada = True
        f.origen_doc = f.origen_doc or "Proveedor"
        f.estado_contable = "Pendiente"
        if datos.get("fecha_envio_contab"):
            try:
                f.fecha_envio_contab = datetime.fromisoformat(str(datos["fecha_envio_contab"]).replace("Z", "+00:00"))
            except ValueError:
                raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "Fecha de envío a contabilidad inválida.")
        else:
            f.fecha_envio_contab = ahora
        cat = (datos.get("categoria_contable") or "OPERACIÓN").upper()
        if cat not in CATEGORIAS_GASTO:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, f"Categoría no válida: {cat}.")
        f.categoria_contable = cat
        if datos.get("num_orden_compra") is not None:
            oc = str(datos["num_orden_compra"]).strip().upper()
            f.num_orden_compra = (oc or "N/A")[:30]
            f.oc_vinculada = (oc or None) if oc != "N/A" else f.oc_vinculada
        if datos.get("observacion_recepcion") is not None:
            f.observacion_recepcion = str(datos["observacion_recepcion"]).strip().upper() or None
    elif accion == "ENVIAR_LEGALIZACION":
        # El original «cajaMenor(id)»: la factura se manda a legalización en caja menor
        if f.estado != EstadoFactura.PENDIENTE.value:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Solo pendientes van a legalización en caja menor.")
        f.estado = EstadoFactura.LEGALIZACION.value
        f.validada = True
    elif accion == "RADICAR":
        if f.estado != EstadoFactura.TRAMITADA.value:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Solo se radican facturas tramitadas.")
        if not es_conta:
            raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo Contabilidad/Tesorería radican facturas.")
        oc = (transicion.datos_adicionales or {}).get("oc")
        f.oc_vinculada = str(oc).strip()[:30] if oc else f.oc_vinculada
        f.estado = EstadoFactura.RADICADA.value
        f.radicada_por_id = usuario.id
        f.fecha_radicacion = ahora
    elif accion == "CAUSAR":
        if f.estado != EstadoFactura.RADICADA.value:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Solo se causan facturas radicadas.")
        if usuario.rol != RolUsuario.CONTABILIDAD and not _es_admin(usuario):
            raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo Contabilidad causa facturas.")
        f.estado = EstadoFactura.CAUSADA.value
        f.causada_por_id = usuario.id
        f.fecha_causacion = ahora
        _generar_movimientos(db, f, usuario)
    elif accion == "GLOSAR":
        if not es_conta:
            raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo Contabilidad/Tesorería glosan facturas.")
        if not (transicion.observacion or "").strip():
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "Escribe el motivo de la glosa.")
        glosas = list(f.glosas or [])
        glosas.append({"motivo": transicion.observacion, "por": usuario.nombre,
                       "at": ahora.isoformat(), "estado_previo": f.estado})
        f.glosas = glosas
        f.estado = EstadoFactura.GLOSADA.value
    elif accion == "ANULAR":
        if not _es_admin(usuario):
            raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo el administrador anula facturas.")
        f.estado = EstadoFactura.ANULADA.value
    else:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Acción '{accion}' desconocida.")

    f.version += 1
    f.updated_by = usuario.username
    f.historial_cambios = [*(f.historial_cambios or []),
                           _evento(usuario, accion, anterior, f.estado, transicion.observacion or "")]
    db.commit()
    db.refresh(f)
    AuditService.registrar(db=db, modulo="contabilidad", accion=accion, entidad_id=str(f.id), usuario=usuario,
                           ip_cliente=get_client_ip(request),
                           detalles={"numero": f.numero_factura, "proveedor": f.proveedor, "estado": f.estado})
    return _factura_dict(f)


def _generar_movimientos(db: Session, f: FacturaDian, usuario: Usuario):
    """Asiento simple de causación: gasto (débito) por el total sin IVA e IVA
    descontable (débito) contra cuentas por pagar (crédito) por el total."""
    periodo = f.fecha_causacion.strftime("%Y-%m")
    base_neta = Decimal(str(f.total)) - Decimal(str(f.iva))
    filas = [
        ("GASTO", base_neta, Decimal("0")),
        ("IVA_DESCONTABLE", Decimal(str(f.iva)), Decimal("0")),
        ("CXPPROVEEDORES", Decimal("0"), Decimal(str(f.total))),
    ]
    for cuenta, debito, credito in filas:
        db.add(MovimientoContable(
            origen="factura", referencia_id=f.id, referencia=f.numero_factura, periodo=periodo,
            cuenta=cuenta, tercero=f.proveedor, debito=debito, credito=credito,
            detalle={"proveedor_nit": f.nit_proveedor, "cufe": f.cufe, "oc": f.oc_vinculada},
            creado_por_id=usuario.id,
        ))


@router.get("/movimientos")
def listar_movimientos(
    periodo: Optional[str] = None,
    db: Session = Depends(get_db_fin),
    _: Usuario = Depends(get_usuario_fin),
    __seccion=Depends(require_seccion("facturas")),
):
    stmt = select(MovimientoContable).order_by(MovimientoContable.created_at.desc()).limit(5000)
    if periodo:
        stmt = stmt.where(MovimientoContable.periodo == periodo)
    total_debito = Decimal("0")
    total_credito = Decimal("0")
    out = []
    for m in db.scalars(stmt).all():
        total_debito += m.debito
        total_credito += m.credito
        out.append({
            "id": str(m.id), "origen": m.origen, "referencia": m.referencia, "periodo": m.periodo,
            "cuenta": m.cuenta, "tercero": m.tercero, "debito": float(m.debito), "credito": float(m.credito),
            "detalle": m.detalle, "created_at": m.created_at.isoformat() if m.created_at else None,
        })
    return {"movimientos": out, "total_debito": float(total_debito), "total_credito": float(total_credito),
            "cuadra": total_debito == total_credito}


# ═══════════════════════════════════════════════════════════════════════════
# Paridad DIAN (views-facturas.js / views-contabilidad.js / dian.js)
# ═══════════════════════════════════════════════════════════════════════════

CATEGORIAS_GASTO = {"OPERACIÓN", "ADMINISTRACIÓN", "LOGÍSTICA", "MANTENIMIENTO", "COMERCIAL", "INFRAESTRUCTURA", "OTRA"}
CATEGORIAS_CUENTA_COBRO = {"OPERACIÓN", "HONORARIOS", "LOGÍSTICA Y TRANSPORTES", "ADMINISTRACIÓN",
                           "MANTENIMIENTO", "COMERCIAL", "INFRAESTRUCTURA", "OTRA"}
AREAS_APROBACION = {"Compras", "Gerencia", "Operaciones", "General"}
MOTIVOS_RECHAZO = [
    "Error en tarifa / valores facturados",
    "Mercancía o servicio no recibido / no conforme",
    "Falta orden de compra / soportes obligatorios",
    "Solicitada refacturación al proveedor",
    "Factura duplicada o errónea",
]
GLOSAS_CONTABILIDAD = [
    "CENTRO DE COSTO NO CREADO", "PENDIENTE RUT Y CERTIFICACIÓN", "NOTA CRÉDITO PENDIENTE",
    "ERROR EN VALOR O RETENCIÓN", "FALTA APROBACIÓN DE ÁREA", "ORDEN DE COMPRA ERRADA",
    "FACTURA DUPLICADA", "DATOS FISCALES / NIT ERRADO", "FALTA SOPORTE DE ENTREGA O REMISIÓN",
    "PROVEEDOR BLOQUEADO",
]
CONCEPTOS_SUGERIDOS_CC = ["HONORARIOS SERVICIOS", "TRANSPORTE Y FLETES", "LEGALIZACIÓN CAJA MENOR",
                          "MANTENIMIENTO SEDE", "REEMBOLSO DE GASTOS", "URGENTE CAUSACIÓN"]


def _digits(s) -> str:
    return "".join(c for c in str(s or "") if c.isdigit())


def _es_transportador(fila: dict, transportadores) -> bool:
    """Puerto de esTransportadorOmitido: match por NIT o nombre incluido."""
    nit = _digits(fila.get("nit"))
    nombre = str(fila.get("proveedor") or "").strip().upper()
    for t in transportadores:
        if not t.activo:
            continue
        if nit and _digits(t.nit) == nit:
            return True
        tn = (t.nombre or "").strip().upper()
        if tn and (tn == nombre or tn in nombre or nombre in tn):
            return True
    return False


def _clasificar_carga(db: Session, contenido: bytes, nombre: str) -> dict:
    """Puerto del preview del original: clasifica las filas en las 5 categorías
    sin insertar nada (nuevas / propias / transportadores / duplicadas / otros)."""
    from financiero.models.dian import TransportadorOmitido
    from financiero.services.parser_dian import (
        _localizar_columnas, es_documento_valido, es_propia_empresa,
    )
    transportadores = db.scalars(select(TransportadorOmitido)).all()
    existentes = _claves_existentes(db)

    filas: list[dict] = []
    errores: list[str] = []
    es_excel = False
    if nombre.endswith(".zip"):
        r = extraer_zip_dian(contenido)
        errores = r["errores"][:20]
        filas = r["facturas"]
    elif nombre.endswith((".xlsx", ".xls")):
        filas = leer_excel_dian(contenido)
        es_excel = True
    else:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Sube el Excel del portal DIAN (.xlsx) o un ZIP con XMLs.")

    # El Excel viene con encabezados crudos del portal: se normalizan con el
    # mismo localizador de columnas que usa analizar_filas.
    col: dict[str, str] = {}
    if es_excel and filas:
        col, _ = _localizar_columnas(list(filas[0].keys()))

    def g(fila: dict, clave: str):
        if es_excel:
            nombre_col = col.get(clave)
            return fila.get(nombre_col) if nombre_col else None
        return fila.get({"tipo": "tipo_documento", "nit": "nit", "prov": "proveedor"}.get(clave, clave))

    vistas: set[str] = set()
    categorias = {"nuevas": [], "propias": [], "transportadores": [], "duplicadas": [], "otros": []}
    for fila in filas:
        tipo = str(g(fila, "tipo") or "").strip() or "Factura electrónica"
        nit = str(g(fila, "nit") or "").strip().replace(".0", "")
        prov = str(g(fila, "prov") or "").strip().upper()
        fila_view = {k: v for k, v in fila.items() if k != "_pdf"}
        if es_propia_empresa(nit, prov):
            categorias["propias"].append(fila_view)
            continue
        if _es_transportador({"nit": nit, "proveedor": prov}, transportadores):
            categorias["transportadores"].append(fila_view)
            continue
        if any(k in existentes or k in vistas for k in claves_duplicacion(fila)):
            categorias["duplicadas"].append(fila_view)
            continue
        if not es_documento_valido(tipo):
            categorias["otros"].append(fila_view)  # acuses / application response
            continue
        vistas.update(claves_duplicacion(fila))
        categorias["nuevas"].append(fila_view)
    return {"categorias": categorias, "errores": errores}


@router.post("/facturas/previsualizar")
async def previsualizar_carga(
    archivo: UploadFile = File(...),
    db: Session = Depends(get_db_fin),
    usuario: Usuario = Depends(require_not_consulta),
):
    """Vista previa ANTES de confirmar la carga: conteos por categoría y las
    primeras filas de cada una (el botón «Cargar N nuevas» confirma)."""
    contenido = await archivo.read()
    if len(contenido) > MAX_ARCHIVO_MB * 1024 * 1024:
        raise HTTPException(status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, f"El archivo supera {MAX_ARCHIVO_MB} MB.")
    r = _clasificar_carga(db, contenido, (archivo.filename or "").lower())
    cat = r["categorias"]
    return {
        "archivo": archivo.filename,
        "conteos": {k: len(v) for k, v in cat.items()},
        "nuevas": cat["nuevas"][:30],
        "propias": cat["propias"][:30],
        "transportadores": cat["transportadores"][:30],
        "duplicadas": cat["duplicadas"][:10],
        "otros": cat["otros"][:10],
        "errores": r["errores"],
    }


# ── Transportadores omitidos ───────────────────────────────────────────────

class TransportadorPayload(BaseModel):
    nit: str
    nombre: str
    notas: Optional[str] = None


@router.get("/transportadores")
def listar_transportadores(db: Session = Depends(get_db_fin), _: Usuario = Depends(require_not_consulta)):
    from financiero.models.dian import TransportadorOmitido
    return [{"id": str(t.id), "nit": t.nit, "nombre": t.nombre, "notas": t.notas, "activo": t.activo}
            for t in db.scalars(select(TransportadorOmitido).order_by(TransportadorOmitido.nombre)).all()]


@router.post("/transportadores")
def crear_transportador(payload: TransportadorPayload, db: Session = Depends(get_db_fin),
                        usuario: Usuario = Depends(require_not_consulta)):
    from financiero.models.dian import TransportadorOmitido
    t = TransportadorOmitido(nit=payload.nit.strip(), nombre=payload.nombre.strip(), notas=payload.notas,
                             created_by=usuario.username)
    db.add(t)
    db.commit()
    db.refresh(t)
    return {"id": str(t.id), "nit": t.nit, "nombre": t.nombre, "notas": t.notas, "activo": t.activo}


@router.put("/transportadores/{id}")
def editar_transportador(id: uuid.UUID, payload: TransportadorPayload, db: Session = Depends(get_db_fin),
                         usuario: Usuario = Depends(require_not_consulta)):
    from financiero.models.dian import TransportadorOmitido
    t = db.get(TransportadorOmitido, id)
    if not t:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Transportador no encontrado.")
    t.nit, t.nombre, t.notas = payload.nit.strip(), payload.nombre.strip(), payload.notas
    t.updated_by = usuario.username
    db.commit()
    return {"id": str(t.id), "nit": t.nit, "nombre": t.nombre, "notas": t.notas, "activo": t.activo}


@router.delete("/transportadores/{id}")
def eliminar_transportador(id: uuid.UUID, db: Session = Depends(get_db_fin),
                           usuario: Usuario = Depends(require_not_consulta)):
    from financiero.models.dian import TransportadorOmitido
    t = db.get(TransportadorOmitido, id)
    if not t:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Transportador no encontrado.")
    db.delete(t)
    db.commit()
    return {"mensaje": "Transportador eliminado."}


@router.post("/transportadores/{id}/archivar-pendientes")
def archivar_pendientes_transportador(id: uuid.UUID, db: Session = Depends(get_db_fin),
                                      usuario: Usuario = Depends(require_not_consulta)):
    """Puerto de depurarFacturasTransportadores: las PENDIENTES que matcheen al
    transportador se anulan con nota (no pasan por recepción)."""
    from financiero.models.dian import TransportadorOmitido
    t = db.get(TransportadorOmitido, id)
    if not t:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Transportador no encontrado.")
    archivadas = 0
    for f in db.scalars(select(FacturaDian).where(FacturaDian.estado == EstadoFactura.PENDIENTE.value)).all():
        if _es_transportador({"nit": f.nit_proveedor, "proveedor": f.proveedor}, [t]):
            f.estado = EstadoFactura.ANULADA.value
            f.version += 1
            f.historial_cambios = [*(f.historial_cambios or []),
                                   _evento(usuario, "ARCHIVAR_TRANSPORTADOR", EstadoFactura.PENDIENTE, EstadoFactura.ANULADA,
                                           f"Transportador omitido: {t.nombre}")]
            archivadas += 1
    db.commit()
    return {"archivadas": archivadas}


# ── Validación por área / rechazo / reactivación ───────────────────────────

class ValidarPayload(BaseModel):
    area: str                       # Compras | Gerencia | Operaciones | General
    nota: Optional[str] = None


@router.post("/facturas/{id}/validar")
def validar_factura(id: uuid.UUID, payload: ValidarPayload, request: Request,
                    db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta)):
    if payload.area not in AREAS_APROBACION:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, f"Área no válida: {payload.area}.")
    f = cargar_con_bloqueo(db, FacturaDian, id, None, "Factura")
    f.validada = True
    f.area_aprobacion = payload.area
    f.nota_validacion = payload.nota
    f.fecha_solicitud_aprobacion = datetime.now(timezone.utc)
    f.version += 1
    f.updated_by = usuario.username
    f.historial_cambios = [*(f.historial_cambios or []),
                           _evento(usuario, "VALIDAR", f.estado, f.estado, f"Esperando aprobación de {payload.area}")]
    db.commit()
    db.refresh(f)
    AuditService.registrar(db=db, modulo="contabilidad", accion="VALIDAR", entidad_id=str(f.id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"area": payload.area})
    return _factura_dict(f)


@router.post("/facturas/{id}/quitar-validacion")
def quitar_validacion(id: uuid.UUID, db: Session = Depends(get_db_fin),
                      usuario: Usuario = Depends(require_not_consulta)):
    f = cargar_con_bloqueo(db, FacturaDian, id, None, "Factura")
    f.validada = False
    f.area_aprobacion = None
    f.nota_validacion = None
    f.fecha_solicitud_aprobacion = None
    f.version += 1
    db.commit()
    db.refresh(f)
    return _factura_dict(f)


@router.post("/facturas/{id}/rechazar")
def rechazar_factura(id: uuid.UUID, body: dict, request: Request,
                     db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta)):
    motivo = str(body.get("motivo") or "").strip()
    if not motivo:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "Selecciona o escribe el motivo del rechazo.")
    f = cargar_con_bloqueo(db, FacturaDian, id, None, "Factura")
    if f.estado in (EstadoFactura.CAUSADA.value, EstadoFactura.ANULADA.value):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"No se rechaza una factura en estado '{f.estado}'.")
    f.estado = EstadoFactura.RECHAZADA.value
    f.motivo_rechazo = motivo[:120]
    f.fecha_rechazo = datetime.now(timezone.utc)
    f.rechazado_por = usuario.nombre
    f.version += 1
    f.updated_by = usuario.username
    f.historial_cambios = [*(f.historial_cambios or []), _evento(usuario, "RECHAZAR", EstadoFactura.PENDIENTE, f.estado, motivo)]
    db.commit()
    db.refresh(f)
    AuditService.registrar(db=db, modulo="contabilidad", accion="RECHAZAR", entidad_id=str(f.id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"motivo": motivo[:100]})
    return _factura_dict(f)


@router.post("/facturas/{id}/reactivar")
def reactivar_factura(id: uuid.UUID, db: Session = Depends(get_db_fin),
                      usuario: Usuario = Depends(require_not_consulta)):
    f = cargar_con_bloqueo(db, FacturaDian, id, None, "Factura")
    if f.estado != EstadoFactura.RECHAZADA.value:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Solo se reactivan facturas rechazadas.")
    f.estado = EstadoFactura.PENDIENTE.value
    f.motivo_rechazo = None
    f.fecha_rechazo = None
    f.rechazado_por = None
    f.version += 1
    f.historial_cambios = [*(f.historial_cambios or []),
                           _evento(usuario, "REACTIVAR", EstadoFactura.RECHAZADA, EstadoFactura.PENDIENTE, "")]
    db.commit()
    db.refresh(f)
    return _factura_dict(f)


# ── Cruce de facturas (nota crédito / refacturación) ──────────────────────

@router.get("/facturas/{id}/cruce-candidatos")
def cruce_candidatos(id: uuid.UUID, db: Session = Depends(get_db_fin),
                     _: Usuario = Depends(require_not_consulta)):
    """Candidatas de la misma empresa: NIT igual o proveedor contenido."""
    f = db.get(FacturaDian, id)
    if not f:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Factura no encontrada.")
    prefijo_prov = (f.proveedor or "")[:8].upper()
    out = []
    for otra in db.scalars(select(FacturaDian).where(FacturaDian.id != f.id)).all():
        mismo_nit = _digits(otra.nit_proveedor) == _digits(f.nit_proveedor)
        prov_contiene = prefijo_prov and prefijo_prov in (otra.proveedor or "").upper()
        if mismo_nit or prov_contiene:
            out.append({**_factura_dict(otra), "diferencia": float(Decimal(str(otra.total)) - Decimal(str(f.total)))})
    return out[:50]


@router.post("/facturas/{id}/cruce")
def cruzar_facturas(id: uuid.UUID, body: dict, request: Request,
                    db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta)):
    """La rechazada queda cruzada con su reemplazo (o viceversa, según cuál esté rechazada)."""
    try:
        otra_id = uuid.UUID(str(body.get("cruzada_id") or ""))
    except ValueError:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "cruzada_id no válido.")
    motivo = str(body.get("motivo") or "Factura reemplazada por nueva emisión formal").strip()
    a = db.get(FacturaDian, id)
    b = db.get(FacturaDian, otra_id)
    if not a or not b:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Factura no encontrada.")
    ahora = datetime.now(timezone.utc)
    # ¿Quién es la rechazada? Si ninguna, la activa (a) queda como rechazada.
    if b.estado == EstadoFactura.RECHAZADA.value:
        rechazada, reemplazo = b, a
    else:
        rechazada, reemplazo = a, b
    rechazada.estado = EstadoFactura.RECHAZADA.value
    rechazada.cruzada_id = reemplazo.id
    rechazada.reemplaza_id = None
    rechazada.motivo_rechazo = rechazada.motivo_rechazo or motivo
    rechazada.fecha_cruce = ahora
    rechazada.cruzado_por = usuario.nombre
    reemplazo.reemplaza_id = rechazada.id
    reemplazo.reemplaza_factura = rechazada.numero_factura
    reemplazo.fecha_cruce = ahora
    reemplazo.cruzado_por = usuario.nombre
    for f in (rechazada, reemplazo):
        f.version += 1
        f.historial_cambios = [*(f.historial_cambios or []),
                               _evento(usuario, "CRUCE", f.estado, f.estado, f"{rechazada.numero_factura} ↔ {reemplazo.numero_factura}: {motivo}")]
    db.commit()
    for f in (rechazada, reemplazo):
        db.refresh(f)
    AuditService.registrar(db=db, modulo="contabilidad", accion="CRUCE", entidad_id=str(rechazada.id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"rechazada": rechazada.numero_factura,
                                                                        "reemplazo": reemplazo.numero_factura, "motivo": motivo[:100]})
    return {"rechazada": _factura_dict(rechazada), "reemplazo": _factura_dict(reemplazo)}


@router.post("/facturas/{id}/cruce/deshacer")
def deshacer_cruce(id: uuid.UUID, db: Session = Depends(get_db_fin),
                   usuario: Usuario = Depends(require_not_consulta)):
    f = db.get(FacturaDian, id)
    if not f:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Factura no encontrada.")
    pareja = db.get(FacturaDian, f.cruzada_id or f.reemplaza_id)
    if not pareja:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Esta factura no tiene cruce.")
    for x in (f, pareja):
        x.cruzada_id = x.reemplaza_id = None
        x.reemplaza_factura = None
        x.fecha_cruce = None
        x.cruzado_por = None
        x.version += 1
        x.historial_cambios = [*(x.historial_cambios or []),
                               _evento(usuario, "DESHACER_CRUCE", x.estado, x.estado, "")]
    # la que quedó rechazada SOLO por el cruce vuelve a pendiente
    if f.estado == EstadoFactura.RECHAZADA.value:
        f.estado = EstadoFactura.PENDIENTE.value
    db.commit()
    for x in (f, pareja):
        db.refresh(x)
    return {"rechazada": _factura_dict(f), "reemplazo": _factura_dict(pareja)}


# ── Legalización en caja menor ─────────────────────────────────────────────

class LegalizarCajaPayload(BaseModel):
    sede: str                       # sede del maestro o «otra» libre
    comprobante: Optional[str] = None
    responsable: Optional[str] = None
    observaciones: Optional[str] = None
    fecha: Optional[str] = None


@router.post("/facturas/{id}/enviar-caja-menor")
def legalizar_en_caja(id: uuid.UUID, payload: LegalizarCajaPayload, request: Request,
                      db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta)):
    """Puerto de confirmarLegalizarCajaMenor: la factura queda TRAMITADA con
    origen 'Caja Menor: <sede>' lista para vincularse a la caja."""
    f = cargar_con_bloqueo(db, FacturaDian, id, None, "Factura")
    if f.estado != EstadoFactura.LEGALIZACION.value:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Solo se legalizan facturas en estado de legalización.")
    f.estado = EstadoFactura.TRAMITADA.value
    f.caja_menor_sede = payload.sede.strip()[:120]
    f.origen_doc = f"Caja Menor: {payload.sede.strip()[:40]}"
    f.detalles = {**(f.detalles or {}), "soporte_legalizacion": payload.comprobante,
                  "responsable_caja": payload.responsable, "observacion_legalizacion": payload.observaciones}
    f.version += 1
    f.updated_by = usuario.username
    f.historial_cambios = [*(f.historial_cambios or []),
                           _evento(usuario, "LEGALIZAR_CAJA", EstadoFactura.LEGALIZACION, f.estado, f"Caja Menor: {payload.sede}")]
    db.commit()
    db.refresh(f)
    return _factura_dict(f)


# ── Novedad contable (Drive) y reabrir ─────────────────────────────────────

class NovedadPayload(BaseModel):
    estado_contable: str            # Pendiente | Con Novedad | Contabilizado
    glosa: Optional[str] = None     # del catálogo de 10 u 'OTRA'
    otra_glosa: Optional[str] = None
    num_causacion: Optional[str] = None
    observacion: Optional[str] = None


@router.post("/facturas/{id}/novedad")
def registrar_novedad(id: uuid.UUID, payload: NovedadPayload, request: Request,
                      db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta)):
    """Puerto de confirmarNovedadContabilidad: los 3 estados + 'PENDIENTE DE
    REVISIÓN' como observación literal (Filtro 1 del Drive)."""
    if payload.estado_contable not in ("Pendiente", "Con Novedad", "Contabilizado"):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "estado_contable no válido.")
    if usuario.rol not in (RolUsuario.CONTABILIDAD, RolUsuario.ADMIN) and not _es_admin(usuario):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo Contabilidad registra novedades del Drive.")
    f = cargar_con_bloqueo(db, FacturaDian, id, None, "Factura")
    ahora = datetime.now(timezone.utc)

    observacion = payload.observacion
    if payload.estado_contable == "Pendiente" and payload.glosa == "PENDIENTE DE REVISIÓN":
        observacion = "PENDIENTE DE REVISIÓN"
    if payload.estado_contable == "Con Novedad":
        if payload.glosa == "OTRA":
            if not (payload.otra_glosa or "").strip():
                raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "Describe la otra novedad.")
            observacion = payload.otra_glosa.strip().upper()
        elif payload.glosa:
            if payload.glosa not in GLOSAS_CONTABILIDAD:
                raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "Glosa fuera del catálogo.")
            observacion = payload.glosa
        elif not (payload.observacion or "").strip():
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "Selecciona la glosa del catálogo.")
    if payload.estado_contable == "Contabilizado" and not (payload.num_causacion or "").strip():
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "Indica el número de causación / comprobante contable.")

    f.estado_contable = payload.estado_contable
    if observacion is not None:
        f.detalles = {**(f.detalles or {}), "observacion_contab": observacion}
    if payload.estado_contable == "Contabilizado":
        f.num_causacion = payload.num_causacion.strip()[:30]
        f.fecha_contabilizacion = f.fecha_contabilizacion or ahora
        if f.estado != EstadoFactura.CAUSADA.value:
            f.estado = EstadoFactura.CAUSADA.value
            f.causada_por_id = usuario.id
            f.fecha_causacion = ahora
            _generar_movimientos(db, f, usuario)
    f.version += 1
    f.updated_by = usuario.username
    f.historial_cambios = [*(f.historial_cambios or []),
                           _evento(usuario, "NOVEDAD", f.estado, f.estado,
                                   f"{payload.estado_contable}{(' · ' + (observacion or '')) if observacion else ''}")]
    db.commit()
    db.refresh(f)
    AuditService.registrar(db=db, modulo="contabilidad", accion="NOVEDAD", entidad_id=str(f.id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"estado_contable": payload.estado_contable,
                                                                        "num_causacion": payload.num_causacion})
    return _factura_dict(f)


@router.post("/facturas/{id}/reabrir")
def reabrir_contabilizacion(id: uuid.UUID, db: Session = Depends(get_db_fin),
                            usuario: Usuario = Depends(require_not_consulta)):
    if usuario.rol not in (RolUsuario.CONTABILIDAD, RolUsuario.ADMIN) and not _es_admin(usuario):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo Contabilidad reabre la contabilización.")
    f = cargar_con_bloqueo(db, FacturaDian, id, None, "Factura")
    if f.estado != EstadoFactura.CAUSADA.value:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Solo se reabren facturas causadas.")
    f.estado = EstadoFactura.RADICADA.value
    f.fecha_contabilizacion = None
    f.num_causacion = None
    f.estado_contable = "Pendiente"
    f.version += 1
    f.historial_cambios = [*(f.historial_cambios or []),
                           _evento(usuario, "REABRIR_CONTAB", EstadoFactura.CAUSADA, f.estado, "")]
    db.commit()
    db.refresh(f)
    return _factura_dict(f)


# ── Cuenta de cobro manual ─────────────────────────────────────────────────

class CuentaCobroPayload(BaseModel):
    beneficiario: str
    nit: str
    numero_documento: Optional[str] = None
    valor: float
    iva: float = 0
    fecha_documento: Optional[str] = None
    fecha_envio: Optional[str] = None
    categoria: str = "OPERACIÓN"
    origen: str = "Directo"         # 'Directo' o 'Caja Menor: <sede>'
    concepto: Optional[str] = None


@router.post("/facturas/cuenta-cobro")
def radicar_cuenta_cobro(payload: CuentaCobroPayload, request: Request,
                         db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta)):
    if payload.categoria.upper() not in CATEGORIAS_CUENTA_COBRO:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "Categoría no válida.")
    ahora = datetime.now(timezone.utc)
    nueva = FacturaDian(
        cufe=None,
        numero_factura=payload.numero_documento or f"CC-{ahora.strftime('%y%m%d')}-{uuid.uuid4().hex[:4]}",
        nit_proveedor=payload.nit[:50],
        proveedor=payload.beneficiario.strip()[:250],
        fecha_emision=datetime.fromisoformat(payload.fecha_documento) if payload.fecha_documento else ahora,
        tipo_documento="Cuenta de cobro",
        origen="manual",
        iva=Decimal(str(payload.iva or 0)),
        total=Decimal(str(payload.valor)),
        estado=EstadoFactura.TRAMITADA.value,
        validada=True,
        es_cuenta_cobro=True,
        origen_doc=payload.origen[:40],
        caja_menor_sede=payload.origen if payload.origen.startswith("Caja Menor") else None,
        categoria_contable=payload.categoria.upper(),
        estado_contable="Pendiente",
        fecha_envio_contab=datetime.fromisoformat(payload.fecha_envio) if payload.fecha_envio else ahora,
        observacion_recepcion=(payload.concepto or "").strip().upper() or None,
        historial_cambios=[_evento(usuario, "CUENTA_COBRO", None, EstadoFactura.TRAMITADA, payload.origen)],
        created_by=usuario.username,
    )
    db.add(nueva)
    db.commit()
    db.refresh(nueva)
    AuditService.registrar(db=db, modulo="contabilidad", accion="CUENTA_COBRO", entidad_id=str(nueva.id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"beneficiario": nueva.proveedor, "total": str(nueva.total)})
    return _factura_dict(nueva)


# ── Histórico de cargas y token DIAN ───────────────────────────────────────

@router.get("/uploads")
def listar_uploads(db: Session = Depends(get_db_fin), _: Usuario = Depends(require_not_consulta)):
    from financiero.models.dian import DianUpload
    return [{"id": str(u.id), "archivo": u.archivo, "usuario": u.usuario,
             "conteos": {"nuevas": u.conteo_nuevas, "duplicadas": u.conteo_duplicadas,
                         "propias": u.conteo_propias, "transportadores": u.conteo_transportadores,
                         "otros": u.conteo_otros},
             "fecha": u.created_at.isoformat() if u.created_at else None}
            for u in db.scalars(select(DianUpload).order_by(DianUpload.created_at.desc()).limit(10)).all()]


@router.get("/token")
def leer_token(db: Session = Depends(get_db_fin), _: Usuario = Depends(require_not_consulta)):
    from financiero.models.dian import DianToken
    fila = db.get(DianToken, 1)
    return {"archivo": fila.archivo if fila else None, "token": fila.token if fila else [],
            "actualizado_en": fila.actualizado_en.isoformat() if fila and fila.actualizado_en else None}


@router.put("/token")
def guardar_token(body: dict, db: Session = Depends(get_db_fin),
                  usuario: Usuario = Depends(require_not_consulta)):
    from financiero.models.dian import DianToken
    fila = db.get(DianToken, 1) or DianToken(id=1)
    fila.archivo = str(body.get("archivo") or "")[:200]
    fila.token = body.get("token") or []
    fila.actualizado_en = datetime.now(timezone.utc)
    db.add(fila)
    db.commit()
    return {"mensaje": "Token guardado.", "items": len(fila.token)}


# ── ZIP de soportes (backend: los blobs viven en GCS privado) ──────────────

@router.get("/facturas/zip-soportes")
def zip_soportes(
    desde: Optional[str] = None,
    hasta: Optional[str] = None,
    estado_contable: Optional[str] = None,
    db: Session = Depends(get_db_fin),
    usuario: Usuario = Depends(require_not_consulta),
):
    """ZIP con los PDFs de facturas y órdenes de compra + índice CSV
    (puerto de descargarSoportesContabilidadZip)."""
    import io as _io
    import zipfile
    from fastapi.responses import StreamingResponse
    from financiero.models.adjunto import Adjunto
    from rutas.fin_adjuntos import _obtener_cliente_storage
    from financiero.constantes import FIN_BUCKET

    stmt = select(FacturaDian)
    if desde:
        stmt = stmt.where(FacturaDian.fecha_emision >= desde)
    if hasta:
        stmt = stmt.where(FacturaDian.fecha_emision <= f"{hasta}T23:59:59")
    if estado_contable:
        stmt = stmt.where(FacturaDian.estado_contable == estado_contable)
    facturas = db.scalars(stmt.order_by(FacturaDian.fecha_emision.desc()).limit(500)).all()

    cliente = _obtener_cliente_storage()
    bucket = cliente.bucket(FIN_BUCKET)
    buf = _io.BytesIO()
    indice = ["Factura;Proveedor;NIT;Total;Fecha Emision;Fecha Envio Contab;Estado Contable;Num Causacion;Orden Compra;Tiene Factura PDF;Tiene OC PDF"]
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for f in facturas:
            adjuntos = db.scalars(select(Adjunto).where(Adjunto.modulo_origen == "factura",
                                                        Adjunto.referencia_id == str(f.id))).all()
            tiene_factura = tiene_oc = False
            for adj in adjuntos:
                tipo = adj.tipo_documento or ""
                try:
                    contenido = bucket.blob(adj.ruta_almacenamiento).download_as_bytes()
                except Exception as exc:
                    logger.warning("[FIN] Blob de soporte no descargable (%s): %s", adj.ruta_almacenamiento, exc)
                    continue
                proveedor25 = (f.proveedor or "")[:25].replace("/", "-")
                if tipo == "orden_compra":
                    z.writestr(f"2_Ordenes_de_Compra/OC_{(f.num_orden_compra or 'NA')}_{proveedor25}.pdf", contenido)
                    tiene_oc = True
                else:
                    z.writestr(f"1_Facturas_DIAN/FAC_{f.numero_factura}_{proveedor25}.pdf", contenido)
                    tiene_factura = True
            indice.append(";".join([
                f.numero_factura, (f.proveedor or "").replace(";", ","), f.nit_proveedor, str(f.total),
                f.fecha_emision.strftime("%Y-%m-%d") if f.fecha_emision else "",
                f.fecha_envio_contab.strftime("%Y-%m-%d") if f.fecha_envio_contab else "",
                f.estado_contable or "", f.num_causacion or "", f.num_orden_compra or "",
                "SÍ" if tiene_factura else "NO", "SÍ" if tiene_oc else "NO",
            ]))
        z.writestr("Indice_Causacion_Contable.csv", "﻿" + "\n".join(indice))
    buf.seek(0)
    return StreamingResponse(buf, media_type="application/zip",
                             headers={"Content-Disposition": 'attachment; filename="Soportes_Contabilidad.zip"'})


# ── Correos «Notificar Compras» / «Notificar Caja Menor» (Resend) ──────────

class CorreoPayload(BaseModel):
    destinatarios: list[str]
    asunto: str
    lineas: list[str]


@router.post("/correos/notificar")
def enviar_correo_notificacion(payload: CorreoPayload, background: BackgroundTasks,
                                db: Session = Depends(get_db_fin),
                                usuario: Usuario = Depends(require_not_consulta)):
    """Envía por Resend la plantilla con la tabla de facturas; los
    destinatarios y el asunto los edita el front en el modal."""
    if not payload.destinatarios:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "Indica al menos un destinatario.")
    notificaciones.avisar(background, db, payload.destinatarios, payload.asunto, payload.lineas)
    return {"mensaje": "Correo en camino.", "destinatarios": len(payload.destinatarios)}


# ── Agregación para la Torre de control del inicio ─────────────────────────

@router.get("/torre")
def torre_control(db: Session = Depends(get_db_fin), usuario: Usuario = Depends(get_usuario_fin),
                  __seccion=Depends(require_seccion("facturas"))):
    from financiero.models.contabilidad import PagoUrgente, ServicioPublico
    hoy = datetime.now(timezone.utc).date().isoformat()
    pagos = db.scalars(select(PagoUrgente).where(PagoUrgente.estado == "PENDIENTE")).all()
    servicios = db.scalars(select(ServicioPublico).where(ServicioPublico.estado != "PAGADO")).all()
    facturas = db.scalars(select(FacturaDian).limit(2000)).all()
    ultima = db.scalar(select(FacturaDian).order_by(FacturaDian.created_at.desc()))
    return {
        "pagos_hoy": [{"id": str(p.id), "proveedor": p.proveedor, "valor": float(p.valor),
                       "fecha": p.fecha_requerida.date().isoformat() if p.fecha_requerida else None}
                      for p in pagos if p.fecha_requerida and p.fecha_requerida.date().isoformat() == hoy],
        "servicios_hoy": [{"servicio": s.servicio, "inmueble": s.inmueble, "valor": float(s.valor),
                           "fecha_limite": s.fecha_limite.date().isoformat() if s.fecha_limite else None}
                          for s in servicios if s.fecha_limite and s.fecha_limite.date().isoformat() == hoy],
        "agenda": [{"fecha": p.fecha_requerida.date().isoformat(), "tipo": "pago",
                    "detalle": p.proveedor or p.oc_numero, "valor": float(p.valor)}
                   for p in pagos if p.fecha_requerida]
                  + [{"fecha": s.fecha_limite.date().isoformat(), "tipo": "servicio",
                      "detalle": f"{s.servicio} · {s.inmueble}", "valor": float(s.valor)}
                     for s in servicios if s.fecha_limite],
        "kpis": {
            "facturas_pendientes": sum(1 for f in facturas if f.estado == EstadoFactura.PENDIENTE.value),
            "facturas_por_causar": sum(1 for f in facturas if f.estado in (EstadoFactura.TRAMITADA.value, EstadoFactura.RADICADA.value)),
            "facturas_caja_menor": sum(1 for f in facturas if f.estado == EstadoFactura.LEGALIZACION.value),
            "pagos_pendientes": len(pagos),
            "servicios_pendientes": len(servicios),
        },
        "ultima_carga": ultima.created_at.isoformat() if ultima and ultima.created_at else None,
    }
