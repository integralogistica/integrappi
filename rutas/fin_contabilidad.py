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
        "historial_cambios": f.historial_cambios or [], "version": f.version,
        "created_at": f.created_at.isoformat() if f.created_at else None,
    }


@router.get("/facturas")
def listar_facturas(
    estado: Optional[str] = None,
    proveedor: Optional[str] = None,
    desde: Optional[str] = None,
    hasta: Optional[str] = None,
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
        f.estado = EstadoFactura.TRAMITADA.value
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
