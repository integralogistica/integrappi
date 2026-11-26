"""Pagos urgentes: nacen al aprobarse una requisición con pago urgente (o se
crean manuales, o desde un anticipo interno); Tesorería (o aprobador de
pagos) los paga con referencia o los devuelve.

# puerto de views-pagos.js del módulo externo: bandeja con vista día/semana,
búsqueda, banner de vencidos, chip Contabilizada que habilita/bloquea el
pago, reversiones (reabrir/reconsiderar), edición y eliminación, motivos
frecuentes de rechazo y datos bancarios heredados del catálogo de proveedores.
"""
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from financiero.database import get_db_fin
from financiero.dependencies import get_usuario_fin, require_not_consulta, require_seccion
from financiero.models.catalogo import Proveedor
from financiero.models.contabilidad import EstadoPagoUrgente, PagoUrgente
from financiero.models.usuario import RolUsuario, Usuario
from financiero.services import notificaciones
from financiero.services.audit_service import AuditService
from financiero.services.configuracion import obtener as cfg_obtener
from financiero.services.locking import cargar_con_bloqueo
from rutas.fin_traslados import get_client_ip

router = APIRouter(prefix="/fin/pagos", tags=["Financiero · Pagos urgentes"])

MOTIVOS_RECHAZO_DEFAULT = [
    "Falta soporte de entrega",
    "Programar para pago ordinario (No urgente)",
    "Datos bancarios errados",
    "Factura con inconsistencia de valor",
    "Falta visto bueno del área solicitante",
    "Proveedor con documentos vencidos",
]


def _es_admin(u: Usuario) -> bool:
    return u.rol == RolUsuario.ADMIN or u.es_admin_adicional


def _puede_pagar(u: Usuario) -> bool:
    return u.rol == RolUsuario.TESORERIA or u.es_aprobador_pagos or _es_admin(u)


def _puede_contabilizar(u: Usuario) -> bool:
    return u.rol in (RolUsuario.CONTABILIDAD, RolUsuario.TESORERIA) or _es_admin(u)


def _digits(s) -> str:
    return "".join(c for c in str(s or "") if c.isdigit())


def datos_bancarios(db: Session, nit: str | None) -> dict:
    """Lookup del catálogo de proveedores por NIT (puerto de providerDirectory):
    llena banco/tipo de cuenta/nº de cuenta del pago."""
    if not nit:
        return {}
    d = _digits(nit)
    prov = db.scalar(select(Proveedor).where(Proveedor.activo.is_(True), Proveedor.nit == d))
    if not prov:
        prov = db.scalar(select(Proveedor).where(Proveedor.activo.is_(True),
                                                Proveedor.nit.like(f"%{d}%")))
    if not prov:
        return {}
    return {"banco": prov.banco, "tipo_cuenta": prov.tipo_cuenta, "no_cuenta": prov.numero_cuenta, "nit": prov.nit}


def _evento(usuario: Usuario, accion: str, anterior: str, nuevo: str, obs: str = "") -> dict:
    return {"fecha": datetime.now(timezone.utc).isoformat(), "usuario": usuario.nombre,
            "username": usuario.username, "accion": accion, "estado_anterior": anterior,
            "estado_nuevo": nuevo, "observacion": obs}


def _pago_dict(p: PagoUrgente) -> dict:
    return {
        "id": str(p.id), "requisicion_id": str(p.requisicion_id) if p.requisicion_id else None,
        "consecutivo_req": p.consecutivo_req, "oc_numero": p.oc_numero, "origen": p.origen,
        "solicitante_nombre": p.solicitante_nombre, "proveedor": p.proveedor,
        "valor": float(p.valor),
        "fecha_requerida": p.fecha_requerida.isoformat() if p.fecha_requerida else None,
        "estado": p.estado, "referencia": p.referencia,
        "fecha_pago": p.fecha_pago.isoformat() if p.fecha_pago else None,
        "motivo_rechazo": p.motivo_rechazo,
        "nit": p.nit, "nit_pago": p.nit_pago, "banco": p.banco,
        "no_cuenta": p.no_cuenta, "tipo_cuenta": p.tipo_cuenta, "area": p.area,
        "tipo_doc": p.tipo_doc, "numero_doc": p.numero_doc,
        "requerimiento": p.requerimiento, "asunto": p.asunto,
        "contabilizada": p.contabilizada,
        "historial_cambios": p.historial_cambios or [], "version": p.version,
        "created_at": p.created_at.isoformat() if p.created_at else None,
    }


@router.get("")
def listar_pagos(
    estado: Optional[str] = None,
    q: Optional[str] = None,
    tipo_doc: Optional[str] = None,
    desde: Optional[str] = None,          # AAAA-MM-DD (vista día)
    hasta: Optional[str] = None,          # AAAA-MM-DD (semana)
    solo_vencidos: bool = False,
    orden: str = "fecha",
    db: Session = Depends(get_db_fin),
    _: Usuario = Depends(get_usuario_fin),
    __seccion=Depends(require_seccion("pagos")),
):
    """Bandeja del original: filtros + búsqueda + resumen con KPIs y vencidos."""
    stmt = select(PagoUrgente)
    if estado:
        stmt = stmt.where(PagoUrgente.estado == estado.upper())
    if tipo_doc:
        stmt = stmt.where(PagoUrgente.tipo_doc == tipo_doc)
    if desde:
        stmt = stmt.where(PagoUrgente.fecha_requerida >= f"{desde}T00:00:00")
    if hasta:
        stmt = stmt.where(PagoUrgente.fecha_requerida <= f"{hasta}T23:59:59")
    if orden == "proveedor":
        stmt = stmt.order_by(PagoUrgente.proveedor.asc(), PagoUrgente.fecha_requerida.asc())
    else:
        stmt = stmt.order_by(PagoUrgente.fecha_requerida.asc())
    pagos = db.scalars(stmt.limit(2000)).all()

    if q and q.strip():
        needle = f"%{q.strip().lower()}%"
        pagos = [p for p in pagos if needle.strip("%") in " ".join(filter(None, [
            p.proveedor, p.nit, p.numero_doc, p.requerimiento, p.asunto,
            p.consecutivo_req, p.oc_numero, p.solicitante_nombre,
        ])).lower()]

    hoy = datetime.now(timezone.utc).date()
    vencidos = [p for p in pagos if p.estado == "PENDIENTE" and p.fecha_requerida.date() < hoy]
    if solo_vencidos:
        pagos = vencidos

    pendientes = [p for p in pagos if p.estado == "PENDIENTE"]
    return {
        "pagos": [_pago_dict(p) for p in pagos],
        "resumen": {
            "cantidad": len(pagos),
            "pendientes": len(pendientes),
            "total_pendiente": float(sum((Decimal(str(p.valor)) for p in pendientes), Decimal("0"))),
            "vencidos": len(vencidos),
            "total_vencido": float(sum((Decimal(str(p.valor)) for p in vencidos), Decimal("0"))),
        },
    }


@router.get("/motivos-rechazo")
def motivos_rechazo(db: Session = Depends(get_db_fin), _: Usuario = Depends(get_usuario_fin)):
    """Los 6 motivos frecuentes del original (editables vía configuración)."""
    return {"motivos": cfg_obtener(db, "pagos_motivos_rechazo") or MOTIVOS_RECHAZO_DEFAULT}


class PagoManualPayload(BaseModel):
    proveedor: str
    nit: Optional[str] = None
    nit_pago: Optional[str] = None
    banco: Optional[str] = None
    no_cuenta: Optional[str] = None
    tipo_cuenta: Optional[str] = None
    area: Optional[str] = None
    tipo_doc: str = "Cuenta de cobro"      # Factura | Cotización | Cuenta de cobro | Anticipo
    numero_doc: Optional[str] = None
    valor: float
    fecha_requerida: str
    requerimiento: Optional[str] = None
    asunto: Optional[str] = None


@router.post("")
def crear_pago_manual(payload: PagoManualPayload, request: Request,
                      db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta)):
    """Pago manual de Tesorería (sin requisición): autocompleta los datos
    bancarios del catálogo de proveedores por NIT (puerto del formulario del
    original)."""
    if not _puede_pagar(usuario):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo Tesorería o un aprobador de pagos crean pagos.")
    try:
        fecha = datetime.fromisoformat(payload.fecha_requerida.replace("Z", "+00:00"))
    except ValueError:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "fecha_requerida inválida.")
    heredado = {k: v for k, v in datos_bancarios(db, payload.nit).items()
                if k in ("banco", "tipo_cuenta", "no_cuenta")}
    p = PagoUrgente(
        solicitante_nombre=usuario.nombre,
        proveedor=payload.proveedor.strip()[:250],
        valor=Decimal(str(payload.valor)),
        fecha_requerida=fecha,
        estado=EstadoPagoUrgente.PENDIENTE.value,
        origen="manual",
        tipo_doc=payload.tipo_doc,
        numero_doc=payload.numero_doc,
        area=payload.area,
        requerimiento=payload.requerimiento,
        asunto=payload.asunto,
        nit=payload.nit, nit_pago=payload.nit_pago or payload.nit,
        banco=payload.banco or heredado.get("banco"),
        tipo_cuenta=payload.tipo_cuenta or heredado.get("tipo_cuenta"),
        no_cuenta=payload.no_cuenta or heredado.get("no_cuenta"),
        historial_cambios=[_evento(usuario, "CREAR", None, EstadoPagoUrgente.PENDIENTE.value, "Pago manual")],
        created_by=usuario.username,
    )
    db.add(p)
    db.commit()
    db.refresh(p)
    AuditService.registrar(db=db, modulo="pagos_urgentes", accion="CREAR", entidad_id=str(p.id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"proveedor": p.proveedor, "valor": str(p.valor)})
    return _pago_dict(p)


@router.put("/{id}")
def editar_pago(id: uuid.UUID, payload: PagoManualPayload, request: Request,
               db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta)):
    if not _puede_pagar(usuario):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo Tesorería o un aprobador de pagos editan pagos.")
    p = cargar_con_bloqueo(db, PagoUrgente, id, None, "Pago urgente")
    for campo in ("proveedor", "nit", "nit_pago", "banco", "no_cuenta", "tipo_cuenta", "area",
                  "tipo_doc", "numero_doc", "requerimiento", "asunto"):
        if getattr(payload, campo) is not None:
            setattr(p, campo, getattr(payload, campo))
    p.valor = Decimal(str(payload.valor))
    try:
        p.fecha_requerida = datetime.fromisoformat(payload.fecha_requerida.replace("Z", "+00:00"))
    except ValueError:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "fecha_requerida inválida.")
    p.version += 1
    p.updated_by = usuario.username
    p.historial_cambios = [*(p.historial_cambios or []), _evento(usuario, "EDITAR", p.estado, p.estado, "")]
    db.commit()
    db.refresh(p)
    AuditService.registrar(db=db, modulo="pagos_urgentes", accion="EDITAR", entidad_id=str(p.id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"proveedor": p.proveedor})
    return _pago_dict(p)


@router.delete("/{id}")
def eliminar_pago(id: uuid.UUID, request: Request, db: Session = Depends(get_db_fin),
                  usuario: Usuario = Depends(require_not_consulta)):
    """Elimina el pago (solo admin) limpiando los vínculos de quotes."""
    if not _es_admin(usuario):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo el administrador elimina pagos.")
    p = db.get(PagoUrgente, id)
    if not p:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Pago no encontrado.")
    from financiero.models.contabilidad import Quote
    for qta in db.scalars(select(Quote).where(Quote.pago_id == p.id)).all():
        qta.pago_id = None
        qta.version += 1
    snapshot = {"proveedor": p.proveedor, "valor": str(p.valor), "estado": p.estado}
    db.delete(p)
    db.commit()
    AuditService.registrar(db=db, modulo="pagos_urgentes", accion="ELIMINAR", entidad_id=str(id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"snapshot": snapshot})
    return {"mensaje": "Pago eliminado."}


class PagarRequest(BaseModel):
    referencia: Optional[str] = None
    version: Optional[int] = None


@router.post("/{id}/pagar")
def pagar(
    id: uuid.UUID, req: PagarRequest, request: Request, background: BackgroundTasks,
    db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta),
):
    if not _puede_pagar(usuario):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo Tesorería o un aprobador de pagos registran el pago.")
    p = cargar_con_bloqueo(db, PagoUrgente, id, req.version, "Pago urgente")
    if p.estado != EstadoPagoUrgente.PENDIENTE.value:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"El pago ya está en estado '{p.estado}'.")
    # Regla del original: sin contabilizar no se paga (Contabilidad lo habilita con el chip)
    if not p.contabilizada:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Contabilidad debe marcar la factura como contabilizada antes de pagar.")
    p.estado = EstadoPagoUrgente.PAGADO.value
    p.pagado_por_id = usuario.id
    p.fecha_pago = datetime.now(timezone.utc)
    p.referencia = (req.referencia or "").strip()[:100] or None
    p.version += 1
    p.updated_by = usuario.username
    p.historial_cambios = [*(p.historial_cambios or []), {
        "fecha": datetime.now(timezone.utc).isoformat(), "usuario": usuario.nombre,
        "username": usuario.username, "accion": "PAGAR",
        "estado_anterior": EstadoPagoUrgente.PENDIENTE.value, "estado_nuevo": EstadoPagoUrgente.PAGADO.value,
        "observacion": f"Referencia: {p.referencia or 'sin referencia'}"}]
    db.commit()
    db.refresh(p)
    AuditService.registrar(db=db, modulo="pagos_urgentes", accion="PAGAR", entidad_id=str(p.id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"req": p.consecutivo_req, "referencia": p.referencia})
    if background is not None:
        notificaciones.avisar(background, db, [usuario.correo] if usuario.correo else [],
                              f"Pago urgente registrado: {p.proveedor or p.consecutivo_req}",
                              [f"Tesorería registró el pago de <b>{p.proveedor}</b> por <b>${p.valor:,.0f}</b> COP.",
                               f"Referencia: {p.referencia or 'sin referencia'}."])
    return _pago_dict(p)


@router.post("/{id}/rechazar")
def rechazar(
    id: uuid.UUID, body: dict, request: Request,
    db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta),
):
    if not _puede_pagar(usuario):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo Tesorería o un aprobador de pagos rechazan pagos.")
    motivo = str(body.get("motivo") or "").strip()
    if not motivo:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "Escribe el motivo del rechazo.")
    p = cargar_con_bloqueo(db, PagoUrgente, id, body.get("version"), "Pago urgente")
    if p.estado != EstadoPagoUrgente.PENDIENTE.value:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"El pago ya está en estado '{p.estado}'.")
    p.estado = EstadoPagoUrgente.RECHAZADO.value
    p.motivo_rechazo = motivo[:500]
    p.version += 1
    p.updated_by = usuario.username
    p.historial_cambios = [*(p.historial_cambios or []), {
        "fecha": datetime.now(timezone.utc).isoformat(), "usuario": usuario.nombre,
        "username": usuario.username, "accion": "RECHAZAR",
        "estado_anterior": EstadoPagoUrgente.PENDIENTE.value, "estado_nuevo": EstadoPagoUrgente.RECHAZADO.value,
        "observacion": motivo[:200]}]
    db.commit()
    db.refresh(p)
    AuditService.registrar(db=db, modulo="pagos_urgentes", accion="RECHAZAR", entidad_id=str(p.id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"req": p.consecutivo_req, "motivo": motivo[:200]})
    return _pago_dict(p)


@router.post("/{id}/contabilizada")
def toggle_contabilizada(id: uuid.UUID, body: dict, request: Request,
                         db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta)):
    """Chip clicable del original: Contabilidad habilita/bloquea el pago."""
    if not _puede_contabilizar(usuario):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo Contabilidad o Tesorería conmutan la contabilizada.")
    p = cargar_con_bloqueo(db, PagoUrgente, id, body.get("version"), "Pago urgente")
    if p.estado != EstadoPagoUrgente.PENDIENTE.value:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"El pago ya está en estado '{p.estado}'.")
    p.contabilizada = not p.contabilizada
    p.version += 1
    p.updated_by = usuario.username
    p.historial_cambios = [*(p.historial_cambios or []), _evento(
        usuario, "CONTABILIZADA", p.estado, p.estado,
        "Factura contabilizada: pago habilitado" if p.contabilizada else "Sin contabilizar: pago bloqueado")]
    db.commit()
    db.refresh(p)
    AuditService.registrar(db=db, modulo="pagos_urgentes", accion="CONTABILIZADA", entidad_id=str(p.id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"contabilizada": p.contabilizada})
    return _pago_dict(p)


@router.post("/{id}/reabrir")
def reabrir_pago(id: uuid.UUID, body: dict, request: Request,
                 db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta)):
    """Reversión del original: un pago registrado vuelve a PENDIENTE."""
    if not _puede_pagar(usuario):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo Tesorería o un aprobador de pagos reabren pagos.")
    p = cargar_con_bloqueo(db, PagoUrgente, id, body.get("version"), "Pago urgente")
    if p.estado != EstadoPagoUrgente.PAGADO.value:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Solo se reabren pagos ya pagados.")
    p.estado = EstadoPagoUrgente.PENDIENTE.value
    p.pagado_por_id = None
    p.fecha_pago = None
    p.referencia = None
    p.version += 1
    p.updated_by = usuario.username
    p.historial_cambios = [*(p.historial_cambios or []), _evento(
        usuario, "REABRIR", EstadoPagoUrgente.PAGADO.value, EstadoPagoUrgente.PENDIENTE.value, "")]
    db.commit()
    db.refresh(p)
    AuditService.registrar(db=db, modulo="pagos_urgentes", accion="REABRIR", entidad_id=str(p.id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"req": p.consecutivo_req})
    return _pago_dict(p)


@router.post("/{id}/reconsiderar")
def reconsiderar_pago(id: uuid.UUID, body: dict, request: Request,
                      db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta)):
    """resetearAprobacionPago del original: un rechazado vuelve a PENDIENTE."""
    if not _puede_pagar(usuario):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo Tesorería o un aprobador de pagos reconsideran pagos.")
    p = cargar_con_bloqueo(db, PagoUrgente, id, body.get("version"), "Pago urgente")
    if p.estado != EstadoPagoUrgente.RECHAZADO.value:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Solo se reconsideran pagos devueltos.")
    p.estado = EstadoPagoUrgente.PENDIENTE.value
    p.motivo_rechazo = None
    p.version += 1
    p.updated_by = usuario.username
    p.historial_cambios = [*(p.historial_cambios or []), _evento(
        usuario, "RECONSIDERAR", EstadoPagoUrgente.RECHAZADO.value, EstadoPagoUrgente.PENDIENTE.value, "")]
    db.commit()
    db.refresh(p)
    AuditService.registrar(db=db, modulo="pagos_urgentes", accion="RECONSIDERAR", entidad_id=str(p.id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"req": p.consecutivo_req})
    return _pago_dict(p)
