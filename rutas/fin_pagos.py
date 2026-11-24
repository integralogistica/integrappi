"""Pagos urgentes: nacen al aprobarse una requisición con pago urgente;
Tesorería (o aprobador de pagos) los paga con referencia o los devuelve."""
import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from financiero.database import get_db_fin
from financiero.dependencies import get_usuario_fin, require_not_consulta, require_seccion
from financiero.models.contabilidad import EstadoPagoUrgente, PagoUrgente
from financiero.models.usuario import RolUsuario, Usuario
from financiero.services import notificaciones
from financiero.services.audit_service import AuditService
from financiero.services.locking import cargar_con_bloqueo
from rutas.fin_traslados import get_client_ip

router = APIRouter(prefix="/fin/pagos", tags=["Financiero · Pagos urgentes"])


def _es_admin(u: Usuario) -> bool:
    return u.rol == RolUsuario.ADMIN or u.es_admin_adicional


def _puede_pagar(u: Usuario) -> bool:
    return u.rol == RolUsuario.TESORERIA or u.es_aprobador_pagos or _es_admin(u)


def _pago_dict(p: PagoUrgente) -> dict:
    return {
        "id": str(p.id), "requisicion_id": str(p.requisicion_id),
        "consecutivo_req": p.consecutivo_req, "oc_numero": p.oc_numero,
        "solicitante_nombre": p.solicitante_nombre, "proveedor": p.proveedor,
        "valor": float(p.valor),
        "fecha_requerida": p.fecha_requerida.isoformat() if p.fecha_requerida else None,
        "estado": p.estado, "referencia": p.referencia,
        "fecha_pago": p.fecha_pago.isoformat() if p.fecha_pago else None,
        "motivo_rechazo": p.motivo_rechazo,
        "historial_cambios": p.historial_cambios or [], "version": p.version,
    }


@router.get("")
def listar_pagos(
    estado: Optional[str] = None,
    db: Session = Depends(get_db_fin),
    _: Usuario = Depends(get_usuario_fin),
    __seccion=Depends(require_seccion("pagos")),
):
    stmt = select(PagoUrgente)
    if estado:
        stmt = stmt.where(PagoUrgente.estado == estado.upper())
    stmt = stmt.order_by(PagoUrgente.fecha_requerida.asc()).limit(1000)
    return [_pago_dict(p) for p in db.scalars(stmt).all()]


class PagarRequest(BaseModel):
    referencia: Optional[str] = None
    version: Optional[int] = None


@router.post("/{id}/pagar")
def pagar(
    id: uuid.UUID, req: PagarRequest, request: Request,
    db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta),
):
    if not _puede_pagar(usuario):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo Tesorería o un aprobador de pagos registran el pago.")
    p = cargar_con_bloqueo(db, PagoUrgente, id, req.version, "Pago urgente")
    if p.estado != EstadoPagoUrgente.PENDIENTE.value:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"El pago ya está en estado '{p.estado}'.")
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
