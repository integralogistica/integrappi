"""Servicios públicos y corporativos por inmueble y periodo.

Diseño nuevo sobre fin.servicios_publicos (en el externo era la matriz
'utilities' del documento base_central con semilla de 28 contratos en el
navegador). El soporte de pago va como adjunto GCS (modulo 'servicio').
"""
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from financiero.database import get_db_fin
from financiero.dependencies import get_usuario_fin, require_not_consulta, require_seccion
from financiero.models.contabilidad import ServicioPublico
from financiero.models.usuario import RolUsuario, Usuario
from financiero.services.audit_service import AuditService
from financiero.services.locking import cargar_con_bloqueo
from rutas.fin_traslados import get_client_ip

router = APIRouter(prefix="/fin/servicios", tags=["Financiero · Servicios públicos"])

ESTADOS_VALIDOS = ("PENDIENTE", "CAUSADO", "PAGADO")


def _es_admin(u: Usuario) -> bool:
    return u.rol == RolUsuario.ADMIN or u.es_admin_adicional


def _puede_editar(u: Usuario) -> bool:
    return u.rol in (RolUsuario.TESORERIA, RolUsuario.CONTABILIDAD) or _es_admin(u)


def _evento(usuario: Usuario, accion: str, anterior: str, nuevo: str, obs: str = "") -> dict:
    return {"fecha": datetime.now(timezone.utc).isoformat(), "usuario": usuario.nombre,
            "username": usuario.username, "accion": accion, "estado_anterior": anterior,
            "estado_nuevo": nuevo, "observacion": obs}


def _dict(s: ServicioPublico) -> dict:
    return {
        "id": str(s.id), "servicio": s.servicio, "inmueble": s.inmueble,
        "periodo": s.periodo, "valor": float(s.valor),
        "fecha_limite": s.fecha_limite.isoformat()[:10] if s.fecha_limite else None,
        "estado": s.estado,
        "fecha_pago": s.fecha_pago.isoformat()[:10] if s.fecha_pago else None,
        "datos": s.datos or {}, "version": s.version,
        "created_at": s.created_at.isoformat() if s.created_at else None,
        "updated_at": s.updated_at.isoformat() if s.updated_at else None,
    }


@router.get("")
def listar_servicios(
    periodo: Optional[str] = None,
    servicio: Optional[str] = None,
    estado: Optional[str] = None,
    db: Session = Depends(get_db_fin),
    _: Usuario = Depends(get_usuario_fin),
    __seccion=Depends(require_seccion("servicios")),
):
    stmt = select(ServicioPublico)
    if periodo:
        stmt = stmt.where(ServicioPublico.periodo == periodo)
    if servicio:
        stmt = stmt.where(ServicioPublico.servicio.ilike(f"%{servicio.strip()}%"))
    if estado:
        stmt = stmt.where(ServicioPublico.estado == estado.upper())
    filas = db.scalars(stmt.order_by(ServicioPublico.periodo.desc(), ServicioPublico.inmueble).limit(2000)).all()
    total = sum((Decimal(str(s.valor)) for s in filas), Decimal("0"))
    pendiente = sum((Decimal(str(s.valor)) for s in filas if s.estado == "PENDIENTE"), Decimal("0"))
    return {"servicios": [_dict(s) for s in filas],
            "resumen": {"total": float(total), "pendiente": float(pendiente),
                        "cantidad": len(filas)}}


class ServicioPayload(BaseModel):
    servicio: str = Field(..., min_length=2)
    inmueble: str = Field(..., min_length=2)
    periodo: str = Field(..., pattern=r"^\d{4}-\d{2}$")
    valor: Decimal = Field(ge=0)
    fecha_limite: Optional[str] = None
    datos: Optional[dict] = None   # contrato, nit, portal de pago…


@router.post("")
def crear_servicio(payload: ServicioPayload, request: Request,
                   db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta)):
    if not _puede_editar(usuario):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo Tesorería/Contabilidad administran los servicios.")
    limite = None
    if payload.fecha_limite:
        try:
            limite = datetime.fromisoformat(payload.fecha_limite)
        except ValueError:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "fecha_limite inválida (AAAA-MM-DD).")
    s = ServicioPublico(
        servicio=payload.servicio.strip()[:80], inmueble=payload.inmueble.strip()[:150],
        periodo=payload.periodo, valor=payload.valor, fecha_limite=limite,
        datos=payload.datos, estado="PENDIENTE",
        created_by=usuario.username,
    )
    db.add(s)
    db.commit()
    db.refresh(s)
    AuditService.registrar(db=db, modulo="servicios", accion="CREAR", entidad_id=str(s.id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"inmueble": s.inmueble, "periodo": s.periodo})
    return _dict(s)


@router.put("/{id}")
def editar_servicio(id: uuid.UUID, payload: ServicioPayload, request: Request,
                    db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta)):
    if not _puede_editar(usuario):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo Tesorería/Contabilidad administran los servicios.")
    s = db.get(ServicioPublico, id)
    if not s:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Servicio no encontrado.")
    if s.estado != "PENDIENTE":
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Solo se editan servicios pendientes.")
    s.servicio, s.inmueble, s.periodo = payload.servicio.strip()[:80], payload.inmueble.strip()[:150], payload.periodo
    s.valor = payload.valor
    if payload.fecha_limite:
        try:
            s.fecha_limite = datetime.fromisoformat(payload.fecha_limite)
        except ValueError:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "fecha_limite inválida.")
    if payload.datos is not None:
        s.datos = payload.datos
    s.version += 1
    s.updated_by = usuario.username
    db.commit()
    db.refresh(s)
    AuditService.registrar(db=db, modulo="servicios", accion="EDITAR", entidad_id=str(s.id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"inmueble": s.inmueble, "periodo": s.periodo})
    return _dict(s)


class TransicionServicio(BaseModel):
    accion: str                      # CAUSAR | PAGAR
    fecha_pago: Optional[str] = None  # para PAGAR (default hoy)
    version: Optional[int] = None


@router.post("/{id}/transicion")
def transicion_servicio(id: uuid.UUID, body: TransicionServicio, request: Request,
                        db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta)):
    if not _puede_editar(usuario):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo Tesorería/Contabilidad cambian el estado.")
    s = cargar_con_bloqueo(db, ServicioPublico, id, body.version, "Servicio")
    accion = body.accion.upper()
    ahora = datetime.now(timezone.utc)
    anterior = s.estado
    if accion == "CAUSAR":
        if s.estado != "PENDIENTE":
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Solo se causan servicios pendientes.")
        s.estado = "CAUSADO"
    elif accion == "PAGAR":
        if s.estado not in ("PENDIENTE", "CAUSADO"):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "El servicio ya está pagado.")
        s.estado = "PAGADO"
        s.fecha_pago = ahora
        if body.fecha_pago:
            try:
                s.fecha_pago = datetime.fromisoformat(body.fecha_pago)
            except ValueError:
                raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "fecha_pago inválida.")
    else:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Acción desconocida (CAUSAR o PAGAR).")
    s.version += 1
    s.updated_by = usuario.username
    db.commit()
    db.refresh(s)
    AuditService.registrar(db=db, modulo="servicios", accion=accion, entidad_id=str(s.id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"inmueble": s.inmueble, "estado": s.estado})
    return _dict(s)


@router.delete("/{id}")
def eliminar_servicio(id: uuid.UUID, request: Request, db: Session = Depends(get_db_fin),
                      usuario: Usuario = Depends(require_not_consulta)):
    if not _es_admin(usuario):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo el administrador elimina servicios.")
    s = db.get(ServicioPublico, id)
    if not s:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Servicio no encontrado.")
    db.delete(s)
    db.commit()
    AuditService.registrar(db=db, modulo="servicios", accion="ELIMINAR", entidad_id=str(id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"inmueble": getattr(s, 'inmueble', '?')})
    return {"mensaje": "Servicio eliminado."}
