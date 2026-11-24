"""Cajas menores: legalización de gastos por sede y periodo.

Diseño nuevo sobre fin.cajas_menores (en el externo era 'minorCash' del
base_centular con ~3.400 líneas de front). Flujo: el responsable arma los
renglones (EN_EDICION) → LEGALIZADA → CONTABILIZADA (asiento cuadrado en
fin.movimientos_contables). Los soportes de cada renglón van como adjuntos
GCS (modulo 'caja', referencia = id de caja).
"""
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from financiero.database import get_db_fin
from financiero.dependencies import get_usuario_fin, require_not_consulta, require_seccion
from financiero.models.contabilidad import CajaMenor, MovimientoContable
from financiero.models.usuario import RolUsuario, Usuario
from financiero.services import notificaciones
from financiero.services.audit_service import AuditService
from financiero.services.locking import cargar_con_bloqueo
from rutas.fin_traslados import get_client_ip

router = APIRouter(prefix="/fin/cajas", tags=["Financiero · Cajas menores"])


def _es_admin(u: Usuario) -> bool:
    return u.rol == RolUsuario.ADMIN or u.es_admin_adicional


def _es_conta(u: Usuario) -> bool:
    return u.rol == RolUsuario.CONTABILIDAD or _es_admin(u)


def _evento(usuario: Usuario, accion: str, anterior: str, nuevo: str, obs: str = "") -> dict:
    return {"fecha": datetime.now(timezone.utc).isoformat(), "usuario": usuario.nombre,
            "username": usuario.username, "accion": accion, "estado_anterior": anterior,
            "estado_nuevo": nuevo, "observacion": obs}


def _total_movimientos(caja: CajaMenor) -> Decimal:
    return sum((Decimal(str(m.get("valor") or 0)) for m in (caja.movimientos or [])), Decimal("0"))


def _dict(c: CajaMenor) -> dict:
    return {
        "id": str(c.id), "sede": c.sede, "responsable": c.responsable, "periodo": c.periodo,
        "saldo_inicial": float(c.saldo_inicial), "total_gastos": float(_total_movimientos(c)),
        "estado": c.estado, "movimientos": c.movimientos or [], "datos": c.datos or {},
        "version": c.version, "created_at": c.created_at.isoformat() if c.created_at else None,
        "updated_at": c.updated_at.isoformat() if c.updated_at else None,
    }


@router.get("")
def listar_cajas(
    periodo: Optional[str] = None,
    sede: Optional[str] = None,
    estado: Optional[str] = None,
    db: Session = Depends(get_db_fin),
    _: Usuario = Depends(get_usuario_fin),
    __seccion=Depends(require_seccion("cajas")),
):
    stmt = select(CajaMenor)
    if periodo:
        stmt = stmt.where(CajaMenor.periodo == periodo)
    if sede:
        stmt = stmt.where(CajaMenor.sede.ilike(f"%{sede.strip()}%"))
    if estado:
        stmt = stmt.where(CajaMenor.estado == estado.upper())
    return [_dict(c) for c in db.scalars(
        stmt.order_by(CajaMenor.periodo.desc(), CajaMenor.sede).limit(500)).all()]


@router.get("/{id}")
def obtener_caja(id: uuid.UUID, db: Session = Depends(get_db_fin), _: Usuario = Depends(get_usuario_fin),
                  __seccion=Depends(require_seccion("cajas"))):
    c = db.get(CajaMenor, id)
    if not c:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Caja menor no encontrada.")
    return _dict(c)


class MovimientoCaja(BaseModel):
    fecha: Optional[str] = None
    concepto: str = Field(..., min_length=2)
    valor: Decimal = Field(gt=0)
    tipo: str = "gasto"           # gasto | reposicion
    centro_costo: Optional[str] = None
    factura_dian_id: Optional[str] = None  # vínculo opcional con una factura cargada
    datos: Optional[dict] = None


class CajaPayload(BaseModel):
    sede: str = Field(..., min_length=2)
    responsable: str = Field(..., min_length=2)
    periodo: str = Field(..., pattern=r"^\d{4}-\d{2}$")
    saldo_inicial: Decimal = Field(ge=0, default=Decimal("0"))
    movimientos: list[MovimientoCaja] = Field(default_factory=list)
    datos: Optional[dict] = None


@router.post("")
def crear_caja(payload: CajaPayload, request: Request, db: Session = Depends(get_db_fin),
               usuario: Usuario = Depends(require_not_consulta)):
    dup = db.scalar(select(CajaMenor).where(CajaMenor.sede == payload.sede.strip(),
                                            CajaMenor.periodo == payload.periodo))
    if dup:
        raise HTTPException(status.HTTP_409_CONFLICT, f"Ya existe la caja de {payload.sede} para {payload.periodo}.")
    c = _aplicar_payload(None, payload, usuario)
    c.estado = "EN_EDICION"
    c.created_by = usuario.username
    db.add(c)
    db.commit()
    db.refresh(c)
    AuditService.registrar(db=db, modulo="cajas", accion="CREAR", entidad_id=str(c.id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"sede": c.sede, "periodo": c.periodo})
    return _dict(c)


def _aplicar_payload(caja: CajaMenor | None, payload: CajaPayload, usuario: Usuario) -> CajaMenor:
    c = caja or CajaMenor()
    c.sede = payload.sede.strip()[:120]
    c.responsable = payload.responsable.strip()[:150]
    c.periodo = payload.periodo
    c.saldo_inicial = payload.saldo_inicial
    c.movimientos = [m.model_dump(mode="json") for m in payload.movimientos]
    if payload.datos is not None:
        c.datos = payload.datos
    c.updated_by = usuario.username
    return c


@router.put("/{id}")
def guardar_caja(id: uuid.UUID, payload: CajaPayload, request: Request,
                 db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta)):
    c = cargar_con_bloqueo(db, CajaMenor, id, None, "Caja menor")
    if c.estado != "EN_EDICION":
        raise HTTPException(status.HTTP_400_BAD_REQUEST,
                            f"La caja está {c.estado}: solo se edita en EN_EDICION (el admin puede reabrir).")
    _aplicar_payload(c, payload, usuario)
    c.version += 1
    db.commit()
    db.refresh(c)
    AuditService.registrar(db=db, modulo="cajas", accion="EDITAR", entidad_id=str(c.id), usuario=usuario,
                           ip_cliente=get_client_ip(request),
                           detalles={"sede": c.sede, "movimientos": len(c.movimientos or [])})
    return _dict(c)


class TransicionCaja(BaseModel):
    accion: str  # LEGALIZAR | CONTABILIZAR | REABRIR (solo admin)
    version: Optional[int] = None
    observacion: Optional[str] = None


@router.post("/{id}/transicion")
def transicion_caja(id: uuid.UUID, body: TransicionCaja, request: Request, background: BackgroundTasks,
                    db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta)):
    c = cargar_con_bloqueo(db, CajaMenor, id, body.version, "Caja menor")
    accion = body.accion.upper()
    anterior = c.estado
    ahora = datetime.now(timezone.utc)

    if accion == "LEGALIZAR":
        if c.estado != "EN_EDICION":
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Solo se legalizan cajas en edición.")
        if not c.movimientos:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "Agrega al menos un renglón antes de legalizar.")
        c.estado = "LEGALIZADA"
        c.datos = {**(c.datos or {}), "legalizada_por": usuario.nombre, "legalizada_at": ahora.isoformat()}
    elif accion == "CONTABILIZAR":
        if c.estado != "LEGALIZADA":
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Solo se contabilizan cajas legalizadas.")
        if not _es_conta(usuario):
            raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo Contabilidad contabiliza cajas menores.")
        c.estado = "CONTABILIZADA"
        c.datos = {**(c.datos or {}), "contabilizada_por": usuario.nombre, "contabilizada_at": ahora.isoformat()}
        _generar_asiento(db, c, usuario)
    elif accion == "REABRIR":
        if not _es_admin(usuario):
            raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo el administrador reabre cajas.")
        if c.estado == "EN_EDICION":
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "La caja ya está en edición.")
        c.estado = "EN_EDICION"
    else:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Acción desconocida (LEGALIZAR, CONTABILIZAR o REABRIR).")

    c.version += 1
    c.updated_by = usuario.username
    c.datos = {**(c.datos or {}), "historial": [
        *((c.datos or {}).get("historial") or []),
        _evento(usuario, accion, anterior, c.estado, body.observacion or ""),
    ]}
    db.commit()
    db.refresh(c)
    AuditService.registrar(db=db, modulo="cajas", accion=accion, entidad_id=str(c.id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"sede": c.sede, "estado": c.estado})
    if accion == "LEGALIZAR":
        notificaciones.avisar(background, db, [u.correo for u in db.scalars(
            select(Usuario).where(Usuario.rol == RolUsuario.CONTABILIDAD, Usuario.activo.is_(True))).all() if u.correo],
            f"Caja menor {c.sede} {c.periodo} legalizada",
            [f"El responsable <b>{c.responsable}</b> legalizó la caja de <b>{c.sede}</b> ({c.periodo}).",
             f"Total gastos: <b>${_total_movimientos(c):,.0f}</b> COP."])
    return _dict(c)


def _generar_asiento(db: Session, c: CajaMenor, usuario: Usuario):
    """Asiento de la caja: gasto por renglón (débito) contra caja menor
    (crédito) por el total — cuadra exacto."""
    periodo = c.periodo
    for m in (c.movimientos or []):
        db.add(MovimientoContable(
            origen="caja", referencia_id=c.id, referencia=f"{c.sede} {periodo}", periodo=periodo,
            cuenta="GASTOCAJAMENOR", tercero=c.sede, debito=Decimal(str(m.get("valor") or 0)),
            credito=Decimal("0"),
            detalle={"concepto": m.get("concepto"), "centro_costo": m.get("centro_costo"),
                     "tipo": m.get("tipo", "gasto")},
            creado_por_id=usuario.id,
        ))
    db.add(MovimientoContable(
        origen="caja", referencia_id=c.id, referencia=f"{c.sede} {periodo}", periodo=periodo,
        cuenta="CAJAMENOR", tercero=c.sede, debito=Decimal("0"), credito=_total_movimientos(c),
        detalle={"sede": c.sede, "renglones": len(c.movimientos or [])},
        creado_por_id=usuario.id,
    ))


@router.delete("/{id}")
def eliminar_caja(id: uuid.UUID, request: Request, db: Session = Depends(get_db_fin),
                  usuario: Usuario = Depends(require_not_consulta)):
    if not _es_admin(usuario):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo el administrador elimina cajas.")
    c = db.get(CajaMenor, id)
    if not c:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Caja menor no encontrada.")
    if c.estado == "CONTABILIZADA":
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "No se elimina una caja contabilizada (generó asiento).")
    db.delete(c)
    db.commit()
    AuditService.registrar(db=db, modulo="cajas", accion="ELIMINAR", entidad_id=str(id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"sede": getattr(c, 'sede', '?')})
    return {"mensaje": "Caja eliminada."}
