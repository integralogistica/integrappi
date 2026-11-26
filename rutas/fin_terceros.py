"""Terceros de Compras (proveedores nuevos) con aprobación y carga en software.

# puerto de las bandejas "Prov. nuevos" del módulo externo (allá vivían en
# db.tercerosCompras, Firestore, sin backend). Flujo:
#   solicitante crea desde la requisición (3 adjuntos obligatorios: RUT,
#   certificación bancaria y cámara de comercio) → Compras aprueba/rechaza →
#   Contabilidad marca la creación en el software contable (Vulcano).
"""
import logging
import uuid
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from financiero.database import get_db_fin
from financiero.dependencies import (
    get_usuario_fin, require_admin, require_not_consulta, require_roles, require_seccion,
)
from financiero.models.adjunto import Adjunto
from financiero.models.tercero import EstadoTercero, TerceroCompras
from financiero.models.usuario import RolUsuario, Usuario
from financiero.schemas.tercero import (
    TerceroCreate, TerceroMarcaSoftware, TerceroRechazo, TerceroResponse, TerceroUpdate,
)
from financiero.services import notificaciones
from financiero.services.audit_service import AuditService
from rutas.fin_traslados import get_client_ip

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/fin/terceros", tags=["Financiero · Proveedores nuevos"])


def _evento(usuario: Usuario, accion: str, observacion: str = "") -> dict:
    return {
        "fecha": datetime.now(timezone.utc).isoformat(),
        "usuario": usuario.nombre,
        "username": usuario.username,
        "accion": accion,
        "observacion": observacion,
    }


@router.get("", response_model=List[TerceroResponse])
def listar_terceros(
    estado: Optional[EstadoTercero] = None,
    q: Optional[str] = None,
    pendientes_software: bool = False,
    db: Session = Depends(get_db_fin),
    usuario: Usuario = Depends(get_usuario_fin),
    __seccion=Depends(require_seccion("proveedores")),
):
    stmt = select(TerceroCompras)
    if estado:
        stmt = stmt.where(TerceroCompras.estado_compras == estado)
    if pendientes_software:
        stmt = stmt.where(
            TerceroCompras.estado_compras == EstadoTercero.APROBADO,
            TerceroCompras.creado_en_software == False,  # noqa: E712
        )
    if q:
        like = f"%{q.strip()}%"
        stmt = stmt.where(TerceroCompras.razon_social.ilike(like) | TerceroCompras.nit.ilike(like))
    stmt = stmt.order_by(TerceroCompras.created_at.desc())
    return list(db.scalars(stmt).all())


@router.post("", response_model=TerceroResponse, status_code=status.HTTP_201_CREATED)
def crear_tercero(
    tercero_in: TerceroCreate,
    request: Request,
    background: BackgroundTasks,
    db: Session = Depends(get_db_fin),
    usuario: Usuario = Depends(require_not_consulta),
):
    """Crea un tercero pendiente de aprobación de Compras.

    El solicitante lo crea desde la requisición (origen_requisicion); Compras
    también puede registrarlo directo (queda igualmente visible para
    Contabilidad). Si ya hay un tercero PENDIENTE con el mismo NIT, se
    devuelve 409 con su id para reanudar ese en vez de duplicar.
    """
    if not tercero_in.origen_requisicion and not (
        usuario.rol == RolUsuario.COMPRAS or usuario.rol == RolUsuario.ADMIN or usuario.es_admin_adicional
    ):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo Compras registra proveedores directo; usa una requisición.")

    nit_digitos = "".join(c for c in tercero_in.nit if c.isdigit())
    # ignora ceros a la izquierda (Excel y el catálogo los pierden, como el original)
    nit_limpio = nit_digitos.lstrip("0") or nit_digitos or tercero_in.nit.strip()
    previo = db.scalar(
        select(TerceroCompras).where(
            TerceroCompras.nit == nit_limpio,
            TerceroCompras.estado_compras == EstadoTercero.PENDIENTE_COMPRAS,
        )
    )
    if previo:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            detail=f"Ya existe un proveedor nuevo pendiente con NIT {nit_limpio} (id {previo.id}).",
        )

    ahora = datetime.now(timezone.utc)
    nuevo = TerceroCompras(
        razon_social=tercero_in.razon_social.strip().upper(),
        nit=nit_limpio,
        nit_pago=tercero_in.nit_pago or nit_limpio,
        tipo_cuenta=tercero_in.tipo_cuenta,
        banco=tercero_in.banco,
        no_cuenta=tercero_in.no_cuenta,
        area=tercero_in.area or usuario.area,
        origen_requisicion=tercero_in.origen_requisicion,
        requisicion_id=tercero_in.requisicion_id,
        consecutivo_req=tercero_in.consecutivo_req,
        creado_por=usuario.username,
        creado_por_nombre=usuario.nombre,
        estado_compras=EstadoTercero.PENDIENTE_COMPRAS,
        historial_cambios=[_evento(usuario, "CREAR", "Proveedor nuevo: queda pendiente de aprobación de Compras")],
        created_by=usuario.username,
    )
    db.add(nuevo)
    db.commit()
    db.refresh(nuevo)

    notificaciones.tercero_pendiente_compras(background, db, nuevo)
    AuditService.registrar(
        db=db, modulo="terceros", accion="CREAR", entidad_id=str(nuevo.id), usuario=usuario,
        ip_cliente=get_client_ip(request), detalles={"razon_social": nuevo.razon_social, "nit": nuevo.nit},
    )
    return nuevo


@router.patch("/{id}", response_model=TerceroResponse)
def editar_tercero(
    id: uuid.UUID, tercero_in: TerceroUpdate, request: Request,
    db: Session = Depends(get_db_fin),
    usuario: Usuario = Depends(require_roles(RolUsuario.COMPRAS)),
):
    """Compras corrige los datos del tercero (p. ej. cuenta bancaria) antes de aprobar."""
    ter = db.get(TerceroCompras, id)
    if not ter:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Tercero no encontrado.")
    antes = {}
    for campo, valor in tercero_in.model_dump(exclude_unset=True).items():
        antes[campo] = getattr(ter, campo)
        setattr(ter, campo, valor)
    ter.version += 1
    ter.updated_by = usuario.username
    ter.historial_cambios = [*(ter.historial_cambios or []),
                             _evento(usuario, "EDICION", ", ".join(antes.keys()))]
    db.commit()
    db.refresh(ter)
    AuditService.registrar(
        db=db, modulo="terceros", accion="EDICION", entidad_id=str(id), usuario=usuario,
        ip_cliente=get_client_ip(request),
        detalles={"antes": {k: str(v)[:120] for k, v in antes.items()},
                  "despues": {k: str(getattr(ter, k))[:120] for k in antes}},
    )
    return ter


@router.post("/{id}/aprobar", response_model=TerceroResponse)
def aprobar_tercero(
    id: uuid.UUID, request: Request, background: BackgroundTasks,
    db: Session = Depends(get_db_fin),
    usuario: Usuario = Depends(require_roles(RolUsuario.COMPRAS)),
):
    """Compras aprueba el proveedor nuevo y lo remite a Contabilidad para crearlo en el software contable."""
    ter = db.get(TerceroCompras, id)
    if not ter:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Tercero no encontrado.")
    if ter.estado_compras != EstadoTercero.PENDIENTE_COMPRAS:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"No se puede aprobar en estado '{ter.estado_compras.value}'.")
    ahora = datetime.now(timezone.utc)
    ter.estado_compras = EstadoTercero.APROBADO
    ter.aprobado_por = usuario.nombre
    ter.fecha_aprobacion = ahora
    ter.motivo_rechazo_compras = None
    ter.version += 1
    ter.updated_by = usuario.username
    ter.historial_cambios = [*(ter.historial_cambios or []),
                             _evento(usuario, "APROBAR", "Aprobado por Compras · remitido a Contabilidad para creación en el software contable")]
    db.commit()
    db.refresh(ter)
    notificaciones.tercero_aprobado(background, db, ter)
    AuditService.registrar(
        db=db, modulo="terceros", accion="APROBAR", entidad_id=str(id), usuario=usuario,
        ip_cliente=get_client_ip(request), detalles={"razon_social": ter.razon_social},
    )
    return ter


@router.post("/{id}/rechazar", response_model=TerceroResponse)
def rechazar_tercero(
    id: uuid.UUID, rechazo: TerceroRechazo, request: Request,
    db: Session = Depends(get_db_fin),
    usuario: Usuario = Depends(require_roles(RolUsuario.COMPRAS)),
):
    ter = db.get(TerceroCompras, id)
    if not ter:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Tercero no encontrado.")
    if ter.estado_compras != EstadoTercero.PENDIENTE_COMPRAS:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"No se puede rechazar en estado '{ter.estado_compras.value}'.")
    ter.estado_compras = EstadoTercero.RECHAZADO_COMPRAS
    ter.motivo_rechazo_compras = rechazo.motivo.strip()
    ter.version += 1
    ter.updated_by = usuario.username
    ter.historial_cambios = [*(ter.historial_cambios or []),
                             _evento(usuario, "RECHAZAR", rechazo.motivo.strip())]
    db.commit()
    db.refresh(ter)
    AuditService.registrar(
        db=db, modulo="terceros", accion="RECHAZAR", entidad_id=str(id), usuario=usuario,
        ip_cliente=get_client_ip(request), detalles={"razon_social": ter.razon_social, "motivo": rechazo.motivo[:200]},
    )
    return ter


@router.post("/{id}/cargado-software", response_model=TerceroResponse)
def marcar_cargado_software(
    id: uuid.UUID, marca: TerceroMarcaSoftware, request: Request,
    db: Session = Depends(get_db_fin),
    usuario: Usuario = Depends(require_roles(RolUsuario.CONTABILIDAD)),
):
    """Contabilidad marca/desmarca que el proveedor ya fue creado en el software contable (toggle)."""
    ter = db.get(TerceroCompras, id)
    if not ter:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Tercero no encontrado.")
    if ter.estado_compras == EstadoTercero.PENDIENTE_COMPRAS:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Primero Compras debe aprobar el proveedor.")
    ahora = datetime.now(timezone.utc)
    nuevo_valor = marca.marcar if marca.marcar is not None else not ter.creado_en_software
    ter.creado_en_software = nuevo_valor
    if nuevo_valor:
        ter.creado_en_software_por = usuario.nombre
        ter.fecha_creado_en_software = ahora
    else:
        ter.creado_en_software_por = None
        ter.fecha_creado_en_software = None
    ter.version += 1
    ter.updated_by = usuario.username
    ter.historial_cambios = [*(ter.historial_cambios or []),
                             _evento(usuario, "CARGADO_SOFTWARE", "Marcado como creado en el software contable" if nuevo_valor else "Desmarcado: aún no está en el software contable")]
    db.commit()
    db.refresh(ter)
    AuditService.registrar(
        db=db, modulo="terceros", accion="CARGADO_SOFTWARE", entidad_id=str(id), usuario=usuario,
        ip_cliente=get_client_ip(request), detalles={"creado_en_software": nuevo_valor},
    )
    return ter


@router.delete("/{id}")
def eliminar_tercero(
    id: uuid.UUID, request: Request,
    db: Session = Depends(get_db_fin),
    admin: Usuario = Depends(require_admin),
):
    """Elimina el tercero (solo admin) con snapshot en auditoría; borra sus adjuntos de GCS best-effort."""
    ter = db.get(TerceroCompras, id)
    if not ter:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Tercero no encontrado.")
    snapshot = {
        "razon_social": ter.razon_social, "nit": ter.nit, "banco": ter.banco,
        "estado_compras": ter.estado_compras.value, "origen_requisicion": ter.origen_requisicion,
        "consecutivo_req": ter.consecutivo_req,
    }
    adjuntos = db.scalars(
        select(Adjunto).where(Adjunto.modulo_origen == "tercero", Adjunto.referencia_id == str(id))
    ).all()
    borrados = 0
    for adj in adjuntos:  # best-effort: los blobs huérfanos no rompen nada
        try:
            from rutas.fin_adjuntos import _obtener_cliente_storage
            from financiero.constantes import FIN_BUCKET
            _obtener_cliente_storage().bucket(FIN_BUCKET).blob(adj.ruta_almacenamiento).delete()
            borrados += 1
        except Exception as exc:
            logger.warning("[FIN] Blob de tercero no se pudo borrar de GCS (%s): %s", adj.ruta_almacenamiento, exc)
        db.delete(adj)
    db.delete(ter)
    db.commit()
    AuditService.registrar(
        db=db, modulo="terceros", accion="ELIMINAR", entidad_id=str(id), usuario=admin,
        ip_cliente=get_client_ip(request),
        detalles={"snapshot": snapshot, "adjuntos_borrados": borrados},
    )
    return {"mensaje": "Tercero eliminado.", "adjuntos_borrados": borrados}
