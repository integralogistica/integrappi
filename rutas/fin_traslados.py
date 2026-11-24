"""Traslados de presupuesto (jefe de área pide → Presidencia decide).

# puerto de app/routers/traslados.py del módulo externo. Cambios:
- prefix /fin/traslados, dependencias de financiero.dependencies.
- Sin ws_manager (aquí el front refresca por polling tras cada mutación).
"""
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from financiero.database import get_db_fin
from financiero.dependencies import get_usuario_fin, require_not_consulta, require_seccion
from financiero.models.traslado import Traslado, EstadoTraslado
from financiero.models.usuario import RolUsuario, Usuario
from financiero.schemas import AccionTransicionRequest
from financiero.schemas.traslado import TrasladoCreate, TrasladoLinea, TrasladoReenvio, TrasladoResponse
from financiero.services import notificaciones
from financiero.services.audit_service import AuditService
from financiero.services.configuracion import obtener as cfg_obtener
from financiero.services.consecutivo_service import ConsecutivoService
from financiero.services.locking import cargar_con_bloqueo
from financiero.services.state_machines import StateMachineService

router = APIRouter(prefix="/fin/traslados", tags=["Financiero · Traslados de presupuesto"])


def get_client_ip(request: Request) -> str:
    """# puerto de app/dependencies.py::get_client_ip."""
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "127.0.0.1"


def es_admin(u: Usuario) -> bool:
    return u.rol == RolUsuario.ADMIN or u.es_admin_adicional


def puede_crear(u: Usuario) -> bool:
    return u.rol == RolUsuario.JEFE_AREA or es_admin(u)


def visible(t: Traslado, u: Usuario) -> bool:
    if es_admin(u) or u.rol == RolUsuario.PRESIDENCIA:
        return True
    if u.rol in (RolUsuario.CONTROLLER, RolUsuario.GERENTE_FINANCIERO):
        return t.estado == EstadoTraslado.APROBADO       # Presidencia les remite lo aprobado
    return str(t.solicitante_id) == str(u.id)


def _evento(usuario: Usuario, accion: str, anterior, nuevo, obs: str = "") -> dict:
    return {"fecha": datetime.now(timezone.utc).isoformat(), "usuario": usuario.nombre, "username": usuario.username,
            "accion": accion, "estado_anterior": anterior.value if anterior else None, "estado_nuevo": nuevo.value, "observacion": obs}


@router.get("", response_model=List[TrasladoResponse])
def listar_traslados(estado: Optional[EstadoTraslado] = None, db: Session = Depends(get_db_fin),
                     current_user: Usuario = Depends(get_usuario_fin),
                     __seccion=Depends(require_seccion("traslados"))):
    stmt = select(Traslado)
    if estado:
        stmt = stmt.where(Traslado.estado == estado)
    return [t for t in db.scalars(stmt.order_by(Traslado.created_at.desc())).all() if visible(t, current_user)]


@router.get("/{id}", response_model=TrasladoResponse)
def obtener_traslado(id: uuid.UUID, db: Session = Depends(get_db_fin), current_user: Usuario = Depends(get_usuario_fin),
                       __seccion=Depends(require_seccion("traslados"))):
    t = db.get(Traslado, id)
    if not t or not visible(t, current_user):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Traslado no encontrado.")
    return t


def _area_destino(req_area: Optional[str], u: Usuario, db: Session) -> str:
    areas = cfg_obtener(db, "areas")
    area = (req_area or u.area or "").strip()
    if not area:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Falta el área destino.")
    return area


@router.post("", response_model=TrasladoResponse)
def crear_traslado(tr_in: TrasladoCreate, request: Request, background: BackgroundTasks,
                   db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta)):
    if not puede_crear(usuario):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo los responsables de área (Jefe de área) piden traslados de presupuesto.")
    area = _area_destino(tr_in.area, usuario, db)
    lineas = [l.model_dump(mode="json") for l in tr_in.lineas]
    total = sum((Decimal(str(l["monto"])) for l in lineas), Decimal("0"))
    ahora = datetime.now(timezone.utc)
    nuevo = Traslado(
        consecutivo=ConsecutivoService.generar_consecutivo(db, "tr"), solicitante_id=usuario.id, solicitante_nombre=usuario.nombre,
        area=area, fecha=ahora, observaciones=tr_in.observaciones, datos_front=tr_in.datos_front, lineas=lineas, total=total,
        estado=EstadoTraslado.PENDIENTE_PRESIDENCIA, created_by=usuario.username,
        historial_cambios=[_evento(usuario, "RADICACION", None, EstadoTraslado.PENDIENTE_PRESIDENCIA, "Enviada a Presidencia")],
    )
    db.add(nuevo)
    db.commit()
    db.refresh(nuevo)
    AuditService.registrar(db=db, modulo="traslados", accion="CREAR", entidad_id=str(nuevo.id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"consecutivo": nuevo.consecutivo, "total": str(nuevo.total), "area": area})
    notificaciones.traslado_pendiente(background, db, nuevo)
    return nuevo


@router.put("/{id}", response_model=TrasladoResponse)
def corregir_y_reenviar(id: uuid.UUID, req: TrasladoReenvio, request: Request, db: Session = Depends(get_db_fin),
                        usuario: Usuario = Depends(require_not_consulta)):
    """Quien creó la solicitud la corrige y la vuelve a enviar a Presidencia (solo si fue rechazada)."""
    tr = cargar_con_bloqueo(db, Traslado, id, req.version, "Traslado")
    if str(tr.solicitante_id) != str(usuario.id):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo quien creó la solicitud puede corregirla.")
    if tr.estado != EstadoTraslado.RECHAZADO:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Solo se corrigen solicitudes rechazadas.")
    anterior = tr.estado
    tr.lineas = [l.model_dump(mode="json") for l in req.lineas]
    tr.total = sum((Decimal(str(l["monto"])) for l in tr.lineas), Decimal("0"))
    tr.observaciones = req.observaciones
    if req.datos_front is not None:
        tr.datos_front = req.datos_front
    tr.estado = EstadoTraslado.PENDIENTE_PRESIDENCIA
    tr.motivo_rechazo, tr.aprobacion, tr.aprobador_id, tr.fecha_aprobacion = None, None, None, None
    tr.version += 1
    tr.updated_by = usuario.username
    tr.historial_cambios = [*(tr.historial_cambios or []), _evento(usuario, "REENVIAR", anterior, tr.estado, "Corregida y reenviada")]
    db.commit()
    db.refresh(tr)
    AuditService.registrar(db=db, modulo="traslados", accion="REENVIAR", entidad_id=str(id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"consecutivo": tr.consecutivo})
    return tr


@router.post("/{id}/transicion", response_model=TrasladoResponse)
def transicion_estado_traslado(id: uuid.UUID, transicion: AccionTransicionRequest, request: Request,
                               db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta)):
    tr = cargar_con_bloqueo(db, Traslado, id, transicion.version, "Traslado")
    accion = transicion.accion.upper()
    ahora = datetime.now(timezone.utc)
    anterior = tr.estado

    if accion == "APROBAR":
        nuevo = EstadoTraslado.APROBADO
    elif accion == "RECHAZAR":
        if not (transicion.observacion or "").strip():
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "Escribe el motivo del rechazo.")
        nuevo = EstadoTraslado.RECHAZADO
    elif accion == "ANULAR":
        nuevo = EstadoTraslado.ANULADO
    else:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Acción desconocida.")

    StateMachineService.validar_transicion_traslado(tr, nuevo, usuario)

    if nuevo == EstadoTraslado.APROBADO:
        enviadas = (transicion.datos_adicionales or {}).get("lineas")
        if enviadas is not None:      # el formulario congela el efecto sobre el presupuesto vigente al aprobar
            nuevas = [l.model_dump(mode="json") for l in [TrasladoLinea.model_validate(x) for x in enviadas]]
            if len(nuevas) != len(tr.lineas) or sum(Decimal(str(l["monto"])) for l in nuevas) != tr.total:
                raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "Las líneas no coinciden con la solicitud original.")
            tr.lineas = nuevas
        tr.aprobador_id, tr.fecha_aprobacion, tr.observacion_aprobacion = usuario.id, ahora, transicion.observacion
        tr.aprobacion = {"nombre": usuario.nombre, "cargo": "PRESIDENTE", "at": ahora.isoformat(), "comentario": transicion.observacion or ""}
        tr.motivo_rechazo = None
    elif nuevo == EstadoTraslado.RECHAZADO:
        tr.aprobador_id, tr.fecha_aprobacion, tr.motivo_rechazo = usuario.id, ahora, transicion.observacion
    elif nuevo == EstadoTraslado.ANULADO:
        tr.motivo_rechazo = transicion.observacion
    tr.estado = nuevo
    tr.version += 1
    tr.updated_by = usuario.username
    tr.historial_cambios = [*(tr.historial_cambios or []), _evento(usuario, accion, anterior, nuevo, transicion.observacion or "")]
    db.commit()
    db.refresh(tr)
    AuditService.registrar(db=db, modulo="traslados", accion=accion, entidad_id=str(id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"consecutivo": tr.consecutivo, "estado": tr.estado.value})
    return tr
