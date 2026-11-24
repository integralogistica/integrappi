"""Viajes y anticipos (autorizar → presupuesto → tiquetes → anticipo →
legalización → cierre).

# puerto de app/routers/viajes.py del módulo externo. Cambios: prefix
/fin/viajes, dependencias de financiero.dependencies, sin ws_manager.
"""
import unicodedata
import uuid
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from financiero.database import get_db_fin
from financiero.dependencies import get_usuario_fin, require_not_consulta, require_seccion
from financiero.models.usuario import RolUsuario, Usuario
from financiero.models.viaje import Viaje, EstadoViaje, CiudadViaje
from financiero.schemas import AccionTransicionRequest
from financiero.schemas.viaje import ViajeCreate, ViajeResponse
from financiero.services import notificaciones
from financiero.services.audit_service import AuditService
from financiero.services.configuracion import obtener as cfg_obtener
from financiero.services.consecutivo_service import ConsecutivoService
from financiero.services.locking import cargar_con_bloqueo
from financiero.services.state_machines import StateMachineService
from rutas.fin_traslados import get_client_ip

router = APIRouter(prefix="/fin/viajes", tags=["Financiero · Viajes y Anticipos"])


def _sin_acentos(t: str) -> str:
    return " ".join(unicodedata.normalize("NFD", t or "").encode("ascii", "ignore").decode().lower().split())


def exigir_viajero_propio(vj_in, usuario: Usuario):
    """Nadie pide viajes para otra persona: el viajero es siempre quien solicita."""
    if _sin_acentos(vj_in.viajero_nombre) != _sin_acentos(usuario.nombre):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo puedes solicitar viajes para ti mismo: el viajero debe ser el usuario que solicita.")


def firmas_viaje(vj: Viaje) -> dict:
    """Firmas electrónicas del formato, en el orden en que se van aprobando:
    solicitó -> autorizó -> controller. Cada una queda en None hasta que esa persona actúa."""
    hist = vj.historial_cambios or []
    ctrl = next((h for h in reversed(hist) if h.get("accion") == "ASIGNAR_PRESUPUESTO"), None)
    aut = next((h for h in reversed(hist) if h.get("accion") == "AUTORIZAR"), None)
    return {
        "solicito": {"nombre": vj.solicitante_nombre, "fecha": vj.created_at.isoformat() if vj.created_at else None},
        "autorizo": {"nombre": aut["usuario"], "fecha": aut["fecha"]} if aut else None,
        "controller": {"nombre": ctrl["usuario"], "fecha": ctrl["fecha"]} if ctrl else None,
    }


def enmascarar_viaje_segun_rol(vj: Viaje, usuario: Usuario) -> ViajeResponse:
    """
    REGLA DE SEGURIDAD EXIGIDA:
    La Categoría A/B/C es visible ÚNICAMENTE para Controller y autorizadores oficiales.
    Para el resto de usuarios se devuelve como None.
    """
    resp = ViajeResponse.model_validate(vj)
    es_autorizado = (
        usuario.rol in [RolUsuario.ADMIN, RolUsuario.CONTROLLER, RolUsuario.GERENTE_FINANCIERO, RolUsuario.PRESIDENCIA]
        or usuario.es_admin_adicional
        or usuario.es_autorizador_viajes
        or (vj.autorizador_id and str(vj.autorizador_id) == str(usuario.id))
    )
    if not es_autorizado:
        resp.categoria_tarifa = None
    resp.firmas = firmas_viaje(vj)
    return resp


def es_admin(u: Usuario) -> bool:
    return u.rol == RolUsuario.ADMIN or u.es_admin_adicional


def visible(v: Viaje, u: Usuario) -> bool:
    """Quien lo creó, su autorizador y el admin lo ven siempre; Tesorería, Compras, Controller
    y Gerente financiero cuando ya pasó la autorización; los aprobadores de pagos cuando llega el anticipo."""
    if es_admin(u) or str(v.solicitante_id) == str(u.id) or (v.autorizador_id and str(v.autorizador_id) == str(u.id)):
        return True
    if u.rol in (RolUsuario.TESORERIA, RolUsuario.COMPRAS, RolUsuario.CONTROLLER, RolUsuario.GERENTE_FINANCIERO):
        return v.estado not in (EstadoViaje.SOLICITADO, EstadoViaje.RECHAZADO, EstadoViaje.BORRADOR)
    if u.es_aprobador_pagos:
        return v.estado in (EstadoViaje.TIQUETES_GESTIONADOS, EstadoViaje.ANTICIPO_PAGADO, EstadoViaje.LEGALIZADO, EstadoViaje.CERRADO)
    return False


@router.get("", response_model=List[ViajeResponse])
def listar_viajes(
    estado: Optional[EstadoViaje] = None,
    db: Session = Depends(get_db_fin),
    current_user: Usuario = Depends(get_usuario_fin),
    __seccion=Depends(require_seccion("viajes")),
):
    stmt = select(Viaje)
    if estado:
        stmt = stmt.where(Viaje.estado == estado)
    stmt = stmt.order_by(Viaje.created_at.desc())
    return [enmascarar_viaje_segun_rol(v, current_user) for v in db.scalars(stmt).all() if visible(v, current_user)]


@router.get("/autorizadores", response_model=List[dict])
def listar_autorizadores(db: Session = Depends(get_db_fin), _: Usuario = Depends(get_usuario_fin),
                           __seccion=Depends(require_seccion("viajes"))):
    """Usuarios que pueden autorizar viajes (para el dropdown del formulario)."""
    ids_config = {str(x) for x in cfg_obtener(db, "viajes_autorizadores")}
    out = []
    for u in db.query(Usuario).filter(Usuario.activo.is_(True)).all():
        if (u.rol in (RolUsuario.ADMIN, RolUsuario.CONTROLLER, RolUsuario.GERENTE_FINANCIERO, RolUsuario.PRESIDENCIA, RolUsuario.AUTORIZADOR_VIAJES)
                or u.es_autorizador_viajes or str(u.id) in ids_config):
            out.append({"id": str(u.id), "nombre": u.nombre, "rol": u.rol.value})
    return out


@router.get("/{id}", response_model=ViajeResponse)
def obtener_viaje(id: uuid.UUID, db: Session = Depends(get_db_fin), current_user: Usuario = Depends(get_usuario_fin),
                    __seccion=Depends(require_seccion("viajes"))):
    v = db.get(Viaje, id)
    if not v or not visible(v, current_user):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Viaje no encontrado.")
    return enmascarar_viaje_segun_rol(v, current_user)


@router.post("", response_model=ViajeResponse)
def crear_viaje(
    vj_in: ViajeCreate,
    request: Request,
    background: BackgroundTasks,
    db: Session = Depends(get_db_fin),
    usuario: Usuario = Depends(require_not_consulta),
):
    exigir_viajero_propio(vj_in, usuario)
    # Validar autorizador existente y con casilla/rol
    autorizador = db.get(Usuario, vj_in.autorizador_id)
    if not autorizador or not autorizador.activo:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="El autorizador seleccionado no es válido o está inactivo.")

    es_autorizador_valido = (
        autorizador.rol in [RolUsuario.ADMIN, RolUsuario.CONTROLLER, RolUsuario.GERENTE_FINANCIERO, RolUsuario.PRESIDENCIA,
                            RolUsuario.AUTORIZADOR_VIAJES]
        or autorizador.es_autorizador_viajes
        or str(autorizador.id) in [str(x) for x in cfg_obtener(db, "viajes_autorizadores")]   # sumados por el administrador
    )
    if not es_autorizador_valido:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="El usuario asignado no cuenta con facultades para autorizar viajes.")

    # Consecutivo atómico 'vj_AAAA'
    consecutivo = ConsecutivoService.generar_consecutivo(db, "vj")
    ahora = datetime.now(timezone.utc)

    nuevo = Viaje(
        consecutivo=consecutivo,
        solicitante_id=usuario.id,
        solicitante_nombre=usuario.nombre,
        viajero_nombre=vj_in.viajero_nombre,
        viajero_cedula=vj_in.viajero_cedula,
        origen=vj_in.origen.value,
        destino=vj_in.destino.value,
        fecha_inicio=vj_in.fecha_inicio,
        fecha_fin=vj_in.fecha_fin,
        objetivo=vj_in.objetivo,
        autorizador_id=autorizador.id,
        requiere_tiquetes=vj_in.requiere_tiquetes,
        viajero_cargo=vj_in.viajero_cargo,
        categoria_solicitud=vj_in.categoria_solicitud,
        observaciones=vj_in.observaciones,
        datos_front=vj_in.datos_front,
        estado=EstadoViaje.SOLICITADO,
        historial_cambios=[{
            "fecha": ahora.isoformat(),
            "usuario": usuario.nombre,
            "username": usuario.username,
            "accion": "RADICACION",
            "estado_nuevo": EstadoViaje.SOLICITADO.value,
            "observacion": f"Solicitud de viaje radicada y asignada para visto bueno a {autorizador.nombre}."
        }],
        created_by=usuario.username,
    )
    db.add(nuevo)
    db.commit()
    db.refresh(nuevo)

    AuditService.registrar(
        db=db,
        modulo="viajes",
        accion="CREAR",
        entidad_id=str(nuevo.id),
        usuario=usuario,
        ip_cliente=get_client_ip(request),
        detalles={"consecutivo": nuevo.consecutivo, "viajero": nuevo.viajero_nombre},
    )
    notificaciones.viaje_pendiente(background, db, nuevo)
    return enmascarar_viaje_segun_rol(nuevo, usuario)


@router.post("/{id}/transicion", response_model=ViajeResponse)
def transicion_estado_viaje(
    id: uuid.UUID,
    transicion: AccionTransicionRequest,
    request: Request,
    db: Session = Depends(get_db_fin),
    usuario: Usuario = Depends(require_not_consulta),
):
    vj = cargar_con_bloqueo(db, Viaje, id, transicion.version, "Viaje")

    accion = transicion.accion.upper()
    ahora = datetime.now(timezone.utc)
    estado_anterior = vj.estado

    if accion == "AUTORIZAR":
        nuevo_estado = EstadoViaje.AUTORIZADO
        vj.fecha_autorizacion = ahora
        vj.observacion_autorizacion = transicion.observacion
    elif accion == "RECHAZAR":
        nuevo_estado = EstadoViaje.RECHAZADO
        vj.fecha_autorizacion = ahora
        vj.observacion_autorizacion = transicion.observacion
    elif accion == "REGISTRAR_TIQUETES":
        nuevo_estado = EstadoViaje.TIQUETES_GESTIONADOS
        if transicion.datos_adicionales:
            vj.tiquetes_detalle = transicion.datos_adicionales
    elif accion == "ASIGNAR_PRESUPUESTO":
        nuevo_estado = EstadoViaje.PRESUPUESTO_ASIGNADO
        if transicion.datos_adicionales:
            cat = transicion.datos_adicionales.get("categoria_tarifa")
            if cat not in ("A", "B", "C"):
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="La categoría de tarifa debe ser A, B o C.")
            vj.categoria_tarifa = cat
            vj.presupuesto_items = transicion.datos_adicionales.get("items", [])
            vj.presupuesto_detalle = transicion.datos_adicionales.get("detalle")
            vj.total_presupuesto = transicion.datos_adicionales.get("total", 0)
    elif accion == "PAGAR_ANTICIPO":
        nuevo_estado = EstadoViaje.ANTICIPO_PAGADO
        vj.anticipo_pagado = True
        vj.fecha_pago_anticipo = ahora
        if transicion.datos_adicionales:
            vj.anticipo_detalle = {**transicion.datos_adicionales, "por": usuario.nombre, "at": ahora.isoformat()}
    elif accion == "LEGALIZAR":
        nuevo_estado = EstadoViaje.LEGALIZADO
        if transicion.datos_adicionales:
            vj.legalizacion_detalle = transicion.datos_adicionales
    elif accion == "CERRAR":
        nuevo_estado = EstadoViaje.CERRADO
    elif accion == "ANULAR":
        nuevo_estado = EstadoViaje.ANULADO
    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Acción de viaje no reconocida.")

    StateMachineService.validar_transicion_viaje(vj, nuevo_estado, usuario)

    vj.estado = nuevo_estado
    vj.version += 1
    vj.updated_by = usuario.username

    hist = list(vj.historial_cambios or [])
    hist.append({
        "fecha": ahora.isoformat(),
        "usuario": usuario.nombre,
        "username": usuario.username,
        "accion": accion,
        "estado_anterior": estado_anterior.value,
        "estado_nuevo": nuevo_estado.value,
        "observacion": transicion.observacion or "",
    })
    vj.historial_cambios = hist

    db.commit()
    db.refresh(vj)

    AuditService.registrar(db=db, modulo="viajes", accion=accion, entidad_id=str(vj.id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"consecutivo": vj.consecutivo, "estado": vj.estado.value})
    return enmascarar_viaje_segun_rol(vj, usuario)


@router.put("/{id}", response_model=ViajeResponse)
def corregir_y_reenviar(id: uuid.UUID, vj_in: ViajeCreate, request: Request, version: Optional[int] = None,
                        db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta)):
    """Quien solicitó el viaje lo corrige y lo reenvía a autorización (solo si fue rechazado)."""
    vj = cargar_con_bloqueo(db, Viaje, id, version, "Viaje")
    if str(vj.solicitante_id) != str(usuario.id):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo quien solicitó el viaje puede corregirlo.")
    if vj.estado != EstadoViaje.RECHAZADO:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Solo se corrigen viajes rechazados.")
    exigir_viajero_propio(vj_in, usuario)
    autorizador = db.get(Usuario, vj_in.autorizador_id)
    if not autorizador or not autorizador.activo or not (
            autorizador.rol in (RolUsuario.ADMIN, RolUsuario.CONTROLLER, RolUsuario.GERENTE_FINANCIERO, RolUsuario.PRESIDENCIA)
            or autorizador.es_autorizador_viajes):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "El autorizador seleccionado no es válido.")
    anterior = vj.estado
    for campo in ("viajero_nombre", "viajero_cedula", "viajero_cargo", "categoria_solicitud", "observaciones", "fecha_inicio",
                  "fecha_fin", "objetivo", "requiere_tiquetes", "datos_front"):
        setattr(vj, campo, getattr(vj_in, campo))
    vj.origen, vj.destino, vj.autorizador_id = vj_in.origen.value, vj_in.destino.value, autorizador.id
    vj.fecha_autorizacion = vj.observacion_autorizacion = None
    vj.categoria_tarifa, vj.presupuesto_items, vj.total_presupuesto = None, [], 0   # se rehace el presupuesto
    vj.presupuesto_detalle = vj.tiquetes_detalle = vj.anticipo_detalle = None
    vj.estado = EstadoViaje.SOLICITADO
    vj.version += 1
    vj.updated_by = usuario.username
    vj.historial_cambios = [*(vj.historial_cambios or []), {
        "fecha": datetime.now(timezone.utc).isoformat(), "usuario": usuario.nombre, "username": usuario.username, "accion": "REENVIAR",
        "estado_anterior": anterior.value, "estado_nuevo": vj.estado.value, "observacion": f"Corregido y reenviado. Autoriza: {autorizador.nombre}"}]
    db.commit()
    db.refresh(vj)
    AuditService.registrar(db=db, modulo="viajes", accion="REENVIAR", entidad_id=str(id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"consecutivo": vj.consecutivo})
    return enmascarar_viaje_segun_rol(vj, usuario)


class EdicionPaso(BaseModel):
    paso: str      # presupuesto | tiquetes | anticipo
    datos: dict
    version: Optional[int] = None


@router.post("/{id}/editar-paso", response_model=ViajeResponse)
def editar_paso(id: uuid.UUID, req: EdicionPaso, request: Request, db: Session = Depends(get_db_fin),
                usuario: Usuario = Depends(require_not_consulta)):
    """Corrige un paso ya cumplido (presupuesto, tiquetes o anticipo) sin cambiar el estado del viaje.
    Lo puede hacer quien tiene ese paso a su cargo; queda en el historial y en la bitácora."""
    vj = cargar_con_bloqueo(db, Viaje, id, req.version, "Viaje")
    E = EstadoViaje
    orden = [E.SOLICITADO, E.AUTORIZADO, E.PRESUPUESTO_ASIGNADO, E.TIQUETES_GESTIONADOS, E.ANTICIPO_PAGADO, E.LEGALIZADO, E.CERRADO]
    admin = usuario.rol == RolUsuario.ADMIN or usuario.es_admin_adicional
    if vj.estado not in orden:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "El viaje no está en un estado editable.")
    avance = orden.index(vj.estado)
    if req.paso == "presupuesto":
        if avance < orden.index(E.PRESUPUESTO_ASIGNADO):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "El presupuesto aún no se ha generado.")
        if usuario.rol not in (RolUsuario.CONTROLLER, RolUsuario.GERENTE_FINANCIERO) and not admin:
            raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo el Controller edita el presupuesto.")
        cat = req.datos.get("categoria_tarifa")
        if cat is not None:
            if cat not in ("A", "B", "C"):
                raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "La categoría de tarifa debe ser A, B o C.")
            vj.categoria_tarifa = cat
        vj.presupuesto_items = req.datos.get("items", vj.presupuesto_items)
        vj.total_presupuesto = req.datos.get("total", vj.total_presupuesto)
        vj.presupuesto_detalle = req.datos.get("detalle", vj.presupuesto_detalle)
    elif req.paso == "tiquetes":
        if avance < orden.index(E.TIQUETES_GESTIONADOS):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Los tiquetes aún no se han registrado.")
        if usuario.rol != RolUsuario.COMPRAS and not admin:
            raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo Compras edita los tiquetes.")
        vj.tiquetes_detalle = req.datos
    elif req.paso == "anticipo":
        if avance < orden.index(E.ANTICIPO_PAGADO):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "El anticipo aún no se ha registrado.")
        if not (usuario.rol == RolUsuario.TESORERIA or usuario.es_aprobador_pagos or admin):
            raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo Tesorería o un aprobador de pagos editan el anticipo.")
        vj.anticipo_detalle = req.datos
    else:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "Paso desconocido (presupuesto, tiquetes o anticipo).")
    vj.version += 1
    vj.updated_by = usuario.username
    vj.historial_cambios = [*(vj.historial_cambios or []), {
        "fecha": datetime.now(timezone.utc).isoformat(), "usuario": usuario.nombre, "username": usuario.username,
        "accion": f"EDITAR_{req.paso.upper()}", "estado_anterior": vj.estado.value, "estado_nuevo": vj.estado.value, "observacion": ""}]
    db.commit()
    db.refresh(vj)
    AuditService.registrar(db=db, modulo="viajes", accion=f"EDITAR_{req.paso.upper()}", entidad_id=str(id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"consecutivo": vj.consecutivo})
    return enmascarar_viaje_segun_rol(vj, usuario)
