"""Requisiciones de compra + órdenes de compra automáticas.

# puerto de app/routers/requisiciones.py del módulo externo. Cambios: prefix
/fin/requisiciones, dependencias de financiero.dependencies, sin ws_manager,
y notificaciones por correo (Resend) al responsable de cada paso vía
BackgroundTasks — fire-and-forget, jamás bloquean el flujo.
"""
import io
import uuid
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, Request, UploadFile, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from financiero.database import get_db_fin
from financiero.dependencies import get_usuario_fin, require_not_consulta, require_seccion
from financiero.models.requisicion import Requisicion, EstadoRequisicion
from financiero.models.usuario import RolUsuario, Usuario
from financiero.schemas import AccionTransicionRequest
from financiero.schemas.requisicion import (
    CalificacionProveedor, RequisicionCreate, RequisicionReenvio, RequisicionResponse,
)
from financiero.services import notificaciones
from financiero.services.audit_service import AuditService
from financiero.services.configuracion import limite_controller
from financiero.services.consecutivo_service import ConsecutivoService
from financiero.services.locking import cargar_con_bloqueo
from financiero.services.state_machines import StateMachineService
from rutas.fin_traslados import get_client_ip

router = APIRouter(prefix="/fin/requisiciones", tags=["Financiero · Requisiciones de Compra"])


@router.get("", response_model=List[RequisicionResponse])
def listar_requisiciones(
    estado: Optional[EstadoRequisicion] = None,
    area: Optional[str] = None,
    solo_urgentes: bool = False,
    db: Session = Depends(get_db_fin),
    current_user: Usuario = Depends(get_usuario_fin),
    __seccion=Depends(require_seccion("requisiciones")),
):
    stmt = select(Requisicion)
    if estado:
        stmt = stmt.where(Requisicion.estado == estado)
    if area:
        stmt = stmt.where(Requisicion.area == area)
    if solo_urgentes:
        stmt = stmt.where(Requisicion.es_urgente == True)  # noqa: E712
    stmt = stmt.order_by(Requisicion.created_at.desc())
    return [r for r in db.scalars(stmt).all() if visible(r, current_user)]


def es_admin(u: Usuario) -> bool:
    return u.rol == RolUsuario.ADMIN or u.es_admin_adicional


def visible(r: Requisicion, u: Usuario) -> bool:
    """Roles globales ven todo; solicitante y jefe de área, su área o lo que crearon."""
    if es_admin(u) or u.rol in (RolUsuario.TESORERIA, RolUsuario.COMPRAS, RolUsuario.CONTROLLER, RolUsuario.GERENTE_FINANCIERO):
        return True
    if u.rol in (RolUsuario.REQUISICION, RolUsuario.JEFE_AREA):
        return r.area == u.area or str(r.solicitante_id) == str(u.id)
    return False


def _resolver_area_y_flujo(req_in, usuario: Usuario, db: Session):
    """Devuelve (área, estado inicial, jefe_id, texto del historial). Compras omite al jefe de área."""
    if not (usuario.rol in (RolUsuario.REQUISICION, RolUsuario.COMPRAS) or es_admin(usuario)):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo los usuarios Requisición o Compras crean requisiciones.")
    if es_admin(usuario) and req_in.area:
        area = req_in.area.strip()
    elif usuario.rol == RolUsuario.COMPRAS:
        area = "Compras"
    else:
        area = (usuario.area or "").strip()
    if not area:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Tu usuario no tiene un Área asignada para radicar requisiciones.")
    if usuario.rol == RolUsuario.COMPRAS:
        return area, EstadoRequisicion.PENDIENTE_COMPRAS, None, "Compras: omite jefe de área"
    jefe = db.scalar(select(Usuario).where(Usuario.rol == RolUsuario.JEFE_AREA, Usuario.area == area, Usuario.activo == True))  # noqa: E712
    if not jefe:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"El área «{area}» no tiene un jefe de área activo asignado.")
    return area, EstadoRequisicion.PENDIENTE_JEFE, jefe.id, f"Enviada al jefe de área ({area})"


@router.post("", response_model=RequisicionResponse)
def crear_requisicion(
    req_in: RequisicionCreate,
    request: Request,
    background: BackgroundTasks,
    db: Session = Depends(get_db_fin),
    usuario: Usuario = Depends(require_not_consulta),
):
    area, estado_inicial, jefe_id, texto_flujo = _resolver_area_y_flujo(req_in, usuario, db)

    # Generación atómica de consecutivo 'req_AAAA' (sin colisiones en Postgres)
    consecutivo = ConsecutivoService.generar_consecutivo(db, "req")

    ahora = datetime.now(timezone.utc)
    nueva = Requisicion(
        consecutivo=consecutivo,
        solicitante_id=usuario.id,
        solicitante_nombre=usuario.nombre,
        area=area,
        lugar_entrega=req_in.lugar_entrega,
        es_urgente=req_in.es_urgente,
        motivo_urgencia=req_in.motivo_urgencia,
        proveedor_sugerido=req_in.proveedor_sugerido,
        proveedor_nit=req_in.proveedor_nit,
        items=[item.model_dump(mode="json") for item in req_in.items],
        proveedor_datos=req_in.proveedor_datos,
        datos_front=req_in.datos_front,
        observaciones=req_in.observaciones,
        subtotal=req_in.subtotal,
        iva=req_in.iva,
        total=req_in.total,
        estado=estado_inicial,
        historial_cambios=[{
            "fecha": ahora.isoformat(),
            "usuario": usuario.nombre,
            "username": usuario.username,
            "accion": "RADICACION",
            "estado_nuevo": estado_inicial.value,
            "observacion": texto_flujo
        }],
        created_by=usuario.username,
    )
    db.add(nueva)
    db.commit()
    db.refresh(nueva)

    AuditService.registrar(
        db=db, modulo="requisiciones", accion="CREAR", entidad_id=str(nueva.id), usuario=usuario,
        ip_cliente=get_client_ip(request), detalles={"consecutivo": nueva.consecutivo, "total": str(nueva.total)},
    )

    # Aviso al responsable del primer paso (fire-and-forget)
    if estado_inicial == EstadoRequisicion.PENDIENTE_JEFE:
        notificaciones.req_pendiente_jefe(background, db, jefe_id, nueva)
    else:
        notificaciones.req_pendiente_compras(background, db, nueva)

    return nueva


@router.post("/{id}/transicion", response_model=RequisicionResponse)
def transicion_estado_requisicion(
    id: uuid.UUID,
    transicion: AccionTransicionRequest,
    request: Request,
    background: BackgroundTasks,
    db: Session = Depends(get_db_fin),
    usuario: Usuario = Depends(require_not_consulta),
):
    """
    Ejecuta una transición de estado con validación estricta de máquina de estados
    en el servidor según el rol, área y topes financieros ($100M COP configurables).
    """
    req = cargar_con_bloqueo(db, Requisicion, id, transicion.version, "Requisición")

    accion = transicion.accion.upper()
    ahora = datetime.now(timezone.utc)
    estado_anterior = req.estado

    # Determinar el estado destino según la acción solicitada
    if accion == "APROBAR":
        if req.estado == EstadoRequisicion.PENDIENTE_JEFE:
            nuevo_estado = EstadoRequisicion.PENDIENTE_COMPRAS
            req.jefe_aprobador_id = usuario.id
            req.fecha_aprobacion_jefe = ahora
            req.observacion_jefe = transicion.observacion
        elif req.estado == EstadoRequisicion.PENDIENTE_COMPRAS:
            nuevo_estado = EstadoRequisicion.PENDIENTE_FINANCIERA
            req.compras_analista_id = usuario.id
            req.fecha_compras = ahora
            datos = transicion.datos_adicionales or {}
            if datos.get("pago_urgente"):
                if not datos.get("pago_fecha"):
                    raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Indica la fecha en la que se debe pagar.")
                try:
                    req.pago_fecha = datetime.fromisoformat(str(datos["pago_fecha"]).replace("Z", "+00:00"))
                except ValueError:
                    raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Fecha de pago inválida.")
                if req.pago_fecha.date() < ahora.date():
                    raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, detail="La fecha del pago urgente no puede ser anterior a hoy.")
                req.pago_urgente = True
        elif req.estado == EstadoRequisicion.PENDIENTE_FINANCIERA:
            nuevo_estado = EstadoRequisicion.APROBADA
            req.aprobador_financiero_id = usuario.id
            req.rol_aprobador_financiero = usuario.rol.value
            if req.pago_urgente:
                decision = (transicion.datos_adicionales or {}).get("pago_urgente_aprobado")
                if decision is None:
                    raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Decide si apruebas o no el pago urgente.")
                req.pago_aprobado = bool(decision)
                req.pago_decidido_por = usuario.nombre
            req.fecha_aprobacion_financiera = ahora
            req.observacion_financiera = transicion.observacion
        else:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"No se puede aprobar en estado '{req.estado.value}'.")

    elif accion == "RECHAZAR":
        if req.estado == EstadoRequisicion.PENDIENTE_JEFE:
            nuevo_estado = EstadoRequisicion.RECHAZADA_JEFE
            req.jefe_aprobador_id = usuario.id
            req.fecha_aprobacion_jefe = ahora
            req.observacion_jefe = transicion.observacion
        elif req.estado == EstadoRequisicion.PENDIENTE_FINANCIERA:
            nuevo_estado = EstadoRequisicion.RECHAZADA_FINANCIERA
            req.aprobador_financiero_id = usuario.id
            req.fecha_aprobacion_financiera = ahora
            req.observacion_financiera = transicion.observacion
        else:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No aplicable rechazo en este estado.")
        notificaciones.req_rechazada(background, db, req, transicion.observacion or "", usuario.nombre)

    elif accion == "ENTREGAR":
        # Compras marca la entrega; el solicitante debe confirmar la recepción después.
        if usuario.rol != RolUsuario.COMPRAS and not StateMachineService.es_admin(usuario):
            raise HTTPException(status.HTTP_403_FORBIDDEN, detail="Solo Compras puede marcar entregas.")
        if req.estado != EstadoRequisicion.EN_ORDEN_COMPRA or req.entregado_en:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="La requisición debe estar en orden de compra y sin entrega registrada.")
        req.entregado_en = ahora
        req.entregado_por_id = usuario.id
        req.version += 1
        req.updated_by = usuario.username
        hist = list(req.historial_cambios or [])
        hist.append({"fecha": ahora.isoformat(), "usuario": usuario.nombre, "username": usuario.username, "accion": "ENTREGAR",
                     "estado_anterior": req.estado.value, "estado_nuevo": req.estado.value,
                     "observacion": transicion.observacion or f"Pendiente de confirmación de {req.solicitante_nombre}"})
        req.historial_cambios = hist
        db.commit()
        db.refresh(req)
        return req

    elif accion == "RECIBIR_TOTAL":
        nuevo_estado = EstadoRequisicion.RECIBIDO_TOTAL
        if str(req.solicitante_id) != str(usuario.id) and not StateMachineService.es_admin(usuario):
            raise HTTPException(status.HTTP_403_FORBIDDEN, detail="Solo quien hizo la requisición puede confirmar la recepción.")
        if not req.entregado_en:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Compras aún no ha marcado la entrega.")
        # El seguimiento a proveedor es OBLIGATORIO en este paso (lo califica quien hizo la requisición).
        datos = transicion.datos_adicionales or {}
        try:
            cal = CalificacionProveedor.model_validate({"notas": datos.get("notas") or (datos.get("calificacion") or {}).get("notas") or {}})
        except Exception as e:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f"Seguimiento a proveedor obligatorio y completo (9 criterios de 0 a 15): {e}")
        req.calificacion_proveedor = {"notas": cal.notas, "puntaje": cal.puntaje(), "letra": cal.letra(), "por": usuario.nombre, "at": ahora.isoformat()}
        req.recibido_por_id = usuario.id
        req.fecha_recibido = ahora

    elif accion == "ANULAR":
        nuevo_estado = EstadoRequisicion.ANULADA

    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Acción '{accion}' desconocida.")

    # Validar máquina de estados en servidor
    StateMachineService.validar_transicion_requisicion(req, nuevo_estado, usuario, limite_controller(db))

    # REGLA DE NEGOCIO: Si pasa a APROBADA -> Generar Orden de Compra Automática (AAAA-386+)
    if nuevo_estado == EstadoRequisicion.APROBADA and not req.oc_numero:
        req.oc_numero = ConsecutivoService.generar_consecutivo(db, "oc")
        req.estado = EstadoRequisicion.EN_ORDEN_COMPRA
        nuevo_estado = EstadoRequisicion.EN_ORDEN_COMPRA
        notificaciones.req_aprobada(background, db, req)
        # Pago urgente aprobado → nace la fila en la bandeja de Tesorería
        if req.pago_urgente and req.pago_aprobado:
            from financiero.models.contabilidad import EstadoPagoUrgente, PagoUrgente
            db.add(PagoUrgente(
                requisicion_id=req.id,
                consecutivo_req=req.consecutivo,
                oc_numero=req.oc_numero,
                solicitante_nombre=req.solicitante_nombre,
                proveedor=req.proveedor_sugerido,
                valor=req.total,
                fecha_requerida=req.pago_fecha or ahora,
                estado=EstadoPagoUrgente.PENDIENTE.value,
                historial_cambios=[{
                    "fecha": ahora.isoformat(), "usuario": usuario.nombre, "username": usuario.username,
                    "accion": "CREAR", "estado_anterior": None, "estado_nuevo": EstadoPagoUrgente.PENDIENTE.value,
                    "observacion": f"Pago urgente aprobado en la aprobación financiera de {req.consecutivo}"}],
                created_by=usuario.username,
            ))
            notificaciones.avisar(background, db, [u.correo for u in db.scalars(
                select(Usuario).where(
                    ((Usuario.rol == RolUsuario.TESORERIA) | (Usuario.es_aprobador_pagos.is_(True)))
                    & Usuario.activo.is_(True)
                )).all() if u.correo],
                f"Pago urgente {req.consecutivo} listo para pagar",
                [f"La requisición <b>{req.consecutivo}</b> (OC {req.oc_numero}) aprobó su pago urgente.",
                 f"Valor: <b>${req.total:,.0f}</b> COP · Requerido para: {req.pago_fecha.date() if req.pago_fecha else '—'}."])
    else:
        req.estado = nuevo_estado
        if nuevo_estado == EstadoRequisicion.PENDIENTE_COMPRAS:
            notificaciones.req_pendiente_compras(background, db, req)
        elif nuevo_estado == EstadoRequisicion.PENDIENTE_FINANCIERA:
            notificaciones.req_pendiente_financiera(background, db, req, limite_controller(db))

    # Incrementar versión para concurrencia optimista
    req.version += 1
    req.updated_by = usuario.username

    # Registrar en historial
    hist = list(req.historial_cambios or [])
    hist.append({
        "fecha": ahora.isoformat(),
        "usuario": usuario.nombre,
        "username": usuario.username,
        "accion": accion,
        "estado_anterior": estado_anterior.value,
        "estado_nuevo": nuevo_estado.value,
        "observacion": transicion.observacion or ""
    })
    req.historial_cambios = hist

    db.commit()
    db.refresh(req)

    AuditService.registrar(
        db=db, modulo="requisiciones", accion=accion, entidad_id=str(req.id), usuario=usuario,
        ip_cliente=get_client_ip(request), detalles={"consecutivo": req.consecutivo, "oc": req.oc_numero, "estado": req.estado.value},
    )

    return req


# -----------------------------------------------------------------------------
# CARGUE MASIVO DE LÍNEAS DESDE EXCEL (requisiciones de muchísimas líneas)
# Formato: CODIGO | CENTRO DE COSTO | CANTIDAD | UNIDAD | DESCRIPCIÓN DEL ARTÍCULO | PRECIO UNITARIO | DESCUENTO | PRECIO TOTAL
# -----------------------------------------------------------------------------
COLUMNAS_CARGUE = ["CODIGO", "CENTRO DE COSTO", "CANTIDAD", "UNIDAD", "DESCRIPCIÓN DEL ARTÍCULO", "PRECIO UNITARIO", "DESCUENTO", "PRECIO TOTAL"]


def _num(v) -> float:
    if v is None or v == "":
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    t = str(v).replace("$", "").replace(" ", "")
    if "," in t and "." in t:
        t = t.replace(".", "").replace(",", ".") if t.rfind(",") > t.rfind(".") else t.replace(",", "")
    elif "," in t:
        t = t.replace(",", ".") if len(t.split(",")[-1]) <= 2 else t.replace(",", "")
    elif t.count(".") > 1:
        t = t.replace(".", "")
    try:
        return float(t)
    except ValueError:
        return 0.0


@router.get("/formato-cargue")
def descargar_formato_cargue(_: Usuario = Depends(get_usuario_fin)):
    """Descarga el Excel con el formato para cargar las líneas de una requisición."""
    import openpyxl
    from openpyxl.styles import Font, PatternFill
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Hoja1"
    ws.append(COLUMNAS_CARGUE)
    for c in ws[1]:
        c.font = Font(bold=True)
        c.fill = PatternFill("solid", fgColor="EEF2F6")
    for i, w in enumerate([14, 18, 11, 10, 52, 17, 13, 16], start=1):
        ws.column_dimensions[chr(64 + i)].width = w
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return StreamingResponse(
        buf, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="CARGUE REQUISICION.xlsx"'},
    )


@router.post("/importar-lineas")
async def importar_lineas(archivo: UploadFile = File(...), _: Usuario = Depends(require_not_consulta)):
    """Lee el Excel de cargue y devuelve las líneas listas para la requisición, con errores por fila."""
    import openpyxl
    contenido = await archivo.read()
    if len(contenido) > 5 * 1024 * 1024:
        raise HTTPException(status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail="El archivo supera 5 MB.")
    try:
        wb = openpyxl.load_workbook(io.BytesIO(contenido), read_only=True, data_only=True)
    except Exception:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="No se pudo leer el archivo. Usa el formato .xlsx descargado.")
    ws = wb.worksheets[0]
    filas = list(ws.iter_rows(values_only=True))
    hi = next((i for i, f in enumerate(filas) if f and any("centro" in str(c or "").lower() for c in f) and any("cantidad" in str(c or "").lower() for c in f)), None)
    if hi is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="No se encontraron los encabezados del formato de cargue.")
    col = {}
    for i, c in enumerate(filas[hi]):
        k = str(c or "").lower().strip()
        if k.startswith("codigo") or k.startswith("código"): col["codigo"] = i
        elif "centro" in k: col["cc"] = i
        elif k.startswith("cantidad"): col["cantidad"] = i
        elif k.startswith("unidad"): col["unidad"] = i
        elif "descrip" in k: col["descripcion"] = i
        elif "unitario" in k: col["precio"] = i
        elif k.startswith("descuento"): col["descuento"] = i
    faltan = [n for n in ("cc", "cantidad", "descripcion", "precio") if n not in col]
    if faltan:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=f"Faltan columnas obligatorias: {', '.join(faltan)}.")
    g = lambda f, k: (f[col[k]] if k in col and col[k] < len(f) else None)  # noqa: E731
    items, errores = [], []
    for n, f in enumerate(filas[hi + 1:], start=hi + 2):
        if not f or all(c in (None, "") for c in f):
            continue
        cc = str(g(f, "cc") or "").strip()
        if cc.endswith(".0"):
            cc = cc[:-2]
        desc = str(g(f, "descripcion") or "").strip()
        cant, precio, dto = _num(g(f, "cantidad")), _num(g(f, "precio")), _num(g(f, "descuento"))
        err = []
        if not desc: err.append("falta la descripción")
        if not cc: err.append("falta el centro de costo")
        if cant <= 0: err.append("cantidad inválida")
        if precio <= 0: err.append("precio unitario inválido")
        if err:
            errores.append({"fila": n, "errores": err})
            continue
        total = max(0.0, cant * precio - dto)
        items.append({"codigo": str(g(f, "codigo") or "").strip(), "centro_costo": cc, "cantidad": cant,
                      "unidad": str(g(f, "unidad") or "UND").strip() or "UND", "descripcion": desc,
                      "precio_unitario": precio, "descuento": dto, "total": total})
    return {"items": items, "total_lineas": len(items), "subtotal": sum(i["total"] for i in items), "errores": errores}


@router.put("/{id}", response_model=RequisicionResponse)
def corregir_y_reenviar(id: uuid.UUID, req_in: RequisicionReenvio, request: Request,
                        background: BackgroundTasks, db: Session = Depends(get_db_fin),
                        usuario: Usuario = Depends(require_not_consulta)):
    """Quien creó la requisición la corrige y la reenvía (solo si fue rechazada). Se reinician las aprobaciones."""
    req = cargar_con_bloqueo(db, Requisicion, id, req_in.version, "Requisición")
    if str(req.solicitante_id) != str(usuario.id):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo quien creó la requisición puede corregirla.")
    if req.estado not in (EstadoRequisicion.RECHAZADA_JEFE, EstadoRequisicion.RECHAZADA_FINANCIERA):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Solo se corrigen requisiciones rechazadas.")
    area, estado_inicial, jefe_id, texto = _resolver_area_y_flujo(req_in, usuario, db)
    anterior = req.estado
    for campo in ("lugar_entrega", "es_urgente", "motivo_urgencia", "proveedor_sugerido", "proveedor_nit", "proveedor_datos",
                  "datos_front", "observaciones", "subtotal", "iva", "total"):
        setattr(req, campo, getattr(req_in, campo))
    req.items = [item.model_dump(mode="json") for item in req_in.items]
    req.area = area
    for campo in ("jefe_aprobador_id", "fecha_aprobacion_jefe", "observacion_jefe", "compras_analista_id", "fecha_compras",
                  "aprobador_financiero_id", "rol_aprobador_financiero", "fecha_aprobacion_financiera", "observacion_financiera",
                  "pago_fecha", "pago_aprobado", "pago_decidido_por"):
        setattr(req, campo, None)
    req.pago_urgente = False
    req.estado = estado_inicial
    req.version += 1
    req.updated_by = usuario.username
    req.historial_cambios = [*(req.historial_cambios or []), {
        "fecha": datetime.now(timezone.utc).isoformat(), "usuario": usuario.nombre, "username": usuario.username, "accion": "REENVIAR",
        "estado_anterior": anterior.value, "estado_nuevo": estado_inicial.value, "observacion": f"Corregida y reenviada. {texto}"}]
    db.commit()
    db.refresh(req)
    AuditService.registrar(db=db, modulo="requisiciones", accion="REENVIAR", entidad_id=str(id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"consecutivo": req.consecutivo})
    if estado_inicial == EstadoRequisicion.PENDIENTE_JEFE:
        notificaciones.req_pendiente_jefe(background, db, jefe_id, req)
    else:
        notificaciones.req_pendiente_compras(background, db, req)
    return req


@router.get("/{id}", response_model=RequisicionResponse)
def obtener_requisicion(id: uuid.UUID, db: Session = Depends(get_db_fin), current_user: Usuario = Depends(get_usuario_fin),
                          __seccion=Depends(require_seccion("requisiciones"))):
    r = db.get(Requisicion, id)
    if not r or not visible(r, current_user):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Requisición no encontrada.")
    return r


MARCAS_PERMITIDAS = {"pagoCreado", "pagoCreadoEn"}


class MarcasReq(BaseModel):
    marcas: dict


@router.post("/{id}/marcas", response_model=RequisicionResponse)
def poner_marcas(id: uuid.UUID, req: MarcasReq, db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta)):
    """Banderas que el formulario guarda junto a la requisición (p. ej. que el pago urgente ya se creó). No cambian el estado."""
    r = db.get(Requisicion, id)
    if not r or not visible(r, usuario):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Requisición no encontrada.")
    if set(req.marcas) - MARCAS_PERMITIDAS:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, f"Marcas permitidas: {', '.join(sorted(MARCAS_PERMITIDAS))}")
    r.datos_front = {**(r.datos_front or {}), **req.marcas}
    r.version += 1
    db.commit()
    db.refresh(r)
    return r
