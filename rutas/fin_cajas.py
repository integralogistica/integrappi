"""Cajas menores: circuito completo del FORM-FIN-007.

# puerto del circuito de views-cajas-menores.js del módulo externo (~3.400
# líneas): el responsable arma los renglones (EN_EDICION) → radica (consecutivo
# CM-AAAA-NNN + política semanal martes/viernes) → jefe inmediato → Controller
# → Contabilidad registra el comprobante FC (asiento) → Tesorería cruza la
# cuenta 110510 (facturas y cuentas de cobro) → Tesorería registra el egreso
# del reembolso. Los PDFs de circuito (FC, cruce, auxiliar, reembolso) van
# como adjuntos GCS tipados; los soportes por renglón como 'soporte_caja'.
"""
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from financiero.database import get_db_fin
from financiero.dependencies import get_usuario_fin, require_not_consulta, require_seccion
from financiero.models.catalogo import SedeCajaMenor
from financiero.models.contabilidad import CajaMenor, FacturaDian, EstadoFactura, MovimientoContable
from financiero.models.usuario import RolUsuario, Usuario
from financiero.services import notificaciones
from financiero.services.audit_service import AuditService
from financiero.services.consecutivo_service import ConsecutivoService
from financiero.services.locking import cargar_con_bloqueo
from financiero.services.state_machines import StateMachineService
from rutas.fin_traslados import get_client_ip

router = APIRouter(prefix="/fin/cajas", tags=["Financiero · Cajas menores"])

DIAS_SEMANA = ['Domingo', 'Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado']
ROLES_GLOBALES_CM = {RolUsuario.TESORERIA, RolUsuario.CONTABILIDAD, RolUsuario.CONTROLLER,
                     RolUsuario.GERENTE_FINANCIERO, RolUsuario.PRESIDENCIA, RolUsuario.ADMIN}
TIPOS_DOC_CM = {"recibo_caja", "cuenta_cobro", "factura_electronica"}


def _es_admin(u: Usuario) -> bool:
    return u.rol == RolUsuario.ADMIN or u.es_admin_adicional


def _evento(usuario: Usuario, etapa: str, obs: str = "") -> dict:
    return {"fecha": datetime.now(timezone.utc).isoformat(), "usuario": usuario.nombre,
            "username": usuario.username, "etapa": etapa, "observacion": obs}


def _requiere_cruce(m: dict) -> bool:
    return m.get("tipo_documento") in ("cuenta_cobro", "factura_electronica")


def _total_reembolso(caja: CajaMenor) -> Decimal:
    """Suma de los renglones NO excluidos de la causación (checkbox Contabilidad)."""
    return sum((Decimal(str(m.get("valor") or 0)) for m in (caja.movimientos or [])
                if m.get("contabilizado") is not False), Decimal("0"))


def _politica_semanal(fecha_envio: date) -> dict:
    """A tiempo solo Lunes/Martes; el reembolso llega el viernes (o el siguiente)."""
    dia = fecha_envio.weekday()  # 0=Lun
    a_tiempo = dia in (0, 1)
    viernes = fecha_envio.toordinal() + ((4 - dia) % 7 if a_tiempo else (4 - dia) % 7 + 7)
    objetivo = date.fromordinal(viernes)
    return {
        "fechaEnvio": fecha_envio.isoformat(),
        "diaSemanaEnvio": DIAS_SEMANA[dia],
        "aTiempoParaViernes": a_tiempo,
        "fechaEstimadaReembolso": objetivo.isoformat(),
    }


def _sede_de(caja: CajaMenor, db: Session) -> Optional[SedeCajaMenor]:
    return db.scalar(select(SedeCajaMenor).where(SedeCajaMenor.nombre == caja.sede))


def _dict(c: CajaMenor) -> dict:
    return {
        "id": str(c.id), "consecutivo": c.consecutivo, "sede": c.sede, "responsable": c.responsable,
        "responsable_cc": c.responsable_cc, "centro_costo_principal": c.centro_costo_principal,
        "periodo": c.periodo, "saldo_inicial": float(c.saldo_inicial),
        "total_gastos": float(sum((Decimal(str(m.get("valor") or 0)) for m in (c.movimientos or [])), Decimal("0"))),
        "total_reembolso": float(_total_reembolso(c)),
        "estado": c.estado, "movimientos": c.movimientos or [], "datos": c.datos or {},
        "politica": c.politica or {}, "version": c.version,
        "created_at": c.created_at.isoformat() if c.created_at else None,
        "updated_at": c.updated_at.isoformat() if c.updated_at else None,
    }


def _sede_exclusiva(u: Usuario, db: Session) -> Optional[str]:
    """Los roles globales ven todas las sedes; el resto solo la suya."""
    if _es_admin(u) or u.rol in ROLES_GLOBALES_CM:
        return None
    if u.sede_cm:
        return u.sede_cm
    # Heurística del original: el nombre del usuario contra el responsable de la sede
    nombre = (u.nombre or "").upper()
    for s in db.scalars(select(SedeCajaMenor).where(SedeCajaMenor.activo.is_(True))).all():
        resp = (s.responsable or "").upper()
        if resp and (resp in nombre or nombre in resp):
            return s.nombre
        for token in (s.nombre, (s.etiqueta or "").split("(")[0].strip()):
            if token and token.upper() in nombre:
                return s.nombre
    return None


@router.get("")
def listar_cajas(
    periodo: Optional[str] = None,
    sede: Optional[str] = None,
    estado: Optional[str] = None,
    semaforo: Optional[str] = None,   # atiempo | extemporanea
    q: Optional[str] = None,
    db: Session = Depends(get_db_fin),
    usuario: Usuario = Depends(get_usuario_fin),
    __seccion=Depends(require_seccion("cajas")),
):
    stmt = select(CajaMenor)
    if periodo:
        stmt = stmt.where(CajaMenor.periodo == periodo)
    if estado:
        stmt = stmt.where(CajaMenor.estado == estado.upper())
    if sede:
        stmt = stmt.where(CajaMenor.sede.ilike(f"%{sede.strip()}%"))
    exclusiva = _sede_exclusiva(usuario, db)
    if exclusiva:
        stmt = stmt.where(CajaMenor.sede == exclusiva)
    cajas = db.scalars(stmt.order_by(CajaMenor.created_at.desc()).limit(500)).all()
    if semaforo:
        cajas = [c for c in cajas if (c.politica or {}).get("aTiempoParaViernes") == (semaforo == "atiempo")]
    if q:
        needle = q.strip().lower()
        cajas = [c for c in cajas if needle in f"{c.consecutivo} {c.sede} {c.responsable} {c.periodo}".lower()]
    return [_dict(c) for c in cajas]


@router.get("/kpis")
def kpis_cajas(db: Session = Depends(get_db_fin), usuario: Usuario = Depends(get_usuario_fin),
               __seccion=Depends(require_seccion("cajas"))):
    """Las 6 tarjetas del pipeline del original."""
    exclusiva = _sede_exclusiva(usuario, db)
    stmt = select(CajaMenor)
    if exclusiva:
        stmt = stmt.where(CajaMenor.sede == exclusiva)
    pool = db.scalars(stmt).all()

    def en_legalizacion(c: CajaMenor) -> bool:
        return c.estado in ("APROBADA_CONTROLLER", "CONTABILIZADA", "CRUZADA")

    activas = [c for c in pool if c.estado not in ("REEMBOLSADA", "RECHAZADA")]
    por_cruce = [c for c in pool if en_legalizacion(c) and (c.datos or {}).get("no_comprobante_fc")
                 and any(_requiere_cruce(m) for m in (c.movimientos or [])) and not (c.datos or {}).get("cruzado_en")]
    return {
        "total_en_proceso": len(activas),
        "monto_en_proceso": float(sum((_total_reembolso(c) for c in activas), Decimal("0"))),
        "por_jefe": sum(1 for c in pool if c.estado == "RADICADA"),
        "por_controller": sum(1 for c in pool if c.estado == "APROBADA_JEFE"),
        "por_contabilizar": sum(1 for c in pool if en_legalizacion(c) and not (c.datos or {}).get("no_comprobante_fc")),
        "por_cruce": len(por_cruce),
        "por_egreso": sum(1 for c in pool if c.estado == "CRUZADA"),
    }


@router.get("/{id}")
def obtener_caja(id: uuid.UUID, db: Session = Depends(get_db_fin), _: Usuario = Depends(get_usuario_fin),
                  __seccion=Depends(require_seccion("cajas"))):
    c = db.get(CajaMenor, id)
    if not c:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Caja menor no encontrada.")
    return _dict(c)


class MovimientoCaja(BaseModel):
    fecha: Optional[str] = None              # fecha del documento
    concepto: str = Field(..., min_length=2)
    valor: Decimal = Field(gt=0)
    tipo: str = "gasto"                      # gasto | reposicion
    centro_costo: Optional[str] = None
    nit: Optional[str] = None
    proveedor: Optional[str] = None
    tipo_documento: Optional[str] = None     # recibo_caja | cuenta_cobro | factura_electronica
    no_factura: Optional[str] = None
    folio: Optional[int] = None
    invoice_id: Optional[str] = None         # factura DIAN vinculada
    contabilizado: Optional[bool] = None     # tri-state del checkbox Contabilidad
    cruzado: Optional[bool] = None           # tri-state del checkbox Tesorería
    datos: Optional[dict] = None             # pdf_adjunto (referencia GCS), etc.


class CajaPayload(BaseModel):
    sede: str = Field(..., min_length=2)
    responsable: str = Field(..., min_length=2)
    periodo: str = Field(..., pattern=r"^\d{4}-\d{2}$")
    responsable_cc: Optional[str] = None
    centro_costo_principal: Optional[str] = None
    saldo_inicial: Decimal = Field(ge=0, default=Decimal("0"))
    movimientos: list[MovimientoCaja] = Field(default_factory=list)
    datos: Optional[dict] = None


def _aplicar_payload(caja: CajaMenor | None, payload: CajaPayload, usuario: Usuario, db: Session) -> CajaMenor:
    c = caja or CajaMenor()
    sede = db.scalar(select(SedeCajaMenor).where(SedeCajaMenor.nombre == payload.sede.strip()))
    c.sede = payload.sede.strip()[:120]
    c.sede_id = sede.id if sede else None
    if sede and not payload.responsable_cc:
        c.responsable_cc = sede.identificacion
    else:
        c.responsable_cc = payload.responsable_cc
    if sede and not payload.centro_costo_principal:
        c.centro_costo_principal = sede.cco
    else:
        c.centro_costo_principal = payload.centro_costo_principal
    c.responsable = payload.responsable.strip()[:150]
    c.periodo = payload.periodo
    c.saldo_inicial = payload.saldo_inicial
    # renglones con folio renumerado (puerto del original)
    movs = []
    for i, m in enumerate(payload.movimientos, start=1):
        d = m.model_dump(mode="json")
        d["folio"] = i
        if d.get("tipo_documento") and d["tipo_documento"] not in TIPOS_DOC_CM:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, f"tipo_documento no válido: {d['tipo_documento']}.")
        movs.append(d)
    c.movimientos = movs
    c.total_reembolso = sum((Decimal(str(m.get("valor") or 0)) for m in movs), Decimal("0"))
    if payload.datos is not None:
        c.datos = payload.datos
    c.updated_by = usuario.username
    return c


@router.post("")
def crear_caja(payload: CajaPayload, request: Request, db: Session = Depends(get_db_fin),
               usuario: Usuario = Depends(require_not_consulta)):
    if usuario.rol == RolUsuario.CONTABILIDAD and not _es_admin(usuario):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Contabilidad no radica cajas menores: las arma el responsable de la sede.")
    dup = db.scalar(select(CajaMenor).where(CajaMenor.sede == payload.sede.strip(),
                                            CajaMenor.periodo == payload.periodo))
    if dup:
        raise HTTPException(status.HTTP_409_CONFLICT, f"Ya existe la caja de {payload.sede} para {payload.periodo}.")
    c = _aplicar_payload(None, payload, usuario, db)
    c.estado = "EN_EDICION"
    c.created_by = usuario.username
    db.add(c)
    db.commit()
    db.refresh(c)
    AuditService.registrar(db=db, modulo="cajas", accion="CREAR", entidad_id=str(c.id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"sede": c.sede, "periodo": c.periodo})
    return _dict(c)


@router.put("/{id}")
def guardar_caja(id: uuid.UUID, payload: CajaPayload, request: Request,
                 db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta)):
    c = cargar_con_bloqueo(db, CajaMenor, id, None, "Caja menor")
    if c.estado != "EN_EDICION" and not _es_admin(usuario):
        raise HTTPException(status.HTTP_400_BAD_REQUEST,
                            f"La caja está {c.estado}: solo se edita en EN_EDICION (el admin puede reabrir).")
    _aplicar_payload(c, payload, usuario, db)
    c.version += 1
    db.commit()
    db.refresh(c)
    AuditService.registrar(db=db, modulo="cajas", accion="EDITAR", entidad_id=str(c.id), usuario=usuario,
                           ip_cliente=get_client_ip(request),
                           detalles={"sede": c.sede, "movimientos": len(c.movimientos or [])})
    return _dict(c)


@router.post("/{id}/radicar")
def radicar_caja(id: uuid.UUID, request: Request, background: BackgroundTasks,
                 db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta)):
    """Pasa el borrador al circuito: consecutivo CM-AAAA-NNN + política semanal."""
    c = cargar_con_bloqueo(db, CajaMenor, id, None, "Caja menor")
    StateMachineService.validar_transicion_caja(c, "RADICADA", usuario)
    if not c.movimientos:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "Agrega al menos un renglón antes de radicar.")
    ahora = datetime.now(timezone.utc)
    c.consecutivo = ConsecutivoService.generar_consecutivo(db, "cm")
    c.estado = "RADICADA"
    c.politica = _politica_semanal(ahora.date())
    c.version += 1
    c.updated_by = usuario.username
    c.datos = {**(c.datos or {}), "radicado_por": usuario.nombre, "radicado_en": ahora.isoformat(),
               "historial": [*((c.datos or {}).get("historial") or []),
                             _evento(usuario, "Radicada", f"Radicación de {len(c.movimientos)} folios por ${_total_reembolso(c):,.0f}")]}
    db.commit()
    db.refresh(c)
    AuditService.registrar(db=db, modulo="cajas", accion="RADICAR", entidad_id=str(c.id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"consecutivo": c.consecutivo, "sede": c.sede})
    sede = _sede_de(c, db)
    jefe = sede.jefe_inmediato if sede else None
    notificaciones.caja_radicada(background, db, c, jefe)
    return _dict(c)


class TransicionCaja(BaseModel):
    accion: str  # APROBAR_JEFE | APROBAR_CONTROLLER | CONTABILIZAR | CRUZAR | EGRESAR | RECHAZAR | REABRIR
    version: Optional[int] = None
    motivo: Optional[str] = None
    no_comprobante_fc: Optional[str] = None
    bg_ef: Optional[str] = None                 # nº de comprobante BG-EF del cruce (opcional)
    subcuenta: Optional[str] = None             # subcuenta 110510xx de la sede
    no_egreso_banco: Optional[str] = None
    banco: Optional[str] = None                 # Bancolombia | Banco de Bogotá | Davivienda | Occidente


@router.post("/{id}/transicion")
def transicion_caja(id: uuid.UUID, body: TransicionCaja, request: Request, background: BackgroundTasks,
                    db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta)):
    c = cargar_con_bloqueo(db, CajaMenor, id, body.version, "Caja menor")
    accion = body.accion.upper()
    ahora = datetime.now(timezone.utc)
    anterior = c.estado
    sede = _sede_de(c, db)
    datos = c.datos or {}

    destinos = {
        "APROBAR_JEFE": "APROBADA_JEFE", "APROBAR_CONTROLLER": "APROBADA_CONTROLLER",
        "CONTABILIZAR": "CONTABILIZADA", "CRUZAR": "CRUZADA", "EGRESAR": "REEMBOLSADA",
        "RECHAZAR": "RECHAZADA", "REABRIR": "EN_EDICION",
    }
    nuevo_estado = destinos.get(accion)
    if nuevo_estado is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Acción desconocida.")
    if accion == "REABRIR" and not _es_admin(usuario):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo el administrador reabre cajas.")

    # Datos que acompañan cada paso
    if accion == "CONTABILIZAR":
        if not (body.no_comprobante_fc or "").strip():
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "Indica el número del comprobante FC.")
        datos = {**datos, "no_comprobante_fc": body.no_comprobante_fc.strip(),
                 "contabilizado_por": usuario.nombre, "contabilizado_en": ahora.isoformat()}
    elif accion == "CRUZAR":
        if not datos.get("contabilizado_en"):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Contabilidad debe causar primero (falta el comprobante FC).")
        datos = {**datos, "bg_ef": (body.bg_ef or "").strip() or datos.get("bg_ef"),
                 "subcuenta": body.subcuenta or (sede.cuenta if sede else None) or datos.get("subcuenta"),
                 "cruzado_por": usuario.nombre, "cruzado_en": ahora.isoformat()}
    elif accion == "EGRESAR":
        if not (body.no_egreso_banco or "").strip():
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "Indica el número del egreso bancario.")
        datos = {**datos, "no_egreso_banco": body.no_egreso_banco.strip(), "banco_pago": body.banco or "Bancolombia",
                 "reembolsado_por": usuario.nombre, "reembolsado_en": ahora.isoformat()}
    elif accion == "APROBAR_JEFE":
        datos = {**datos, "aprobado_jefe_por": usuario.nombre, "aprobado_jefe_en": ahora.isoformat()}
    elif accion == "APROBAR_CONTROLLER":
        datos = {**datos, "aprobado_controller_por": usuario.nombre, "aprobado_controller_en": ahora.isoformat()}
    elif accion == "RECHAZAR":
        if not (body.motivo or "").strip():
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "Escribe el motivo de la devolución.")

    # La máquina de estados lee caja.datos (FC/egreso ya registrados): se asigna antes de validar
    c.datos = datos
    StateMachineService.validar_transicion_caja(c, nuevo_estado, usuario, jefe_nombre=sede.jefe_inmediato if sede else None)

    etapa_obs = {
        "APROBAR_JEFE": "Aprobado por Jefe Inmediato (1ª aprobación)",
        "APROBAR_CONTROLLER": "Confirmada aprobación por Controller",
        "CONTABILIZAR": f"Causación de recibos/CxC bajo comprobante FC {datos.get('no_comprobante_fc')}",
        "CRUZAR": f"Cruce de facturas y cuentas de cobro en cuenta 110510 (BG-EF: {datos.get('bg_ef') or '—'})",
        "EGRESAR": f"Reembolso dispersado desde {datos.get('banco_pago')} por ${_total_reembolso(c):,.0f} (#{datos.get('no_egreso_banco')})",
        "RECHAZAR": body.motivo or "",
        "REABRIR": "Reabierta por el administrador",
    }
    etapa_nombre = {"APROBAR_JEFE": "Aprobación Jefe Inmediato", "APROBAR_CONTROLLER": "Aprobación Controller",
                    "CONTABILIZAR": "Contabilizada", "CRUZAR": "Cruce Tesorería", "EGRESAR": "Reembolsada",
                    "RECHAZAR": "Rechazada", "REABRIR": "Reabierta"}[accion]

    # Asiento solo con los renglones causados; auto-avance del original
    if accion == "CONTABILIZAR":
        _generar_asiento(db, c, usuario)
        tiene_cruce = any(_requiere_cruce(m) and m.get("cruzado") is not False for m in (c.movimientos or []))
        nuevo_estado = "CONTABILIZADA" if tiene_cruce else "CRUZADA"  # sin FE/CxC se considera lista para egreso

    c.estado = nuevo_estado
    c.version += 1
    c.updated_by = usuario.username
    datos["historial"] = [*(datos.get("historial") or []), _evento(usuario, etapa_nombre, etapa_obs[accion])]
    c.datos = datos
    db.commit()
    db.refresh(c)
    AuditService.registrar(db=db, modulo="cajas", accion=accion, entidad_id=str(c.id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"consecutivo": c.consecutivo, "estado": c.estado})

    if accion == "APROBAR_JEFE":
        notificaciones.caja_aprobada_jefe(background, db, c)
    elif accion == "APROBAR_CONTROLLER":
        notificaciones.caja_aprobada_controller(background, db, c)
    elif accion == "CONTABILIZAR":
        notificaciones.caja_contabilizada(background, db, c)
    elif accion in ("CRUZAR", "EGRESAR", "RECHAZAR"):
        notificaciones.caja_aviso_responsable(background, db, c, etapa_nombre)
    return _dict(c)


class ItemPatch(BaseModel):
    folio: int
    contabilizado: Optional[bool] = None      # checkbox Contabilidad (FC)
    cruzado: Optional[bool] = None            # checkbox Tesorería (110510)


@router.patch("/{id}/items")
def parchear_item(id: uuid.UUID, body: ItemPatch, request: Request,
                  db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta)):
    """Checkboxes por folio del original: Contabilidad excluye de la causación
    (recalcula total_reembolso); Tesorería marca el cruce (no recalcula)."""
    c = cargar_con_bloqueo(db, CajaMenor, id, None, "Caja menor")
    # copia profunda: mutar los dicts internos NO dispara el UPDATE de JSONB
    import copy
    movs = copy.deepcopy(c.movimientos or [])
    idx = next((i for i, m in enumerate(movs) if m.get("folio") == body.folio), None)
    if idx is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"No existe el folio {body.folio}.")
    ya_causada = bool((c.datos or {}).get("contabilizado_en"))

    if body.contabilizado is not None:
        if usuario.rol != RolUsuario.CONTABILIDAD and not _es_admin(usuario):
            raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo Contabilidad excluye folios de la causación.")
        movs[idx]["contabilizado"] = body.contabilizado
    if body.cruzado is not None:
        if usuario.rol != RolUsuario.TESORERIA and not _es_admin(usuario):
            raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo Tesorería marca el cruce de folios.")
        if not ya_causada:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Espera el comprobante FC de Contabilidad.")
        movs[idx]["cruzado"] = body.cruzado
    if body.contabilizado is None and body.cruzado is None:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "Nada que cambiar.")

    c.movimientos = movs
    c.total_reembolso = _total_reembolso(c)
    c.version += 1
    c.updated_by = usuario.username
    etiqueta = f"Folio {body.folio}"
    if body.contabilizado is not None:
        etiqueta += f" {'incluido en' if body.contabilizado else 'EXCLUIDO de'} la causación"
    if body.cruzado is not None:
        etiqueta += f" · {'cruzado' if body.cruzado else 'NO cruzado'} en 110510"
    c.datos = {**(c.datos or {}), "historial": [*((c.datos or {}).get("historial") or []),
                                                _evento(usuario, "Folio", etiqueta)]}
    db.commit()
    db.refresh(c)
    AuditService.registrar(db=db, modulo="cajas", accion="ITEM", entidad_id=str(c.id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"folio": body.folio, "cambio": etiqueta})
    return _dict(c)


class VinculoFactura(BaseModel):
    folio: int                                    # renglón destino (se convierte en FE)
    factura_id: str


@router.post("/{id}/vincular-factura")
def vincular_factura(id: uuid.UUID, body: VinculoFactura, request: Request,
                     db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta)):
    """Convierte el renglón en factura electrónica vinculada a la factura DIAN
    (que debe estar en LEGALIZACION) y la pasa a TRAMITADA con origen caja menor."""
    c = cargar_con_bloqueo(db, CajaMenor, id, None, "Caja menor")
    if c.estado not in ("EN_EDICION", "RECHAZADA", "RADICADA"):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "La caja ya está en el circuito: no se vinculan facturas.")
    try:
        factura_uuid = uuid.UUID(body.factura_id)
    except ValueError:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "factura_id no válido.")
    f = db.get(FacturaDian, factura_uuid)
    if not f or f.estado != EstadoFactura.LEGALIZACION:
        raise HTTPException(status.HTTP_400_BAD_REQUEST,
                            "Solo se vinculan facturas electrónicas en estado de legalización.")
    movs = list(c.movimientos or [])
    idx = next((i for i, m in enumerate(movs) if m.get("folio") == body.folio), None)
    if idx is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"No existe el folio {body.folio}.")
    movs[idx] = {**movs[idx], "tipo_documento": "factura_electronica", "invoice_id": str(f.id),
                 "valor": float(f.total), "nit": f.nit_proveedor, "proveedor": f.proveedor,
                 "no_factura": f.numero_factura, "concepto": f"Factura {f.numero_factura} ({f.proveedor})"}
    c.movimientos = movs
    c.total_reembolso = _total_reembolso(c)
    c.version += 1
    c.updated_by = usuario.username
    c.datos = {**(c.datos or {}), "vinculadas": [*((c.datos or {}).get("vinculadas") or []), str(f.id)]}
    # La factura viaja con la caja
    f.estado = EstadoFactura.TRAMITADA
    f.origen_doc = f"Caja Menor: {c.sede}"
    f.caja_menor_sede = c.sede
    f.historial_cambios = [*(f.historial_cambios or []),
                           {"fecha": datetime.now(timezone.utc).isoformat(), "usuario": usuario.nombre,
                            "accion": "VINCULAR_CAJA", "observacion": f"Legalizada en Caja Menor {c.consecutivo or c.periodo} ({c.sede})"}]
    db.commit()
    db.refresh(c)
    AuditService.registrar(db=db, modulo="cajas", accion="VINCULAR_FACTURA", entidad_id=str(c.id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"factura": f.numero_factura, "folio": body.folio})
    return _dict(c)


def _desvincular_facturas_de_caja(c: CajaMenor, db: Session):
    """Al eliminar/anular la caja, las facturas FE vuelven a LEGALIZACION."""
    for m in (c.movimientos or []):
        inv = m.get("invoice_id")
        if not inv:
            continue
        try:
            f = db.get(FacturaDian, uuid.UUID(str(inv)))
        except ValueError:
            continue
        if f and f.estado == EstadoFactura.TRAMITADA and m.get("tipo_documento") == "factura_electronica":
            f.estado = EstadoFactura.LEGALIZACION
            f.historial_cambios = [*(f.historial_cambios or []),
                                   {"fecha": datetime.now(timezone.utc).isoformat(), "usuario": "sistema",
                                    "accion": "DESVINCULAR_CAJA", "observacion": "La caja menor fue eliminada/anulada"}]


@router.delete("/{id}")
def eliminar_caja(id: uuid.UUID, request: Request, db: Session = Depends(get_db_fin),
                  usuario: Usuario = Depends(require_not_consulta)):
    if not _es_admin(usuario):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo el administrador elimina cajas.")
    c = db.get(CajaMenor, id)
    if not c:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Caja menor no encontrada.")
    if (c.datos or {}).get("contabilizado_en"):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "No se elimina una caja contabilizada (generó asiento).")
    _desvincular_facturas_de_caja(c, db)
    db.delete(c)
    db.commit()
    AuditService.registrar(db=db, modulo="cajas", accion="ELIMINAR", entidad_id=str(id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"sede": getattr(c, 'sede', '?')})
    return {"mensaje": "Caja eliminada."}


def _generar_asiento(db: Session, c: CajaMenor, usuario: Usuario):
    """Asiento de la caja: gasto por renglón CAUSADO (débito) contra caja menor
    (crédito) por el total aprobado — cuadra exacto."""
    periodo = c.periodo
    causados = [m for m in (c.movimientos or []) if m.get("contabilizado") is not False]
    for m in causados:
        db.add(MovimientoContable(
            origen="caja", referencia_id=c.id,
            referencia=f"{c.consecutivo or c.sede} {periodo}", periodo=periodo,
            cuenta="GASTOCAJAMENOR", tercero=m.get("proveedor") or c.sede,
            debito=Decimal(str(m.get("valor") or 0)), credito=Decimal("0"),
            detalle={"concepto": m.get("concepto"), "centro_costo": m.get("centro_costo"),
                     "tipo": m.get("tipo", "gasto"), "folio": m.get("folio")},
            creado_por_id=usuario.id,
        ))
    db.add(MovimientoContable(
        origen="caja", referencia_id=c.id, referencia=f"{c.consecutivo or c.sede} {periodo}", periodo=periodo,
        cuenta="CAJAMENOR", tercero=c.sede, debito=Decimal("0"), credito=_total_reembolso(c),
        detalle={"sede": c.sede, "renglones": len(causados), "fc": (c.datos or {}).get("no_comprobante_fc")},
        creado_por_id=usuario.id,
    ))
