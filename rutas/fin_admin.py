"""Administración de registros del Módulo Financiero: edición de campos,
reapertura y eliminación con snapshot.

# puerto de app/routers/admin_registros.py del módulo externo (configuración
declarativa por módulo).
"""
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel
from sqlalchemy.orm import Session

from financiero.database import get_db_fin
from financiero.dependencies import require_admin
from financiero.models.requisicion import EstadoRequisicion, Requisicion
from financiero.models.traslado import EstadoTraslado, Traslado
from financiero.models.usuario import Usuario
from financiero.models.viaje import EstadoViaje, Viaje
from financiero.services.audit_service import AuditService
from rutas.fin_traslados import get_client_ip

router = APIRouter(prefix="/fin/admin", tags=["Financiero · Administración de registros"])

MODULOS = {
    "requisiciones": {
        "modelo": Requisicion,
        "estado_inicial": EstadoRequisicion.PENDIENTE_JEFE,
        "reabrir_desde": (EstadoRequisicion.ANULADA, EstadoRequisicion.RECHAZADA_JEFE, EstadoRequisicion.RECHAZADA_FINANCIERA),
        "limpiar": ("jefe_aprobador_id", "fecha_aprobacion_jefe", "observacion_jefe", "compras_analista_id",
                    "fecha_compras", "aprobador_financiero_id", "rol_aprobador_financiero",
                    "fecha_aprobacion_financiera", "observacion_financiera", "pago_fecha", "pago_aprobado",
                    "pago_decidido_por", "entregado_en", "entregado_por_id", "recibido_por_id", "fecha_recibido"),
        "editables": ("lugar_entrega", "es_urgente", "motivo_urgencia", "proveedor_sugerido", "proveedor_nit",
                      "observaciones", "subtotal", "iva", "total", "items", "proveedor_datos", "datos_front"),
    },
    "traslados": {
        "modelo": Traslado,
        "estado_inicial": EstadoTraslado.PENDIENTE_PRESIDENCIA,
        "reabrir_desde": (EstadoTraslado.ANULADO, EstadoTraslado.RECHAZADO),
        "limpiar": ("aprobador_id", "fecha_aprobacion", "observacion_aprobacion", "motivo_rechazo", "aprobacion"),
        "editables": ("area", "motivo", "observaciones", "lineas", "total", "datos_front"),
    },
    "viajes": {
        "modelo": Viaje,
        "estado_inicial": EstadoViaje.SOLICITADO,
        "reabrir_desde": (EstadoViaje.ANULADO, EstadoViaje.RECHAZADO),
        "limpiar": ("autorizador_id", "fecha_autorizacion", "observacion_autorizacion", "categoria_tarifa",
                    "presupuesto_items", "total_presupuesto", "presupuesto_detalle", "tiquetes_detalle",
                    "anticipo_detalle", "anticipo_pagado", "fecha_pago_anticipo", "legalizacion_detalle"),
        "editables": ("viajero_nombre", "viajero_cedula", "viajero_cargo", "objetivo", "observaciones",
                      "origen", "destino", "fecha_inicio", "fecha_fin", "requiere_tiquetes", "datos_front"),
    },
}


def _obtener_modulo(nombre: str) -> dict:
    if nombre not in MODULOS:
        raise HTTPException(status.HTTP_404_NOT_FOUND,
                            f"Módulo desconocido. Permitidos: {', '.join(MODULOS)}")
    return MODULOS[nombre]


def _coaccionar(valor, campo: str):
    v = valor
    if isinstance(v, str):
        if campo in ("subtotal", "iva", "total", "total_presupuesto"):
            try:
                return Decimal(v)
            except Exception:
                raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, f"'{campo}' debe ser numérico.")
        if campo in ("fecha_inicio", "fecha_fin"):
            try:
                return datetime.fromisoformat(v.replace("Z", "+00:00"))
            except ValueError:
                raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, f"'{campo}' debe ser fecha ISO.")
        if campo in ("es_urgente", "requiere_tiquetes", "anticipo_pagado"):
            return v.lower() in ("true", "1", "si", "sí")
    return v


class EdicionAdmin(BaseModel):
    campos: dict
    version: Optional[int] = None
    motivo: str


@router.patch("/{modulo}/{id}")
def editar_registro(modulo: str, id: uuid.UUID, req: EdicionAdmin, request: Request,
                    db: Session = Depends(get_db_fin), admin: Usuario = Depends(require_admin)):
    cfg = _obtener_modulo(modulo)
    obj = db.get(cfg["modelo"], id)
    if not obj:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Registro no encontrado.")
    if req.version is not None and obj.version != req.version:
        raise HTTPException(status.HTTP_409_CONFLICT, "El registro cambió: recarga y reintenta.")
    invalidos = set(req.campos) - set(cfg["editables"])
    if invalidos:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            f"Campos no editables: {', '.join(sorted(invalidos))}. "
                            f"Permitidos: {', '.join(cfg['editables'])}")
    antes = {c: getattr(obj, c) for c in req.campos}
    for campo, valor in req.campos.items():
        setattr(obj, campo, _coaccionar(valor, campo))
    obj.version += 1
    obj.updated_by = admin.username
    db.commit()
    db.refresh(obj)
    AuditService.registrar(db=db, modulo=modulo, accion="EDICION_ADMIN", entidad_id=str(id), usuario=admin,
                           ip_cliente=get_client_ip(request),
                           detalles={"motivo": req.motivo,
                                     "antes": {k: str(v)[:200] for k, v in antes.items()},
                                     "despues": {k: str(req.campos[k])[:200] for k in req.campos}})
    return {"mensaje": "Registro actualizado.", "version": obj.version}


@router.post("/{modulo}/{id}/reabrir")
def reabrir(modulo: str, id: uuid.UUID, request: Request,
            motivo: str = Query(..., min_length=5),
            db: Session = Depends(get_db_fin), admin: Usuario = Depends(require_admin)):
    cfg = _obtener_modulo(modulo)
    obj = db.get(cfg["modelo"], id)
    if not obj:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Registro no encontrado.")
    if obj.estado not in [e.value for e in cfg["reabrir_desde"]]:
        raise HTTPException(status.HTTP_400_BAD_REQUEST,
                            f"Solo se reabren registros en {[e.value for e in cfg['reabrir_desde']]} "
                            f"(está en '{obj.estado}').")
    if modulo == "requisiciones" and getattr(obj, "oc_numero", None):
        raise HTTPException(status.HTTP_409_CONFLICT,
                            "La requisición ya generó orden de compra: no se puede reabrir.")
    anterior = obj.estado
    for campo in cfg["limpiar"]:
        setattr(obj, campo, None)
    obj.estado = cfg["estado_inicial"]
    obj.version += 1
    obj.updated_by = admin.username
    hist = list(getattr(obj, "historial_cambios", None) or [])
    hist.append({"fecha": datetime.now(timezone.utc).isoformat(), "usuario": admin.nombre,
                 "username": admin.username, "accion": "REABRIR", "estado_anterior": anterior,
                 "estado_nuevo": obj.estado.value if hasattr(obj.estado, "value") else obj.estado,
                 "observacion": motivo})
    if hasattr(obj, "historial_cambios"):
        obj.historial_cambios = hist
    db.commit()
    AuditService.registrar(db=db, modulo=modulo, accion="REABRIR", entidad_id=str(id), usuario=admin,
                           ip_cliente=get_client_ip(request), detalles={"motivo": motivo})
    return {"mensaje": "Registro reabierto.", "estado": obj.estado.value if hasattr(obj.estado, "value") else obj.estado}


@router.delete("/{modulo}/{id}")
def eliminar(modulo: str, id: uuid.UUID, request: Request,
             motivo: str = Query(..., min_length=5),
             db: Session = Depends(get_db_fin), admin: Usuario = Depends(require_admin)):
    """Borra el registro dejando COPIA COMPLETA en la bitácora."""
    cfg = _obtener_modulo(modulo)
    obj = db.get(cfg["modelo"], id)
    if not obj:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Registro no encontrado.")
    snapshot = {c.name: str(getattr(obj, c.name))[:500]
                for c in cfg["modelo"].__table__.columns}
    db.delete(obj)
    db.commit()
    AuditService.registrar(db=db, modulo=modulo, accion="ELIMINAR", entidad_id=str(id), usuario=admin,
                           ip_cliente=get_client_ip(request), detalles={"motivo": motivo, "snapshot": snapshot})
    return {"mensaje": "Registro eliminado (copia en bitácora)."}


# ── Respaldo del schema fin ──────────────────────────────────────────────
# # puerto simplificado de app/routers/respaldo.py: descarga JSON completa y
# restauración SOLO en modo simulación (análisis del diff; no escribe).

router_respaldo = APIRouter(prefix="/fin/respaldo", tags=["Financiero · Respaldo"],
                            dependencies=[Depends(require_admin)])


def _serializar_fila(obj) -> dict:
    out = {}
    for c in obj.__table__.columns:
        v = getattr(obj, c.name)
        if hasattr(v, "isoformat"):
            v = v.isoformat()
        elif isinstance(v, (Decimal, uuid.UUID)) or hasattr(v, "value"):
            v = str(v)
        out[c.name] = v
    return out


@router_respaldo.get("")
def descargar_respaldo(request: Request, db: Session = Depends(get_db_fin),
                       admin: Usuario = Depends(require_admin)):
    from financiero.models.auditoria import RegistroAuditoria
    from financiero.models.configuracion import Configuracion
    from financiero.models.contabilidad import (
        CajaMenor, FacturaDian, MovimientoContable, PagoUrgente, ServicioPublico,
    )
    from financiero.models.consecutivo import Consecutivo

    datos = {
        "formato": 1,
        "generado": datetime.now(timezone.utc).isoformat(),
        "por": admin.username,
        "usuarios": [_serializar_fila(u) for u in db.query(Usuario).all()],
        "consecutivos": [_serializar_fila(c) for c in db.query(Consecutivo).all()],
        "configuracion": [_serializar_fila(c) for c in db.query(Configuracion).all()],
        "documentos": {
            nombre: [_serializar_fila(o) for o in db.query(cfg["modelo"]).all()]
            for nombre, cfg in MODULOS.items()
        },
        "contabilidad": {
            "facturas_dian": [_serializar_fila(f) for f in db.query(FacturaDian).all()],
            "pagos_urgentes": [_serializar_fila(p) for p in db.query(PagoUrgente).all()],
            "servicios_publicos": [_serializar_fila(s) for s in db.query(ServicioPublico).all()],
            "cajas_menores": [_serializar_fila(c) for c in db.query(CajaMenor).all()],
            "movimientos_contables": [_serializar_fila(m) for m in db.query(MovimientoContable).all()],
        },
        # la bitácora es inmutable y grande: solo conteo (se exporta aparte en Excel)
        "auditoria_eventos": db.query(RegistroAuditoria).count(),
    }
    AuditService.registrar(db=db, modulo="respaldo", accion="DESCARGAR", usuario=admin,
                           ip_cliente=get_client_ip(request), detalles={})
    return datos


@router_respaldo.post("/restaurar")
async def restaurar_respaldo(request: Request, confirmar: bool = False,
                             db: Session = Depends(get_db_fin),
                             admin: Usuario = Depends(require_admin)):
    """Sin confirmar:true SOLO SIMULA: analiza el respaldo subido y reporta el
    diff sin escribir nada. La restauración real queda para hacerla con
    acompañamiento (por diseño: no retrocede consecutivos)."""
    import json as _json
    cuerpo = await request.body()
    try:
        datos = _json.loads(cuerpo or b"{}")
    except Exception:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "El cuerpo no es un respaldo JSON válido.")
    if datos.get("formato") != 1:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Formato de respaldo desconocido.")

    resumen = {"modo": "SIMULACION", "confirmado": confirmar}
    docs = datos.get("documentos", {})
    for nombre, cfg in MODULOS.items():
        resumen[nombre] = {"en_respaldo": len(docs.get(nombre, [])),
                           "en_base": db.query(cfg["modelo"]).count()}
    resumen["usuarios"] = {"en_respaldo": len(datos.get("usuarios", [])),
                           "en_base": db.query(Usuario).count()}
    if not confirmar:
        return {**resumen,
                "aviso": "Simulación: nada se escribió. La restauración real debe hacerla el "
                         "administrador del sistema con la base detenida (no retrocede consecutivos)."}
    raise HTTPException(status.HTTP_501_NOT_IMPLEMENTED,
                        "La restauración real está deshabilitada en la app: usa la simulación y "
                        "restaura con acompañamiento sobre la base.")
