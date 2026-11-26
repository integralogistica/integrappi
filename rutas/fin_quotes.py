"""Cotizaciones y anticipos internos (ex colección 'quotes' del original).

# puerto de views-pagos.js §Cotizaciones y anticipos: anticipos con
# comprobante EGC/ANT/BG-TR (proceso Cajas Menores / Viajes Viáticos / Otros)
# que opcionalmente se programan en Pagos Urgentes, y cotizaciones que se
# cruzan con la factura DIAN que las reemplaza (candidatos por NIT ordenados
# por cercanía de valor). Estados: PENDIENTE → CRUZADA → LEGALIZADA → CERRADA.
"""
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from financiero.database import get_db_fin
from financiero.dependencies import require_not_consulta, require_seccion
from financiero.models.catalogo import SedeCajaMenor
from financiero.models.contabilidad import EstadoFactura, EstadoQuote, EstadoPagoUrgente, FacturaDian, PagoUrgente, Quote
from financiero.models.usuario import Usuario
from financiero.services.audit_service import AuditService
from financiero.services.locking import cargar_con_bloqueo
from rutas.fin_pagos import datos_bancarios
from rutas.fin_traslados import get_client_ip

router = APIRouter(prefix="/fin/quotes", tags=["Financiero · Cotizaciones y anticipos"])

TIPOS_COMPROBANTE = {"EGC", "ANT", "BG-TR"}
PROCESOS = {"CAJAS MENORES", "VIAJES VIATICOS", "OTROS"}


def _digits(s) -> str:
    return "".join(c for c in str(s or "") if c.isdigit())


def _evento(usuario: Usuario, accion: str, anterior, nuevo, obs: str = "") -> dict:
    return {"fecha": datetime.now(timezone.utc).isoformat(), "usuario": usuario.nombre,
            "username": usuario.username, "accion": accion,
            "estado_anterior": anterior.value if hasattr(anterior, "value") else anterior,
            "estado_nuevo": nuevo.value if hasattr(nuevo, "value") else nuevo, "observacion": obs}


def _quote_dict(q: Quote, factura: FacturaDian | None = None) -> dict:
    return {
        "id": str(q.id), "tipo": q.tipo, "tipo_comprobante": q.tipo_comprobante,
        "proceso": q.proceso, "sede_cm": q.sede_cm,
        "proveedor": q.proveedor, "nit": q.nit, "valor": float(q.valor),
        "fecha": q.fecha.isoformat() if q.fecha else None, "estado": q.estado,
        "factura_id": str(q.factura_id) if q.factura_id else None,
        "factura_numero": factura.numero_factura if factura else None,
        "pago_id": str(q.pago_id) if q.pago_id else None,
        "legalizacion": q.legalizacion or {},
        "historial_cambios": q.historial_cambios or [], "version": q.version,
        "created_at": q.created_at.isoformat() if q.created_at else None,
    }


@router.get("")
def listar_quotes(
    segmento: str = "pendientes",       # pendientes | cotizaciones | anticipos | historial
    q: Optional[str] = None,
    db: Session = Depends(get_db_fin),
    _: Usuario = Depends(require_not_consulta),
    __seccion=Depends(require_seccion("pagos")),
):
    stmt = select(Quote)
    if segmento == "cotizaciones":
        stmt = stmt.where(Quote.tipo == "COTIZACION", Quote.estado.in_(["PENDIENTE", "CRUZADA"]))
    elif segmento == "anticipos":
        stmt = stmt.where(Quote.tipo == "ANTICIPO", Quote.estado.in_(["PENDIENTE", "CRUZADA"]))
    elif segmento == "historial":
        stmt = stmt.where(Quote.estado.in_(["LEGALIZADA", "CERRADA"]))
    else:  # pendientes: todo lo que aún no cierra
        stmt = stmt.where(Quote.estado.in_(["PENDIENTE", "CRUZADA"]))
    filas = db.scalars(stmt.order_by(Quote.created_at.desc()).limit(500)).all()
    if q and q.strip():
        needle = q.strip().lower()
        filas = [f for f in filas if needle in " ".join(filter(None, [f.proveedor, f.nit, f.sede_cm])).lower()]
    facturas = {str(f.id): f for f in db.scalars(select(FacturaDian)).all()}
    return [_quote_dict(f, facturas.get(str(f.factura_id))) for f in filas]


class QuotePayload(BaseModel):
    tipo: str = "ANTICIPO"              # COTIZACION | ANTICIPO
    tipo_comprobante: Optional[str] = None
    proceso: Optional[str] = None
    sede_cm: Optional[str] = None
    proveedor: str
    nit: Optional[str] = None
    valor: float
    fecha: Optional[str] = None
    crear_pago: bool = False            # «Programar también en Pagos Urgentes»
    observaciones: Optional[str] = None


@router.post("")
def crear_quote(payload: QuotePayload, request: Request,
                db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta)):
    if payload.tipo not in ("COTIZACION", "ANTICIPO"):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "tipo no válido (COTIZACION | ANTICIPO).")
    if payload.tipo_comprobante and payload.tipo_comprobante not in TIPOS_COMPROBANTE:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, f"tipo_comprobante no válido: {', '.join(sorted(TIPOS_COMPROBANTE))}.")
    if payload.proceso and payload.proceso.upper() not in PROCESOS:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, f"proceso no válido: {', '.join(sorted(PROCESOS))}.")
    if payload.sede_cm:
        sede = db.scalar(select(SedeCajaMenor).where(SedeCajaMenor.nombre == payload.sede_cm.strip()))
        if not sede:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, f"La sede {payload.sede_cm} no existe en el maestro de cajas.")
    ahora = datetime.now(timezone.utc)
    q = Quote(
        tipo=payload.tipo,
        tipo_comprobante=payload.tipo_comprobante,
        proceso=(payload.proceso.upper() if payload.proceso else None),
        sede_cm=payload.sede_cm,
        proveedor=payload.proveedor.strip()[:250],
        nit=payload.nit,
        valor=Decimal(str(payload.valor)),
        fecha=datetime.fromisoformat(payload.fecha.replace("Z", "+00:00")) if payload.fecha else ahora,
        estado=EstadoQuote.PENDIENTE.value,
        historial_cambios=[_evento(usuario, "CREAR", None, EstadoQuote.PENDIENTE,
                                   payload.observaciones or (f"Anticipo {payload.tipo_comprobante} · {payload.proceso}" if payload.tipo == "ANTICIPO" else "Cotización"))],
        created_by=usuario.username,
    )
    # «Programar también en Pagos Urgentes»: nace el pago ya aprobado por quien lo crea
    if payload.crear_pago:
        heredado = datos_bancarios(db, payload.nit)
        pago = PagoUrgente(
            solicitante_nombre=usuario.nombre,
            proveedor=payload.proveedor.strip()[:250],
            valor=Decimal(str(payload.valor)),
            fecha_requerida=q.fecha,
            estado=EstadoPagoUrgente.PENDIENTE.value,
            origen="anticipo",
            tipo_doc="Anticipo",
            nit=payload.nit, nit_pago=payload.nit,
            banco=heredado.get("banco"), tipo_cuenta=heredado.get("tipo_cuenta"),
            no_cuenta=heredado.get("no_cuenta"),
            area=payload.sede_cm or (payload.proceso.upper() if payload.proceso else None),
            requerimiento=(f"ANTICIPO {payload.tipo_comprobante} · {payload.proceso}" if payload.tipo == "ANTICIPO" else "COTIZACIÓN").upper(),
            asunto=payload.proveedor.upper(),
            historial_cambios=[{"fecha": ahora.isoformat(), "usuario": usuario.nombre, "username": usuario.username,
                                "accion": "CREAR", "estado_nuevo": EstadoPagoUrgente.PENDIENTE.value,
                                "observacion": "Programado desde Cotizaciones y anticipos"}],
            created_by=usuario.username,
        )
        db.add(pago)
        db.flush()
        q.pago_id = pago.id
    db.add(q)
    db.commit()
    db.refresh(q)
    AuditService.registrar(db=db, modulo="quotes", accion="CREAR", entidad_id=str(q.id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"tipo": q.tipo, "proveedor": q.proveedor, "pago": bool(q.pago_id)})
    return _quote_dict(q)


class LegalizarPayload(BaseModel):
    fecha: Optional[str] = None
    observaciones: Optional[str] = None
    recibos: list[str] = []             # referencias de los recibos que la componen


@router.post("/{id}/legalizar")
def legalizar(id: uuid.UUID, payload: LegalizarPayload, request: Request,
              db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta)):
    q = cargar_con_bloqueo(db, Quote, id, None, "Quote")
    if q.estado not in (EstadoQuote.PENDIENTE.value, EstadoQuote.CRUZADA.value):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"No se legaliza en estado '{q.estado}'.")
    ahora = datetime.now(timezone.utc)
    anterior = q.estado
    q.legalizacion = {
        "fecha": payload.fecha or ahora.date().isoformat(),
        "observaciones": payload.observaciones,
        "recibos": payload.recibos,
        "por": usuario.nombre, "at": ahora.isoformat(),
    }
    q.estado = EstadoQuote.LEGALIZADA.value
    q.version += 1
    q.updated_by = usuario.username
    q.historial_cambios = [*(q.historial_cambios or []),
                           _evento(usuario, "LEGALIZAR", anterior, q.estado, payload.observaciones or "")]
    db.commit()
    db.refresh(q)
    AuditService.registrar(db=db, modulo="quotes", accion="LEGALIZAR", entidad_id=str(q.id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"recibos": len(payload.recibos)})
    return _quote_dict(q)


@router.post("/{id}/deshacer-legalizacion")
def deshacer_legalizacion(id: uuid.UUID, db: Session = Depends(get_db_fin),
                          usuario: Usuario = Depends(require_not_consulta)):
    q = cargar_con_bloqueo(db, Quote, id, None, "Quote")
    if q.estado != EstadoQuote.LEGALIZADA.value:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Solo se deshace la legalización de una legalizada.")
    anterior = q.estado
    q.legalizacion = None
    q.estado = EstadoQuote.CRUZADA.value if q.factura_id else EstadoQuote.PENDIENTE.value
    q.version += 1
    q.updated_by = usuario.username
    q.historial_cambios = [*(q.historial_cambios or []),
                           _evento(usuario, "DESHACER_LEGALIZAR", anterior, q.estado, "")]
    db.commit()
    db.refresh(q)
    return _quote_dict(q)


@router.get("/{id}/candidatos")
def candidatos(id: uuid.UUID, db: Session = Depends(get_db_fin),
               _: Usuario = Depends(require_not_consulta)):
    """Facturas DIAN del mismo NIT (cruzables), ordenadas por cercanía de valor."""
    q = db.get(Quote, id)
    if not q:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "No encontrado.")
    if not q.nit:
        return []
    estados = (EstadoFactura.PENDIENTE.value, EstadoFactura.TRAMITADA.value, EstadoFactura.CAUSADA.value)
    out = []
    for f in db.scalars(select(FacturaDian).where(FacturaDian.estado.in_(estados))).all():
        if _digits(f.nit_proveedor) != _digits(q.nit):
            continue
        out.append({
            "id": str(f.id), "numero_factura": f.numero_factura, "proveedor": f.proveedor,
            "total": float(f.total), "estado": f.estado,
            "diferencia": float(Decimal(str(f.total)) - Decimal(str(q.valor))),
        })
    out.sort(key=lambda x: abs(x["diferencia"]))
    return out[:30]


@router.post("/{id}/cruzar")
def cruzar(id: uuid.UUID, body: dict, request: Request,
           db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta)):
    try:
        factura_uuid = uuid.UUID(str(body.get("factura_id") or ""))
    except ValueError:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "factura_id no válido.")
    q = cargar_con_bloqueo(db, Quote, id, None, "Quote")
    if q.estado != EstadoQuote.PENDIENTE.value:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"No se cruza en estado '{q.estado}'.")
    f = db.get(FacturaDian, factura_uuid)
    if not f:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Factura no encontrada.")
    if _digits(f.nit_proveedor) != _digits(q.nit or ""):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "La factura no es del mismo NIT de la cotización.")
    ahora = datetime.now(timezone.utc)
    q.factura_id = f.id
    q.estado = EstadoQuote.CRUZADA.value
    q.version += 1
    q.updated_by = usuario.username
    q.historial_cambios = [*(q.historial_cambios or []),
                           _evento(usuario, "CRUZAR", EstadoQuote.PENDIENTE, q.estado, f"Factura {f.numero_factura}")]
    f.detalles = {**(f.detalles or {}), "quote_id": str(q.id)}
    f.version += 1
    db.commit()
    db.refresh(q)
    db.refresh(f)
    AuditService.registrar(db=db, modulo="quotes", accion="CRUZAR", entidad_id=str(q.id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"factura": f.numero_factura})
    return _quote_dict(q, f)


@router.post("/{id}/descruzar")
def descruzar(id: uuid.UUID, db: Session = Depends(get_db_fin),
              usuario: Usuario = Depends(require_not_consulta)):
    q = cargar_con_bloqueo(db, Quote, id, None, "Quote")
    if not q.factura_id:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Esta cotización no tiene cruce.")
    f = db.get(FacturaDian, q.factura_id)
    if f and isinstance(f.detalles, dict):
        f.detalles = {k: v for k, v in f.detalles.items() if k != "quote_id"}
        f.version += 1
    anterior = q.estado
    q.factura_id = None
    q.estado = EstadoQuote.PENDIENTE.value
    q.version += 1
    q.updated_by = usuario.username
    q.historial_cambios = [*(q.historial_cambios or []),
                           _evento(usuario, "DESCRUZAR", anterior, q.estado, "")]
    db.commit()
    db.refresh(q)
    return _quote_dict(q, f)


@router.post("/{id}/cerrar")
def cerrar(id: uuid.UUID, db: Session = Depends(get_db_fin),
           usuario: Usuario = Depends(require_not_consulta)):
    q = cargar_con_bloqueo(db, Quote, id, None, "Quote")
    if q.estado != EstadoQuote.LEGALIZADA.value:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Solo se cierran cotizaciones legalizadas.")
    q.estado = EstadoQuote.CERRADA.value
    q.version += 1
    q.updated_by = usuario.username
    q.historial_cambios = [*(q.historial_cambios or []),
                           _evento(usuario, "CERRAR", EstadoQuote.LEGALIZADA, q.estado, "")]
    db.commit()
    db.refresh(q)
    return _quote_dict(q)


@router.delete("/{id}")
def eliminar_quote(id: uuid.UUID, request: Request, db: Session = Depends(get_db_fin),
                   usuario: Usuario = Depends(require_not_consulta)):
    if usuario.rol != "admin" and not usuario.es_admin_adicional:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo el administrador elimina cotizaciones.")
    q = db.get(Quote, id)
    if not q:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "No encontrado.")
    # el pago programado vuelve a quedar huérfano (no se borra)
    snapshot = {"tipo": q.tipo, "proveedor": q.proveedor, "valor": str(q.valor), "estado": q.estado}
    db.delete(q)
    db.commit()
    AuditService.registrar(db=db, modulo="quotes", accion="ELIMINAR", entidad_id=str(id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"snapshot": snapshot})
    return {"mensaje": "Cotización eliminada."}
