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

import logging  # noqa: E402

logger = logging.getLogger(__name__)

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
        "categoria": s.categoria, "concepto": s.concepto, "contrato": s.contrato,
        "nit_tercero": s.nit_tercero, "tercero": s.tercero, "fecha_exp": s.fecha_exp,
        "observacion": s.observacion, "urgente_recepcion": s.urgente_recepcion,
        "url_pago": s.url_pago, "url_drive": s.url_drive,
        "datos": s.datos or {}, "version": s.version,
        "created_at": s.created_at.isoformat() if s.created_at else None,
        "updated_at": s.updated_at.isoformat() if s.updated_at else None,
    }


@router.get("")
def listar_servicios(
    periodo: Optional[str] = None,
    servicio: Optional[str] = None,
    estado: Optional[str] = None,
    categoria: Optional[str] = None,
    solo_vencidos: bool = False,
    modo_calidad: Optional[str] = None,   # con_soporte | sin_soporte
    q: Optional[str] = None,
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
    if categoria:
        stmt = stmt.where(ServicioPublico.categoria == categoria)
    filas = db.scalars(stmt.order_by(ServicioPublico.periodo.desc(), ServicioPublico.inmueble).limit(2000)).all()

    # Modo auditoría Calidad: con/sin soporte adjunto (modulo 'servicio')
    if modo_calidad in ("con_soporte", "sin_soporte"):
        from financiero.models.adjunto import Adjunto
        con_soporte = {str(a.referencia_id) for a in db.scalars(
            select(Adjunto).where(Adjunto.modulo_origen == "servicio")).all()}
        filas = [f for f in filas if (str(f.id) in con_soporte) == (modo_calidad == "con_soporte")]

    hoy = datetime.now(timezone.utc).date()
    if solo_vencidos:
        filas = [f for f in filas if f.estado != "PAGADO" and f.fecha_limite and f.fecha_limite.date() < hoy]
    if q and q.strip():
        needle = q.strip().lower()
        filas = [f for f in filas if needle in " ".join(filter(None, [
            f.concepto, f.contrato, f.nit_tercero, f.tercero, f.observacion, f.servicio, f.inmueble,
        ])).lower()]

    total = sum((Decimal(str(s.valor)) for s in filas), Decimal("0"))
    pendiente = sum((Decimal(str(s.valor)) for s in filas if s.estado not in ("PAGADO", "PAGADO_NO_CONTAB")), Decimal("0"))
    # Subtotales por categoría + KPIs del original (el semáforo lo calcula el front)
    por_categoria: dict[str, dict] = {}
    for f in filas:
        c = por_categoria.setdefault(f.categoria or "publicos", {"cantidad": 0, "total": 0.0, "pagados": 0})
        c["cantidad"] += 1
        c["total"] += float(f.valor)
        if f.estado in ("PAGADO", "PAGADO_NO_CONTAB"):
            c["pagados"] += 1
    pagados = sum(1 for f in filas if f.estado in ("PAGADO", "PAGADO_NO_CONTAB"))
    no_pagados = [f for f in filas if f.estado not in ("PAGADO", "PAGADO_NO_CONTAB")]
    vence_hoy = [f for f in no_pagados if f.fecha_limite and f.fecha_limite.date() == hoy]
    vencidos = [f for f in no_pagados if f.fecha_limite and f.fecha_limite.date() < hoy]
    return {"servicios": [_dict(s) for s in filas],
            "resumen": {"total": float(total), "pendiente": float(pendiente),
                        "cantidad": len(filas), "pagados": pagados,
                        "vencidos": len(vencidos), "vence_hoy": len(vence_hoy),
                        "por_categoria": por_categoria}}


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


# ═══════════════════════════════════════════════════════════════════════════
# Paridad (views-servicios.js): contratos fijos, 5 estados, edición en línea
# ═══════════════════════════════════════════════════════════════════════════

from financiero.models.catalogo import ContratoServicio  # noqa: E402
from financiero.services import notificaciones  # noqa: E402
from fastapi import BackgroundTasks, Query  # noqa: E402

ESTADOS_CAUSADO = ("PAGADO", "CAUSADO", "ENVIADO_CONTAB", "PAGADO_NO_CONTAB", "NO_GENERADO")


class CeldaPayload(BaseModel):
    campo: str                     # valor | fecha_limite | fecha_pago | estado | observacion | url_pago | url_drive | urgente_recepcion
    valor: str | float | None = None
    version: Optional[int] = None


CAMPOS_CELDA = {
    "valor": lambda s, v: setattr(s, "valor", Decimal(str(v or 0))),
    "observacion": lambda s, v: setattr(s, "observacion", str(v) if v is not None else None),
    "url_pago": lambda s, v: setattr(s, "url_pago", str(v)[:500] if v else None),
    "url_drive": lambda s, v: setattr(s, "url_drive", str(v)[:500] if v else None),
    "urgente_recepcion": lambda s, v: setattr(s, "urgente_recepcion", str(v) if v else None),
}


@router.patch("/{id}/celda")
def parchear_celda(id: uuid.UUID, body: CeldaPayload, request: Request,
                   db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta)):
    """Edición en línea por celda del original (valor, fechas, estado, URLs)."""
    if not _puede_editar(usuario):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo Tesorería/Contabilidad editan celdas.")
    s = cargar_con_bloqueo(db, ServicioPublico, id, body.version, "Servicio")
    if body.campo in CAMPOS_CELDA:
        CAMPOS_CELDA[body.campo](s, body.valor)
    elif body.campo in ("fecha_limite", "fecha_pago"):
        if body.valor:
            try:
                setattr(s, body.campo, datetime.fromisoformat(str(body.valor)))
            except ValueError:
                raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, f"{body.campo} inválida (AAAA-MM-DD).")
        else:
            setattr(s, body.campo, None)
    elif body.campo == "estado":
        estado = str(body.valor or "").upper()
        if estado not in ESTADOS_CAUSADO:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                                f"Estado no válido. Permitidos: {', '.join(ESTADOS_CAUSADO)}.")
        s.estado = estado
        if estado in ("PAGADO", "PAGADO_NO_CONTAB") and not s.fecha_pago:
            s.fecha_pago = datetime.now(timezone.utc)
    else:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "Campo no editable en línea.")
    s.version += 1
    s.updated_by = usuario.username
    db.commit()
    db.refresh(s)
    AuditService.registrar(db=db, modulo="servicios", accion="CELDA", entidad_id=str(s.id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"campo": body.campo})
    return _dict(s)


@router.post("/{id}/urgencia")
def ciclar_urgencia(id: uuid.UUID, db: Session = Depends(get_db_fin),
                    usuario: Usuario = Depends(require_not_consulta)):
    """Botón ciclado del original: (nada) → URGENTE → PROXIMA → (nada)."""
    if not _puede_editar(usuario):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo Tesorería/Contabilidad cambian la urgencia.")
    s = db.get(ServicioPublico, id)
    if not s:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Servicio no encontrado.")
    s.urgente_recepcion = {"URGENTE": "PROXIMA", "PROXIMA": None}.get(s.urgente_recepcion, "URGENTE")
    s.version += 1
    db.commit()
    db.refresh(s)
    return _dict(s)


@router.post("/{id}/soporte-cargado")
def soporte_cargado(id: uuid.UUID, request: Request, background: BackgroundTasks,
                    db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta)):
    """El front lo invoca tras subir la factura: marca el soporte y avisa a Tesorería."""
    s = db.get(ServicioPublico, id)
    if not s:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Servicio no encontrado.")
    s.datos = {**(s.datos or {}), "tiene_soporte": True, "soporte_por": usuario.nombre,
               "soporte_at": datetime.now(timezone.utc).isoformat()}
    if s.estado == "NO_GENERADO":
        s.estado = "PENDIENTE"   # llegó la factura
    s.version += 1
    db.commit()
    db.refresh(s)
    notificaciones.avisar(background, db, [u.correo for u in db.scalars(
        select(Usuario).where(Usuario.rol == RolUsuario.TESORERIA, Usuario.activo.is_(True))).all() if u.correo],
        f"Factura de {s.servicio} · {s.inmueble} cargada ({s.periodo})",
        [f"<b>{s.servicio}</b> de {s.inmueble} periodo {s.periodo} ya tiene su soporte.",
         f"Valor: <b>${s.valor:,.0f}</b> COP."])
    return _dict(s)


@router.post("/generar-periodo")
def generar_periodo(request: Request, periodo: str = Query(..., pattern=r"^\d{4}-\d{2}$"),
                    db: Session = Depends(get_db_fin), usuario: Usuario = Depends(require_not_consulta)):
    """Puerto de ensureServiciosParaMes: replica los contratos fijos activos del
    maestro en el periodo pedido (contrato+periodo único, estado NO_GENERADO,
    valor 0) heredando NIT/tercero/URL de pago/día de corte. Idempotente."""
    if not _puede_editar(usuario):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo Tesorería/Contabilidad generan periodos.")
    contratos = db.scalars(select(ContratoServicio).where(ContratoServicio.activo.is_(True))).all()
    existentes = {(s.contrato, s.periodo) for s in db.scalars(
        select(ServicioPublico).where(ServicioPublico.periodo == periodo)).all() if s.contrato}
    creados = 0
    for cta in contratos:
        if (cta.contrato, periodo) in existentes:
            continue
        db.add(ServicioPublico(
            servicio=cta.servicio, inmueble=cta.inmueble, periodo=periodo,
            categoria=cta.categoria, concepto=cta.concepto, contrato=cta.contrato,
            nit_tercero=cta.nit_tercero, tercero=cta.tercero, fecha_exp=cta.fecha_exp,
            url_pago=cta.url_pago, valor=Decimal("0"), estado="NO_GENERADO",
            created_by=usuario.username,
        ))
        creados += 1
    db.commit()
    AuditService.registrar(db=db, modulo="servicios", accion="GENERAR_PERIODO", entidad_id=periodo, usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"periodo": periodo, "creados": creados})
    return {"creados": creados, "contratos_activos": len(contratos)}


class ContratoPayload(BaseModel):
    servicio: str
    inmueble: str
    categoria: str = "publicos"
    concepto: Optional[str] = None
    contrato: str
    nit_tercero: Optional[str] = None
    tercero: Optional[str] = None
    fecha_exp: Optional[str] = None
    url_pago: Optional[str] = None


def _contrato_dict(c: ContratoServicio) -> dict:
    return {"id": str(c.id), "servicio": c.servicio, "inmueble": c.inmueble, "categoria": c.categoria,
            "concepto": c.concepto, "contrato": c.contrato, "nit_tercero": c.nit_tercero,
            "tercero": c.tercero, "fecha_exp": c.fecha_exp, "url_pago": c.url_pago, "activo": c.activo}


@router.get("/contratos")
def listar_contratos(db: Session = Depends(get_db_fin), _: Usuario = Depends(require_not_consulta)):
    return [_contrato_dict(c) for c in db.scalars(
        select(ContratoServicio).where(ContratoServicio.activo.is_(True)).order_by(ContratoServicio.inmueble)).all()]


@router.post("/contratos")
def crear_contrato(payload: ContratoPayload, db: Session = Depends(get_db_fin),
                   usuario: Usuario = Depends(require_not_consulta)):
    if not _puede_editar(usuario):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo Tesorería/Contabilidad administran contratos.")
    dup = db.scalar(select(ContratoServicio).where(ContratoServicio.contrato == payload.contrato.strip()))
    if dup:
        raise HTTPException(status.HTTP_409_CONFLICT, f"El contrato {payload.contrato} ya existe.")
    c = ContratoServicio(**{**payload.model_dump(), "contrato": payload.contrato.strip()},
                         created_by=usuario.username)
    db.add(c)
    db.commit()
    db.refresh(c)
    return _contrato_dict(c)


@router.put("/contratos/{id}")
def editar_contrato(id: uuid.UUID, payload: ContratoPayload, db: Session = Depends(get_db_fin),
                    usuario: Usuario = Depends(require_not_consulta)):
    if not _puede_editar(usuario):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo Tesorería/Contabilidad administran contratos.")
    c = db.get(ContratoServicio, id)
    if not c:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Contrato no encontrado.")
    for campo, valor in payload.model_dump().items():
        setattr(c, campo, valor)
    c.updated_by = usuario.username
    db.commit()
    db.refresh(c)
    return _contrato_dict(c)


@router.put("/contratos/{id}/propagar")
def propagar_contrato(id: uuid.UUID, request: Request, db: Session = Depends(get_db_fin),
                      usuario: Usuario = Depends(require_not_consulta),
                      alcance: str = Query("todos", pattern="^(todos|futuros)$")):
    """Aplica los datos del contrato base a las filas mensuales existentes
    (todas o solo las de periodos >= el actual), sin tocar valor/estado."""
    if not _puede_editar(usuario):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo Tesorería/Contabilidad propagan contratos.")
    c = db.get(ContratoServicio, id)
    if not c:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Contrato no encontrado.")
    hoy_periodo = datetime.now(timezone.utc).strftime("%Y-%m")
    filas = db.scalars(select(ServicioPublico).where(ServicioPublico.contrato == c.contrato)).all()
    cambiadas = 0
    for f in filas:
        if alcance == "futuros" and f.periodo < hoy_periodo:
            continue
        f.servicio, f.inmueble, f.categoria = c.servicio, c.inmueble, c.categoria
        f.concepto, f.nit_tercero, f.tercero = c.concepto, c.nit_tercero, c.tercero
        f.fecha_exp, f.url_pago = c.fecha_exp, c.url_pago
        f.version += 1
        cambiadas += 1
    db.commit()
    AuditService.registrar(db=db, modulo="servicios", accion="PROPAGAR_CONTRATO", entidad_id=str(id), usuario=usuario,
                           ip_cliente=get_client_ip(request), detalles={"contrato": c.contrato, "filas": cambiadas})
    return {"cambiadas": cambiadas, "alcance": alcance}


@router.delete("/contratos/{id}")
def eliminar_contrato(id: uuid.UUID, db: Session = Depends(get_db_fin),
                      usuario: Usuario = Depends(require_not_consulta)):
    if not _es_admin(usuario):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo el administrador elimina contratos.")
    c = db.get(ContratoServicio, id)
    if not c:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Contrato no encontrado.")
    c.activo = False
    db.commit()
    return {"mensaje": "Contrato desactivado."}


@router.get("/zip-facturas")
def zip_facturas(periodo: Optional[str] = None, db: Session = Depends(get_db_fin),
                 usuario: Usuario = Depends(require_not_consulta)):
    """ZIP con los soportes de pago del periodo + índice CSV (patrón de
    descargarServiciosZip del original; blobs desde GCS privado)."""
    import io as _io
    import zipfile
    from fastapi.responses import StreamingResponse
    from financiero.models.adjunto import Adjunto
    from rutas.fin_adjuntos import _obtener_cliente_storage
    from financiero.constantes import FIN_BUCKET

    stmt = select(ServicioPublico)
    if periodo:
        stmt = stmt.where(ServicioPublico.periodo == periodo)
    servicios = db.scalars(stmt.limit(500)).all()
    bucket = _obtener_cliente_storage().bucket(FIN_BUCKET)
    buf = _io.BytesIO()
    indice = ["Servicio;Inmueble;Periodo;Valor;Estado;Fecha Pago;Tiene Soporte"]
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for s in servicios:
            adjuntos = db.scalars(select(Adjunto).where(Adjunto.modulo_origen == "servicio",
                                                        Adjunto.referencia_id == str(s.id))).all()
            tiene = False
            for adj in adjuntos:
                try:
                    contenido = bucket.blob(adj.ruta_almacenamiento).download_as_bytes()
                except Exception as exc:
                    logger.warning("[FIN] Blob de servicio no descargable (%s): %s", adj.ruta_almacenamiento, exc)
                    continue
                nombre = f"{s.periodo}_{s.servicio}_{s.inmueble}"[:60].replace(" ", "_").replace("/", "-")
                z.writestr(f"Facturas_Servicios/{nombre}.pdf", contenido)
                tiene = True
            indice.append(";".join([
                s.servicio, s.inmueble.replace(";", ","), s.periodo, str(s.valor), s.estado,
                s.fecha_pago.strftime("%Y-%m-%d") if s.fecha_pago else "", "SÍ" if tiene else "NO",
            ]))
        z.writestr("Indice_Servicios.csv", "﻿" + "\n".join(indice))
    buf.seek(0)
    return StreamingResponse(buf, media_type="application/zip",
                             headers={"Content-Disposition": f'attachment; filename="Servicios_{periodo or "Consolidado"}.zip"'})
