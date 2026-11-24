"""Bitácora del Módulo Financiero (solo admin): consulta filtrada, paginada y
export a Excel.

# puerto de app/routers/auditoria.py del módulo externo.
"""
import io
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from financiero.database import get_db_fin
from financiero.dependencies import require_admin
from financiero.models.auditoria import RegistroAuditoria
from financiero.models.usuario import Usuario

router = APIRouter(prefix="/fin/auditoria", tags=["Financiero · Bitácora"])


def _filtros(modulo, accion, usuario, entidad, desde, hasta):
    stmt = select(RegistroAuditoria)
    if modulo:
        stmt = stmt.where(RegistroAuditoria.modulo == modulo)
    if accion:
        stmt = stmt.where(RegistroAuditoria.accion == accion)
    if usuario:
        stmt = stmt.where(RegistroAuditoria.usuario_username.ilike(f"%{usuario.strip()}%"))
    if entidad:
        stmt = stmt.where(RegistroAuditoria.entidad_id == entidad.strip())
    if desde:
        stmt = stmt.where(RegistroAuditoria.fecha >= desde)
    if hasta:
        stmt = stmt.where(RegistroAuditoria.fecha <= f"{hasta}T23:59:59")
    return stmt


def _dict(e: RegistroAuditoria) -> dict:
    return {
        "id": str(e.id), "fecha": e.fecha.isoformat() if e.fecha else None,
        "usuario": e.usuario_username, "rol": e.usuario_rol,
        "modulo": e.modulo, "accion": e.accion, "entidad_id": e.entidad_id,
        "ip": e.ip_cliente, "detalles": e.detalles,
    }


@router.get("")
def consultar(
    modulo: Optional[str] = None,
    accion: Optional[str] = None,
    usuario: Optional[str] = None,
    entidad: Optional[str] = None,
    desde: Optional[str] = None,
    hasta: Optional[str] = None,
    limite: int = Query(200, ge=1, le=1000),
    desplazamiento: int = Query(0, ge=0),
    db: Session = Depends(get_db_fin),
    _: Usuario = Depends(require_admin),
):
    stmt = _filtros(modulo, accion, usuario, entidad, desde, hasta)
    from sqlalchemy import func
    total = db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    filas = db.scalars(stmt.order_by(RegistroAuditoria.fecha.desc())
                       .offset(desplazamiento).limit(limite)).all()
    return {"eventos": [_dict(e) for e in filas], "total": total}


@router.get("/exportar")
def exportar(
    modulo: Optional[str] = None,
    accion: Optional[str] = None,
    usuario: Optional[str] = None,
    entidad: Optional[str] = None,
    desde: Optional[str] = None,
    hasta: Optional[str] = None,
    db: Session = Depends(get_db_fin),
    _: Usuario = Depends(require_admin),
):
    stmt = _filtros(modulo, accion, usuario, entidad, desde, hasta)
    filas = db.scalars(stmt.order_by(RegistroAuditoria.fecha.desc()).limit(50_000)).all()

    import openpyxl
    from openpyxl.styles import Font
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Bitacora"
    ws.append(["Fecha", "Usuario", "Rol", "Módulo", "Acción", "Entidad", "IP", "Detalles"])
    for c in ws[1]:
        c.font = Font(bold=True)
    for e in filas:
        ws.append([
            e.fecha.strftime("%Y-%m-%d %H:%M:%S") if e.fecha else None,
            e.usuario_username, e.usuario_rol, e.modulo, e.accion, e.entidad_id,
            e.ip_cliente, str(e.detalles or "")[:500],
        ])
    for i, w in enumerate([19, 24, 18, 16, 16, 38, 16, 60], start=1):
        ws.column_dimensions[chr(64 + i)].width = w
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return StreamingResponse(
        buf, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="BitacoraFinanciera_{datetime.now():%Y%m%d_%H%M}.xlsx"'},
    )
