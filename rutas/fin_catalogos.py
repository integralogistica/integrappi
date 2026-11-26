"""Catálogos corporativos del Módulo Financiero (centros de costo ~49k,
proveedores ~2.7k) con búsqueda paginada.

# puerto LITERAL de app/routers/catalogos.py del módulo externo (solo cambian
prefix y dependencias) + presupuesto-responsables y personal-viajes que allá
vivían como JSON estático del front.
"""
from typing import Optional
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from financiero.database import get_db_fin
from financiero.dependencies import get_usuario_fin, require_admin, require_seccion
from financiero.models.catalogo import CentroCosto, PersonalViaje, PresupuestoResponsable, Proveedor, SedeCajaMenor
from financiero.models.usuario import Usuario

router = APIRouter(prefix="/fin/catalogos", tags=["Financiero · Catálogos"])


class PaginacionCatalogo(BaseModel):
    items: list[dict]
    total: int
    pagina: int
    limite: int
    total_paginas: int


@router.get("/centros-costo", response_model=PaginacionCatalogo)
def buscar_centros_costo(
    q: Optional[str] = Query(None, description="Búsqueda por código o nombre del centro de costo"),
    pagina: int = Query(1, ge=1),
    limite: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db_fin),
    _=Depends(get_usuario_fin),
):
    """Búsqueda paginada y optimizada para el catálogo de centros de costo (~49.000 registros)."""
    stmt = select(CentroCosto).where(CentroCosto.activo == True)  # noqa: E712

    if q and q.strip():
        term = f"%{q.strip()}%"
        stmt = stmt.where(or_(
            CentroCosto.codigo.ilike(term),
            CentroCosto.nombre.ilike(term)
        ))

    total = db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    offset = (pagina - 1) * limite
    items = db.scalars(stmt.order_by(CentroCosto.codigo.asc()).offset(offset).limit(limite)).all()

    total_paginas = (total + limite - 1) // limite if total > 0 else 1

    return PaginacionCatalogo(
        items=[{"codigo": c.codigo, "nombre": c.nombre, "area": c.area} for c in items],
        total=total,
        pagina=pagina,
        limite=limite,
        total_paginas=total_paginas
    )


@router.get("/proveedores", response_model=PaginacionCatalogo)
def buscar_proveedores(
    q: Optional[str] = Query(None, description="Búsqueda por NIT o Razón Social"),
    pagina: int = Query(1, ge=1),
    limite: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db_fin),
    _=Depends(get_usuario_fin),
):
    """Búsqueda paginada y optimizada para el catálogo de proveedores (~2.700 registros)."""
    stmt = select(Proveedor).where(Proveedor.activo == True)  # noqa: E712

    if q and q.strip():
        term = f"%{q.strip()}%"
        stmt = stmt.where(or_(
            Proveedor.nit.ilike(term),
            Proveedor.razon_social.ilike(term)
        ))

    total = db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    offset = (pagina - 1) * limite
    items = db.scalars(stmt.order_by(Proveedor.razon_social.asc()).offset(offset).limit(limite)).all()

    total_paginas = (total + limite - 1) // limite if total > 0 else 1

    return PaginacionCatalogo(
        items=[{
            "nit": p.nit,
            "razon_social": p.razon_social,
            "correo": p.correo,
            "telefono": p.telefono,
        } for p in items],
        total=total,
        pagina=pagina,
        limite=limite,
        total_paginas=total_paginas
    )


@router.get("/presupuesto")
def leer_presupuesto(
    periodo: int = Query(..., description="Año del presupuesto (p. ej. 2026)"),
    db: Session = Depends(get_db_fin),
    usuario: Usuario = Depends(get_usuario_fin),
    __seccion=Depends(require_seccion("traslados")),
):
    """Presupuesto por responsable/rubro/mes — base del efecto de los traslados
    (ex data/presupuesto-responsables.json del front): {responsable: {rubro: {mes: [ppto, ejecutado]}}}."""
    filas = db.scalars(
        select(PresupuestoResponsable).where(PresupuestoResponsable.periodo == periodo)
    ).all()
    out: dict = {}
    for f in filas:
        out.setdefault(f.responsable, {}).setdefault(f.rubro, {})[str(f.mes)] = [f.presupuesto, f.ejecutado]
    return out


@router.get("/personal-viajes")
def listar_personal_viajes(
    q: Optional[str] = Query(None, description="Filtro por nombre"),
    limite: int = Query(500, ge=1, le=2000),
    db: Session = Depends(get_db_fin),
    usuario: Usuario = Depends(get_usuario_fin),
    __seccion=Depends(require_seccion("viajes")),
):
    """Personal habilitado para viajar con su cargo y categoría de tarifa
    (ex data/personal-viajes.json del front): autocompleta el formulario de solicitud."""
    stmt = select(PersonalViaje).where(PersonalViaje.activo == True)  # noqa: E712
    if q and q.strip():
        stmt = stmt.where(PersonalViaje.nombre.ilike(f"%{q.strip()}%"))
    return [
        {"id": p.id, "nombre": p.nombre, "cargo": p.cargo, "categoria": p.categoria}
        for p in db.scalars(stmt.order_by(PersonalViaje.nombre.asc()).limit(limite)).all()
    ]


# ── Maestro de sedes de caja menor (FORM-FIN-007) ──────────────────────────

def _sede_dict(s: SedeCajaMenor) -> dict:
    return {"id": str(s.id), "nombre": s.nombre, "etiqueta": s.etiqueta, "responsable": s.responsable,
            "identificacion": s.identificacion, "jefe_inmediato": s.jefe_inmediato,
            "cco": s.cco, "cuenta": s.cuenta, "activo": s.activo}


@router.get("/sedes-cajas")
def listar_sedes_cajas(db: Session = Depends(get_db_fin), usuario: Usuario = Depends(get_usuario_fin),
                       __seccion=Depends(require_seccion("cajas"))):
    return [_sede_dict(s) for s in db.scalars(
        select(SedeCajaMenor).where(SedeCajaMenor.activo == True)  # noqa: E712
    ).order_by(SedeCajaMenor.nombre).all()]


class SedeCajaPayload(BaseModel):
    nombre: str
    etiqueta: Optional[str] = None
    responsable: Optional[str] = None
    identificacion: Optional[str] = None
    jefe_inmediato: Optional[str] = None
    cco: Optional[str] = None
    cuenta: Optional[str] = None


@router.post("/sedes-cajas")
def crear_sede_caja(payload: SedeCajaPayload, db: Session = Depends(get_db_fin),
                    admin: Usuario = Depends(require_admin)):
    dup = db.scalar(select(SedeCajaMenor).where(SedeCajaMenor.nombre == payload.nombre.strip()))
    if dup:
        raise HTTPException(status.HTTP_409_CONFLICT, f"La sede {payload.nombre} ya existe.")
    s = SedeCajaMenor(**payload.model_dump())
    db.add(s)
    db.commit()
    db.refresh(s)
    return _sede_dict(s)


@router.put("/sedes-cajas/{id}")
def editar_sede_caja(id: uuid.UUID, payload: SedeCajaPayload, db: Session = Depends(get_db_fin),
                     admin: Usuario = Depends(require_admin)):
    s = db.get(SedeCajaMenor, id)
    if not s:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Sede no encontrada.")
    for campo, valor in payload.model_dump().items():
        setattr(s, campo, valor)
    db.commit()
    db.refresh(s)
    return _sede_dict(s)


@router.delete("/sedes-cajas/{id}")
def eliminar_sede_caja(id: uuid.UUID, db: Session = Depends(get_db_fin),
                       admin: Usuario = Depends(require_admin)):
    s = db.get(SedeCajaMenor, id)
    if not s:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Sede no encontrada.")
    s.activo = False  # borrado lógico: las cajas existentes referencian el nombre
    db.commit()
    return {"mensaje": "Sede desactivada."}
