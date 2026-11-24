"""Catálogos corporativos del Módulo Financiero (centros de costo ~49k,
proveedores ~2.7k) con búsqueda paginada.

# puerto LITERAL de app/routers/catalogos.py del módulo externo (solo cambian
prefix y dependencias).
"""
from typing import Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from financiero.database import get_db_fin
from financiero.dependencies import get_usuario_fin
from financiero.models.catalogo import CentroCosto, Proveedor
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
