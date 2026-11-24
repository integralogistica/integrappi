"""Bloqueo pesimista + optimista para transiciones.

# puerto LITERAL de app/services/locking.py del módulo externo.
"""
import uuid
from typing import Optional, Type, TypeVar

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

T = TypeVar("T")


def cargar_con_bloqueo(db: Session, modelo: Type[T], id: uuid.UUID, version: Optional[int] = None, nombre: str = "Registro") -> T:
    """Carga la fila con SELECT ... FOR UPDATE (Postgres) para serializar
    transiciones simultáneas y, si el cliente envía la versión que leyó,
    devuelve 409 cuando otro usuario ya la cambió."""
    obj = db.scalars(select(modelo).where(modelo.id == id).with_for_update()).first()
    if not obj:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"{nombre} no encontrado.")
    if version is not None and obj.version != version:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"Conflicto: otra persona modificó este registro (versión actual {obj.version}, la tuya {version}). Recarga y reintenta.",
        )
    return obj
