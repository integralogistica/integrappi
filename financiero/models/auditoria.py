"""Bitácora inmutable del Módulo Financiero.

# puerto de app/models/auditoria.py del módulo externo (schema fin).
No se expone update/delete: los eventos nunca se editan ni se borran.
"""
import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from financiero.constantes import ESQUEMA_FIN
from financiero.database import Base
from financiero.models.base import JSONFin


class RegistroAuditoria(Base):
    __tablename__ = "auditoria_eventos"
    __table_args__ = ({"schema": ESQUEMA_FIN},)

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    fecha: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
        index=True,
    )
    usuario_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), nullable=True)
    usuario_username: Mapped[str] = mapped_column(String(60), nullable=True, index=True)
    usuario_rol: Mapped[str] = mapped_column(String(50), nullable=True)

    modulo: Mapped[str] = mapped_column(String(50), nullable=False, index=True)  # usuarios, requisiciones, traslados, viajes...
    accion: Mapped[str] = mapped_column(String(50), nullable=False, index=True)  # CREAR, APROBAR, RECHAZAR, ELIMINAR, LOGIN...
    entidad_id: Mapped[str] = mapped_column(String(100), nullable=True, index=True)

    ip_cliente: Mapped[str] = mapped_column(String(50), nullable=True)
    detalles: Mapped[dict] = mapped_column(JSONFin, default=dict, nullable=False)
