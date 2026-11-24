"""Configuración editable del módulo (áreas, límite del Controller, tarifas
de viajes...). Nunca guarda secretos.

# puerto de app/models/configuracion.py del módulo externo (schema fin).
"""
from datetime import datetime, timezone

from sqlalchemy import DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from financiero.constantes import ESQUEMA_FIN
from financiero.database import Base
from financiero.models.base import JSONFin


class Configuracion(Base):
    __tablename__ = "configuracion"
    __table_args__ = ({"schema": ESQUEMA_FIN},)

    clave: Mapped[str] = mapped_column(String(60), primary_key=True)
    valor: Mapped[dict] = mapped_column(JSONFin, nullable=False)
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_by: Mapped[str] = mapped_column(String(100), nullable=True)
