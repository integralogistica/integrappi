"""Contadores de consecutivos atómicos (req/oc/tr/vj).

# puerto de app/models/consecutivo.py del módulo externo (+ schema fin).
"""
from datetime import datetime, timezone

from sqlalchemy import DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from financiero.constantes import ESQUEMA_FIN
from financiero.database import Base


class Consecutivo(Base):
    __tablename__ = "consecutivos"
    __table_args__ = ({"schema": ESQUEMA_FIN},)

    tipo: Mapped[str] = mapped_column(String(20), primary_key=True)  # 'req', 'oc', 'tr', 'vj'
    anio: Mapped[int] = mapped_column(Integer, primary_key=True)
    ultimo_numero: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
