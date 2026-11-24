"""Traslados de presupuesto (jefe de área pide → Presidencia decide).

# puerto de app/models/traslado.py del módulo externo (schema fin).
"""
import uuid
from datetime import datetime
from decimal import Decimal
from enum import Enum

from sqlalchemy import DateTime, Enum as SQLEnum, ForeignKey, Numeric, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from financiero.constantes import ESQUEMA_FIN
from financiero.database import Base
from financiero.models.base import JSONFin, TimestampMixin, VersionMixin


class EstadoTraslado(str, Enum):
    """Traslados de presupuesto: los responsables de área piden a Presidencia
    pasar recursos de la reserva a su área. Presidencia aprueba o rechaza; lo
    aprobado queda visible para Gerente financiero y Controller."""
    PENDIENTE_PRESIDENCIA = "PENDIENTE_PRESIDENCIA"
    APROBADO = "APROBADO"
    RECHAZADO = "RECHAZADO"
    ANULADO = "ANULADO"


class Traslado(Base, TimestampMixin, VersionMixin):
    __tablename__ = "traslados"
    __table_args__ = ({"schema": ESQUEMA_FIN},)

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    consecutivo: Mapped[str] = mapped_column(String(30), unique=True, index=True, nullable=False)  # TRP-AAAA-0001

    solicitante_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey(f"{ESQUEMA_FIN}.usuarios.id"), nullable=False
    )
    solicitante_nombre: Mapped[str] = mapped_column(String(150), nullable=False)
    area: Mapped[str] = mapped_column(String(100), nullable=False, index=True)  # área destino que recibe los recursos
    fecha: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    motivo: Mapped[str] = mapped_column(Text, nullable=True)  # el motivo va por línea; este es opcional
    observaciones: Mapped[str] = mapped_column(Text, nullable=True)

    # Líneas: {destinoRubro, monto, motivo, mes?, origen?, efecto?}; el servidor recalcula el total
    lineas: Mapped[list] = mapped_column(JSONFin, default=list, nullable=False)
    total: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=Decimal("0.00"), nullable=False)

    estado: Mapped[EstadoTraslado] = mapped_column(
        SQLEnum(
            EstadoTraslado,
            native_enum=False,
            length=50,
            values_callable=lambda e: [m.value for m in e],
        ),
        default=EstadoTraslado.PENDIENTE_PRESIDENCIA,
        nullable=False,
        index=True,
    )

    # Decisión de Presidencia
    aprobador_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey(f"{ESQUEMA_FIN}.usuarios.id"), nullable=True
    )
    fecha_aprobacion: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    observacion_aprobacion: Mapped[str] = mapped_column(Text, nullable=True)
    motivo_rechazo: Mapped[str] = mapped_column(Text, nullable=True)
    aprobacion: Mapped[dict] = mapped_column(JSONFin, nullable=True)  # {nombre, cargo, at, comentario} para el formato impreso

    # Campos de presentación del formulario que no participan en las reglas
    datos_front: Mapped[dict] = mapped_column(JSONFin, nullable=True)

    historial_cambios: Mapped[list] = mapped_column(JSONFin, default=list, nullable=False)
