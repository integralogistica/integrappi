"""Solicitudes de viaje (autorización → presupuesto → tiquetes → anticipo →
legalización → cierre).

# puerto de app/models/viaje.py del módulo externo (schema fin).
"""
import uuid
from datetime import datetime
from decimal import Decimal
from enum import Enum

from sqlalchemy import Boolean, DateTime, Enum as SQLEnum, ForeignKey, Numeric, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from financiero.constantes import ESQUEMA_FIN
from financiero.database import Base
from financiero.models.base import JSONFin, TimestampMixin, VersionMixin


class EstadoViaje(str, Enum):
    BORRADOR = "BORRADOR"
    SOLICITADO = "SOLICITADO"
    AUTORIZADO = "AUTORIZADO"
    RECHAZADO = "RECHAZADO"
    TIQUETES_GESTIONADOS = "TIQUETES_GESTIONADOS"
    PRESUPUESTO_ASIGNADO = "PRESUPUESTO_ASIGNADO"
    ANTICIPO_PAGADO = "ANTICIPO_PAGADO"
    LEGALIZADO = "LEGALIZADO"
    CERRADO = "CERRADO"
    ANULADO = "ANULADO"


class CiudadViaje(str, Enum):
    BARRANQUILLA = "BARRANQUILLA"
    BOGOTA = "BOGOTA"
    BUCARAMANGA = "BUCARAMANGA"
    SANTA_MARTA = "SANTA MARTA"
    PASTO = "PASTO"
    CALI = "CALI"
    MEDELLIN = "MEDELLIN"


class Viaje(Base, TimestampMixin, VersionMixin):
    __tablename__ = "viajes"
    __table_args__ = ({"schema": ESQUEMA_FIN},)

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    consecutivo: Mapped[str] = mapped_column(String(30), unique=True, index=True, nullable=False)

    solicitante_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey(f"{ESQUEMA_FIN}.usuarios.id"), nullable=False
    )
    solicitante_nombre: Mapped[str] = mapped_column(String(150), nullable=False)

    viajero_nombre: Mapped[str] = mapped_column(String(150), nullable=False)
    viajero_cedula: Mapped[str] = mapped_column(String(30), nullable=True)

    origen: Mapped[str] = mapped_column(String(50), nullable=False)
    destino: Mapped[str] = mapped_column(String(50), nullable=False)

    fecha_inicio: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    fecha_fin: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    objetivo: Mapped[str] = mapped_column(Text, nullable=False)

    viajero_cargo: Mapped[str] = mapped_column(String(150), nullable=True)
    categoria_solicitud: Mapped[str] = mapped_column(String(10), nullable=True)  # categoría A/B/C propuesta al solicitar
    observaciones: Mapped[str] = mapped_column(Text, nullable=True)

    # Autorización (Controller, Gerente Financiero, Presidencia o con casilla autorizador)
    autorizador_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey(f"{ESQUEMA_FIN}.usuarios.id"), nullable=True
    )
    fecha_autorizacion: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    observacion_autorizacion: Mapped[str] = mapped_column(Text, nullable=True)

    estado: Mapped[EstadoViaje] = mapped_column(
        SQLEnum(
            EstadoViaje,
            native_enum=False,
            length=50,
            values_callable=lambda e: [m.value for m in e],
        ),
        default=EstadoViaje.SOLICITADO,
        nullable=False,
        index=True,
    )

    # Gestión de Tiquetes por Compras
    requiere_tiquetes: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    tiquetes_detalle: Mapped[dict] = mapped_column(JSONFin, default=dict, nullable=True)

    # Presupuesto gestionado por Controller (Categoría A/B/C confidencial)
    # NOTA DE SEGURIDAD: Visible solo para Controller, Gerente Financiero,
    # Presidencia o Autorizador (se enmascara en el serializer).
    categoria_tarifa: Mapped[str] = mapped_column(String(10), nullable=True)
    presupuesto_items: Mapped[list] = mapped_column(JSONFin, default=list, nullable=False)
    total_presupuesto: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=Decimal("0.00"), nullable=False)

    presupuesto_detalle: Mapped[dict] = mapped_column(JSONFin, nullable=True)  # días, otros gastos, agregados/quitados, nota
    anticipo_detalle: Mapped[dict] = mapped_column(JSONFin, nullable=True)     # valor, fecha, referencia, quién lo gestionó

    # Pago de Anticipo por Tesorería
    anticipo_pagado: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    fecha_pago_anticipo: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    comprobante_anticipo_adjunto_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), nullable=True)

    # Legalización
    legalizacion_detalle: Mapped[dict] = mapped_column(JSONFin, default=dict, nullable=True)

    # Campos de presentación del formulario que no participan en las reglas
    datos_front: Mapped[dict] = mapped_column(JSONFin, nullable=True)

    historial_cambios: Mapped[list] = mapped_column(JSONFin, default=list, nullable=False)
