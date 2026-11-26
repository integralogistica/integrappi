"""Terceros de Compras (proveedores nuevos) con su aprobación y carga en software.

# puerto de db.tercerosCompras del módulo externo (vivía en Firestore, sin
# backend). Aquí es tabla propia: el solicitante lo crea desde la requisición
# con los 3 documentos obligatorios (RUT, certificación bancaria y cámara de
# comercio como adjuntos modulo 'tercero'), Compras aprueba/rechaza y
# Contabilidad marca la creación en el software contable (Vulcano).
"""
import uuid
from datetime import datetime
from enum import Enum

from sqlalchemy import Boolean, CheckConstraint, DateTime, Enum as SQLEnum, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from financiero.constantes import ESQUEMA_FIN
from financiero.database import Base
from financiero.models.base import JSONFin, TimestampMixin, VersionMixin


class EstadoTercero(str, Enum):
    PENDIENTE_COMPRAS = "pendiente_compras"
    APROBADO = "aprobado"
    RECHAZADO_COMPRAS = "rechazado_compras"


class TerceroCompras(Base, TimestampMixin, VersionMixin):
    __tablename__ = "terceros_compras"
    __table_args__ = (
        CheckConstraint(
            "estado_compras IN ('pendiente_compras', 'aprobado', 'rechazado_compras')",
            name="ck_terceros_estado",
        ),
        {"schema": ESQUEMA_FIN},
    )

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    razon_social: Mapped[str] = mapped_column(String(250), nullable=False, index=True)
    nit: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    nit_pago: Mapped[str] = mapped_column(String(50), nullable=True)
    tipo_cuenta: Mapped[str] = mapped_column(String(20), nullable=True)  # Ahorros / Corriente
    banco: Mapped[str] = mapped_column(String(100), nullable=True)
    no_cuenta: Mapped[str] = mapped_column(String(50), nullable=True)

    area: Mapped[str] = mapped_column(String(100), nullable=True)
    origen_requisicion: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    requisicion_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), nullable=True)
    consecutivo_req: Mapped[str] = mapped_column(String(30), nullable=True)

    creado_por: Mapped[str] = mapped_column(String(100), nullable=True)
    creado_por_nombre: Mapped[str] = mapped_column(String(150), nullable=True)

    # Paso 1: aprobación de Compras
    estado_compras: Mapped[str] = mapped_column(
        SQLEnum(
            EstadoTercero,
            native_enum=False,
            length=30,
            values_callable=lambda e: [m.value for m in e],
        ),
        default=EstadoTercero.PENDIENTE_COMPRAS,
        nullable=False,
        index=True,
    )
    motivo_rechazo_compras: Mapped[str] = mapped_column(Text, nullable=True)
    aprobado_por: Mapped[str] = mapped_column(String(150), nullable=True)
    fecha_aprobacion: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)

    # Paso 2: creación en el software contable (Contabilidad)
    creado_en_software: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    creado_en_software_por: Mapped[str] = mapped_column(String(150), nullable=True)
    fecha_creado_en_software: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)

    historial_cambios: Mapped[list] = mapped_column(JSONFin, default=list, nullable=False)
