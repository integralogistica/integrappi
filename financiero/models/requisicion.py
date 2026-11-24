"""Requisiciones de compra + órdenes de compra.

# puerto de app/models/requisicion.py del módulo externo (schema fin,
# UUID genérico, JSONFin en vez de JSONB crudo).
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


class EstadoRequisicion(str, Enum):
    BORRADOR = "BORRADOR"
    PENDIENTE_JEFE = "PENDIENTE_JEFE"
    RECHAZADA_JEFE = "RECHAZADA_JEFE"
    PENDIENTE_COMPRAS = "PENDIENTE_COMPRAS"
    PENDIENTE_FINANCIERA = "PENDIENTE_FINANCIERA"
    RECHAZADA_FINANCIERA = "RECHAZADA_FINANCIERA"
    APROBADA = "APROBADA"
    EN_ORDEN_COMPRA = "EN_ORDEN_COMPRA"
    RECIBIDO_PARCIAL = "RECIBIDO_PARCIAL"
    RECIBIDO_TOTAL = "RECIBIDO_TOTAL"
    ANULADA = "ANULADA"


class Requisicion(Base, TimestampMixin, VersionMixin):
    __tablename__ = "requisiciones"
    __table_args__ = ({"schema": ESQUEMA_FIN},)

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    consecutivo: Mapped[str] = mapped_column(String(30), unique=True, index=True, nullable=False)
    oc_numero: Mapped[str] = mapped_column(String(30), unique=True, index=True, nullable=True)

    solicitante_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey(f"{ESQUEMA_FIN}.usuarios.id"), nullable=False
    )
    solicitante_nombre: Mapped[str] = mapped_column(String(150), nullable=False)
    area: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    lugar_entrega: Mapped[str] = mapped_column(String(150), nullable=False)

    es_urgente: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    motivo_urgencia: Mapped[str] = mapped_column(Text, nullable=True)

    proveedor_sugerido: Mapped[str] = mapped_column(String(200), nullable=True)
    proveedor_nit: Mapped[str] = mapped_column(String(50), nullable=True)

    # Datos del proveedor y de la entrega que el formulario FORM-CPR-011 ya captura
    proveedor_datos: Mapped[dict] = mapped_column(JSONFin, nullable=True)  # direccion, telefono, contacto, receptor, fecha_inicio, fecha_fin, cotizacion_ref
    observaciones: Mapped[str] = mapped_column(Text, nullable=True)

    # Pago urgente (lo propone Compras y lo decide quien aprueba al final)
    pago_urgente: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    pago_fecha: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    pago_aprobado: Mapped[bool] = mapped_column(Boolean, nullable=True)
    pago_decidido_por: Mapped[str] = mapped_column(String(150), nullable=True)

    # Detalle de líneas de compra
    items: Mapped[list] = mapped_column(JSONFin, default=list, nullable=False)
    subtotal: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=Decimal("0.00"), nullable=False)
    iva: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=Decimal("0.00"), nullable=False)
    total: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=Decimal("0.00"), nullable=False)

    estado: Mapped[EstadoRequisicion] = mapped_column(
        SQLEnum(
            EstadoRequisicion,
            native_enum=False,
            length=50,
            values_callable=lambda e: [m.value for m in e],
        ),
        default=EstadoRequisicion.PENDIENTE_JEFE,
        nullable=False,
        index=True,
    )

    # Aprobación Jefe de Área
    jefe_aprobador_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey(f"{ESQUEMA_FIN}.usuarios.id"), nullable=True
    )
    fecha_aprobacion_jefe: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    observacion_jefe: Mapped[str] = mapped_column(Text, nullable=True)

    # Gestión de Compras
    compras_analista_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey(f"{ESQUEMA_FIN}.usuarios.id"), nullable=True
    )
    fecha_compras: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    cotizaciones_adjuntos: Mapped[list] = mapped_column(JSONFin, default=list, nullable=True)

    # Aprobación Financiera (Controller si <= tope, Gerente Financiero si > tope)
    aprobador_financiero_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey(f"{ESQUEMA_FIN}.usuarios.id"), nullable=True
    )
    rol_aprobador_financiero: Mapped[str] = mapped_column(String(50), nullable=True)
    fecha_aprobacion_financiera: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    observacion_financiera: Mapped[str] = mapped_column(Text, nullable=True)

    # Recepción y entrega final
    recibido_por_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey(f"{ESQUEMA_FIN}.usuarios.id"), nullable=True
    )
    fecha_recibido: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    soporte_entrega_adjunto_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), nullable=True)

    # Entrega (Compras) y seguimiento a proveedor (lo califica quien hizo la
    # requisición al confirmar la recepción)
    entregado_en: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    entregado_por_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey(f"{ESQUEMA_FIN}.usuarios.id"), nullable=True
    )
    calificacion_proveedor: Mapped[dict] = mapped_column(JSONFin, nullable=True)

    # Campos de presentación del formulario que no participan en las reglas
    datos_front: Mapped[dict] = mapped_column(JSONFin, nullable=True)

    # Trazabilidad completa e inmutable
    historial_cambios: Mapped[list] = mapped_column(JSONFin, default=list, nullable=False)
