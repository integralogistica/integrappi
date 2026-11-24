"""Contabilidad normalizada del Módulo Financiero.

Diseño NUEVO (no puerto): en el módulo externo todo esto vivía en UN
documento JSON (`base_central`) con un solo `version` global — cualquier
escritura simultánea en cualquier sub-módulo disparaba 409. Aquí cada
entidad es una tabla con su propio versionado y los campos flexibles en
JSONB (siguiendo el diseño objetivo de MODELO_RELACIONAL_OBJETIVO.md).
"""
import uuid
from datetime import datetime
from decimal import Decimal
from enum import Enum

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Numeric, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from financiero.constantes import ESQUEMA_FIN
from financiero.database import Base
from financiero.models.base import JSONFin, TimestampMixin, VersionMixin


class EstadoFactura(str, Enum):
    PENDIENTE = "PENDIENTE"      # cargada desde Excel/XML DIAN, espera trámite
    TRAMITADA = "TRAMITADA"      # enviada a trámite (aprobación de compra)
    RADICADA = "RADICADA"        # contabilidad la radicó (con OC cuando aplica)
    CAUSADA = "CAUSADA"          # contabilizada: generó movimientos contables
    GLOSADA = "GLOSADA"          # con glosa/devolución al proveedor
    ANULADA = "ANULADA"


class FacturaDian(Base, TimestampMixin, VersionMixin):
    __tablename__ = "facturas_dian"
    __table_args__ = (
        Index("ix_fin_facturas_proveedor", "proveedor"),
        Index("ix_fin_facturas_estado_fecha", "estado", "fecha_emision"),
        {"schema": ESQUEMA_FIN},
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    cufe: Mapped[str] = mapped_column(String(150), nullable=True, index=True)   # CUFE/CUDE; sintético si no trae
    numero_factura: Mapped[str] = mapped_column(String(50), nullable=False)     # prefijo+folio o 'S/N'
    nit_proveedor: Mapped[str] = mapped_column(String(50), nullable=False)
    proveedor: Mapped[str] = mapped_column(String(250), nullable=False)
    fecha_emision: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    tipo_documento: Mapped[str] = mapped_column(String(80), nullable=False, default="Factura electrónica")
    origen: Mapped[str] = mapped_column(String(10), nullable=False, default="excel")  # excel | xml
    iva: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=Decimal("0.00"), nullable=False)
    total: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=Decimal("0.00"), nullable=False)
    estado: Mapped[str] = mapped_column(String(20), nullable=False, default=EstadoFactura.PENDIENTE.value, index=True)

    # Trámite / radicación / causación
    oc_vinculada: Mapped[str] = mapped_column(String(30), nullable=True)
    radicada_por_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), ForeignKey(f"{ESQUEMA_FIN}.usuarios.id"), nullable=True)
    causada_por_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), ForeignKey(f"{ESQUEMA_FIN}.usuarios.id"), nullable=True)
    fecha_radicacion: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    fecha_causacion: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)

    # Campos flexibles (lo demás del parseo UBL/Excel y las glosas)
    detalles: Mapped[dict] = mapped_column(JSONFin, nullable=True)
    glosas: Mapped[list] = mapped_column(JSONFin, default=list, nullable=True)
    historial_cambios: Mapped[list] = mapped_column(JSONFin, default=list, nullable=False)


class EstadoPagoUrgente(str, Enum):
    PENDIENTE = "PENDIENTE"
    PAGADO = "PAGADO"
    RECHAZADO = "RECHAZADO"


class PagoUrgente(Base, TimestampMixin, VersionMixin):
    """Nace cuando la aprobación financiera de una requisición aprueba el
    pago urgente; Tesorería lo paga (con referencia) o lo devuelve."""
    __tablename__ = "pagos_urgentes"
    __table_args__ = (
        Index("ix_fin_pagos_fecha", "fecha_requerida"),
        {"schema": ESQUEMA_FIN},
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    requisicion_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), ForeignKey(f"{ESQUEMA_FIN}.requisiciones.id"), nullable=False)
    consecutivo_req: Mapped[str] = mapped_column(String(30), nullable=False)
    oc_numero: Mapped[str] = mapped_column(String(30), nullable=True)
    solicitante_nombre: Mapped[str] = mapped_column(String(150), nullable=False)
    proveedor: Mapped[str] = mapped_column(String(250), nullable=True)
    valor: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    fecha_requerida: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    estado: Mapped[str] = mapped_column(String(20), nullable=False, default=EstadoPagoUrgente.PENDIENTE.value, index=True)

    pagado_por_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), ForeignKey(f"{ESQUEMA_FIN}.usuarios.id"), nullable=True)
    fecha_pago: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    referencia: Mapped[str] = mapped_column(String(100), nullable=True)
    motivo_rechazo: Mapped[str] = mapped_column(Text, nullable=True)
    historial_cambios: Mapped[list] = mapped_column(JSONFin, default=list, nullable=False)


class ServicioPublico(Base, TimestampMixin, VersionMixin):
    """Servicios públicos/corporativos por inmueble y periodo (ex 'utilities')."""
    __tablename__ = "servicios_publicos"
    __table_args__ = ({"schema": ESQUEMA_FIN},)

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    servicio: Mapped[str] = mapped_column(String(80), nullable=False)        # energía, agua, internet...
    inmueble: Mapped[str] = mapped_column(String(150), nullable=False)
    periodo: Mapped[str] = mapped_column(String(7), nullable=False)          # AAAA-MM
    valor: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=Decimal("0.00"), nullable=False)
    fecha_limite: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    estado: Mapped[str] = mapped_column(String(20), nullable=False, default="PENDIENTE")  # PENDIENTE|CAUSADO|PAGADO
    fecha_pago: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    datos: Mapped[dict] = mapped_column(JSONFin, nullable=True)              # contrato, portal de pago, soporte…


class CajaMenor(Base, TimestampMixin, VersionMixin):
    """Legalización de gastos por sede (ex 'minorCash')."""
    __tablename__ = "cajas_menores"
    __table_args__ = ({"schema": ESQUEMA_FIN},)

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sede: Mapped[str] = mapped_column(String(120), nullable=False, index=True)
    responsable: Mapped[str] = mapped_column(String(150), nullable=False)
    periodo: Mapped[str] = mapped_column(String(7), nullable=False)          # AAAA-MM
    saldo_inicial: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=Decimal("0.00"), nullable=False)
    estado: Mapped[str] = mapped_column(String(20), nullable=False, default="EN_EDICION")  # EN_EDICION|LEGALIZADA|CONTABILIZADA
    movimientos: Mapped[list] = mapped_column(JSONFin, default=list, nullable=False)       # renglones con adjuntos GCS
    datos: Mapped[dict] = mapped_column(JSONFin, nullable=True)


class MovimientoContable(Base, TimestampMixin):
    """Asientos generados al causar facturas / contabilizar cajas (ex
    'accountingEntries'). Sin versionado: son el registro histórico."""
    __tablename__ = "movimientos_contables"
    __table_args__ = ({"schema": ESQUEMA_FIN},)

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    origen: Mapped[str] = mapped_column(String(30), nullable=False, index=True)  # factura | caja | servicio
    referencia_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), nullable=True, index=True)
    referencia: Mapped[str] = mapped_column(String(50), nullable=True)           # nº factura / sede+periodo
    periodo: Mapped[str] = mapped_column(String(7), nullable=False)              # AAAA-MM de causación
    cuenta: Mapped[str] = mapped_column(String(60), nullable=False)
    tercero: Mapped[str] = mapped_column(String(250), nullable=True)             # proveedor / sede
    debito: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=Decimal("0.00"), nullable=False)
    credito: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=Decimal("0.00"), nullable=False)
    detalle: Mapped[dict] = mapped_column(JSONFin, nullable=True)
    creado_por_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), ForeignKey(f"{ESQUEMA_FIN}.usuarios.id"), nullable=True)
