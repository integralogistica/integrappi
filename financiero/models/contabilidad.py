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
    LEGALIZACION = "LEGALIZACION"  # en camino a una caja menor (ex 'legalizacion')
    RADICADA = "RADICADA"        # contabilidad la radicó (con OC cuando aplica)
    CAUSADA = "CAUSADA"          # contabilizada: generó movimientos contables
    GLOSADA = "GLOSADA"          # con glosa/devolución al proveedor
    RECHAZADA = "RECHAZADA"      # rechazada con motivo (se puede reactivar o cruzar con NC)
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

    # Validación por área (ex 'validada'/'areaAprobacion' del original)
    validada: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    area_aprobacion: Mapped[str] = mapped_column(String(20), nullable=True)  # Compras|Gerencia|Operaciones|General
    nota_validacion: Mapped[str] = mapped_column(Text, nullable=True)
    fecha_solicitud_aprobacion: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)

    # Rechazo con motivo (catálogo de 5) y reactivación
    motivo_rechazo: Mapped[str] = mapped_column(String(120), nullable=True)
    fecha_rechazo: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    rechazado_por: Mapped[str] = mapped_column(String(150), nullable=True)

    # Cruce de facturas (nota crédito / refacturación reemplaza la rechazada)
    cruzada_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), ForeignKey(f"{ESQUEMA_FIN}.facturas_dian.id"), nullable=True)
    reemplaza_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), ForeignKey(f"{ESQUEMA_FIN}.facturas_dian.id"), nullable=True)
    reemplaza_factura: Mapped[str] = mapped_column(String(50), nullable=True)
    fecha_cruce: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    cruzado_por: Mapped[str] = mapped_column(String(150), nullable=True)

    # Trámite enriquecido y Drive Contable
    fecha_envio_contab: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    categoria_contable: Mapped[str] = mapped_column(String(20), nullable=True)  # 7 categorías de gasto
    num_orden_compra: Mapped[str] = mapped_column(String(30), nullable=True)
    observacion_recepcion: Mapped[str] = mapped_column(Text, nullable=True)
    estado_contable: Mapped[str] = mapped_column(String(15), nullable=True)  # Pendiente|Con Novedad|Contabilizado
    num_causacion: Mapped[str] = mapped_column(String(30), nullable=True)
    fecha_contabilizacion: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)

    # Cuenta de cobro manual y legalización en caja menor
    es_cuenta_cobro: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    origen_doc: Mapped[str] = mapped_column(String(40), nullable=True)   # Directo | 'Caja Menor: <sede>' | Proveedor
    caja_menor_sede: Mapped[str] = mapped_column(String(120), nullable=True)

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
    pago urgente (o manual, o desde un anticipo interno); Tesorería lo paga
    (con referencia) o lo devuelve."""
    __tablename__ = "pagos_urgentes"
    __table_args__ = (
        Index("ix_fin_pagos_fecha", "fecha_requerida"),
        {"schema": ESQUEMA_FIN},
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    requisicion_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), ForeignKey(f"{ESQUEMA_FIN}.requisiciones.id"), nullable=True)
    consecutivo_req: Mapped[str] = mapped_column(String(30), nullable=True)
    oc_numero: Mapped[str] = mapped_column(String(30), nullable=True)
    solicitante_nombre: Mapped[str] = mapped_column(String(150), nullable=False)
    proveedor: Mapped[str] = mapped_column(String(250), nullable=True)
    valor: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    fecha_requerida: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    estado: Mapped[str] = mapped_column(String(20), nullable=False, default=EstadoPagoUrgente.PENDIENTE.value, index=True)

    # Datos bancarios y de identificación del original (heredados del catálogo
    # de proveedores por NIT cuando existe; editables por Tesorería)
    nit: Mapped[str] = mapped_column(String(50), nullable=True)
    nit_pago: Mapped[str] = mapped_column(String(50), nullable=True)   # NIT de giro si difiere
    banco: Mapped[str] = mapped_column(String(100), nullable=True)
    no_cuenta: Mapped[str] = mapped_column(String(50), nullable=True)
    tipo_cuenta: Mapped[str] = mapped_column(String(20), nullable=True)
    area: Mapped[str] = mapped_column(String(100), nullable=True)
    tipo_doc: Mapped[str] = mapped_column(String(30), nullable=True)   # Factura | Cotización | Cuenta de cobro | Anticipo
    numero_doc: Mapped[str] = mapped_column(String(50), nullable=True)
    requerimiento: Mapped[str] = mapped_column(Text, nullable=True)    # texto copiable
    asunto: Mapped[str] = mapped_column(Text, nullable=True)           # texto copiable
    # Contabilidad habilita/bloquea el pago con el chip del original
    contabilizada: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    origen: Mapped[str] = mapped_column(String(15), default="requisicion", nullable=False)  # requisicion | manual | anticipo

    pagado_por_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), ForeignKey(f"{ESQUEMA_FIN}.usuarios.id"), nullable=True)
    fecha_pago: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    referencia: Mapped[str] = mapped_column(String(100), nullable=True)
    motivo_rechazo: Mapped[str] = mapped_column(Text, nullable=True)
    historial_cambios: Mapped[list] = mapped_column(JSONFin, default=list, nullable=False)


class ServicioPublico(Base, TimestampMixin, VersionMixin):
    """Servicios públicos/corporativos por inmueble y periodo (ex 'utilities').
    El circuito del original replica los contratos fijos cada mes desde
    fin.cat_contratos_servicios (estado NO_GENERADO hasta que llega la factura)."""
    __tablename__ = "servicios_publicos"
    __table_args__ = ({"schema": ESQUEMA_FIN},)

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    servicio: Mapped[str] = mapped_column(String(80), nullable=False)        # energía, agua, internet...
    inmueble: Mapped[str] = mapped_column(String(150), nullable=False)
    periodo: Mapped[str] = mapped_column(String(7), nullable=False)          # AAAA-MM
    valor: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=Decimal("0.00"), nullable=False)
    fecha_limite: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    # PENDIENTE | CAUSADO | ENVIADO_CONTAB | PAGADO | PAGADO_NO_CONTAB | NO_GENERADO
    estado: Mapped[str] = mapped_column(String(20), nullable=False, default="PENDIENTE")
    fecha_pago: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    datos: Mapped[dict] = mapped_column(JSONFin, nullable=True)              # soporte, extras…

    # Contrato fijo y categorías del original
    categoria: Mapped[str] = mapped_column(String(20), default="publicos", nullable=False)  # publicos|corporativos|orion
    concepto: Mapped[str] = mapped_column(String(150), nullable=True)
    contrato: Mapped[str] = mapped_column(String(80), nullable=True, index=True)
    nit_tercero: Mapped[str] = mapped_column(String(50), nullable=True)
    tercero: Mapped[str] = mapped_column(String(250), nullable=True)
    fecha_exp: Mapped[str] = mapped_column(String(60), nullable=True)        # día de corte («20 de cada mes»)
    observacion: Mapped[str] = mapped_column(Text, nullable=True)
    urgente_recepcion: Mapped[str] = mapped_column(String(10), nullable=True)  # URGENTE | PROXIMA | null
    url_pago: Mapped[str] = mapped_column(String(500), nullable=True)        # portal de pago
    url_drive: Mapped[str] = mapped_column(String(500), nullable=True)       # soporte externo


class EstadoCajaMenor(str, Enum):
    """Circuito del original (FORM-FIN-007): el responsable radica → jefe
    inmediato → Controller → Contabilidad (FC) → cruce Tesorería (110510) →
    reembolso. EN_EDICION es el borrador previo a radicar."""
    EN_EDICION = "EN_EDICION"
    RADICADA = "RADICADA"
    APROBADA_JEFE = "APROBADA_JEFE"
    APROBADA_CONTROLLER = "APROBADA_CONTROLLER"
    CONTABILIZADA = "CONTABILIZADA"
    CRUZADA = "CRUZADA"
    REEMBOLSADA = "REEMBOLSADA"
    RECHAZADA = "RECHAZADA"


class CajaMenor(Base, TimestampMixin, VersionMixin):
    """Legalización de gastos por sede (ex 'minorCash')."""
    __tablename__ = "cajas_menores"
    __table_args__ = ({"schema": ESQUEMA_FIN},)

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    consecutivo: Mapped[str] = mapped_column(String(20), unique=True, nullable=True, index=True)  # CM-AAAA-NNN al radicar
    sede: Mapped[str] = mapped_column(String(120), nullable=False, index=True)
    sede_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), ForeignKey(f"{ESQUEMA_FIN}.cat_sedes_cm.id"), nullable=True)
    responsable: Mapped[str] = mapped_column(String(150), nullable=False)
    responsable_cc: Mapped[str] = mapped_column(String(20), nullable=True)
    centro_costo_principal: Mapped[str] = mapped_column(String(60), nullable=True)
    periodo: Mapped[str] = mapped_column(String(7), nullable=False)          # AAAA-MM
    saldo_inicial: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=Decimal("0.00"), nullable=False)
    estado: Mapped[str] = mapped_column(String(20), nullable=False, default="EN_EDICION")
    movimientos: Mapped[list] = mapped_column(JSONFin, default=list, nullable=False)       # renglones (folio, tipoDocumento, contabilizado, cruzado...)
    datos: Mapped[dict] = mapped_column(JSONFin, nullable=True)
    politica: Mapped[dict] = mapped_column(JSONFin, nullable=True)           # {fechaEnvio, diaSemanaEnvio, aTiempoParaViernes, fechaEstimadaReembolso}
    total_reembolso: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=Decimal("0.00"), nullable=False)


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


class EstadoQuote(str, Enum):
    PENDIENTE = "PENDIENTE"      # cotización o anticipo esperando cruce con factura DIAN
    CRUZADA = "CRUZADA"          # vinculada a una factura cargada
    LEGALIZADA = "LEGALIZADA"    # con soportes/recibos registrados
    CERRADA = "CERRADA"


class Quote(Base, TimestampMixin, VersionMixin):
    """Cotizaciones y anticipos internos del original (ex colección 'quotes'):
    anticipos con comprobante EGC/ANT/BG-TR (proceso Cajas Menores / Viajes
    Viáticos / Otros) y cotizaciones que se cruzan con la factura DIAN que las
    reemplaza. El checkbox «Programar también en Pagos Urgentes» crea el
    PagoUrgente asociado (pago_id)."""
    __tablename__ = "quotes"
    __table_args__ = ({"schema": ESQUEMA_FIN},)

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tipo: Mapped[str] = mapped_column(String(12), nullable=False, default="COTIZACION")     # COTIZACION | ANTICIPO
    tipo_comprobante: Mapped[str] = mapped_column(String(12), nullable=True)  # EGC | ANT | BG-TR
    proceso: Mapped[str] = mapped_column(String(30), nullable=True)          # CAJAS MENORES | VIAJES VIATICOS | OTROS
    sede_cm: Mapped[str] = mapped_column(String(120), nullable=True)
    proveedor: Mapped[str] = mapped_column(String(250), nullable=True)
    nit: Mapped[str] = mapped_column(String(50), nullable=True)
    valor: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=Decimal("0.00"), nullable=False)
    fecha: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    estado: Mapped[str] = mapped_column(String(20), nullable=False, default=EstadoQuote.PENDIENTE.value, index=True)

    factura_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), ForeignKey(f"{ESQUEMA_FIN}.facturas_dian.id"), nullable=True)
    pago_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), ForeignKey(f"{ESQUEMA_FIN}.pagos_urgentes.id"), nullable=True)

    legalizacion: Mapped[dict] = mapped_column(JSONFin, nullable=True)  # {fecha, observaciones, recibos[]}
    historial_cambios: Mapped[list] = mapped_column(JSONFin, default=list, nullable=False)
