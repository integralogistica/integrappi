"""Registro de todas las tablas del Módulo Financiero.

# puerto de app/models/__init__.py (sin BaseCentral: la contabilidad se
normaliza en tablas propias que llegan con la Fase 4).
"""
from financiero.database import Base
from financiero.models.base import TimestampMixin, VersionMixin, JSONFin
from financiero.models.usuario import Usuario, RolUsuario
from financiero.models.consecutivo import Consecutivo
from financiero.models.requisicion import Requisicion, EstadoRequisicion
from financiero.models.traslado import Traslado, EstadoTraslado
from financiero.models.viaje import Viaje, EstadoViaje, CiudadViaje
from financiero.models.adjunto import Adjunto
from financiero.models.catalogo import CentroCosto, Proveedor, PresupuestoResponsable, PersonalViaje
from financiero.models.auditoria import RegistroAuditoria
from financiero.models.configuracion import Configuracion
from financiero.models.contabilidad import (
    CajaMenor, EstadoFactura, EstadoPagoUrgente, FacturaDian,
    MovimientoContable, PagoUrgente, ServicioPublico,
)

__all__ = [
    "Base",
    "TimestampMixin",
    "VersionMixin",
    "JSONFin",
    "Usuario",
    "RolUsuario",
    "Consecutivo",
    "Requisicion",
    "EstadoRequisicion",
    "Traslado",
    "EstadoTraslado",
    "Viaje",
    "EstadoViaje",
    "CiudadViaje",
    "Adjunto",
    "CentroCosto",
    "Proveedor",
    "PresupuestoResponsable",
    "PersonalViaje",
    "RegistroAuditoria",
    "Configuracion",
    "CajaMenor",
    "EstadoFactura",
    "EstadoPagoUrgente",
    "FacturaDian",
    "MovimientoContable",
    "PagoUrgente",
    "ServicioPublico",
]
