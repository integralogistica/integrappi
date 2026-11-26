"""Catálogos sembrados del Módulo Financiero (siembra inicial con
scripts/fin_semilla.py desde los datos del módulo externo).

# puerto de app/models/catalogo.py del módulo externo (schema fin) +
# tablas nuevas de presupuesto-responsables y personal-viajes que allá vivían
# como JSON estático del front.
"""
import uuid

from sqlalchemy import Boolean, Integer, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from financiero.constantes import ESQUEMA_FIN
from financiero.database import Base
from financiero.models.base import JSONFin


class CentroCosto(Base):
    __tablename__ = "cat_centros_costo"
    __table_args__ = ({"schema": ESQUEMA_FIN},)

    codigo: Mapped[str] = mapped_column(String(50), primary_key=True)
    nombre: Mapped[str] = mapped_column(String(200), nullable=False, index=True)
    area: Mapped[str] = mapped_column(String(100), nullable=True)
    activo: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)


class Proveedor(Base):
    __tablename__ = "cat_proveedores"
    __table_args__ = ({"schema": ESQUEMA_FIN},)

    nit: Mapped[str] = mapped_column(String(50), primary_key=True)
    razon_social: Mapped[str] = mapped_column(String(250), nullable=False, index=True)
    correo: Mapped[str] = mapped_column(String(150), nullable=True)
    telefono: Mapped[str] = mapped_column(String(50), nullable=True)
    banco: Mapped[str] = mapped_column(String(100), nullable=True)
    tipo_cuenta: Mapped[str] = mapped_column(String(50), nullable=True)
    numero_cuenta: Mapped[str] = mapped_column(String(50), nullable=True)
    activo: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)


class PresupuestoResponsable(Base):
    """Presupuesto por responsable/rubro/mes (base de los traslados).

    En el módulo externo era `data/presupuesto-responsables.json` cargado por
    el front: {RESPONSABLE: {"Grupo › Cuenta": {"1": [presupuesto, ejecutado], ...}}}.
    Aquí una fila por (responsable, rubro, mes).
    """
    __tablename__ = "cat_presupuesto_responsables"
    __table_args__ = ({"schema": ESQUEMA_FIN},)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    periodo: Mapped[int] = mapped_column(Integer, nullable=False, index=True)  # año
    responsable: Mapped[str] = mapped_column(String(150), nullable=False, index=True)
    rubro: Mapped[str] = mapped_column(String(250), nullable=False)  # "Grupo › Cuenta"
    mes: Mapped[int] = mapped_column(Integer, nullable=False)  # 1..12
    presupuesto: Mapped[float] = mapped_column(nullable=False, default=0)
    ejecutado: Mapped[float] = mapped_column(nullable=False, default=0)


class PersonalViaje(Base):
    """Personal habilitado para viajar con su categoría de tarifa (A/B/C) y
    destinos frecuentes (ex `data/personal-viajes.json` del front)."""
    __tablename__ = "cat_personal_viajes"
    __table_args__ = ({"schema": ESQUEMA_FIN},)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    nombre: Mapped[str] = mapped_column(String(150), nullable=False, index=True)
    cargo: Mapped[str] = mapped_column(String(150), nullable=True)
    categoria: Mapped[str] = mapped_column(String(2), nullable=True)  # A/B/C
    activo: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)


class SedeCajaMenor(Base):
    """Maestro oficial de sedes de caja menor (ex MAESTRO_CAJAS_MENORES_SEDES
    del front): responsable, cédula, jefe inmediato, CCO y subcuenta 110510xx
    del cruce en Tesorería."""
    __tablename__ = "cat_sedes_cm"
    __table_args__ = ({"schema": ESQUEMA_FIN},)

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    nombre: Mapped[str] = mapped_column(String(120), unique=True, nullable=False)  # id del original (MONTEVIDEO, GALAPA...)
    etiqueta: Mapped[str] = mapped_column(String(150), nullable=True)              # 'CEDI Montevideo'...
    responsable: Mapped[str] = mapped_column(String(150), nullable=True)
    identificacion: Mapped[str] = mapped_column(String(20), nullable=True)
    jefe_inmediato: Mapped[str] = mapped_column(String(150), nullable=True)        # nombre; se mapea a fin.usuarios
    cco: Mapped[str] = mapped_column(String(20), nullable=True)
    cuenta: Mapped[str] = mapped_column(String(20), nullable=True)                 # 110510xx
    activo: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)


class ContratoServicio(Base):
    """Maestro de contratos fijos de servicios públicos (ex getMasterContratos
    que deducía de históricos): cada mes se replica una fila por contrato en
    fin.servicios_publicos vía POST /fin/servicios/generar-periodo."""
    __tablename__ = "cat_contratos_servicios"
    __table_args__ = ({"schema": ESQUEMA_FIN},)

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    servicio: Mapped[str] = mapped_column(String(80), nullable=False)
    inmueble: Mapped[str] = mapped_column(String(150), nullable=False)
    categoria: Mapped[str] = mapped_column(String(20), default="publicos", nullable=False)
    concepto: Mapped[str] = mapped_column(String(150), nullable=True)
    contrato: Mapped[str] = mapped_column(String(80), unique=True, nullable=False)
    nit_tercero: Mapped[str] = mapped_column(String(50), nullable=True)
    tercero: Mapped[str] = mapped_column(String(250), nullable=True)
    fecha_exp: Mapped[str] = mapped_column(String(60), nullable=True)     # día de corte
    url_pago: Mapped[str] = mapped_column(String(500), nullable=True)
    activo: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
