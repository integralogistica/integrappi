"""Usuarios del Módulo Financiero (identidad = correo de IntegrApp).

# puerto de app/models/usuario.py del módulo externo. Cambios: schema `fin`,
# UUID genérico de SQLAlchemy, y SIN hashed_password (aquí el login es el de
IntegrApp; no hay modo local).
"""
import uuid
from enum import Enum

from sqlalchemy import Boolean, Enum as SQLEnum, Index, String, Uuid, func
from sqlalchemy.orm import Mapped, mapped_column

from financiero.constantes import ESQUEMA_FIN
from financiero.database import Base
from financiero.models.base import JSONFin, TimestampMixin


class RolUsuario(str, Enum):
    ADMIN = "admin"
    TESORERIA = "tesoreria"
    CONTABILIDAD = "contabilidad"
    CALIDAD = "calidad"
    CONSULTA = "consulta"
    REQUISICION = "requisicion"
    JEFE_AREA = "jefe_area"
    COMPRAS = "compras"
    CONTROLLER = "controller"
    GERENTE_FINANCIERO = "gerente_financiero"
    PRESIDENCIA = "presidencia"
    AUTORIZADOR_VIAJES = "autorizador_viajes"


class Usuario(Base, TimestampMixin):
    __tablename__ = "usuarios"

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    username: Mapped[str] = mapped_column(String(60), unique=True, index=True, nullable=False)
    nombre: Mapped[str] = mapped_column(String(150), nullable=False)
    correo: Mapped[str] = mapped_column(String(150), unique=True, index=True, nullable=False)
    rol: Mapped[RolUsuario] = mapped_column(
        SQLEnum(
            RolUsuario,
            native_enum=False,
            length=50,
            values_callable=lambda e: [m.value for m in e],
        ),
        nullable=False,
        default=RolUsuario.CONSULTA,
    )
    area: Mapped[str] = mapped_column(String(100), nullable=True)
    activo: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # Casillas y privilegios especiales
    es_aprobador_pagos: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    es_admin_adicional: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    es_autorizador_viajes: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    # Permisos finos por usuario: {seccion: true|false} — SOLO los que difieren
    # del rol (vacío/None = las secciones por defecto de su rol). El backend lo
    # respeta en las lecturas de cada módulo.
    permisos: Mapped[dict] = mapped_column(JSONFin, nullable=True)

    __table_args__ = (
        # El correo es la identidad (el de Integra One): único sin distinguir mayúsculas
        Index("uq_fin_usuarios_correo_lower", func.lower(correo), unique=True),
        # Restricción estricta de base de datos: solo 1 jefe de área activo por área
        Index(
            "uq_fin_jefe_activo_por_area",
            "area",
            unique=True,
            postgresql_where=(rol == RolUsuario.JEFE_AREA) & (activo == True),
            sqlite_where=(rol == RolUsuario.JEFE_AREA) & (activo == True),
        ),
        {"schema": ESQUEMA_FIN},
    )
