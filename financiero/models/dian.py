"""Soporte del circuito DIAN: transportadores omitidos, histórico de cargas
y token del portal (ex db.transporters / db.uploads / db.lastDianToken del
módulo externo)."""
import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from financiero.constantes import ESQUEMA_FIN
from financiero.database import Base
from financiero.models.base import JSONFin, TimestampMixin


class TransportadorOmitido(Base, TimestampMixin):
    """Proveedores de transporte cuyas facturas NO pasan por recepción (se
    archivan al cargar el ZIP/Excel del portal DIAN)."""
    __tablename__ = "transportadores_omitidos"
    __table_args__ = ({"schema": ESQUEMA_FIN},)

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    nit: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    nombre: Mapped[str] = mapped_column(String(250), nullable=False)
    notas: Mapped[str] = mapped_column(Text, nullable=True)
    activo: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)


class DianUpload(Base, TimestampMixin):
    """Registro de cada carga del portal DIAN (archivo, usuario y conteos)."""
    __tablename__ = "dian_uploads"
    __table_args__ = ({"schema": ESQUEMA_FIN},)

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    archivo: Mapped[str] = mapped_column(String(200), nullable=False)
    usuario: Mapped[str] = mapped_column(String(150), nullable=False)
    conteo_nuevas: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    conteo_duplicadas: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    conteo_propias: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    conteo_transportadores: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    conteo_otros: Mapped[int] = mapped_column(Integer, default=0, nullable=False)


class DianToken(Base):
    """Último token descargado del portal DIAN (para el cruce Token vs
    Contabilidad del Excel del Drive). Una sola fila."""
    __tablename__ = "dian_token"
    __table_args__ = ({"schema": ESQUEMA_FIN},)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    archivo: Mapped[str] = mapped_column(String(200), nullable=True)
    token: Mapped[list] = mapped_column(JSONFin, default=list, nullable=False)
    actualizado_en: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
