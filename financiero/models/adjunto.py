"""Adjuntos del Módulo Financiero.

# puerto de app/models/adjunto.py del módulo externo. Cambio: el archivo vive
en Google Cloud Storage (bucket privado `integrapp-privado`, carpeta
Financiero/), no en disco local — `ruta_almacenamiento` guarda la ruta del
blob y la descarga es por URL firmada.
"""
import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, ForeignKey, Integer, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from financiero.constantes import ESQUEMA_FIN
from financiero.database import Base


class Adjunto(Base):
    __tablename__ = "adjuntos"
    __table_args__ = ({"schema": ESQUEMA_FIN},)

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    nombre_archivo: Mapped[str] = mapped_column(String(255), nullable=False)
    tipo_mime: Mapped[str] = mapped_column(String(100), nullable=False)
    tamano_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    hash_sha256: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    # Ruta del blob en GCS (ej: Financiero/traslados/TRP-2026-0001/2026-11-24/abcd_cotizacion.pdf)
    ruta_almacenamiento: Mapped[str] = mapped_column(String(500), nullable=False)

    modulo_origen: Mapped[str] = mapped_column(String(50), nullable=False, index=True)  # requisicion, traslado, viaje, factura, soporte*...
    referencia_id: Mapped[str] = mapped_column(String(100), nullable=True, index=True)

    subido_por_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey(f"{ESQUEMA_FIN}.usuarios.id"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
