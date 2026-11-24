"""Mixins comunes de las tablas del Módulo Financiero.

# puerto de app/models/base.py del módulo externo (literal, salvo el tipo
JSONB que aquí degrada a JSON en SQLite para poder correr los tests sin
Postgres).
"""
from datetime import datetime, timezone

from sqlalchemy import JSON, DateTime, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

# JSONB en Postgres; JSON plano en SQLite (tests de lógica sin Postgres).
JSONFin = JSONB().with_variant(JSON(), "sqlite")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False
    )
    created_by: Mapped[str] = mapped_column(String(100), nullable=True)
    updated_by: Mapped[str] = mapped_column(String(100), nullable=True)


class VersionMixin:
    """Bloqueo optimista para prevenir condiciones de carrera (HTTP 409)."""
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
