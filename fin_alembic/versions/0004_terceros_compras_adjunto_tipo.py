"""Terceros de Compras (proveedores nuevos) + tipo de documento en adjuntos.

Revision ID: 0004_terceros_compras
Revises: 0003_permisos_usuario
Create Date: 2026-11-26
"""
from alembic import op
import sqlalchemy as sa

from financiero.constantes import ESQUEMA_FIN

# revision identifiers, used by Alembic.
revision = "0004_terceros_compras"
down_revision = "0003_permisos_usuario"
branch_labels = None
depends_on = None


def upgrade() -> None:
    from financiero.database import Base
    import financiero.models  # noqa: F401  (registra todas las tablas)
    import financiero.models.tercero  # noqa: F401  (la nueva)
    # create_all es idempotente: solo crea las tablas que faltan (patrón 0002)
    Base.metadata.create_all(bind=op.get_bind(),
                             tables=[t for t in Base.metadata.sorted_tables
                                     if t.schema == ESQUEMA_FIN and t.name == "terceros_compras"])
    # Tipado de adjuntos (rut / cert_bancaria / camara_comercio / cotizacion)
    op.add_column(
        "adjuntos",
        sa.Column("tipo_documento", sa.String(30), nullable=True),
        schema=ESQUEMA_FIN,
    )


def downgrade() -> None:
    op.drop_column("adjuntos", "tipo_documento", schema=ESQUEMA_FIN)
    op.execute(f'DROP TABLE IF EXISTS "{ESQUEMA_FIN}".terceros_compras CASCADE')
