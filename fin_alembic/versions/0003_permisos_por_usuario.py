"""Permisos finos por usuario: fin.usuarios.permisos (JSONB, solo overrides).

Revision ID: 0003_permisos_usuario
Revises: 0002_contabilidad
Create Date: 2026-11-24
"""
from alembic import op
import sqlalchemy as sa

from financiero.constantes import ESQUEMA_FIN

# revision identifiers, used by Alembic.
revision = "0003_permisos_usuario"
down_revision = "0002_contabilidad"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "usuarios",
        sa.Column("permisos", sa.JSON(), nullable=True),
        schema=ESQUEMA_FIN,
    )


def downgrade() -> None:
    op.drop_column("usuarios", "permisos", schema=ESQUEMA_FIN)
