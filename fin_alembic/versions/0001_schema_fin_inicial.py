"""Schema fin inicial: todas las tablas del Módulo Financiero.

Revision ID: 0001_fin_inicial
Revises:
Create Date: 2026-11-24

Crea el schema `fin` (si no existe) y todas las tablas del módulo usando el
metadata de `financiero.models` (fuente única de verdad: lo que está en los
modelos es lo que se crea — imposible que migración y ORM diverjan).

Aplasta las 6 migraciones del módulo externo (001–006) en una sola porque aquí
se empieza de cero. Las futuras alteraciones sí se escriben a mano.

Downgrade: DROP SCHEMA fin CASCADE — solo afecta al módulo financiero, jamás
a informe_guias_tms ni a nada de la app existente.
"""
from alembic import op

from financiero.constantes import ESQUEMA_FIN

# revision identifiers, used by Alembic.
revision = "0001_fin_inicial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "sqlite":
        # SQLite (tests) no tiene schemas: allá se usa create_all con ATTACH.
        op.execute(f'CREATE SCHEMA IF NOT EXISTS "{ESQUEMA_FIN}"')
    from financiero.database import Base
    import financiero.models  # noqa: F401  (registra todas las tablas)
    Base.metadata.create_all(bind=bind)


def downgrade() -> None:
    op.execute(f'DROP SCHEMA IF EXISTS "{ESQUEMA_FIN}" CASCADE')
