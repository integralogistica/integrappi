"""Contabilidad normalizada: facturas DIAN, pagos urgentes, servicios públicos,
cajas menores y movimientos contables.

Revision ID: 0002_contabilidad
Revises: 0001_fin_inicial
Create Date: 2026-11-24
"""
from alembic import op

from financiero.constantes import ESQUEMA_FIN

# revision identifiers, used by Alembic.
revision = "0002_contabilidad"
down_revision = "0001_fin_inicial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    from financiero.database import Base
    import financiero.models  # noqa: F401  (registra las tablas de flujo)
    import financiero.models.contabilidad  # noqa: F401  (las nuevas)
    # create_all es idempotente: solo crea las tablas que faltan
    # (las de 0001 ya existen con su versión registrada).
    Base.metadata.create_all(bind=op.get_bind(),
                             tables=[t for t in Base.metadata.sorted_tables
                                     if t.schema == ESQUEMA_FIN and t.name in (
                                         "facturas_dian", "pagos_urgentes", "servicios_publicos",
                                         "cajas_menores", "movimientos_contables")])


def downgrade() -> None:
    op.execute(f'DROP TABLE IF EXISTS "{ESQUEMA_FIN}".movimientos_contables CASCADE')
    op.execute(f'DROP TABLE IF EXISTS "{ESQUEMA_FIN}".cajas_menores CASCADE')
    op.execute(f'DROP TABLE IF EXISTS "{ESQUEMA_FIN}".servicios_publicos CASCADE')
    op.execute(f'DROP TABLE IF EXISTS "{ESQUEMA_FIN}".pagos_urgentes CASCADE')
    op.execute(f'DROP TABLE IF EXISTS "{ESQUEMA_FIN}".facturas_dian CASCADE')
