"""Paridad Pagos urgentes, Cotizaciones/Anticipos y Servicios públicos:
campos bancarios y de identificación en los pagos, contratos fijos con
categorías en los servicios, tabla quotes y maestro de contratos.

Revision ID: 0006_pagos_quotes
Revises: 0005_cajas_dian
Create Date: 2026-11-26
"""
from alembic import op
import sqlalchemy as sa

from financiero.constantes import ESQUEMA_FIN

# revision identifiers, used by Alembic.
revision = "0006_pagos_quotes"
down_revision = "0005_cajas_dian"
branch_labels = None
depends_on = None


def upgrade() -> None:
    from financiero.database import Base
    import financiero.models  # noqa: F401  (registra todas las tablas)

    # Tablas nuevas (create_all idempotente, patrón de 0002/0004/0005)
    Base.metadata.create_all(bind=op.get_bind(),
                             tables=[t for t in Base.metadata.sorted_tables
                                     if t.schema == ESQUEMA_FIN and t.name in ("quotes", "cat_contratos_servicios")])

    # pagos_urgentes: datos bancarios, identificación y contabilizada
    pag = "pagos_urgentes"
    op.add_column(pag, sa.Column("nit", sa.String(50), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(pag, sa.Column("nit_pago", sa.String(50), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(pag, sa.Column("banco", sa.String(100), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(pag, sa.Column("no_cuenta", sa.String(50), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(pag, sa.Column("tipo_cuenta", sa.String(20), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(pag, sa.Column("area", sa.String(100), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(pag, sa.Column("tipo_doc", sa.String(30), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(pag, sa.Column("numero_doc", sa.String(50), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(pag, sa.Column("requerimiento", sa.Text(), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(pag, sa.Column("asunto", sa.Text(), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(pag, sa.Column("contabilizada", sa.Boolean(), nullable=False, server_default=sa.true()), schema=ESQUEMA_FIN)
    op.add_column(pag, sa.Column("origen", sa.String(15), nullable=False, server_default="requisicion"), schema=ESQUEMA_FIN)
    # Los pagos manuales y de anticipo no tienen requisición
    op.alter_column(pag, "requisicion_id", existing_type=sa.Uuid(), nullable=True, schema=ESQUEMA_FIN)
    op.alter_column(pag, "consecutivo_req", existing_type=sa.String(30), nullable=True, schema=ESQUEMA_FIN)

    # servicios_publicos: contratos fijos, categorías y soportes
    ser = "servicios_publicos"
    op.add_column(ser, sa.Column("categoria", sa.String(20), nullable=False, server_default="publicos"), schema=ESQUEMA_FIN)
    op.add_column(ser, sa.Column("concepto", sa.String(150), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(ser, sa.Column("contrato", sa.String(80), nullable=True, index=True), schema=ESQUEMA_FIN)
    op.add_column(ser, sa.Column("nit_tercero", sa.String(50), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(ser, sa.Column("tercero", sa.String(250), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(ser, sa.Column("fecha_exp", sa.String(60), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(ser, sa.Column("observacion", sa.Text(), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(ser, sa.Column("urgente_recepcion", sa.String(10), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(ser, sa.Column("url_pago", sa.String(500), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(ser, sa.Column("url_drive", sa.String(500), nullable=True), schema=ESQUEMA_FIN)


def downgrade() -> None:
    ser = "servicios_publicos"
    for col in ("url_drive", "url_pago", "urgente_recepcion", "observacion", "fecha_exp", "tercero",
                "nit_tercero", "contrato", "concepto", "categoria"):
        op.drop_column(ser, col, schema=ESQUEMA_FIN)
    pag = "pagos_urgentes"
    op.alter_column(pag, "consecutivo_req", existing_type=sa.String(30), nullable=False, schema=ESQUEMA_FIN)
    op.alter_column(pag, "requisicion_id", existing_type=sa.Uuid(), nullable=False, schema=ESQUEMA_FIN)
    for col in ("origen", "contabilizada", "asunto", "requerimiento", "numero_doc", "tipo_doc", "area",
                "tipo_cuenta", "no_cuenta", "banco", "nit_pago", "nit"):
        op.drop_column(pag, col, schema=ESQUEMA_FIN)
    op.execute(f'DROP TABLE IF EXISTS "{ESQUEMA_FIN}".cat_contratos_servicios CASCADE')
    op.execute(f'DROP TABLE IF EXISTS "{ESQUEMA_FIN}".quotes CASCADE')
