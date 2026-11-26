"""Paridad Cajas Menores y Facturas DIAN: flujo de 6 etapas de caja, sedes
oficiales, y los campos de validación/rechazo/cruce/drive contable de facturas.

Revision ID: 0005_cajas_dian
Revises: 0004_terceros_compras
Create Date: 2026-11-26
"""
from alembic import op
import sqlalchemy as sa

from financiero.constantes import ESQUEMA_FIN

# revision identifiers, used by Alembic.
revision = "0005_cajas_dian"
down_revision = "0004_terceros_compras"
branch_labels = None
depends_on = None

# Maestro oficial de sedes de caja menor (ex MAESTRO_CAJAS_MENORES_SEDES del front)
SEDES_CM_SEED = [
    # (nombre, etiqueta, responsable, identificacion, jefe_inmediato, cco, cuenta)
    ("MONTEVIDEO", "CEDI Montevideo", "CINDY PAOLA VELANDIA GONZALEZ", "1013632389", "DAVID NAVARRO", "519104003", "11051003"),
    ("GALAPA", "CEDI Galapa", "FRANKLIN GRAGORIO ALVAREZ MARMOL", "1234890079", "REGERIO GALVIS", "519104007", "11051007"),
    ("YUMBO", "CEDI Yumbo (Cali)", "JEFFERSON CARDENAS SERRANO", "1013632389", "REGERIO GALVIS", "519104002", "11051008"),
    ("INVENTARIO", "Inventario", "JOSE NICOLAS MONSALVE TENJO", "1033727439", "REGERIO GALVIS", "519007001", "11051006"),
    ("GIRARDOTA", "CEDI Girardota (Medellín)", "MARIA VIRGINIA VALENCIA ZAPATA", "1020396387", "REGERIO GALVIS", "519104005", "11051005"),
    ("BUCARAMANGA", "CEDI Bucaramanga", "JULIO AMILCAR GUALDRON NIETO", "91218995", "REGERIO GALVIS", "519104008", "11051009"),
    ("FUNZA", "CEDI Funza (San Diego)", "LAURA VIVIANA VALDERRAMA CRUZ", "53036712", "WILSON GUITIERREZ", "519104004", "11051004"),
    ("CALIDAD", "Calidad", "OLGA YADIRA LEMUS ACOSTA", "1032409689", "ANDREA PIRAGUA", "519007001", "11051006"),
    ("TALENTO HUMANO", "Talento Humano (Bogotá)", None, None, None, "519007001", "11051006"),
]


def upgrade() -> None:
    from financiero.database import Base
    import financiero.models  # noqa: F401  (registra todas las tablas)
    import financiero.models.dian  # noqa: F401

    # Tablas nuevas (create_all idempotente, patrón de 0002/0004)
    Base.metadata.create_all(bind=op.get_bind(),
                             tables=[t for t in Base.metadata.sorted_tables
                                     if t.schema == ESQUEMA_FIN and t.name in (
                                         "cat_sedes_cm", "transportadores_omitidos",
                                         "dian_uploads", "dian_token")])

    # usuarios.sede_cm (filtro de sede exclusiva de cajas)
    op.add_column("usuarios", sa.Column("sede_cm", sa.String(120), nullable=True), schema=ESQUEMA_FIN)

    # cajas_menores: circuito completo del original
    op.add_column("cajas_menores", sa.Column("consecutivo", sa.String(20), nullable=True), schema=ESQUEMA_FIN)
    op.add_column("cajas_menores", sa.Column("sede_id", sa.Uuid(), nullable=True), schema=ESQUEMA_FIN)
    op.add_column("cajas_menores", sa.Column("responsable_cc", sa.String(20), nullable=True), schema=ESQUEMA_FIN)
    op.add_column("cajas_menores", sa.Column("centro_costo_principal", sa.String(60), nullable=True), schema=ESQUEMA_FIN)
    op.add_column("cajas_menores", sa.Column("politica", sa.JSON(), nullable=True), schema=ESQUEMA_FIN)
    op.add_column("cajas_menores", sa.Column("total_reembolso", sa.Numeric(18, 2), nullable=True), schema=ESQUEMA_FIN)
    op.create_index("uq_fin_cajas_consecutivo", "cajas_menores", ["consecutivo"], unique=True, schema=ESQUEMA_FIN)
    # LEGALIZADA (estado previo del puerto) pasa a RADICADA: la etapa más
    # temprana segura del circuito nuevo (reinicia las aprobaciones).
    # total_reembolso se recalcula con el primer PATCH de items.
    op.execute(f"UPDATE \"{ESQUEMA_FIN}\".cajas_menores SET estado='RADICADA' WHERE estado='LEGALIZADA'")

    # facturas_dian: validación por área, rechazo, cruce, trámite enriquecido y drive contable
    fac = "facturas_dian"
    op.add_column(fac, sa.Column("validada", sa.Boolean(), nullable=False, server_default=sa.false()), schema=ESQUEMA_FIN)
    op.add_column(fac, sa.Column("area_aprobacion", sa.String(20), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(fac, sa.Column("nota_validacion", sa.Text(), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(fac, sa.Column("fecha_solicitud_aprobacion", sa.DateTime(timezone=True), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(fac, sa.Column("motivo_rechazo", sa.String(120), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(fac, sa.Column("fecha_rechazo", sa.DateTime(timezone=True), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(fac, sa.Column("rechazado_por", sa.String(150), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(fac, sa.Column("cruzada_id", sa.Uuid(), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(fac, sa.Column("reemplaza_id", sa.Uuid(), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(fac, sa.Column("reemplaza_factura", sa.String(50), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(fac, sa.Column("fecha_cruce", sa.DateTime(timezone=True), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(fac, sa.Column("cruzado_por", sa.String(150), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(fac, sa.Column("fecha_envio_contab", sa.DateTime(timezone=True), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(fac, sa.Column("categoria_contable", sa.String(20), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(fac, sa.Column("num_orden_compra", sa.String(30), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(fac, sa.Column("observacion_recepcion", sa.Text(), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(fac, sa.Column("estado_contable", sa.String(15), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(fac, sa.Column("num_causacion", sa.String(30), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(fac, sa.Column("fecha_contabilizacion", sa.DateTime(timezone=True), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(fac, sa.Column("es_cuenta_cobro", sa.Boolean(), nullable=False, server_default=sa.false()), schema=ESQUEMA_FIN)
    op.add_column(fac, sa.Column("origen_doc", sa.String(40), nullable=True), schema=ESQUEMA_FIN)
    op.add_column(fac, sa.Column("caja_menor_sede", sa.String(120), nullable=True), schema=ESQUEMA_FIN)
    # Las FKs self-referenciales se declaran en el modelo; el CHECK de estados
    # no existe (String simple), así que LEGALIZACION/RECHAZADA entran solas.

    # Seed del maestro de sedes
    conn = op.get_bind()
    for nombre, etiqueta, responsable, cedula, jefe, cco, cuenta in SEDES_CM_SEED:
        conn.execute(sa.text(
            f'INSERT INTO "{ESQUEMA_FIN}".cat_sedes_cm (id, nombre, etiqueta, responsable, identificacion, jefe_inmediato, cco, cuenta, activo) '
            'VALUES (gen_random_uuid(), :n, :e, :r, :i, :j, :c, :cta, true) ON CONFLICT (nombre) DO NOTHING'
        ), {"n": nombre, "e": etiqueta, "r": responsable, "i": cedula, "j": jefe, "c": cco, "cta": cuenta})


def downgrade() -> None:
    fac = "facturas_dian"
    for col in ("caja_menor_sede", "origen_doc", "es_cuenta_cobro", "fecha_contabilizacion", "num_causacion",
                "estado_contable", "observacion_recepcion", "num_orden_compra", "categoria_contable",
                "fecha_envio_contab", "cruzado_por", "fecha_cruce", "reemplaza_factura", "reemplaza_id",
                "cruzada_id", "rechazado_por", "fecha_rechazo", "motivo_rechazo", "fecha_solicitud_aprobacion",
                "nota_validacion", "area_aprobacion", "validada"):
        op.drop_column(fac, col, schema=ESQUEMA_FIN)
    op.execute(f"UPDATE \"{ESQUEMA_FIN}\".cajas_menores SET estado='LEGALIZADA' WHERE estado IN ('RADICADA','APROBADA_JEFE','APROBADA_CONTROLLER','RECHAZADA')")
    for col in ("total_reembolso", "politica", "centro_costo_principal", "responsable_cc", "sede_id", "consecutivo"):
        op.drop_column("cajas_menores", col, schema=ESQUEMA_FIN)
    op.drop_column("usuarios", "sede_cm", schema=ESQUEMA_FIN)
    op.execute(f'DROP TABLE IF EXISTS "{ESQUEMA_FIN}".dian_token CASCADE')
    op.execute(f'DROP TABLE IF EXISTS "{ESQUEMA_FIN}".dian_uploads CASCADE')
    op.execute(f'DROP TABLE IF EXISTS "{ESQUEMA_FIN}".transportadores_omitidos CASCADE')
    op.execute(f'DROP TABLE IF EXISTS "{ESQUEMA_FIN}".cat_sedes_cm CASCADE')
