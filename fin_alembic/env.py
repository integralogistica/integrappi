"""env.py de la instancia Alembic del Módulo Financiero (schema `fin`).

Aislamiento total respecto de la base existente:
- `version_table_schema` = schema fin (tabla `fin.alembic_version_fin`, no
  toca la convención `public.alembic_version` por si algún día se añade otra
  instancia).
- `include_object` descarta cualquier objeto que NO esté en el schema fin:
  aunque alguien corra un autogenerate descuidado, jamás se generará DDL
  sobre `informe_guias_tms` u otras tablas de la casa.
"""
import os
import sys
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import engine_from_config, pool

# Raíz del repo al sys.path (para importar financiero.*)
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from financiero.constantes import ESQUEMA_FIN  # noqa: E402
from financiero.database import Base  # noqa: E402
import financiero.models  # noqa: E402,F401  (registra todas las tablas)

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _url_desde_entorno() -> str:
    """Misma lógica que financiero/database._construir_url (una sola fuente)."""
    from financiero.database import _construir_url
    return _construir_url()


def _solo_schema_fin(objeto, nombre, tipo_, *args, **kwargs):
    """Deja pasar únicamente objetos del schema fin (tablas, índices, todo)."""
    esquema = getattr(objeto, "schema", None)
    if tipo_ == "table":
        return esquema == ESQUEMA_FIN
    # Índices/columnas heredan el schema de su tabla: si llega sin schema
    # explícito pero pertenece a una tabla nuestra, se acepta más abajo vía
    # el compare de tablas; los sueltos de otros schemas se descartan.
    if esquema is not None:
        return esquema == ESQUEMA_FIN
    tabla = getattr(objeto, "table", None)
    if tabla is not None:
        return tabla.schema == ESQUEMA_FIN
    return False


def run_migrations_offline() -> None:
    context.configure(
        url=_url_desde_entorno(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        version_table="alembic_version_fin",
        version_table_schema=ESQUEMA_FIN,
        include_object=_solo_schema_fin,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    seccion = config.get_section(config.config_ini_section, {})
    seccion["sqlalchemy.url"] = _url_desde_entorno()
    connectable = engine_from_config(
        seccion,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        # El schema debe existir ANTES de que Alembic cree su tabla de
        # versiones (fin.alembic_version_fin) — la migración 001 también lo
        # lleva por si alguien cambia la tabla de versiones de sitio.
        if connectable.dialect.name != "sqlite":
            from sqlalchemy import text
            connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{ESQUEMA_FIN}"'))
            connection.commit()
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            version_table="alembic_version_fin",
            version_table_schema=ESQUEMA_FIN,
            include_object=_solo_schema_fin,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
