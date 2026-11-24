"""Motor SQLAlchemy del Módulo Financiero (schema `fin`).

Mismo criterio que `bd/bd_postgres.py` (lección aprendida): el engine se crea
PEREZOSAMENTE, en el primer uso y bajo lock — nunca en el import. Así, si las
variables PG_* no están (p. ej. entorno de pruebas sin Postgres), integrappi
arranca igual y solo fallan las rutas `/fin/*`.

# puerto de app/database.py del módulo externo (allí el engine era eagler).
"""
import os
import threading
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

# Mismo .env que usan bd_cliente.py y bd_postgres.py (la raíz del repo).
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from financiero.constantes import ESQUEMA_FIN  # noqa: E402

_lock: threading.Lock = threading.Lock()
_engine: Optional[Engine] = None
_SessionLocal: Optional[sessionmaker] = None


def _construir_url() -> str:
    """URL de SQLAlchemy desde las mismas PG_* que usa bd_postgres.py.

    FIN_DATABASE_URL permite sobreescribirla por completo (pruebas con SQLite:
    `sqlite:///fin_test.db`).
    """
    override = os.getenv("FIN_DATABASE_URL")
    if override:
        return override
    host = os.getenv("PG_HOST")
    if not host:
        raise RuntimeError(
            "[FIN] Falta PG_HOST: el Módulo Financiero requiere PostgreSQL "
            "(mismas variables PG_* que bd_postgres.py, o FIN_DATABASE_URL)."
        )
    usuario = os.getenv("PG_USUARIO", "")
    clave = os.getenv("PG_CLAVE", "")
    puerto = os.getenv("PG_PORT", "5432")
    base = os.getenv("PG_DATABASE", "")
    from urllib.parse import quote_plus
    return (
        f"postgresql+psycopg2://{quote_plus(usuario)}:{quote_plus(clave)}"
        f"@{host}:{puerto}/{base}"
    )


def _crear_engine() -> Engine:
    url = _construir_url()
    kwargs: dict = {"pool_pre_ping": True}
    if url.startswith("sqlite"):
        # SQLite: solo pruebas/desarrollo local
        kwargs["connect_args"] = {"check_same_thread": False}
    else:
        # Plan starter de Render: pool deliberadamente pequeño para no sumar
        # presión de memoria al servicio que ya carga Playwright + pandas.
        kwargs["pool_size"] = int(os.getenv("FIN_DB_POOL_SIZE", "2"))
        kwargs["max_overflow"] = int(os.getenv("FIN_DB_MAX_OVERFLOW", "3"))
        kwargs["connect_args"] = {
            # sslmode lo exige Render; search_path deja el schema por defecto.
            "sslmode": os.getenv("PG_SSLMODE", "require"),
            "options": f"-csearch_path={ESQUEMA_FIN}",
            "connect_timeout": 10,
        }
    return create_engine(url, **kwargs)


def obtener_engine() -> Engine:
    """Engine perezoso (doble check bajo lock). Conecta solo al primer uso."""
    global _engine, _SessionLocal
    if _engine is None:
        with _lock:
            if _engine is None:
                _engine = _crear_engine()
                _SessionLocal = sessionmaker(
                    autocommit=False, autoflush=False, bind=_engine
                )
    return _engine


def obtener_session_factory() -> sessionmaker:
    obtener_engine()
    assert _SessionLocal is not None
    return _SessionLocal


class Base(DeclarativeBase):
    """Base declarativa exclusiva del Módulo Financiero (solo schema fin)."""
    pass


def get_db_fin():
    """Dependencia FastAPI: sesión transaccional del módulo financiero."""
    factory = obtener_session_factory()
    db = factory()
    try:
        yield db
    finally:
        db.close()
