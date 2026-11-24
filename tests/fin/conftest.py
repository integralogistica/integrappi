"""Fixtures de los tests del Módulo Financiero.

Estrategia: SQLite en archivo con el schema `fin` EMULADO vía ATTACH (mismo
archivo adjunto como base `fin`). Los tests dialecto-dependientes en serio
(FOR UPDATE real, JSONB, concurrencia multi-conexión) se marcan
`@pytest.mark.postgres` y corren contra un Postgres local (docker) antes de
cada deploy — aquí se cubre la lógica.
"""
import os
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(RAIZ))

# ANTES de importar financiero.* (leéneo en el import):
os.environ.setdefault("JWT_SECRET", "clave-de-prueba-del-modulo-financiero-32-caracteres")
os.environ["FIN_DATABASE_URL"] = "sqlite://"  # se sobreescribe abajo con el archivo real

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import financiero.database as fin_db
from financiero.database import Base
import financiero.models  # noqa: F401  (registra todas las tablas)

_ARCHIVO_DB = RAIZ / "tests" / "fin" / "_fin_test.db"


def crear_engine_sqlite():
    if _ARCHIVO_DB.exists():
        _ARCHIVO_DB.unlink()
    engine = create_engine(
        f"sqlite:///{_ARCHIVO_DB}",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,  # UNA conexión: el ATTACH persiste
    )

    @event.listens_for(engine, "connect")
    def _adjuntar_schema_fin(con_dbapi, _registro):
        con_dbapi.execute(f"ATTACH DATABASE '{_ARCHIVO_DB.as_posix()}' AS fin")

    Base.metadata.create_all(engine)
    return engine


@pytest.fixture(scope="session")
def engine():
    eng = crear_engine_sqlite()
    # Inyecta el engine de pruebas en el módulo (obtener_engine lo devuelve tal cual)
    fin_db._engine = eng
    fin_db._SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=eng)
    yield eng
    eng.dispose()
    if _ARCHIVO_DB.exists():
        _ARCHIVO_DB.unlink()


@pytest.fixture()
def sesion(engine):
    """Sesión limpia por test (borra las tablas de flujo, conserva nada entre tests)."""
    from financiero.models.usuario import Usuario
    factory = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = factory()
    for tabla in reversed(Base.metadata.sorted_tables):
        db.execute(tabla.delete())
    db.commit()
    yield db
    db.close()


@pytest.fixture()
def usuario_admin(sesion):
    from financiero.models.usuario import RolUsuario, Usuario
    u = Usuario(username="admin.test", nombre="Admin Test",
                correo="admin.test@integralogistica.com", rol=RolUsuario.ADMIN, activo=True)
    sesion.add(u)
    sesion.commit()
    return u


@pytest.fixture()
def cliente(engine):
    """TestClient con los routers del módulo sobre el engine de pruebas."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from rutas.fin_auth import router as ruta_fin_auth
    from rutas.fin_salud import router as ruta_fin_salud

    app = FastAPI()
    app.include_router(ruta_fin_auth)
    app.include_router(ruta_fin_salud)
    return TestClient(app)


def token_modulo(correo: str, rol: str = "consulta", horas: float = 12) -> str:
    """Token de sesión del módulo (fin_modulo) firmado para los tests."""
    from datetime import datetime, timedelta, timezone
    import jwt
    ahora = datetime.now(timezone.utc)
    return jwt.encode(
        {
            "sub": correo,
            "usuario": correo.split("@")[0],
            "rol": rol,
            "auth_source": "fin_modulo",
            "iat": ahora,
            "exp": ahora + timedelta(hours=horas),
        },
        os.environ["JWT_SECRET"],
        algorithm="HS256",
    )
