# archivo: bd/bd_postgres.py
"""
Conexión a PostgreSQL (Render) para la tabla ``informe_guias_tms``.

Conexión POR-REQUEST: este módulo NO se conecta al importar (lección del
MongoClient de bd_cliente — cada conexión de red en el arranque cuesta
segundos y colgaba el startup de uvicorn). El llamador abre con
``obtener_conexion()`` y cierra en ``finally``.

La tabla la carga un proceso EXTERNO al repo; aquí solo hay SELECTs.
"""

import os
import logging
import threading
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor

# Cargar variables desde el .env del proyecto (mismo patrón que bd_cliente.py)
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
except Exception:
    pass

logger = logging.getLogger(__name__)


def obtener_conexion():
    """Abre una conexión psycopg2 a Render Postgres (el llamador la cierra)."""
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USUARIO"),
        password=os.getenv("PG_CLAVE"),
        connect_timeout=10,
        sslmode="require",  # Render exige SSL
        application_name="integrappi-guias-tms",
    )


# Índice sobre "guia": no existe (los existentes son por fecha/cliente) y las
# consultas del informe filtran WHERE guia = ANY(...). Se crea UNA sola vez
# de forma perezosa (idempotente); si falla, solo queda lento, no roto.
_INDICE_LISTO = False
_INDICE_LOCK = threading.Lock()


def _asegurar_indice_guia() -> None:
    global _INDICE_LISTO
    if _INDICE_LISTO:
        return
    with _INDICE_LOCK:
        if _INDICE_LISTO:
            return
        conn = None
        try:
            conn = obtener_conexion()
            with conn.cursor() as cur:
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_guias_guia ON informe_guias_tms (guia);"
                )
            conn.commit()
            _INDICE_LISTO = True
            logger.info("[bd_postgres] Índice idx_guias_guia verificado/creado")
        except Exception as e:
            logger.warning(f"[bd_postgres] No se pudo crear idx_guias_guia: {e}")
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass


def consultar_guias(guias: list, lote: int = 500) -> dict:
    """SELECT de estado/fechas por lote de guías.

    Devuelve ``{guia: {"estado": ..., "fecha_entrega": ..., "fecha_digitalizacion": ...,
    "fecha_cita": ...}}`` (guia y fechas como str o None). NO lanza: ante error
    de Postgres devuelve ``{}`` para que el endpoint degrade el informe (solo
    pierde el estado, no las filas de Mongo).
    """
    if not guias:
        return {}

    limpias = [str(g).strip() for g in guias if g and str(g).strip()]
    if not limpias:
        return {}

    _asegurar_indice_guia()

    out = {}
    conn = None
    try:
        conn = obtener_conexion()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            for i in range(0, len(limpias), lote):
                cur.execute(
                    """
                    SELECT guia, estado, fecha_entrega, fecha_digitalizacion, fecha_cita,
                           destinatario, fecha_emision
                    FROM informe_guias_tms
                    WHERE guia = ANY(%s);
                    """,
                    (limpias[i:i + lote],),
                )
                for fila in cur.fetchall():
                    out[str(fila["guia"]).strip()] = {
                        "estado": (fila["estado"] or "").strip() or None,
                        "fecha_entrega": fila["fecha_entrega"],
                        "fecha_digitalizacion": fila["fecha_digitalizacion"],
                        # fecha_cita: TEXT crudo (puede traer basura histórica —
                        # zonas Z_CIU, teléfonos…; sin normalizar a propósito).
                        "fecha_cita": (fila["fecha_cita"] or "").strip() or None,
                        "destinatario": (fila["destinatario"] or "").strip() or None,
                        "fecha_emision": fila["fecha_emision"],
                    }
        return out
    except Exception:
        logger.exception("[bd_postgres] Error consultando informe_guias_tms")
        return {}
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
