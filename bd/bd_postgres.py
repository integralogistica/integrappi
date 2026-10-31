# archivo: bd/bd_postgres.py
"""
Conexión a PostgreSQL (Render) para la tabla ``informe_guias_tms``.

Desde 2026-10-29 hay un POOL pequeño (ThreadedConnectionPool 1-5) creado de
forma PEREZOSA con lock — nunca en el import (lección del MongoClient de
bd_cliente: cada conexión de red en el arranque cuesta segundos y colgaba el
startup de uvicorn). Si el pool no puede crearse, se degrada a
``psycopg2.connect`` por request (más lento, no roto).

La tabla la carga un proceso EXTERNO al repo (Bot 001); aquí solo hay SELECTs.
"""

import os
import logging
import threading
from contextlib import contextmanager
from datetime import date
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

# Cargar variables desde el .env del proyecto (mismo patrón que bd_cliente.py)
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
except Exception:
    pass

logger = logging.getLogger(__name__)


def _parametros_conexion() -> dict:
    return dict(
        host=os.getenv("PG_HOST"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USUARIO"),
        password=os.getenv("PG_CLAVE"),
        connect_timeout=10,
        sslmode="require",  # Render exige SSL
        application_name="integrappi-guias-tms",
    )


# Pool compartido: crear una conexión a Render cuesta ~100-300 ms y el
# informe de guías hace varias queries por request. Lazy + lock para no
# conectar en el import ni crear el pool dos veces bajo concurrencia.
_POOL = None
_POOL_LOCK = threading.Lock()


def _obtener_pool():
    global _POOL
    if _POOL is not None:
        return _POOL
    with _POOL_LOCK:
        if _POOL is None:
            _POOL = ThreadedConnectionPool(minconn=1, maxconn=5, **_parametros_conexion())
            logger.info("[bd_postgres] Pool PostgreSQL creado (1-5 conexiones)")
    return _POOL


@contextmanager
def _conexion():
    """Conexión del pool (o directa si el pool falla); commit/rollback/retorno
    automáticos. Uso: ``with _conexion() as conn:`` — el llamador NO cierra."""
    pool = None
    conn = None
    try:
        pool = _obtener_pool()
        conn = pool.getconn()
    except Exception:
        logger.warning("[bd_postgres] Pool no disponible, conexión directa", exc_info=True)
        conn = psycopg2.connect(**_parametros_conexion())
    try:
        yield conn
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        if pool is not None and conn is not None:
            try:
                pool.putconn(conn)
            except Exception:
                pass
        elif conn is not None:
            try:
                conn.close()
            except Exception:
                pass


# (2026-10-30) Índice trgm de destinatario: el autocomplete «Cliente final»
# hace DISTINCT TRIM + ILIKE %q% — sin índice escanea ~100k filas del cliente
# (0,6-1,2 s). GIN trgm (pg_trgm ya está instalada) lo vuelve index-driven.
_INDICE_DEST_TRGM_LISTO = False
_INDICE_DEST_TRGM_LOCK = threading.Lock()

# Caché en memoria de la lista COMPLETA de destinatarios por cliente (TTL 10
# min) — el autocomplete filtra en memoria, sin tocar la BD por búsqueda.
_DEST_TODOS_CACHE: dict = {}
_DEST_CACHE_TTL = 1800.0


def _asegurar_indice_destinatario_trgm() -> None:
    global _INDICE_DEST_TRGM_LISTO
    if _INDICE_DEST_TRGM_LISTO:
        return
    with _INDICE_DEST_TRGM_LOCK:
        if _INDICE_DEST_TRGM_LISTO:
            return
        try:
            with _conexion() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_guias_destinatario_trgm
                        ON informe_guias_tms USING gin (destinatario gin_trgm_ops);
                        """
                    )
                conn.commit()
            _INDICE_DEST_TRGM_LISTO = True
        except Exception:
            logger.warning("[bd_postgres] No se pudo crear el índice trgm de destinatario", exc_info=True)


# Índice del informe PG-puro (2026-10-29): igualdad por cliente + rango por
# fecha de emisión. El idx_guias_fecha_cliente existente tiene la fecha como
# columna leading (sirve para rangos de fecha de TODOS los clientes); este
# compuesto cliente-first es el correcto para WHERE nombre_cliente = %s AND
# fecha_emision BETWEEN ... A 221k filas, CREATE INDEX es instantáneo.
_INDICE_CLIENTE_FECHA_LISTO = False
_INDICE_CLIENTE_FECHA_LOCK = threading.Lock()


def _asegurar_indice_cliente_fecha() -> None:
    global _INDICE_CLIENTE_FECHA_LISTO
    if _INDICE_CLIENTE_FECHA_LISTO:
        return
    with _INDICE_CLIENTE_FECHA_LOCK:
        if _INDICE_CLIENTE_FECHA_LISTO:
            return
        try:
            with _conexion() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_guias_cliente_fecha
                        ON informe_guias_tms (nombre_cliente, fecha_emision DESC);
                        """
                    )
            _INDICE_CLIENTE_FECHA_LISTO = True
            logger.info("[bd_postgres] Índice idx_guias_cliente_fecha verificado/creado")
        except Exception as e:
            logger.warning(f"[bd_postgres] No se pudo crear idx_guias_cliente_fecha: {e}")


# ── Informe de guías PostgreSQL puro (2026-10-29) ─────────────────────────────
# Fuente ÚNICA del informe del dashboard de cliente (antes: Mongo media milla
# + JOIN por guía). El match por cliente es IGUALDAD EXACTA sobre nombre_cliente
# con una LISTA de valores (un mismo cliente puede llegar al TMS con varios
# nombres, p. ej. 'FRESENIUS KABI' y 'FK SERVICIO TECNICO') para que use el
# índice; NO envolver la columna en TRIM/upper (rompería el uso del índice).

def _condicion_cliente_fecha(nombres: list, anios: list, meses: list):
    """WHERE compartido: igualdad por cliente (ANY sobre la lista) + rango de
    fecha_emision por índice (los EXTRACT solo afinan años no contiguos). Sin
    ``anios`` → todo el histórico. Devuelve ``(fragmento_sql, params)``."""
    partes = ["nombre_cliente = ANY(%s)"]
    params: list = [list(nombres)]
    if anios:
        desde = date(min(anios), 1, 1)
        hasta = date(max(anios) + 1, 1, 1)
        partes.append("fecha_emision >= %s AND fecha_emision < %s")
        params += [desde, hasta]
        partes.append("EXTRACT(YEAR FROM fecha_emision) = ANY(%s)")
        params.append(list(anios))
        if meses:
            partes.append("EXTRACT(MONTH FROM fecha_emision) = ANY(%s)")
            params.append(list(meses))
    return " AND ".join(partes), params


# Cast a numeric aplicado EN SQL a piezas/kilos (TEXT en la tabla; puede traer
# '' o basura del TMS — el CASE lo manda a NULL).
_CAST_NUM = """
CASE WHEN TRIM({col}) ~ '^[0-9]+(\\.[0-9]+)?$' THEN TRIM({col})::numeric ELSE NULL END
"""

_COLUMNAS_PAGINA = f"""
    guia,
    NULLIF(TRIM(planilla), '') AS planilla,
    NULLIF(TRIM(ciudad_destino), '') AS ciudad_destino,
    NULLIF(TRIM(destinatario), '') AS destinatario,
    NULLIF(TRIM(servicio), '') AS servicio,
    {_CAST_NUM.format(col='piezas')} AS piezas,
    {_CAST_NUM.format(col='kilos')} AS kilos,
    estado,
    -- novedad del TMS → novedad_tms (la clave `novedad` queda reservada para
    -- la novedad JUSTIFICADA que adjunta _adjuntar_novedades en el router)
    NULLIF(TRIM(novedad), '') AS novedad_tms,
    fecha_emision, fecha_entrega, fecha_digitalizacion,
    fecha_cita
"""


def _normalizar_fila(fila: dict) -> dict:
    """Fila PG → contrato del endpoint (fechas ISO o None; texto sin espacios)."""
    out = dict(fila)
    for k in ("guia", "estado", "fecha_cita"):
        v = out.get(k)
        out[k] = str(v).strip() or None if v is not None else None
    # planilla '0' = el TMS no le asignó planilla
    if out.get("planilla") == "0":
        out["planilla"] = None
    for k in ("fecha_emision", "fecha_entrega", "fecha_digitalizacion"):
        v = out.get(k)
        out[k] = v.isoformat() if v is not None else None
    for k in ("piezas", "kilos"):
        v = out.get(k)
        out[k] = float(v) if v is not None else None
    return out


def guias_minimas_cliente(nombres: list, anios: list, meses: list):
    """Pasada de agregación del informe: columnas MÍNIMAS de TODAS las filas
    del filtro (el OT/series/resumen se computan en Python sobre esto).
    ~29k filas × 7 columnas para un año completo. SIN LIMIT."""
    _asegurar_indice_cliente_fecha()
    where, params = _condicion_cliente_fecha(nombres, anios, meses)
    sql = f"""
        SELECT guia, planilla, estado, fecha_emision, fecha_entrega,
               ciudad_destino, NULLIF(TRIM(destinatario), '') AS destinatario,
               fecha_cita
        FROM informe_guias_tms
        WHERE {where}
        ORDER BY fecha_emision DESC, guia ASC;
    """
    try:
        with _conexion() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                return [_normalizar_fila(f) for f in cur.fetchall()]
    except Exception:
        logger.exception("[bd_postgres] Error en guias_minimas_cliente")
        raise  # sin fuente Mongo a la que degradar: que el endpoint responda 503


def guia_tms_placa(guia: str):
    """(2026-10-30) Placa, ciudad destino y fecha de emisión del TMS de UNA
    guía en ``informe_guias_tms`` — para la consulta de guía del portal de
    clientes. None si la guía no está en la tabla (no rompe la consulta SOAP)."""
    sql = """
        SELECT NULLIF(TRIM(placa), '') AS placa,
               NULLIF(TRIM(ciudad_destino), '') AS ciudad_destino,
               fecha_emision
        FROM informe_guias_tms
        WHERE guia = %s
        LIMIT 1;
    """
    try:
        with _conexion() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, [guia])
                fila = cur.fetchone()
                if not fila:
                    return None
                if hasattr(fila.get("fecha_emision"), "isoformat"):
                    fila["fecha_emision"] = fila["fecha_emision"].isoformat()
                return dict(fila)
    except Exception:
        logger.exception("[bd_postgres] Error buscando placa de la guía %s", guia)
        return None


def guias_placa_dia(placa: str, fecha_emision: str, excluir_guia: str):
    """(2026-10-30) OTRAS guías del mismo vehículo (placa) emitidas el MISMO
    día — la remesa que viajó junto con la consultada. Solo se sabe en
    ``informe_guias_tms``. [] ante cualquier error (no rompe la consulta)."""
    sql = """
        SELECT guia,
               NULLIF(TRIM(destinatario), '') AS destinatario,
               NULLIF(TRIM(ciudad_destino), '') AS ciudad_destino,
               estado
        FROM informe_guias_tms
        WHERE TRIM(placa) = %s
          AND fecha_emision = %s
          AND guia <> %s
        ORDER BY guia;
    """
    try:
        with _conexion() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, [placa, fecha_emision, excluir_guia])
                return [dict(f) for f in cur.fetchall()]
    except Exception:
        logger.exception("[bd_postgres] Error buscando guías de la placa %s", placa)
        return []


_DEST_TODOS_LOCK = threading.Lock()


def _cargar_destinatarios(nombres: tuple) -> list:
    sql = """
        SELECT DISTINCT TRIM(destinatario) AS destinatario
        FROM informe_guias_tms
        WHERE nombre_cliente = ANY(%s) AND NULLIF(TRIM(destinatario), '') IS NOT NULL
        ORDER BY 1;
    """
    try:
        with _conexion() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, [list(nombres)])
                return [f["destinatario"] for f in cur.fetchall() if f["destinatario"]]
    except Exception:
        logger.exception("[bd_postgres] Error cargando destinatarios del cliente")
        return []


def _destinatarios_todos(nombres: tuple) -> list:
    """Lista DISTINCT de destinatarios del cliente (v4 stale-while-revalidate):
    fresca < 30 min se devuelve directo; VENCIDA se devuelve igual y se
    refresca en un hilo de fondo (el usuario jamás espera la recarga); solo
    la PRIMERA carga es síncrona (~1,3 s). El autocomplete filtra en
    memoria (~4 ms por búsqueda; la query por búsqueda era 0,6-1,5 s)."""
    import time as _t
    hit = _DEST_TODOS_CACHE.get(nombres)
    if hit and _t.monotonic() - hit[0] < _DEST_CACHE_TTL:
        return hit[1]
    if hit:
        def _refrescar():
            with _DEST_TODOS_LOCK:
                h = _DEST_TODOS_CACHE.get(nombres)
                if h and _t.monotonic() - h[0] < _DEST_CACHE_TTL:
                    return  # otro hilo ya refrescó
                todos = _cargar_destinatarios(nombres)
                if todos:
                    _DEST_TODOS_CACHE[nombres] = (_t.monotonic(), todos)
        threading.Thread(target=_refrescar, daemon=True).start()
        return hit[1]
    todos = _cargar_destinatarios(nombres)
    _DEST_TODOS_CACHE[nombres] = (_t.monotonic(), todos)
    return todos



def buscar_destinatarios_cliente(nombres: list, q: str, limite: int = 20):
    """(2026-10-30) Autocomplete de destinatarios («cliente final»): contiene
    case-insensitive sobre la lista cacheada — los que EMPIEZAN por el texto
    primero, luego los que lo contienen. Instantáneo tras la 1ª carga."""
    ql = q.strip().lower()
    if not ql:
        return []
    todos = _destinatarios_todos(tuple(nombres))
    empiezan = sorted(d for d in todos if d.lower().startswith(ql))
    contienen = sorted(d for d in todos if ql in d.lower() and not d.lower().startswith(ql))
    return (empiezan + contienen)[:limite]


def pagina_guias_cliente(nombres: list, anios: list, meses: list, limite: int, offset: int, destinatarios: list | None = None):
    """Página de filas COMPLETAS para la tabla del informe (casts en SQL).
    Devuelve ``(filas, total_del_filtro)`` — el COUNT extra es barato (solo
    el índice) y da el «Mostrando X de Y» del footer de la tabla.
    ``destinatarios`` (2026-10-30): filtro opcional por cliente final."""
    _asegurar_indice_cliente_fecha()
    where, params = _condicion_cliente_fecha(nombres, anios, meses)
    if destinatarios:
        where += " AND NULLIF(TRIM(destinatario), '') = ANY(%s)"
        params = params + [destinatarios]
    sql = f"""
        SELECT{_COLUMNAS_PAGINA}
        FROM informe_guias_tms
        WHERE {where}
        ORDER BY fecha_emision DESC, guia ASC
        LIMIT %s OFFSET %s;
    """
    try:
        with _conexion() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(f"SELECT COUNT(*) AS n FROM informe_guias_tms WHERE {where};", params)
                total = cur.fetchone()["n"]
                cur.execute(sql, params + [limite, offset])
                return [_normalizar_fila(f) for f in cur.fetchall()], total
    except Exception:
        logger.exception("[bd_postgres] Error en pagina_guias_cliente")
        raise


def buscar_guias_cliente(nombres: list, q: str, limite: int, offset: int):
    """Trazabilidad: ILIKE parcial sobre guía/planilla/destinatario en TODO el
    histórico del cliente (sin filtro de fecha). Devuelve ``(filas, total)``."""
    _asegurar_indice_cliente_fecha()
    patron = f"%{q.strip()}%"
    where = "nombre_cliente = ANY(%s) AND (guia ILIKE %s OR planilla ILIKE %s OR destinatario ILIKE %s)"
    params = [list(nombres), patron, patron, patron]
    try:
        with _conexion() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(f"SELECT COUNT(*) AS n FROM informe_guias_tms WHERE {where};", params)
                total = cur.fetchone()["n"]
                cur.execute(
                    f"""
                    SELECT{_COLUMNAS_PAGINA}
                    FROM informe_guias_tms
                    WHERE {where}
                    ORDER BY fecha_emision DESC, guia ASC
                    LIMIT %s OFFSET %s;
                    """,
                    params + [limite, offset],
                )
                return [_normalizar_fila(f) for f in cur.fetchall()], total
    except Exception:
        logger.exception("[bd_postgres] Error en buscar_guias_cliente")
        raise
