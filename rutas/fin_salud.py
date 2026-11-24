"""Salud del Módulo Financiero.

# puerto de app/routers/salud.py del módulo externo, ampliado para operación:
verifica el schema `fin` en Postgres, cuenta tablas, reporta memoria (RSS)
para vigilar el OOM del plan starter de Render y confirma la configuración de
JWT/correo/bucket sin exponer secretos. Público (lo usa el smoke post-deploy).
"""
import logging
import os

from fastapi import APIRouter
from sqlalchemy import text

from financiero.constantes import (
    ESQUEMA_FIN,
    FIN_BUCKET,
    SESION_HORAS,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/fin", tags=["Financiero · Salud"])


def _mem_info_kb() -> int | None:
    try:
        with open("/proc/self/status") as f:
            for linea in f:
                if linea.startswith("VmRSS:"):
                    return int(linea.split()[1])
    except Exception:
        return None
    return None


@router.get("/salud")
def salud():
    estado = "OPERACIONAL"
    detalles: dict = {"schema": ESQUEMA_FIN, "tablas": 0}

    # 1) Postgres + schema fin
    try:
        from financiero.database import obtener_engine
        with obtener_engine().connect() as con:
            tablas = con.execute(text(
                "SELECT count(*) FROM information_schema.tables WHERE table_schema = :esq"
            ), {"esq": ESQUEMA_FIN}).scalar()
            detalles["tablas"] = tablas
            if not tablas:
                estado = "DEGRADADO"
                detalles["aviso"] = "El schema fin está vacío: falta correr la migración (alembic -c fin_alembic.ini upgrade head)."
    except Exception as exc:
        estado = "DEGRADADO"
        detalles["error_bd"] = str(exc)[:300]

    # 2) Configuración (sin exponer valores)
    detalles["jwt_configurado"] = bool(os.getenv("JWT_SECRET"))
    detalles["correo_configurado"] = bool(os.getenv("RESEND_API_KEY"))
    detalles["bucket_adjuntos"] = FIN_BUCKET
    detalles["sesion_horas"] = SESION_HORAS

    # 3) Memoria (vigila el OOM del plan starter)
    rss = _mem_info_kb()
    if rss is not None:
        detalles["memoria_rss_kb"] = rss

    return {"estado": estado, "modulo": "financiero", **detalles}
