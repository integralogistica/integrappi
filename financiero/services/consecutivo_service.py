"""Consecutivos atómicos únicos por (tipo, año).

# puerto de app/services/consecutivo_service.py del módulo externo. Cambio: la
tabla va calificada con el schema `fin` (el SQL crudo no pasa por el metadata).
En SQLite (tests con ATTACH AS fin) el ON CONFLICT DO UPDATE también funciona.
"""
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from financiero.constantes import ESQUEMA_FIN

# La última orden de compra emitida a mano fue la 2026-385: la primera del sistema es la 386.
BASES = {"oc": 385}


class ConsecutivoService:
    @staticmethod
    def generar_consecutivo(db: Session, tipo: str, anio: Optional[int] = None) -> str:
        """
        Genera un consecutivo atómico único y secuencial garantizado por PostgreSQL
        mediante 'INSERT ... ON CONFLICT DO UPDATE ... RETURNING'.
        Es imposible generar duplicados incluso con miles de peticiones simultáneas.
        """
        if not anio:
            anio = datetime.now(timezone.utc).year

        tipo_normalizado = tipo.lower().strip()

        inicio = BASES.get(tipo_normalizado, 0) + 1
        sql = text(f"""
            INSERT INTO {ESQUEMA_FIN}.consecutivos (tipo, anio, ultimo_numero, updated_at)
            VALUES (:tipo, :anio, :inicio, CURRENT_TIMESTAMP)
            ON CONFLICT (tipo, anio)
            DO UPDATE SET
                ultimo_numero = {ESQUEMA_FIN}.consecutivos.ultimo_numero + 1,
                updated_at = CURRENT_TIMESTAMP
            RETURNING ultimo_numero;
        """)

        res = db.execute(sql, {"tipo": tipo_normalizado, "anio": anio, "inicio": inicio})
        numero = res.scalar_one()

        if tipo_normalizado == "oc":
            return f"{anio}-{numero}"          # órdenes de compra: 2026-386 (la 385 fue la última emitida a mano)
        prefijos = {
            "req": "REQ",
            "tr": "TRP",
            "vj": "VJ"
        }
        prefijo = prefijos.get(tipo_normalizado, tipo_normalizado.upper())

        return f"{prefijo}-{anio}-{numero:04d}"
