"""Parámetros editables del administrador, con defaults de la app original.

# puerto LITERAL de app/services/configuracion.py del módulo externo.
"""
from decimal import Decimal
from typing import Any

from sqlalchemy.orm import Session

from financiero.models.configuracion import Configuracion

AREAS_DEFAULT = ["Tecnología", "Operaciones", "Talento Humano", "Comercial", "Calidad y HSEQ", "Inventarios",
                 "Administrativa", "Contabilidad", "Tesorería", "Compras"]

# clave -> (tipo, valor por defecto, descripción). Nunca secretos.
CLAVES: dict[str, dict[str, Any]] = {
    "empresa": {"tipo": str, "defecto": "Logística Integra"},
    "areas": {"tipo": list, "defecto": AREAS_DEFAULT},
    "limite_controller": {"tipo": (int, float), "defecto": 100_000_000},
    "correos_cc": {"tipo": list, "defecto": []},
    "viajes_tarifas": {"tipo": dict, "defecto": {}},            # tarifas A/B/C por concepto + vigencia + historial
    "viajes_autorizadores": {"tipo": list, "defecto": []},       # ids de usuarios que pueden autorizar además de los roles fijos
    "viajes_responsables_anticipo": {"tipo": list, "defecto": []},
    "viajes_festivos": {"tipo": dict, "defecto": {"extra": [], "quitar": []}},
    "pagos_motivos_rechazo": {"tipo": list, "defecto": []},   # vacío = defaults del router
}


def obtener(db: Session, clave: str) -> Any:
    fila = db.get(Configuracion, clave)
    return fila.valor.get("v") if fila else CLAVES[clave]["defecto"]


def limite_controller(db: Session) -> Decimal:
    return Decimal(str(obtener(db, "limite_controller")))
