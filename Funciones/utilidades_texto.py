"""Utilidades de normalización de texto compartidas entre routers.

Un solo origen para que la normalización de ESCRITA (baseusuarios.py al guardar
el alcance de aprobación) y la de LECTURA (otros_costos.py al filtrar/validar)
sea literalmente la misma función.
"""
import unicodedata


def norm_cliente_oc(s) -> str:
    """Normaliza un nombre de cliente de Otros Costos: NFKD → sin diacríticos →
    MAYÚSCULAS → espacios colapsados. Devuelve "" para entradas vacías.

    Ej: "  Fresenius Kabi " → "FRESENIUS KABI"; "ORTOPÉDICOS FUTURO" → "ORTOPEDICOS FUTURO".
    """
    if not s:
        return ""
    txt = unicodedata.normalize("NFKD", str(s))
    txt = "".join(ch for ch in txt if not unicodedata.combining(ch))
    return " ".join(txt.upper().split())
