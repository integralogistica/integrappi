# ------------------------------------------------------------
# Funciones/claves.py — Hash y verificación de claves (bcrypt)
# ------------------------------------------------------------
# Módulo compartido por rutas/baseusuarios.py y rutas/conductores.py.
# Patrón tomado de rutas/aut2.py (colección `usuarios`, flujo propietarios).
#
# La verificación es "dual-mode" y orden-segura: funciona con claves
# guardadas en claro (antes de la migración) y con claves hasheadas
# (después), de modo que el deploy del backend puede ir en cualquier
# orden respecto al script scripts/migrar_claves_bcrypt.py.
#
# Se prueban las variantes de case (tal cual / MAYÚSCULAS / minúsculas)
# para preservar el comportamiento legacy que aceptaba la clave en
# mayúsculas aunque estuviera guardada en otro case.
# ------------------------------------------------------------

import hmac

from passlib.context import CryptContext

# bcrypt trunca silenciosamente en 72 bytes; lo validamos explícito.
LONGITUD_MAX_CLAVE = 72

contexto_pwd = CryptContext(schemes=["bcrypt"], deprecated="auto")


def es_hash(clave) -> bool:
    """True si el valor parece un hash bcrypt ($2a$/... ya sea prefijo $2)."""
    valor = str(clave or "")
    return valor.startswith("$2")


def crear_hash(clave: str) -> str:
    """
    Hashea una clave en claro con bcrypt.

    Rechaza claves vacías o mayores a LONGITUD_MAX_CLAVE bytes para evitar
    truncamiento silencioso de bcrypt.
    """
    valor = str(clave or "").strip()
    if not valor:
        raise ValueError("La clave no puede estar vacía")
    if len(valor.encode("utf-8")) > LONGITUD_MAX_CLAVE:
        raise ValueError(f"La clave no puede superar {LONGITUD_MAX_CLAVE} bytes")
    return contexto_pwd.hash(valor)


def _variantes(s: str):
    """Variantes de case de la clave ingresada, sin duplicados."""
    base = str(s or "").strip()
    vistos = []
    for variante in (base, base.upper(), base.lower()):
        if variante not in vistos:
            vistos.append(variante)
    return vistos


def verificar_clave(ingresada: str, almacenada: str) -> bool:
    """
    Verifica una clave ingresada contra el valor guardado en BD.

    - Si `almacenada` es un hash bcrypt → verify con cada variante de case.
    - Si está en claro (legacy, pre-migración) → comparación constante con
      las mismas variantes.

    Preserva el hack legacy (`clave == ingresada or clave == ingresada.upper()`)
    y añade tolerancia a claves guardadas en minúscula.
    """
    guardada = str(almacenada or "").strip()
    if not guardada:
        return False

    if es_hash(guardada):
        for variante in _variantes(ingresada):
            if contexto_pwd.verify(variante, guardada):
                return True
        return False

    for variante in _variantes(ingresada):
        if hmac.compare_digest(variante, guardada):
            return True
    return False
