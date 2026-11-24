"""Rate limit en memoria (ventana deslizante de 60 s).

# puerto de app/services/rate_limit.py del módulo externo (límites por env en
vez de pydantic-settings). En varias réplicas debería moverse a Redis; aquí
hay un solo worker.
"""
import os
import time
from collections import defaultdict, deque

from fastapi import HTTPException, status

_intentos: dict[str, deque] = defaultdict(deque)
_envios: dict[str, deque] = defaultdict(deque)

LIMITE_LOGIN_POR_MINUTO = int(os.getenv("FIN_RATE_LIMIT_LOGIN", "10"))


def limitar_login(clave: str) -> None:
    limite = LIMITE_LOGIN_POR_MINUTO
    if limite <= 0:
        return
    ahora = time.monotonic()
    q = _intentos[clave]
    while q and ahora - q[0] > 60:
        q.popleft()
    if len(q) >= limite:
        raise HTTPException(status.HTTP_429_TOO_MANY_REQUESTS, detail="Demasiados intentos de ingreso. Espera un minuto.")
    q.append(ahora)


def limitar_envio(usuario: str, por_minuto: int = 10) -> None:
    ahora = time.monotonic()
    q = _envios[usuario]
    while q and ahora - q[0] > 60:
        q.popleft()
    if len(q) >= por_minuto:
        raise HTTPException(status.HTTP_429_TOO_MANY_REQUESTS, detail="Demasiados correos por minuto. Espera un momento.")
    q.append(ahora)


def reiniciar() -> None:
    _intentos.clear()
    _envios.clear()
