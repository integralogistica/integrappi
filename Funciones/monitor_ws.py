# Funciones/monitor_ws.py
# (2026-11-09) Cliente de la API Monitor (Mayasoft) — posición GPS del vehículo
# de una placa, para la pestaña Trazabilidad del portal de clientes
# (/indicadores-cliente/{id}/consulta-guia). Contrato: "api monitor.pdf"
# (DD-EST-02 v17) — login POST form-urlencoded {userId, password} → token;
# GET reportByManifest?licensePlate=<PLACA>&lists=location con header
# Authorization. Devuelve un ARRAY de manifiestos por placa; se elige el
# vigente (delivered != "Finalizado").
#
# Contrato de la función pública: NO LANZA. Todo error (credenciales
# ausentes, timeout, 4xx/5xx, respuesta inesperada) devuelve
# {"ok": False, ...} y la consulta de la guía sigue su curso.
import os
import time
import asyncio
import logging
from typing import Any, Dict, Optional

import httpx

logger = logging.getLogger(__name__)

MONITOR_BASE_URL = os.getenv("MONITOR_BASE_URL", "https://integra.mayasoft.ai").rstrip("/")
MONITOR_USER = os.getenv("MONITOR_USER", "").strip()
MONITOR_PASSWORD = os.getenv("MONITOR_PASSWORD", "")

# El PDF no muestra el response del login ni el formato exacto del header;
# scheme vacío = token tal cual ("Authorization: <token>"). Si Mayasoft
# confirma Bearer, basta MONITOR_AUTH_SCHEME=Bearer sin tocar código.
MONITOR_AUTH_SCHEME = os.getenv("MONITOR_AUTH_SCHEME", "").strip()

MONITOR_TIMEOUT_S = float(os.getenv("MONITOR_TIMEOUT_S", "20"))
MONITOR_TOKEN_TTL_S = float(os.getenv("MONITOR_TOKEN_TTL_S", "1500"))

_LOGIN_PATH = "/api/authUsers/getToken/"
_CONSULTA_PATH = "/api/reportes/integration/reportByManifest"

# Caché del token en memoria (patrón bot_ofac: lock + time.monotonic). Lock
# ASYNC: el llamado vive en un endpoint async; el camino feliz (token vigente)
# ni toma el lock.
_TOKEN_LOCK = asyncio.Lock()
_TOKEN: Optional[str] = None
_TOKEN_EN = 0.0  # time.monotonic() de la emisión


def _hay_credenciales() -> bool:
    return bool(MONITOR_USER and MONITOR_PASSWORD)


def _auth_header(token: str) -> Dict[str, str]:
    valor = f"{MONITOR_AUTH_SCHEME} {token}".strip() if MONITOR_AUTH_SCHEME else token
    return {"Authorization": valor}


def _extraer_token(body: Any) -> Optional[str]:
    """Tolerante ante shapes: {"token": ...} / {"access_token": ...} /
    {"data": {"token": ...}} / string plano. None si no encuentra nada."""
    if isinstance(body, str) and body.strip():
        return body.strip()
    if isinstance(body, dict):
        for clave in ("token", "access_token", "jwt"):
            if isinstance(body.get(clave), str) and body[clave].strip():
                return body[clave].strip()
        return _extraer_token(body.get("data"))
    return None


async def _login(transport: Optional[httpx.AsyncBaseTransport] = None) -> str:
    """Login contra Monitor. Lanza RuntimeError si falla (la llama el flujo
    interno con manejo de error propio)."""
    timeout = httpx.Timeout(MONITOR_TIMEOUT_S, connect=min(10.0, MONITOR_TIMEOUT_S))
    async with httpx.AsyncClient(timeout=timeout, trust_env=False, transport=transport) as client:
        r = await client.post(
            f"{MONITOR_BASE_URL}{_LOGIN_PATH}",
            data={"userId": MONITOR_USER, "password": MONITOR_PASSWORD},
        )
    if r.status_code != 200:
        preview = (r.text or "")[:200]
        logger.warning("[monitor_ws] Login rechazado %s: %s", r.status_code, preview)
        raise RuntimeError(f"Login Monitor rechazado ({r.status_code})")
    token = None
    try:
        token = _extraer_token(r.json())
    except Exception:
        token = _extraer_token(r.text)
    if not token:
        logger.warning("[monitor_ws] Login 200 sin token reconocible: %s", (r.text or "")[:200])
        raise RuntimeError("Login Monitor sin token en la respuesta")
    return token


async def _obtener_token(forzar: bool = False, transport: Optional[httpx.AsyncBaseTransport] = None) -> str:
    """Token cacheado con TTL. Re-login solo cuando vence (o forzar=True,
    tras un 401 en la consulta)."""
    global _TOKEN, _TOKEN_EN
    if not forzar and _TOKEN and (time.monotonic() - _TOKEN_EN) < MONITOR_TOKEN_TTL_S:
        return _TOKEN
    async with _TOKEN_LOCK:
        if not forzar and _TOKEN and (time.monotonic() - _TOKEN_EN) < MONITOR_TOKEN_TTL_S:
            return _TOKEN
        token = await _login(transport=transport)
        _TOKEN = token
        _TOKEN_EN = time.monotonic()
        return token


def _invalidar_token() -> None:
    global _TOKEN
    _TOKEN = None


def _num_seguro(v: Any) -> Optional[float]:
    """lastlatitude/etadistance llegan como STRING; None si no es numérico."""
    try:
        if v is None or str(v).strip() == "":
            return None
        return round(float(str(v).strip()), 6)
    except (TypeError, ValueError):
        return None


def _kilometros(v: Any) -> Optional[int]:
    metros = _num_seguro(v)
    # etadistance en el ejemplo es "852.106091" con destino a ~850 km:
    # el contrato no declara unidad; se entrega crudo redondeado y el
    # frontend lo etiqueta según magnitud.
    return int(round(metros)) if metros is not None else None


def _clave_fecha(doc: Dict[str, Any]) -> str:
    """Orden de los manifiestos: el más reciente primero (para elegir el
    vigente entre varios)."""
    return str(doc.get("manifestcreationdate") or doc.get("etadate") or "")


def _seleccionar_manifiesto(manifiestos: list) -> Optional[Dict[str, Any]]:
    """De la lista que devuelve Monitor por placa elige EL manifiesto que
    viaja hoy: el primero no finalizado (más reciente); si todos terminaron,
    el último (el panel muestra su estado para que el cliente entienda)."""
    if not manifiestos:
        return None
    ordenados = sorted(manifiestos, key=_clave_fecha, reverse=True)
    en_curso = [m for m in ordenados if str(m.get("delivered") or "").strip().lower() != "finalizado"]
    return en_curso[0] if en_curso else ordenados[0]


def _saneado(doc: Dict[str, Any], placa: str, guias: Optional[set] = None) -> Dict[str, Any]:
    """Payload mínimo para el portal — NUNCA el historial location[] completo
    (peso) ni news/zones (no pedidas).

    (2026-11-11) ``coincide_guia``: con ``guias`` (las guías Siscore que viaja
    buscando el portal), True si AL MENOS UNA aparece en el
    ``clientreferencenumber`` de las remesas del manifiesto (texto con guías
    separadas por coma, p. ej. "801233381, 801233382"); False = el manifiesto
    vigente de la placa NO declara esas guías (otro viaje/cliente). None si no
    se pidió validación."""
    ultimo = None
    locs = doc.get("location") or []
    if isinstance(locs, list) and locs:
        ultimo = locs[-1] if isinstance(locs[-1], dict) else None
    velocidad = None
    try:
        velocidad = int(float(str(ultimo.get("velocity")).strip())) if ultimo and str(ultimo.get("velocity") or "").strip() else None
    except (TypeError, ValueError):
        velocidad = None
    out = {
        "placa": str(doc.get("licenceplate") or placa or "").strip(),
        "manifiesto": str(doc.get("manifest") or "").strip(),
        "estado_viaje": str(doc.get("delivered") or "").strip(),
        "conductor": str(doc.get("driver") or "").strip(),
        "ubicacion": str(doc.get("locationdescription") or "").strip(),
        "lat": _num_seguro(doc.get("lastlatitude")),
        "lng": _num_seguro(doc.get("lastlongitude")),
        "fecha_reporte_gps": str(doc.get("gpslastreportdate") or "").strip(),
        "velocidad": velocidad,
        "estado_gps": str((ultimo or {}).get("state") or "").strip(),
        "eta_fecha": str(doc.get("etadate") or "").strip(),
        "eta_distancia_km": _kilometros(doc.get("etadistance")),
    }
    if guias is not None:
        out["coincide_guia"] = bool(_guias_manifiesto(doc) & {str(g).strip().upper() for g in guias})
    return out


def _guias_manifiesto(doc: Dict[str, Any]) -> set:
    """Guías Siscore declaradas en las remesas del manifiesto: cada remesa
    lleva ``clientreferencenumber`` como TEXTO con varias guías separadas por
    coma (p. ej. "801233381, 801233382"; vacío cuando el cliente del monitoreo
    no lo reporta). Se parsea tolerante: coma/salto, espacios, minúsculas."""
    guias: set = set()
    for rem in doc.get("remittances") or []:
        if not isinstance(rem, dict):
            continue
        for token in str(rem.get("clientreferencenumber") or "").replace("\n", ",").split(","):
            token = token.strip().upper()
            if token:
                guias.add(token)
    return guias


async def consultar_vehiculo_monitor(
    placa: str,
    guias: Optional[set] = None,
    transport: Optional[httpx.AsyncBaseTransport] = None,
) -> Dict[str, Any]:
    """Última posición GPS del vehículo de una placa según Monitor.

    ``guias`` (2026-11-11): guías Siscore de la consulta (la guía + las
    compañeras del vehículo) — agrega ``vehiculo.coincide_guia`` para que el
    portal valide que el manifiesto vigente ES el viaje de esas guías.

    Devuelve (nunca lanza):
      {"ok": True, "existe": True,  "vehiculo": {...}}   → hay posición
      {"ok": True, "existe": False}                      → placa sin despachos
      {"ok": False, "sin_credenciales": True}            → sin MONITOR_* en env
      {"ok": False, "error": "..."}                      → fallo de red/API
    """
    placa = (placa or "").strip().upper()
    if not placa:
        return {"ok": False, "error": "placa vacía"}
    if not _hay_credenciales():
        logger.info("[monitor_ws] Sin MONITOR_USER/MONITOR_PASSWORD: se omite la consulta de ubicación")
        return {"ok": False, "sin_credenciales": True}

    timeout = httpx.Timeout(MONITOR_TIMEOUT_S, connect=min(10.0, MONITOR_TIMEOUT_S))
    url = f"{MONITOR_BASE_URL}{_CONSULTA_PATH}"
    # (2026-11-11) remittances: sin la lista de remesas no hay
    # clientreferencenumber con qué validar que el manifiesto es el viaje
    # de las guías consultadas (ver _guias_manifiesto).
    params = {"licensePlate": placa, "lists": "location,remittances"}

    for intento in (1, 2):  # 2º intento solo si el token cacheado expiró a medias (401/403)
        try:
            token = await _obtener_token(forzar=intento == 2, transport=transport)
        except Exception as e:
            return {"ok": False, "error": f"login: {e}"}
        try:
            async with httpx.AsyncClient(timeout=timeout, trust_env=False, transport=transport) as client:
                r = await client.get(url, params=params, headers=_auth_header(token))
            if r.status_code in (401, 403) and intento == 1:
                logger.warning("[monitor_ws] %s en consulta de %s: re-login", r.status_code, placa)
                _invalidar_token()
                continue
            if r.status_code != 200:
                logger.warning("[monitor_ws] Consulta de %s → %s: %s", placa, r.status_code, (r.text or "")[:200])
                return {"ok": False, "error": f"http {r.status_code}"}
            data = r.json()
            manifiestos = data if isinstance(data, list) else []
            doc = _seleccionar_manifiesto(manifiestos)
            if not doc:
                return {"ok": True, "existe": False}
            return {"ok": True, "existe": True, "vehiculo": _saneado(doc, placa, guias=guias)}
        except Exception as e:
            logger.warning("[monitor_ws] Error consultando placa %s: %s", placa, e)
            return {"ok": False, "error": str(e)}
    return {"ok": False, "error": "no autorizado"}
