# vulcano.py
from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional

import requests

# ==============================================================================
# 🧰 HELPERS
# ==============================================================================
def _resp_debug(r: requests.Response) -> str:
    try:
        body = r.text or ""
    except Exception:
        body = "<no-text>"
    body = body[:600]  # evita logs gigantes
    return f"status={r.status_code} url={r.url} body={body!r}"


def _build_url(host: str, base_path: str, path: str) -> str:
    return f"{host.rstrip('/')}{base_path}{path}"


# ✅ NUEVO: arma proxies solo si hay env var
def _get_proxies() -> Optional[Dict[str, str]]:
    """
    Retorna proxies para requests si VULCANO_PROXY_URL está configurada.
    Ej: http://129.212.166.228:8888
    Ej con auth: http://user:pass@129.212.166.228:8888
    """
    proxy_url = os.getenv("VULCANO_PROXY_URL", "").strip()
    if not proxy_url:
        return None
    return {"http": proxy_url, "https": proxy_url}


# ==============================================================================
# 🔗 CONFIG (variables de entorno)
# ==============================================================================
VULCANO_HOST = os.getenv("VULCANO_HOST", "https://api.nescanis.com")
VULCANO_BASE_PATH = os.getenv("VULCANO_BASE_PATH", "/vulcano")

# Endpoints
VULCANO_LOGIN_PATH = os.getenv("VULCANO_LOGIN_PATH", "/cloud/v1/auth/loginDbCustomer")
VULCANO_CUSTOMER_INDEX_PATH = os.getenv(
    "VULCANO_CUSTOMER_INDEX_PATH",
    "/cloud/v1/vulcano/customer/00134/index",
)

# Credenciales
VULCANO_USERNAME = os.getenv("VULCANO_USERNAME", "134APIINTEGRA")
VULCANO_IDNAME = os.getenv("VULCANO_IDNAME")  # obligatorio en tu caso
VULCANO_AGENCY = os.getenv("VULCANO_AGENCY", "001")
VULCANO_PROJECT = os.getenv("VULCANO_PROJECT", "1")
VULCANO_IS_GROUP = int(os.getenv("VULCANO_IS_GROUP", "0"))

# Reporte por defecto
VULCANO_RPT_ID_PAGOS = int(os.getenv("VULCANO_RPT_ID_PAGOS", "27"))

# SSL verify
VULCANO_VERIFY_SSL = os.getenv("VULCANO_VERIFY_SSL", "true").strip().lower() in ("1", "true", "yes", "y")

# Timeouts (connect, read)
VULCANO_CONNECT_TIMEOUT = int(os.getenv("VULCANO_CONNECT_TIMEOUT", "10"))


# ==============================================================================
# 🧩 ERRORES
# ==============================================================================
class VulcanoError(Exception):
    """Error base para Vulcano."""


class VulcanoAuthError(VulcanoError):
    """Error de autenticación con Vulcano."""


class VulcanoRequestError(VulcanoError):
    """Error de petición/response."""


# ==============================================================================
# 🔐 AUTH
# ==============================================================================
def vulcano_login(session: Optional[requests.Session] = None, timeout: int = 120) -> str:
    """
    Inicia sesión y devuelve access_token.
    """
    if not VULCANO_IDNAME:
        raise VulcanoAuthError("Login: VULCANO_IDNAME no está configurado (env var vacía o ausente).")

    s = session or requests.Session()

    url = _build_url(VULCANO_HOST, VULCANO_BASE_PATH, VULCANO_LOGIN_PATH)
    payload = {
        "username": VULCANO_USERNAME,
        "idname": VULCANO_IDNAME,
        "agency": VULCANO_AGENCY,
        "proyect": VULCANO_PROJECT,  # mantener "proyect" si así lo exige Vulcano
        "isGroup": VULCANO_IS_GROUP,
    }

    proxies = _get_proxies()  # ✅ NUEVO

    t0 = time.time()
    try:
        r = s.post(
            url,
            json=payload,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            timeout=(VULCANO_CONNECT_TIMEOUT, timeout),
            verify=VULCANO_VERIFY_SSL,
            proxies=proxies,  # ✅ CAMBIO (agregado)
        )
        r.raise_for_status()

        try:
            data = r.json()
        except ValueError as e:
            raise VulcanoAuthError(f"Login: respuesta NO es JSON. {_resp_debug(r)}") from e

    except requests.Timeout as e:
        raise VulcanoAuthError(f"Login: TIMEOUT ({time.time() - t0:.1f}s) url={url}") from e

    except requests.HTTPError as e:
        resp = getattr(e, "response", None)
        if resp is not None:
            raise VulcanoAuthError(f"Login: HTTPError. {_resp_debug(resp)}") from e
        raise VulcanoAuthError(f"Login: HTTPError sin response. err={e}") from e

    except requests.ConnectionError as e:
        raise VulcanoAuthError(f"Login: ConnectionError url={url} err={e}") from e

    except requests.RequestException as e:
        raise VulcanoAuthError(f"Login: RequestException url={url} err={e}") from e

    token = (data or {}).get("data", {}).get("access_token")
    if not token:
        raise VulcanoAuthError(f"Login: no access_token. data={data}")

    return token


# ==============================================================================
# 📄 CONSULTAS
# ==============================================================================
def consultar_por_tenedor(
    cedula_tenedor: str,
    year: str = "2025",
    pago_saldo: str = "No Aplicado",
    rpt_id: int = VULCANO_RPT_ID_PAGOS,
    page_size: int = 1000,
    page: int = 1,
    session: Optional[requests.Session] = None,
    timeout: int = 120,
) -> List[Dict[str, Any]]:
    """
    Devuelve filas del reporte (lista de dicts) filtradas por:
      - Fecha YEAR> year
      - Tenedor = cedula_tenedor
      - Pago saldo = pago_saldo
    """
    if not cedula_tenedor:
        raise ValueError("cedula_tenedor es requerida")

    s = session or requests.Session()
    token = vulcano_login(session=s, timeout=timeout)

    url = _build_url(VULCANO_HOST, VULCANO_BASE_PATH, VULCANO_CUSTOMER_INDEX_PATH)

    payload = {
        "pageSize": page_size,
        "page": page,
        "rptId": rpt_id,
        "filter": [
            {"campo": "Fecha", "operador": "YEAR>", "valor": str(year)},
            {"campo": "Tenedor", "operador": "=", "valor": str(cedula_tenedor)},
            {"campo": "Pago saldo", "operador": "=", "valor": str(pago_saldo)},
        ],
    }

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }

    proxies = _get_proxies()  # ✅ NUEVO

    t0 = time.time()
    try:
        r = s.post(
            url,
            json=payload,
            headers=headers,
            timeout=(VULCANO_CONNECT_TIMEOUT, timeout),
            verify=VULCANO_VERIFY_SSL,
            proxies=proxies,  # ✅ CAMBIO (agregado)
        )
        r.raise_for_status()

        try:
            data = r.json()
        except ValueError as e:
            raise VulcanoRequestError(f"Consulta: respuesta NO es JSON. {_resp_debug(r)}") from e

    except requests.Timeout as e:
        raise VulcanoRequestError(f"Consulta: TIMEOUT ({time.time() - t0:.1f}s) url={url}") from e

    except requests.HTTPError as e:
        resp = getattr(e, "response", None)
        if resp is not None:
            raise VulcanoRequestError(f"Consulta: HTTPError. {_resp_debug(resp)}") from e
        raise VulcanoRequestError(f"Consulta: HTTPError sin response. err={e}") from e

    except requests.ConnectionError as e:
        raise VulcanoRequestError(f"Consulta: ConnectionError url={url} err={e}") from e

    except requests.RequestException as e:
        raise VulcanoRequestError(f"Consulta: RequestException url={url} err={e}") from e

    filas = (data or {}).get("data", {}).get("data")
    if filas is None:
        raise VulcanoRequestError(f"Consulta: estructura inesperada en respuesta: {data}")

    if not isinstance(filas, list):
        raise VulcanoRequestError(f"Consulta: 'data.data' no es lista. type={type(filas).__name__} data={data}")

    # Normaliza a dicts (por si llega algo raro)
    out: List[Dict[str, Any]] = []
    for row in filas:
        if isinstance(row, dict):
            out.append(row)
        else:
            out.append({"_raw": row})

    return out


def extraer_manifiestos(filas: List[Dict[str, Any]]) -> List[str]:
    out: List[str] = []
    for f in filas or []:
        if isinstance(f, dict):
            m = f.get("Manifiesto")
            if m:
                out.append(str(m))
    return out


# ==============================================================================
# ✅ PRUEBA RÁPIDA
# ==============================================================================
if __name__ == "__main__":
    # ✅ Si vas a probar localmente, exporta antes:
    # export VULCANO_PROXY_URL="http://129.212.166.228:8888"
    # o con auth: "http://user:pass@129.212.166.228:8888"

    cedula = "11200427"  # cambia por la real
    filas = consultar_por_tenedor(
        cedula_tenedor=cedula,
        year="2025",
        pago_saldo="No Aplicado",
        page_size=200,
        page=1,
    )

    print("TOTAL FILAS:", len(filas))
    print("PRIMERAS 5 FILAS:")
    for x in filas[:5]:
        print(x)

    print("MANIFIESTOS:", extraer_manifiestos(filas)[:5])
