# vulcano.py
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import requests

# ==============================================================================
# 🔗 CONFIG (variables de entorno recomendadas)
# ==============================================================================
VULCANO_HOST = os.getenv("VULCANO_HOST", "https://api.nescanis.com")
VULCANO_BASE_PATH = os.getenv("VULCANO_BASE_PATH", "/vulcano")

# Endpoints (confirmados con tu prueba en Thunder)
VULCANO_LOGIN_PATH = os.getenv("VULCANO_LOGIN_PATH", "/cloud/v1/auth/loginDbCustomer")
VULCANO_CUSTOMER_INDEX_PATH = os.getenv(
    "VULCANO_CUSTOMER_INDEX_PATH",
    "/cloud/v1/vulcano/customer/00134/index",
)

# Credenciales
VULCANO_USERNAME = os.getenv("VULCANO_USERNAME", "134APIINTEGRA")
VULCANO_IDNAME = os.getenv(
    "VULCANO_IDNAME",
    # Usa el idname válido (el que te funcionó en Thunder)
    "eyJpdiI6IlZSdVpoaHBhYk02b3ZFRTdMQlhuZnc9PSIsInZhbHVlIjoiTGs4KzM2OGhxWGo4ekVLUkVGMG1yS1EwUDEwNkZxdVl5VzNWcDNCQ0drMD0iLCJtYWMiOiJjMzEzMDEzYTk3OWJhNTM2MTYyYjlmZDRkNDE4ZDFlMzc2OGQ5MTg0ZWYwYzFkMmJkNjY5ZDZhNDI2N2I5ZDBmIiwidGFnIjoiIn0=",
)
VULCANO_AGENCY = os.getenv("VULCANO_AGENCY", "001")
VULCANO_PROJECT = os.getenv("VULCANO_PROJECT", "1")
VULCANO_IS_GROUP = int(os.getenv("VULCANO_IS_GROUP", "0"))

# Reporte por defecto
VULCANO_RPT_ID_PAGOS = int(os.getenv("VULCANO_RPT_ID_PAGOS", "27"))

# SSL verify (True/False). En nescanis debe ser True.
VULCANO_VERIFY_SSL = os.getenv("VULCANO_VERIFY_SSL", "true").strip().lower() in ("1", "true", "yes", "y")


def _build_url(path: str) -> str:
    return f"{VULCANO_HOST.rstrip('/')}{VULCANO_BASE_PATH}{path}"


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
    s = session or requests.Session()

    url = _build_url(VULCANO_LOGIN_PATH)
    payload = {
        "username": VULCANO_USERNAME,
        "idname": VULCANO_IDNAME,
        "agency": VULCANO_AGENCY,
        "proyect": VULCANO_PROJECT,
        "isGroup": VULCANO_IS_GROUP,
    }

    try:
        r = s.post(
            url,
            json=payload,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            timeout=timeout,
            verify=VULCANO_VERIFY_SSL,
        )
        r.raise_for_status()
        data = r.json()
    except requests.RequestException as e:
        raise VulcanoAuthError(f"Error en login Vulcano: {e}") from e
    except ValueError as e:
        raise VulcanoAuthError(f"Login Vulcano no devolvió JSON. Respuesta: {r.text[:300]}") from e

    token = (data or {}).get("data", {}).get("access_token")
    if not token:
        raise VulcanoAuthError(f"No se encontró access_token en respuesta de login: {data}")

    return token


# ==============================================================================
# 📄 CONSULTAS
# ==============================================================================
def consultar_por_tenedor(
    cedula_tenedor: str,
    year: str = "2024",
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

    IMPORTANTE: Usa el formato 'campo/operador/valor' (como tu prueba en Thunder).
    """
    if not cedula_tenedor:
        raise ValueError("cedula_tenedor es requerida")

    s = session or requests.Session()
    token = vulcano_login(session=s, timeout=timeout)

    url = _build_url(VULCANO_CUSTOMER_INDEX_PATH)
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

    try:
        r = s.post(url, json=payload, headers=headers, timeout=timeout, verify=VULCANO_VERIFY_SSL)
        r.raise_for_status()
        data = r.json()
    except requests.RequestException as e:
        raise VulcanoRequestError(f"Error consultando customer/index: {e}") from e
    except ValueError as e:
        raise VulcanoRequestError(f"Consulta no devolvió JSON. Respuesta: {r.text[:300]}") from e

    filas = (data or {}).get("data", {}).get("data")
    if filas is None:
        raise VulcanoRequestError(f"Estructura inesperada en respuesta: {data}")

    return filas


def extraer_manifiestos(filas: List[Dict[str, Any]]) -> List[str]:
    """
    Helper: devuelve lista de manifiestos encontrados en las filas.
    """
    out: List[str] = []
    for f in filas or []:
        m = f.get("Manifiesto")
        if m:
            out.append(str(m))
    return out


# ==============================================================================
# ✅ PRUEBA RÁPIDA
# ==============================================================================
if __name__ == "__main__":
    cedula = "11200427"  # cambia por la real
    filas = consultar_por_tenedor(
        cedula_tenedor=cedula,
        year="2024",
        pago_saldo="No Aplicado",
    )

    print("TOTAL FILAS:", len(filas))
    print("PRIMERAS 3 FILAS:")
    for x in filas[:20]:
        print(x)

    print("MANIFIESTOS:", extraer_manifiestos(filas)[:10])
