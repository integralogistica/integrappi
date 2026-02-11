# rutas/debug_network.py
from fastapi import APIRouter, HTTPException
import os
import requests

ruta_debug_network = APIRouter(
    prefix="/debug",
    tags=["Debug"],
)


@ruta_debug_network.get("/ip")
def debug_ip():
    """
    Devuelve la IP de salida del servidor.

    - Si VULCANO_PROXY_URL está configurado, hace la request usando ESE proxy.
    - Si NO está configurado, fuerza a NO usar proxies del entorno (HTTP_PROXY/HTTPS_PROXY),
      para evitar errores tipo 403 Forbidden al tunelizar en Render.
    """
    proxy = os.getenv("VULCANO_PROXY_URL", "").strip()

    try:
        with requests.Session() as s:
            # Evita que requests use HTTP_PROXY/HTTPS_PROXY del entorno automáticamente
            s.trust_env = False

            if proxy:
                proxies = {"http": proxy, "https": proxy}
                resp = s.get("https://api.ipify.org?format=json", proxies=proxies, timeout=20)
            else:
                resp = s.get("https://api.ipify.org?format=json", timeout=20)

            resp.raise_for_status()
            data = resp.json()

        out_ip = (data.get("ip") or "").strip()
        if not out_ip:
            raise HTTPException(status_code=502, detail="No se pudo obtener la IP desde ipify.")

        return {
            "proxy_configured": bool(proxy),
            "proxy_url": proxy or None,
            "out_ip": out_ip,
        }

    except requests.exceptions.ProxyError as e:
        raise HTTPException(
            status_code=502,
            detail=f"Error de proxy al consultar ipify. Proxy={proxy or 'N/A'}. Detalle={str(e)}",
        )
    except requests.exceptions.RequestException as e:
        raise HTTPException(
            status_code=502,
            detail=f"Error de red al consultar ipify. Detalle={str(e)}",
        )


@ruta_debug_network.get("/env")
def debug_env():
    """
    Confirma si variables de entorno clave están seteadas (sin exponer secretos).
    """
    keys = [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "VULCANO_PROXY_URL",
        "VULCANO_HOST",
        "VULCANO_BASE_PATH",
        "VULCANO_LOGIN_PATH",
        "VULCANO_CUSTOMER_INDEX_PATH",
        "VULCANO_USERNAME",
        "VULCANO_IDNAME",
        "VULCANO_AGENCY",
        "VULCANO_PROJECT",
        "VULCANO_IS_GROUP",
    ]

    out = {}
    for k in keys:
        v = os.getenv(k)
        if v is None:
            out[k] = {"set": False, "value_preview": None}
        else:
            preview = v[:4] + "..." + v[-4:] if len(v) > 10 else v
            out[k] = {"set": True, "value_preview": preview}

    return out
