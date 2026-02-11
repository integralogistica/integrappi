# rutas/debug_network.py
from fastapi import APIRouter
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
    Si VULCANO_PROXY_URL está configurado, hace la request usando el proxy.
    """
    proxy = os.getenv("VULCANO_PROXY_URL", "").strip()
    proxies = {"http": proxy, "https": proxy} if proxy else None

    r = requests.get("https://api.ipify.org", proxies=proxies, timeout=20)
    return {
        "proxy_configured": bool(proxy),
        "proxy_url": proxy or None,
        "out_ip": r.text.strip(),
    }

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
            # preview seguro (no muestra completo)
            preview = v[:4] + "..." + v[-4:] if len(v) > 10 else v
            out[k] = {"set": True, "value_preview": preview}

    return out
