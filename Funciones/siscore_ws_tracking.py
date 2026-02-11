# Funciones/siscore_ws_tracking.py
import os
from typing import Dict, Any


# Se conserva por compatibilidad, aunque ya no se use SOAP en este modo
SISCORE_SOAP_ENDPOINT = os.getenv(
    "SISCORE_SOAP_ENDPOINT",
    "https://integra.appsiscore.com/app/ws/trazabilidad.php",
)
SISCORE_SOAP_TOKEN = os.getenv("SISCORE_SOAP_TOKEN", "")
SOAP_ACTION = os.getenv("SISCORE_SOAP_ACTION", "ConsultarGuiaImagen")
SOAP_NS = "http://schemas.xmlsoap.org/soap/envelope/"
WS_NS = "https://ws.appsiscore.com/alasdecolombia/"

# URL pública para consulta por link
SISCORE_TRACKING_URL_TEMPLATE = os.getenv(
    "SISCORE_TRACKING_URL_TEMPLATE",
    "https://integra.appsiscore.com/app/app-cliente/cons_publica.php?GUIA={guia}",
)


def _tracking_url(guia: str) -> str:
    """
    Construye la URL de consulta pública.
    Permite usar {guia} o {num_guia} en el template.
    """
    tpl = (SISCORE_TRACKING_URL_TEMPLATE or "").strip()
    if not tpl:
        return ""

    guia_str = str(guia or "").strip()
    return tpl.replace("{guia}", guia_str).replace("{num_guia}", guia_str)


async def consultar_guia_ws(num_guia: str, timeout_seconds: float = 20.0) -> Dict[str, Any]:
    
    guia = str(num_guia or "").strip()
    url = _tracking_url(guia)

    return {
        "ok": True,
        "guia": guia,
        "exists": None,       # no verificable sin SOAP
        "not_found": None,    # no verificable sin SOAP
        "tracking_url": url,
        "data": {},
        "mode": "link_only",
        "note": "Validación manual: si al abrir el enlace aparece vacío, la guía no existe.",
    }
