# rutas/debug_siscore.py
from fastapi import APIRouter, HTTPException
import os
import time
import socket
import requests
import httpx
import xml.etree.ElementTree as ET

ruta_debug_siscore = APIRouter(
    prefix="/debug",
    tags=["Debug-Siscore"],
)

SISCORE_SOAP_ENDPOINT = os.getenv(
    "SISCORE_SOAP_ENDPOINT",
    "https://integra.appsiscore.com/app/ws/trazabilidad.php",
)

SISCORE_SOAP_TOKEN = os.getenv("SISCORE_SOAP_TOKEN", "")
SOAP_ACTION = os.getenv("SISCORE_SOAP_ACTION", "ConsultarGuiaImagen")


def _resolve_host_ips(url: str):
    try:
        u = httpx.URL(url)
        host = u.host
        info = socket.getaddrinfo(host, None)
        ips = list({item[4][0] for item in info})
        return host, ips
    except Exception:
        return None, []


@ruta_debug_siscore.get("/siscore-test")
def debug_siscore_test(guia: str = "801203888"):
    """
    Prueba real contra Siscore SOAP usando proxy si está configurado.
    Devuelve detalles técnicos y errores completos.
    """

    if not SISCORE_SOAP_TOKEN:
        raise HTTPException(status_code=500, detail="SISCORE_SOAP_TOKEN no está configurado.")

    proxy = os.getenv("VULCANO_PROXY_URL", "").strip()
    proxies = {"http": proxy, "https": proxy} if proxy else None

    envelope = f"""<?xml version="1.0" encoding="utf-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ws="https://ws.appsiscore.com/alasdecolombia/">
  <soapenv:Header/>
  <soapenv:Body>
    <ws:ConsultarGuiaImagen>
      <NumGui>{guia}</NumGui>
      <Token>{SISCORE_SOAP_TOKEN}</Token>
    </ws:ConsultarGuiaImagen>
  </soapenv:Body>
</soapenv:Envelope>"""

    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": SOAP_ACTION,
        "Accept": "*/*",
        "Accept-Encoding": "identity",
    }

    host, ips = _resolve_host_ips(SISCORE_SOAP_ENDPOINT)

    t0 = time.time()

    try:
        with requests.Session() as s:
            s.trust_env = False  # evita HTTP_PROXY automáticos

            resp = s.post(
                SISCORE_SOAP_ENDPOINT,
                data=envelope.encode("utf-8"),
                headers=headers,
                proxies=proxies,
                timeout=25,
                verify=True,
            )

        elapsed = round(time.time() - t0, 2)

        return {
            "proxy_configured": bool(proxy),
            "proxy_url": proxy or None,
            "endpoint": SISCORE_SOAP_ENDPOINT,
            "dest_host": host,
            "dest_ips": ips,
            "status_code": resp.status_code,
            "elapsed_seconds": elapsed,
            "response_preview": resp.text[:800],
        }

    except requests.exceptions.ProxyError as e:
        raise HTTPException(
            status_code=502,
            detail=f"ProxyError usando proxy={proxy or 'N/A'} | {str(e)}",
        )

    except requests.exceptions.ConnectTimeout as e:
        raise HTTPException(
            status_code=504,
            detail=f"ConnectTimeout | {str(e)}",
        )

    except requests.exceptions.ReadTimeout as e:
        raise HTTPException(
            status_code=504,
            detail=f"ReadTimeout (Siscore no respondió a tiempo) | {str(e)}",
        )

    except requests.exceptions.SSLError as e:
        raise HTTPException(
            status_code=502,
            detail=f"SSL Error | {str(e)}",
        )

    except requests.exceptions.RequestException as e:
        raise HTTPException(
            status_code=502,
            detail=f"RequestException | {str(e)}",
        )
