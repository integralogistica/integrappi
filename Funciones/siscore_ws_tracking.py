# Funciones/siscore_ws_tracking.py
import os
import html
import asyncio
import socket
from typing import Dict, Any, List, Optional, Tuple

import httpx
import xml.etree.ElementTree as ET


SISCORE_SOAP_ENDPOINT = os.getenv(
    "SISCORE_SOAP_ENDPOINT",
    "https://integra.appsiscore.com/app/ws/trazabilidad.php",
)

SISCORE_SOAP_TOKEN = os.getenv("SISCORE_SOAP_TOKEN", "")

SOAP_ACTION = os.getenv("SISCORE_SOAP_ACTION", "ConsultarGuiaImagen")
SOAP_NS = "http://schemas.xmlsoap.org/soap/envelope/"
WS_NS = "https://ws.appsiscore.com/alasdecolombia/"

SISCORE_TRACKING_URL_TEMPLATE = os.getenv(
    "SISCORE_TRACKING_URL_TEMPLATE",
    "https://integra.appsiscore.com/app/app-cliente/cons_publica.php?GUIA={guia}",
)

# ==============================================================================
# ✅ PROXY (mismo que Vulcano)
# ==============================================================================
def _get_proxy_url() -> Optional[str]:
    """
    Usa el mismo proxy que Vulcano.
    Acepta estos formatos en env:
      - http://ip:3128
      - http://user:pass@ip:3128
      - ip:3128              (se normaliza a http://ip:3128)
      - user:pass@ip:3128    (se normaliza a http://user:pass@ip:3128)
    """
    proxy_url = os.getenv("VULCANO_PROXY_URL", "").strip()
    if not proxy_url:
        return None
    if "://" not in proxy_url:
        proxy_url = "http://" + proxy_url
    return proxy_url


def _proxy_safe_preview(proxy_url: Optional[str]) -> str:
    if not proxy_url:
        return "None"
    # quita user:pass@
    return proxy_url.split("@")[-1]


def _build_envelope(num_guia: str, token: str) -> str:
    return f"""<?xml version="1.0" encoding="utf-8"?>
<soapenv:Envelope xmlns:soapenv="{SOAP_NS}" xmlns:ws="{WS_NS}">
  <soapenv:Header/>
  <soapenv:Body>
    <ws:ConsultarGuiaImagen>
      <NumGui>{num_guia}</NumGui>
      <Token>{token}</Token>
    </ws:ConsultarGuiaImagen>
  </soapenv:Body>
</soapenv:Envelope>"""


def _strip_namespace(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _safe_text(v: Any) -> str:
    return str(v or "").strip()


def _tracking_url(guia: str) -> str:
    tpl = SISCORE_TRACKING_URL_TEMPLATE or ""
    return tpl.replace("{guia}", guia).replace("{num_guia}", guia) if tpl else ""


def _parse_inner_result_xml(inner_xml: str) -> Dict[str, Any]:
    root = ET.fromstring(inner_xml)

    data: Dict[str, Any] = {}
    movimientos: List[Dict[str, str]] = []

    for child in list(root):
        tag = _strip_namespace(child.tag)

        if tag == "Mov":
            for inf in child.findall(".//*"):
                if _strip_namespace(inf.tag) == "InformacionMov":
                    mov: Dict[str, str] = {}
                    for f in list(inf):
                        mov[_strip_namespace(f.tag)] = _safe_text(f.text)
                    if mov:
                        movimientos.append(mov)
        else:
            data[tag] = _safe_text(child.text)

    data["Movimientos"] = movimientos
    return data


def _es_guia_no_existente(parsed: Dict[str, Any]) -> bool:
    cliente = _safe_text(parsed.get("Cliente")).lower()
    estado = _safe_text(parsed.get("Estado")).lower()
    envio = _safe_text(parsed.get("Envio"))
    movimientos = parsed.get("Movimientos") or []

    cliente_vacio = (not cliente) or cliente in {"(sin cliente)", "sin cliente", "-", "null", "none"}
    estado_vacio = (not estado) or estado in {"(sin estado)", "sin estado", "-", "null", "none"}
    envio_vacio = (not envio) or envio in {"-", "null", "none"}

    return (len(movimientos) == 0) and cliente_vacio and estado_vacio and envio_vacio


def _extract_host_port(url: str) -> Tuple[Optional[str], Optional[int]]:
    try:
        u = httpx.URL(url)
        host = u.host
        port = u.port or (443 if u.scheme == "https" else 80)
        return host, int(port)
    except Exception:
        return None, None


def _resolve_host_ips(host: str) -> List[str]:
    ips: List[str] = []
    try:
        info = socket.getaddrinfo(host, None)
        for item in info:
            ip = item[4][0]
            if ip and ip not in ips:
                ips.append(ip)
    except Exception:
        pass
    return ips


async def _probar_proxy_si_aplica(proxy_url: Optional[str]) -> None:
    """
    Test liviano para confirmar si el proxy responde (no garantiza acceso a Siscore).
    Si falla, deja log claro. No rompe el flujo principal.
    """
    if not proxy_url:
        return
    try:
        async with httpx.AsyncClient(proxy=proxy_url, timeout=20.0, trust_env=False) as c:
            r = await c.get("https://api.ipify.org?format=json")
            print("➡️ PROXY_TEST ipify STATUS:", r.status_code, "BODY_PREVIEW:", (r.text or "")[:120], flush=True)
    except Exception as e:
        print("❌ PROXY_TEST ERROR:", repr(e), flush=True)


async def consultar_guia_ws(num_guia: str, timeout_seconds: float = 20.0) -> Dict[str, Any]:
    if not SISCORE_SOAP_TOKEN:
        raise RuntimeError("Falta SISCORE_SOAP_TOKEN en variables de entorno.")

    envelope = _build_envelope(num_guia, SISCORE_SOAP_TOKEN)

    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": SOAP_ACTION,
        "Accept": "*/*",
        "Accept-Encoding": "identity",
    }

    timeout = httpx.Timeout(
        connect=min(10.0, float(timeout_seconds)),
        read=float(timeout_seconds),
        write=float(timeout_seconds),
        pool=float(timeout_seconds),
    )

    limits = httpx.Limits(max_connections=20, max_keepalive_connections=10)

    host, port = _extract_host_port(SISCORE_SOAP_ENDPOINT)
    ips = _resolve_host_ips(host) if host else []

    proxy_url = _get_proxy_url()

    print("➡️ SOAP request guia:", num_guia, flush=True)
    print("➡️ ENDPOINT:", SISCORE_SOAP_ENDPOINT, flush=True)
    print("➡️ SOAP_ACTION:", SOAP_ACTION, flush=True)
    print("➡️ TOKEN_LEN:", len(SISCORE_SOAP_TOKEN or ""), flush=True)
    print("➡️ PROXY_ENABLED:", bool(proxy_url), flush=True)
    print("➡️ PROXY:", _proxy_safe_preview(proxy_url), flush=True)
    if host:
        print(f"➡️ DEST_HOST: {host}:{port}", flush=True)
    if ips:
        print(f"➡️ DEST_IPS: {', '.join(ips)}", flush=True)

    # ✅ Test rápido del proxy (para diagnóstico). Si no lo quieres, bórralo.
    await _probar_proxy_si_aplica(proxy_url)

    max_intentos = 3
    last_err: Optional[str] = None
    text: Optional[str] = None

    for intento in range(1, max_intentos + 1):
        try:
            async with httpx.AsyncClient(
                timeout=timeout,
                limits=limits,
                http2=False,
                proxy=proxy_url,  # ✅ SALE POR EL PROXY (si existe)
                trust_env=False,  # ✅ IGNORA HTTP_PROXY/HTTPS_PROXY DEL ENTORNO
            ) as client:
                resp = await client.post(
                    SISCORE_SOAP_ENDPOINT,
                    content=envelope.encode("utf-8"),
                    headers=headers,
                )
                print("⬅️ STATUS_CODE:", resp.status_code, flush=True)
                resp.raise_for_status()
                text = resp.text
            break

        except httpx.ProxyError as e:
            last_err = f"ProxyError intento {intento}/{max_intentos}: {repr(e)}"
            print("❌ SISCORE PROXY ERROR:", last_err, flush=True)
            if intento == max_intentos:
                raise
            await asyncio.sleep(1.2 * intento)

        except (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.WriteTimeout, httpx.PoolTimeout) as e:
            last_err = f"{type(e).__name__} intento {intento}/{max_intentos}: {repr(e)}"
            print("❌ SISCORE TIMEOUT:", last_err, flush=True)
            if intento == max_intentos:
                raise
            await asyncio.sleep(1.2 * intento)

        except httpx.HTTPStatusError as e:
            last_err = f"HTTPStatusError intento {intento}/{max_intentos}: {repr(e)}"
            print("❌ SISCORE HTTP ERROR:", last_err, flush=True)
            if intento == max_intentos:
                raise
            await asyncio.sleep(1.2 * intento)

        except httpx.RequestError as e:
            # otros errores de red (DNS, conexión, etc.)
            last_err = f"RequestError intento {intento}/{max_intentos}: {repr(e)}"
            print("❌ SISCORE REQUEST ERROR:", last_err, flush=True)
            if intento == max_intentos:
                raise
            await asyncio.sleep(1.2 * intento)

    if not text:
        return {"ok": False, "error": last_err or "Sin respuesta del servicio Siscore."}

    try:
        soap_root = ET.fromstring(text)
    except Exception:
        return {"ok": False, "error": "Respuesta SOAP no es XML válido.", "raw_preview": text[:1000]}

    result_node = None
    for node in soap_root.iter():
        if _strip_namespace(node.tag) == "Result":
            result_node = node
            break

    if result_node is None:
        return {"ok": False, "error": "No vino nodo Result en SOAP.", "raw_preview": text[:1000]}

    escaped_inner = (result_node.text or "").strip()
    if not escaped_inner:
        return {"ok": False, "error": "Result vacío.", "raw_preview": text[:1000]}

    inner_xml = html.unescape(escaped_inner)

    try:
        parsed = _parse_inner_result_xml(inner_xml)
    except Exception:
        return {"ok": False, "error": "No se pudo parsear el XML interno.", "inner_preview": inner_xml[:1000]}

    not_found = _es_guia_no_existente(parsed)
    exists = not not_found

    result: Dict[str, Any] = {
        "ok": True,
        "guia": num_guia,
        "exists": exists,
        "not_found": not_found,
        "data": parsed,
    }

    if exists:
        result["tracking_url"] = _tracking_url(num_guia)

    return result
