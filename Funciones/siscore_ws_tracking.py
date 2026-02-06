# Funciones/siscore_ws_tracking.py
import os
import html
import httpx
import xml.etree.ElementTree as ET
from typing import Dict, Any, List, Optional

SISCORE_SOAP_ENDPOINT = os.getenv(
    "SISCORE_SOAP_ENDPOINT",
    "https://integra.appsiscore.com/app/ws/trazabilidad.php",
)

SISCORE_SOAP_TOKEN = os.getenv("SISCORE_SOAP_TOKEN", "")

SOAP_ACTION = os.getenv("SISCORE_SOAP_ACTION", "ConsultarGuiaImagen")
SOAP_NS = "http://schemas.xmlsoap.org/soap/envelope/"
WS_NS = "https://ws.appsiscore.com/alasdecolombia/"

# URL de redirección/seguimiento que ya usaban antes (ajústala si aplica)
# Ejemplo: https://integra.appsiscore.com/app/app-cliente/cons_publica.php?guia={guia}
SISCORE_TRACKING_URL_TEMPLATE = os.getenv(
    "SISCORE_TRACKING_URL_TEMPLATE",
    "https://integra.appsiscore.com/app/app-cliente/cons_publica.php={guia}",
)


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
    # Permite usar {guia} o {num_guia}
    tpl = SISCORE_TRACKING_URL_TEMPLATE or ""
    return (
        tpl.replace("{guia}", guia).replace("{num_guia}", guia)
        if tpl else ""
    )


def _parse_inner_result_xml(inner_xml: str) -> Dict[str, Any]:
    root = ET.fromstring(inner_xml)

    data: Dict[str, Any] = {}
    movimientos: List[Dict[str, str]] = []

    for child in list(root):
        tag = _strip_namespace(child.tag)

        if tag == "Mov":
            for inf in child.findall(".//*"):
                if _strip_namespace(inf.tag) == "InformacionMov":
                    mov = {}
                    for f in list(inf):
                        mov[_strip_namespace(f.tag)] = _safe_text(f.text)
                    if mov:
                        movimientos.append(mov)
        else:
            data[tag] = _safe_text(child.text)

    data["Movimientos"] = movimientos
    return data


def _es_guia_no_existente(parsed: Dict[str, Any]) -> bool:
    """
    Detecta guía inexistente con heurística robusta:
    - sin movimientos
    - cliente vacío/(sin cliente)
    - estado vacío/(sin estado)
    - envío vacío
    """
    cliente = _safe_text(parsed.get("Cliente")).lower()
    estado = _safe_text(parsed.get("Estado")).lower()
    envio = _safe_text(parsed.get("Envio"))
    movimientos = parsed.get("Movimientos") or []

    cliente_vacio = (not cliente) or cliente in {"(sin cliente)", "sin cliente", "-", "null", "none"}
    estado_vacio = (not estado) or estado in {"(sin estado)", "sin estado", "-", "null", "none"}
    envio_vacio = (not envio) or envio in {"-", "null", "none"}

    return (len(movimientos) == 0) and cliente_vacio and estado_vacio and envio_vacio


async def consultar_guia_ws(num_guia: str, timeout_seconds: float = 15.0) -> Dict[str, Any]:
    """
    Consulta SOAP Siscore y retorna:
    - ok=True/False
    - exists=True/False (si la guía existe)
    - not_found=True/False
    - tracking_url (cuando exista)
    """
    if not SISCORE_SOAP_TOKEN:
        raise RuntimeError("Falta SISCORE_SOAP_TOKEN en variables de entorno.")

    envelope = _build_envelope(num_guia, SISCORE_SOAP_TOKEN)

    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": SOAP_ACTION,
        "Accept": "*/*",
    }

    timeout = httpx.Timeout(
        timeout=timeout_seconds,
        connect=30.0,
        read=timeout_seconds,
        write=30.0,
        pool=timeout_seconds,
    )

    print("➡️ SOAP request guia:", num_guia, flush=True)
    print("➡️ ENDPOINT:", SISCORE_SOAP_ENDPOINT, flush=True)
    print("➡️ SOAP_ACTION:", SOAP_ACTION, flush=True)
    print("➡️ TOKEN_LEN:", len(SISCORE_SOAP_TOKEN or ""), flush=True)

    max_intentos = 2
    last_err: Optional[str] = None
    text: Optional[str] = None

    for intento in range(1, max_intentos + 1):
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.post(
                    SISCORE_SOAP_ENDPOINT,
                    content=envelope.encode("utf-8"),
                    headers=headers,
                )
                print("⬅️ STATUS_CODE:", resp.status_code, flush=True)
                resp.raise_for_status()
                text = resp.text
            break

        except httpx.TimeoutException as e:
            last_err = f"TimeoutException intento {intento}/{max_intentos}: {repr(e)}"
            print("❌ SISCORE TIMEOUT:", last_err, flush=True)
            if intento == max_intentos:
                raise

        except httpx.RequestError as e:
            last_err = f"RequestError intento {intento}/{max_intentos}: {repr(e)}"
            print("❌ SISCORE REQUEST ERROR:", last_err, flush=True)
            if intento == max_intentos:
                raise

        except httpx.HTTPStatusError as e:
            last_err = f"HTTPStatusError intento {intento}/{max_intentos}: {repr(e)}"
            print("❌ SISCORE HTTP ERROR:", last_err, flush=True)
            if intento == max_intentos:
                raise

    if not text:
        return {"ok": False, "error": last_err or "Sin respuesta del servicio Siscore."}

    # 1) Parseamos SOAP
    try:
        soap_root = ET.fromstring(text)
    except Exception:
        return {"ok": False, "error": "Respuesta SOAP no es XML válido.", "raw_preview": text[:1000]}

    # 2) Nodo Result
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

    # 3) Unescape del XML interno
    inner_xml = html.unescape(escaped_inner)

    # 4) Parse interno
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
