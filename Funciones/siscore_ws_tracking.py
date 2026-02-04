# Funciones/siscore_ws_tracking.py
import os
import html
import httpx
import xml.etree.ElementTree as ET
from typing import Dict, Any, List

SISCORE_SOAP_ENDPOINT = os.getenv(
    "SISCORE_SOAP_ENDPOINT",
    "https://integra.appsiscore.com/app/ws/trazabilidad.php",
)

SISCORE_SOAP_TOKEN = os.getenv("SISCORE_SOAP_TOKEN", "")

SOAP_ACTION = os.getenv("SISCORE_SOAP_ACTION", "ConsultarGuiaImagen")
SOAP_NS = "http://schemas.xmlsoap.org/soap/envelope/"
WS_NS = "https://ws.appsiscore.com/alasdecolombia/"


def _build_envelope(num_guia: str, token: str) -> str:
    # Nota: el WS espera NumGui + Token
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
    # "{namespace}Tag" -> "Tag"
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _parse_inner_result_xml(inner_xml: str) -> Dict[str, Any]:
    """
    inner_xml es el XML real (ya des-escapado) que viene dentro de <Result>.
    Devuelve dict con cabecera y movimientos.
    """
    root = ET.fromstring(inner_xml)

    data: Dict[str, Any] = {}
    movimientos: List[Dict[str, str]] = []

    for child in list(root):
        tag = _strip_namespace(child.tag)

        if tag == "Mov":
            for inf in child.findall(".//*"):
                # buscamos nodos InformacionMov y extraemos campos
                if _strip_namespace(inf.tag) == "InformacionMov":
                    mov: Dict[str, str] = {}
                    for f in list(inf):
                        mov[_strip_namespace(f.tag)] = (f.text or "").strip()
                    if mov:
                        movimientos.append(mov)
        else:
            data[tag] = (child.text or "").strip()

    data["Movimientos"] = movimientos
    return data


async def consultar_guia_ws(num_guia: str, timeout_seconds: float = 15.0) -> Dict[str, Any]:
    """
    Consulta el SOAP y retorna un dict normalizado.
    Maneja que <Result> venga con XML escapado (&lt;...&gt;).
    Además imprime en logs (Render) el error real cuando falle.
    """
    if not SISCORE_SOAP_TOKEN:
        raise RuntimeError("Falta SISCORE_SOAP_TOKEN en variables de entorno.")

    envelope = _build_envelope(num_guia, SISCORE_SOAP_TOKEN)

    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": SOAP_ACTION,
        "Accept": "*/*",
    }

    # Timeout más explícito (útil en redes inestables)
    timeout = httpx.Timeout(
        timeout_seconds,          # total
        connect=30.0,             # conexión
        read=timeout_seconds,     # lectura
        write=30.0,               # escritura
        pool=timeout_seconds,     # pool
    )

    async with httpx.AsyncClient(timeout=timeout) as client:
        print("➡️ SOAP request guia:", num_guia, flush=True)
        print("➡️ ENDPOINT:", SISCORE_SOAP_ENDPOINT, flush=True)
        print("➡️ SOAP_ACTION:", SOAP_ACTION, flush=True)
        print("➡️ TOKEN_LEN:", len(SISCORE_SOAP_TOKEN or ""), flush=True)

        try:
            resp = await client.post(
                SISCORE_SOAP_ENDPOINT,
                content=envelope.encode("utf-8"),
                headers=headers,
            )

            print("⬅️ STATUS_CODE:", resp.status_code, flush=True)
            print("⬅️ BODY_PREVIEW:", (resp.text or "")[:500], flush=True)

            resp.raise_for_status()
            text = resp.text

        except httpx.TimeoutException as e:
            print("❌ SISCORE TIMEOUT:", repr(e), flush=True)
            raise

        except httpx.HTTPStatusError as e:
            r = e.response
            print("❌ SISCORE HTTPStatusError:", repr(e), flush=True)
            print("❌ STATUS:", r.status_code, flush=True)
            print("❌ BODY_PREVIEW:", (r.text or "")[:800], flush=True)
            raise

        except httpx.RequestError as e:
            # DNS/SSL/conexión/etc.
            print("❌ SISCORE RequestError:", repr(e), flush=True)
            raise

        except Exception as e:
            print("❌ SISCORE UnknownError:", repr(e), flush=True)
            raise

    # 1) Parseamos el SOAP
    soap_root = ET.fromstring(text)

    # 2) Encontramos el nodo Result (sin depender del prefijo ns)
    result_node = None
    for node in soap_root.iter():
        if _strip_namespace(node.tag) == "Result":
            result_node = node
            break

    if result_node is None:
        return {"ok": False, "error": "No vino nodo Result en SOAP.", "raw": text}

    escaped_inner = (result_node.text or "").strip()
    if not escaped_inner:
        return {"ok": False, "error": "Result vacío.", "raw": text}

    # 3) Des-escapamos &lt; &gt; etc.
    inner_xml = html.unescape(escaped_inner)

    # 4) Parseamos el XML interno
    parsed = _parse_inner_result_xml(inner_xml)
    return {"ok": True, "guia": num_guia, "data": parsed}
