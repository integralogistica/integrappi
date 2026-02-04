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
                    mov = {}
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
    """
    if not SISCORE_SOAP_TOKEN:
        raise RuntimeError("Falta SISCORE_SOAP_TOKEN en variables de entorno.")

    envelope = _build_envelope(num_guia, SISCORE_SOAP_TOKEN)

    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": SOAP_ACTION,
        "Accept": "*/*",
    }
    timeout = httpx.Timeout(timeout_seconds, connect=30.0, read=timeout_seconds, write=30.0, pool=timeout_seconds)

    async with httpx.AsyncClient(timeout=timeout_seconds) as client:
        print("➡️ SOAP request guia:", num_guia, flush=True)
        print("➡️ ENDPOINT:", SISCORE_SOAP_ENDPOINT, flush=True)
        print("➡️ TOKEN_LEN:", len(SISCORE_SOAP_TOKEN or ""), flush=True)
        
        resp = await client.post(SISCORE_SOAP_ENDPOINT, content=envelope.encode("utf-8"), headers=headers)
        resp.raise_for_status()

        # La respuesta puede venir en ISO-8859-1; httpx usualmente lo maneja,
        # pero si hay caracteres raros, puedes forzar latin-1:
        text = resp.text

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
