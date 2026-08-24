from __future__ import annotations

import logging
import time
import xml.etree.ElementTree as ET

import httpx

from .errors import (RNDCCredentialsError, RNDCBusinessError, RNDCNoDataError, RNDCResponseParseError,
                     RNDCSoapFaultError, RNDCTransportError)

logger = logging.getLogger(__name__)
SOAP_NS = "http://schemas.xmlsoap.org/soap/envelope/"
ENC_NS = "http://schemas.xmlsoap.org/soap/encoding/"
OP_NS = "urn:BPMServicesIntf-IBPMServices"
XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"
XSD_NS = "http://www.w3.org/2001/XMLSchema"
SOAP_ACTION = "urn:BPMServicesIntf-IBPMServices#AtenderMensajeRNDC"


def construir_xml_rndc(username, password, periodo, combinacion) -> bytes:
    root = ET.Element("root")
    acceso = ET.SubElement(root, "acceso")
    ET.SubElement(acceso, "username").text = username
    ET.SubElement(acceso, "password").text = password
    solicitud = ET.SubElement(root, "solicitud")
    ET.SubElement(solicitud, "tipo").text = "6"
    ET.SubElement(solicitud, "procesoid").text = "26"
    documento = ET.SubElement(root, "documento")
    valores = {"PERIODO": periodo, "CONFIGURACIONESID": combinacion["configuracion_codigo"], "CONDICIONCARGAID": combinacion["condicion_carga_codigo"], "ORIGEN": combinacion["origen_codigo"], "DESTINO": combinacion["destino_codigo"]}
    for name, value in valores.items():
        ET.SubElement(documento, name).text = f"'{value}'"
    try:
        return ET.tostring(root, encoding="iso-8859-1", xml_declaration=True)
    except UnicodeEncodeError as exc:
        raise RNDCResponseParseError("La solicitud contiene caracteres no representables en ISO-8859-1") from exc


def construir_envolvente_soap(request_xml: bytes) -> bytes:
    envelope = ET.Element(ET.QName(SOAP_NS, "Envelope"), {"xmlns:xsi": XSI_NS, "xmlns:xsd": XSD_NS})
    body = ET.SubElement(envelope, ET.QName(SOAP_NS, "Body"))
    operation = ET.SubElement(body, ET.QName(OP_NS, "AtenderMensajeRNDC"), {ET.QName(SOAP_NS, "encodingStyle"): ENC_NS})
    request = ET.SubElement(operation, "Request", {ET.QName(XSI_NS, "type"): "xsd:string"})
    request.text = request_xml.decode("iso-8859-1")
    return ET.tostring(envelope, encoding="utf-8", xml_declaration=True)


def _local(tag): return tag.rsplit("}", 1)[-1]


def interpretar_respuesta_soap(content: bytes) -> list[dict]:
    try:
        envelope = ET.fromstring(content)
    except ET.ParseError as exc:
        raise RNDCResponseParseError("La envoltura SOAP no es XML válido") from exc
    fault = next((x for x in envelope.iter() if _local(x.tag) == "Fault"), None)
    if fault is not None:
        message = " ".join((x.text or "").strip() for x in fault.iter() if _local(x.tag) in {"faultcode", "faultstring"}).strip()
        raise RNDCSoapFaultError(message or "SOAP Fault sin detalle")
    returned = next((x for x in envelope.iter() if _local(x.tag) == "return"), None)
    if returned is None or not (returned.text or "").strip():
        raise RNDCResponseParseError("La respuesta SOAP no contiene return")
    try:
        business = ET.fromstring(returned.text.strip())
    except ET.ParseError as exc:
        raise RNDCResponseParseError("El XML de negocio RNDC no es válido") from exc
    error = next((x for x in business.iter() if _local(x.tag).lower() == "errormsg" and (x.text or "").strip()), None)
    if error is not None:
        msg = (error.text or "Error RNDC").strip()
        if any(x in msg.casefold() for x in ("usuario", "contraseña", "password", "credencial", "autentic")):
            raise RNDCCredentialsError("RNDC rechazó las credenciales")
        if any(x in msg.casefold() for x in ("no existen documentos", "no se encontraron", "sin registros", "no hay registros")):
            raise RNDCNoDataError(msg)
        raise RNDCBusinessError(msg)
    return [{_local(child.tag).lower(): (child.text or "").strip() for child in doc} for doc in business.iter() if _local(doc.tag).lower() == "documento"]


def sanitizar(texto: str, username: str, password: str) -> str:
    for secret in filter(None, (username, password)):
        texto = texto.replace(secret, "***")
    return texto


class RNDCClient:
    def __init__(self, soap_url, username, password, transport=None):
        self.soap_url, self.username, self.password = soap_url, username, password
        self._client = httpx.Client(timeout=httpx.Timeout(45, connect=10), transport=transport)

    def close(self): self._client.close()

    def consultar(self, periodo, combinacion):
        inner = construir_xml_rndc(self.username, self.password, periodo, combinacion)
        body = construir_envolvente_soap(inner)
        headers = {"Content-Type": "text/xml; charset=utf-8", "SOAPAction": SOAP_ACTION}
        for attempt in range(3):
            try:
                response = self._client.post(self.soap_url, content=body, headers=headers)
                if response.status_code >= 500 and attempt < 2:
                    time.sleep(0.25 * (2 ** attempt))
                    continue
                response.raise_for_status()
                return interpretar_respuesta_soap(response.content)
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                if attempt == 2: raise RNDCTransportError("No fue posible comunicarse con RNDC") from exc
                time.sleep(0.25 * (2 ** attempt))
            except httpx.HTTPStatusError as exc:
                raise RNDCTransportError(f"RNDC respondió HTTP {exc.response.status_code}") from exc
        raise RNDCTransportError("RNDC no respondió después de los reintentos")
