"""Consulta exacta por identificación en las listas de sanciones ONU y UE.

Molde de ``bot_ofac``: descarga los datasets XML oficiales, construye un índice
en memoria por identificación normalizada y compara el número de documento SIN
búsqueda difusa por nombre. La fuente agrega DOS listas en una sola consulta:

- ONU — Lista consolidada del Consejo de Seguridad (scsanctions.un.org, XML ~2 MB).
- UE — Consolidated Financial Sanctions File 1.1 (webgate.ec.europa.eu, XML ~25 MB,
  URL con token público documentado en data.europa.eu).

Solo se indexan identificadores INEQUÍVOCOS de cédula/documento de identidad:
ONU ``National Identification Number`` y UE ``identificationTypeCode="id"``
(National identification card). Pasaportes y números tributarios quedan FUERA
(mismo criterio de bot_ofac: minimizar falsos positivos numéricos).

Si UNA de las dos listas no puede descargarse/parsearse, la consulta sale con la
que sí estuvo disponible y lo declara en ``listas_no_disponibles`` (degradación
honesta, jamás un silencio); si fallan AMBAS → ``BotSancionesError``. El dataset
se refresca cada 6 h; una consulta nunca implica descargar de nuevo ~27 MB.
"""
from __future__ import annotations

import hashlib
import io
import os
import re
import threading
import time
import xml.etree.ElementTree as ET

import requests

ONU_XML_URL = os.getenv(
    "SEGURIDAD_ONU_XML_URL",
    "https://scsanctions.un.org/resources/xml/en/consolidated.xml",
)
# Token público ("token-2017" en base64) documentado como distribución oficial
# en data.europa.eu; la descarga anónima de webgate sin token responde EU Login.
UE_XML_URL = os.getenv(
    "SEGURIDAD_UE_XML_URL",
    "https://webgate.ec.europa.eu/fsd/fsf/public/files/xmlFullSanctionsList_1_1/content?token=dG9rZW4tMjAxNw",
)
SANCIONES_DATASET_TTL_S = int(os.getenv("SEGURIDAD_SANCIONES_DATASET_TTL_S", "21600"))
SANCIONES_TIMEOUT_S = float(os.getenv("SEGURIDAD_SANCIONES_TIMEOUT_S", "120"))

# Tipos de documento que identifican una CÉDULA / documento de identidad
# nacional (post-normalización de espacios, en minúsculas).
_TIPOS_DOCUMENTO_ONU = {
    "national identification number",
    "national identification",
    "cedula de identidad",
    "cedula de ciudadania",
    "identity card",
}
# Código de tipo inequívoco de cédula en el XML de la UE.
_TIPOS_DOCUMENTO_UE = {"id"}

_NS_UE = "{http://eu.europa.ec/fpi/fsd/export}"

_LOCK = threading.Lock()
_INDICE: dict[str, list[dict]] = {}
_METADATA: dict = {}
_CARGADO_EN = 0.0


class BotSancionesError(RuntimeError):
    pass


def _normalizar_documento(valor: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(valor or "").upper())


def _normalizar_tipo(valor: str) -> str:
    return " ".join(str(valor or "").lower().split())


def _nombre_onu(nodo: ET.Element) -> str:
    partes = (
        nodo.findtext("FIRST_NAME") or "",
        nodo.findtext("SECOND_NAME") or "",
        nodo.findtext("THIRD_NAME") or "",
        nodo.findtext("FOURTH_NAME") or "",
    )
    return " ".join(p for p in (x.strip() for x in partes) if p)


def _parsear_onu(xml_bytes: bytes) -> tuple[dict[str, list[dict]], dict]:
    """Índice de personas de la lista consolidada ONU + metadatos."""
    indice: dict[str, list[dict]] = {}
    total_personas = 0
    fecha_publicacion = ""
    try:
        contexto = ET.iterparse(io.BytesIO(xml_bytes), events=("end",))
        for _, nodo in contexto:
            if nodo.tag == "CONSOLIDATED_LIST":
                fecha = nodo.get("dateGenerated") or ""
                # "2026-09-12T23:00:05.988Z" → fecha legible sin hora.
                fecha_publicacion = fecha.split("T")[0]
            elif nodo.tag == "INDIVIDUAL":
                total_personas += 1
                programas = [
                    " ".join((t.text or "").split())
                    for t in nodo.findall("UN_LIST_TYPE")
                    if (t.text or "").strip()
                ]
                entrada_base = {
                    "lista": "ONU",
                    "uid": (nodo.findtext("DATAID") or "").strip(),
                    "nombre": _nombre_onu(nodo),
                    "tipo": "Individual",
                    "programas": programas,
                    "referencia": (nodo.findtext("REFERENCE_NUMBER") or "").strip(),
                }
                for documento in nodo.findall("INDIVIDUAL_DOCUMENT"):
                    tipo = _normalizar_tipo(documento.findtext("TYPE_OF_DOCUMENT"))
                    numero = (documento.findtext("NUMBER") or "").strip()
                    if tipo in _TIPOS_DOCUMENTO_ONU and _normalizar_documento(numero):
                        indice.setdefault(_normalizar_documento(numero), []).append({
                            **entrada_base,
                            "tipo_documento": tipo,
                            "numero_documento": numero,
                            "pais_documento": (documento.findtext("ISSUING_COUNTRY") or "").strip(),
                        })
                nodo.clear()
    except (ET.ParseError, ValueError) as exc:
        raise BotSancionesError(f"El dataset XML de la ONU no es válido: {exc}") from exc
    if not total_personas:
        raise BotSancionesError("El dataset de la ONU no incluyó registros de personas")
    return indice, {
        "fecha_publicacion": fecha_publicacion or None,
        "total_registros_lista": total_personas,
        "sha256_dataset": hashlib.sha256(xml_bytes).hexdigest(),
    }


def _nombre_ue(nodo: ET.Element) -> str:
    # El primer nameAlias "strong" es el nombre primario de la designación.
    for alias in nodo.findall(_NS_UE + "nameAlias"):
        if alias.get("strong") == "true":
            nombre = (alias.get("wholeName") or "").strip()
            if nombre:
                return nombre
            partes = (
                alias.get("firstName") or "",
                alias.get("middleName") or "",
                alias.get("lastName") or "",
            )
            return " ".join(p.strip() for p in partes if p.strip())
    return ""


def _parsear_ue(xml_bytes: bytes) -> tuple[dict[str, list[dict]], dict]:
    """Índice de PERSONAS del Consolidated Financial Sanctions File 1.1 + metadatos."""
    indice: dict[str, list[dict]] = {}
    total_personas = 0
    fecha_publicacion = ""
    try:
        contexto = ET.iterparse(io.BytesIO(xml_bytes), events=("end",))
        for _, nodo in contexto:
            if nodo.tag == _NS_UE + "export":
                fecha_publicacion = nodo.get("generationDate") or ""
                continue
            if nodo.tag != _NS_UE + "sanctionEntity":
                continue
            es_persona = any(
                (s.get("code") or "") == "person"
                for s in nodo.findall(_NS_UE + "subjectType")
            )
            if not es_persona:
                nodo.clear()
                continue
            total_personas += 1
            programas = [
                " ".join((r.get("programme") or "").split())
                for r in nodo.findall(_NS_UE + "regulation")
                if (r.get("programme") or "").strip()
            ]
            entrada_base = {
                "lista": "UE",
                "uid": (nodo.get("logicalId") or "").strip(),
                "nombre": _nombre_ue(nodo),
                "tipo": "Individual",
                "programas": sorted(set(programas)),
                "referencia": (nodo.get("euReferenceNumber") or "").strip(),
            }
            for documento in nodo.findall(_NS_UE + "identification"):
                tipo = (documento.get("identificationTypeCode") or "").strip().lower()
                numero = (documento.get("number") or "").strip()
                if tipo in _TIPOS_DOCUMENTO_UE and _normalizar_documento(numero):
                    indice.setdefault(_normalizar_documento(numero), []).append({
                        **entrada_base,
                        "tipo_documento": (documento.get("identificationTypeDescription") or tipo).strip(),
                        "numero_documento": numero,
                        "pais_documento": (documento.get("countryDescription") or "").strip(),
                    })
            nodo.clear()
    except (ET.ParseError, ValueError) as exc:
        raise BotSancionesError(f"El dataset XML de la UE no es válido: {exc}") from exc
    if not total_personas:
        raise BotSancionesError("El dataset de la UE no incluyó registros de personas")
    return indice, {
        "fecha_publicacion": (fecha_publicacion.split("T")[0] if fecha_publicacion else None),
        "total_registros_lista": total_personas,
        "sha256_dataset": hashlib.sha256(xml_bytes).hexdigest(),
    }


def _descargar(url: str) -> bytes:
    try:
        respuesta = requests.get(
            url,
            timeout=SANCIONES_TIMEOUT_S,
            headers={"User-Agent": "IntegrApp-Security-Screening/1.0"},
        )
        respuesta.raise_for_status()
        return respuesta.content
    except requests.RequestException as exc:
        raise BotSancionesError(f"No fue posible descargar la lista ({url.split('?')[0]}): {exc}") from exc


def _actualizar_datasets() -> None:
    """Descarga y parsea ambas listas. Tolerancia: si UNA falla se sigue con
    la otra y la ausencia queda registrada; si fallan AMBAS → error."""
    global _INDICE, _METADATA, _CARGADO_EN
    indice: dict[str, list[dict]] = {}
    metadata: dict = {"listas": {}, "no_disponibles": []}
    for etiqueta, url, parsear in (("ONU", ONU_XML_URL, _parsear_onu), ("UE", UE_XML_URL, _parsear_ue)):
        try:
            indice_lista, meta_lista = parsear(_descargar(url))
        except BotSancionesError:
            metadata["no_disponibles"].append(etiqueta)
            continue
        for documento, coincidencias in indice_lista.items():
            indice.setdefault(documento, []).extend(coincidencias)
        metadata["listas"][etiqueta] = meta_lista
    if not metadata["listas"]:
        raise BotSancionesError("Ninguna lista de sanciones (ONU/UE) pudo descargarse")
    _INDICE, _METADATA, _CARGADO_EN = indice, metadata, time.monotonic()


def consultar_sanciones_sync(cedula: str) -> dict:
    """Coincidencias EXACTAS del documento en las listas ONU y UE (personas)."""
    documento = _normalizar_documento(cedula)
    if not documento:
        raise BotSancionesError("El número de identificación está vacío")
    with _LOCK:
        if not _INDICE or time.monotonic() - _CARGADO_EN >= SANCIONES_DATASET_TTL_S:
            _actualizar_datasets()
        coincidencias = [dict(item) for item in _INDICE.get(documento, [])]
        metadata = dict(_METADATA)
    aplica = bool(coincidencias)
    no_disponibles = metadata.get("no_disponibles") or []
    listas_ok = ", ".join(metadata.get("listas") or {}) or "ninguna"
    mensaje = (
        f"Coincidencia exacta de identificación en listas de sanciones ({len(coincidencias)} registro(s): {listas_ok})."
        if aplica else
        f"No se encontró coincidencia exacta del número de identificación en las listas de sanciones ({listas_ok})."
    )
    if no_disponibles:
        mensaje += f" Lista(s) no disponible(s) al consultar: {', '.join(no_disponibles)}."
    return {
        "cedula": documento,
        "aplica": aplica,
        "no_registra": not aplica,
        "coincidencias": coincidencias,
        "total_coincidencias": len(coincidencias),
        "listas": metadata.get("listas") or {},
        "listas_no_disponibles": no_disponibles,
        "metodo": "coincidencia_exacta_identificacion",
        "mensaje": mensaje,
    }
