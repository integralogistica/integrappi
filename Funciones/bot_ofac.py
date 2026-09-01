"""Consulta exacta por identificación en la lista SDN oficial de OFAC.

No automatiza el formulario de sanctionssearch (búsqueda difusa por nombre).
Descarga el XML oficial publicado por OFAC, construye un índice en memoria y
compara el número de identificación normalizado. La lista se refresca cada seis
horas; una consulta nunca implica descargar nuevamente los ~29 MB del dataset.
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

OFAC_SDN_URL = os.getenv("SEGURIDAD_OFAC_SDN_URL", "https://www.treasury.gov/ofac/downloads/sdn.xml")
OFAC_DATASET_TTL_S = int(os.getenv("SEGURIDAD_OFAC_DATASET_TTL_S", "21600"))
OFAC_TIMEOUT_S = float(os.getenv("SEGURIDAD_OFAC_TIMEOUT_S", "60"))
_NS = "{https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/XML}"
_TIPOS_DOCUMENTO = {"cedula no.", "national id no.", "identification number", "citizenship no."}
# Identificadores inequívocamente tributarios. No incluimos "Registration
# Number": OFAC lo usa para muchos registros mercantiles/no fiscales y podría
# producir un falso positivo numérico al compararlo con un NIT colombiano.
_TIPOS_NIT = {
    "tax id no.", "tax no.", "fiscal code", "italian fiscal code",
    "paraguayan tax identification number", "romanian tax registration",
}

_LOCK = threading.Lock()
_INDICE: dict[str, list[dict]] = {}
_INDICE_NIT: dict[str, list[dict]] = {}
_METADATA: dict = {}
_CARGADO_EN = 0.0


class BotOfacError(RuntimeError):
    pass


def _normalizar_documento(valor: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(valor or "").upper())


def _texto(nodo: ET.Element, etiqueta: str) -> str:
    return (nodo.findtext(_NS + etiqueta) or "").strip()


def _construir_indices(xml_bytes: bytes) -> tuple[dict[str, list[dict]], dict[str, list[dict]], dict]:
    indice: dict[str, list[dict]] = {}
    indice_nit: dict[str, list[dict]] = {}
    metadata = {"sha256_dataset": hashlib.sha256(xml_bytes).hexdigest()}
    try:
        contexto = ET.iterparse(io.BytesIO(xml_bytes), events=("end",))
        for _, nodo in contexto:
            if nodo.tag == _NS + "publshInformation":
                metadata["fecha_publicacion"] = _texto(nodo, "Publish_Date")
                metadata["total_registros"] = int(_texto(nodo, "Record_Count") or 0)
            elif nodo.tag == _NS + "sdnEntry":
                ids = nodo.find(_NS + "idList")
                documentos = [] if ids is None else ids.findall(_NS + "id")
                programas = [str(p.text or "").strip() for p in nodo.findall(_NS + "programList/" + _NS + "program")]
                entrada_base = {
                    "uid": _texto(nodo, "uid"),
                    "nombre": " ".join(x for x in (_texto(nodo, "firstName"), _texto(nodo, "lastName")) if x),
                    "tipo": _texto(nodo, "sdnType"),
                    "programas": programas,
                    "lista": "SDN",
                }
                for documento in documentos:
                    tipo = _texto(documento, "idType")
                    numero = _texto(documento, "idNumber")
                    normalizado = _normalizar_documento(numero)
                    tipo_normalizado = tipo.lower()
                    destino = indice if tipo_normalizado in _TIPOS_DOCUMENTO else indice_nit if tipo_normalizado in _TIPOS_NIT else None
                    if normalizado and destino is not None:
                        coincidencia = {
                            **entrada_base,
                            "tipo_documento": tipo,
                            "numero_documento": numero,
                            "pais_documento": _texto(documento, "idCountry"),
                        }
                        destino.setdefault(normalizado, []).append(coincidencia)
                nodo.clear()
    except (ET.ParseError, ValueError) as exc:
        raise BotOfacError(f"El dataset XML de OFAC no es válido: {exc}") from exc
    if not metadata.get("fecha_publicacion") or not metadata.get("total_registros"):
        raise BotOfacError("El dataset de OFAC no incluyó metadatos de publicación")
    return indice, indice_nit, metadata


def _construir_indice(xml_bytes: bytes) -> tuple[dict[str, list[dict]], dict]:
    """Compatibilidad: índice de documentos personales usado por pruebas."""
    indice, _, metadata = _construir_indices(xml_bytes)
    return indice, metadata


def _actualizar_dataset() -> None:
    global _INDICE, _INDICE_NIT, _METADATA, _CARGADO_EN
    try:
        respuesta = requests.get(
            OFAC_SDN_URL,
            timeout=OFAC_TIMEOUT_S,
            headers={"User-Agent": "IntegrApp-Security-Screening/1.0"},
        )
        respuesta.raise_for_status()
    except requests.RequestException as exc:
        raise BotOfacError(f"OFAC no permitió descargar la lista SDN: {exc}") from exc
    indice, indice_nit, metadata = _construir_indices(respuesta.content)
    _INDICE, _INDICE_NIT, _METADATA, _CARGADO_EN = indice, indice_nit, metadata, time.monotonic()


def consultar_ofac_sync(cedula: str) -> dict:
    """Retorna coincidencias EXACTAS del documento; no hace fuzzy matching."""
    documento = _normalizar_documento(cedula)
    if not documento:
        raise BotOfacError("El número de identificación está vacío")
    with _LOCK:
        if not _INDICE or time.monotonic() - _CARGADO_EN >= OFAC_DATASET_TTL_S:
            _actualizar_dataset()
        coincidencias = [dict(item) for item in _INDICE.get(documento, [])]
        metadata = dict(_METADATA)
    aplica = bool(coincidencias)
    return {
        "cedula": documento,
        "aplica": aplica,
        "no_registra": not aplica,
        "coincidencias": coincidencias,
        "total_coincidencias": len(coincidencias),
        "fecha_publicacion": metadata.get("fecha_publicacion"),
        "total_registros_lista": metadata.get("total_registros"),
        "sha256_dataset": metadata.get("sha256_dataset"),
        "metodo": "coincidencia_exacta_identificacion",
        "mensaje": (
            f"Coincidencia exacta de identificación en la lista SDN de OFAC ({len(coincidencias)} registro(s))."
            if aplica else
            "No se encontró coincidencia exacta del número de identificación en la lista SDN de OFAC."
        ),
    }


def consultar_ofac_nit_sync(nit: str) -> dict:
    """Retorna coincidencias EXACTAS de un identificador tributario empresarial."""
    documento = _normalizar_documento(nit)
    if not documento:
        raise BotOfacError("El NIT está vacío")
    with _LOCK:
        if not _INDICE or time.monotonic() - _CARGADO_EN >= OFAC_DATASET_TTL_S:
            _actualizar_dataset()
        coincidencias = [dict(item) for item in _INDICE_NIT.get(documento, [])]
        metadata = dict(_METADATA)
    aplica = bool(coincidencias)
    return {
        "nit": documento,
        "aplica": aplica,
        "no_registra": not aplica,
        "coincidencias": coincidencias,
        "total_coincidencias": len(coincidencias),
        "fecha_publicacion": metadata.get("fecha_publicacion"),
        "total_registros_lista": metadata.get("total_registros"),
        "sha256_dataset": metadata.get("sha256_dataset"),
        "metodo": "coincidencia_exacta_identificacion_tributaria",
        "mensaje": (
            f"Coincidencia exacta del NIT en la lista SDN de OFAC ({len(coincidencias)} registro(s))."
            if aplica else
            "No se encontró coincidencia exacta del NIT en la lista SDN de OFAC."
        ),
    }
