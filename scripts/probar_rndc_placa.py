"""Sonda exploratoria: consulta de vehículo por placa en el WS RNDC (proceso 12).

Valida la duda pendiente de sicetac.md §15: ¿las credenciales RNDC actuales
permiten consultar información de vehículos por placa?

Estado 2026-08-29: ~20 combinaciones probadas (tipos 1-9, BPM/RNDC, nesting,
atributos, con/sin comillas, variables oficiales del portal de test
NUMNITEMPRESATRANSPORTE + NUMPLACA). Todas rechazadas con RNDC07 genérico
(= proceso no habilitado para el usuario, no error de formato: el barido de
tipos devolvió RNDC05/RNDC22/RNDC23 específicos, demostrando que el XML sí
se parsea). Hipótesis: TI@2425 solo tiene habilitado el proceso 26 SICE-TAC.
Pendiente: preguntar a Mintransporte (servicioalciudadano@mintransporte.gov.co)
qué procesos tiene habilitados el usuario y solicitar la consulta vehicular.

Uso (desde integrappi/):
    python scripts/probar_rndc_placa.py [PLACA]
"""
from __future__ import annotations

import json
import os
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import httpx
from dotenv import load_dotenv

from sicetac.config import ENDPOINTS
from sicetac.errors import SicetacError
from sicetac.rndc_client import SOAP_ACTION, construir_envolvente_soap, interpretar_respuesta_soap

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

SOAP_URL = os.getenv("RNDC_SOAP_URL", "").strip() or ENDPOINTS["production"]["soap"]

# Combinaciones a sondear: (etiqueta, tipo, proceso, campos, campos-sin-comillas)
# Variables confirmadas en el portal oficial de test (proceso 12 VEHICULO):
# filtro de consulta = NUMNITEMPRESATRANSPORTE + NUMPLACA.
INTENTOS = [
    ("tipo 5, proc 12, NUMNITEMPRESATRANSPORTE(num) + NUMPLACA(comillas)", 5, 12,
     {"NUMNITEMPRESATRANSPORTE": "{nit}", "NUMPLACA": "{placa}"}, {"NUMNITEMPRESATRANSPORTE"}),
    ("tipo 5, proc 12, + tenedor (cedula conductor)", 5, 12,
     {"NUMNITEMPRESATRANSPORTE": "{nit}", "NUMPLACA": "{placa}",
      "CODTIPOIDTENEDOR": "'1'", "NUMIDTENEDOR": "'{cedula}'"}, {"NUMNITEMPRESATRANSPORTE"}),
    ("tipo 5, proc 12, + propietario y tenedor (cedula)", 5, 12,
     {"NUMNITEMPRESATRANSPORTE": "{nit}", "NUMPLACA": "{placa}",
      "CODTIPOIDPROPIETARIO": "'1'", "NUMIDPROPIETARIO": "'{cedula}'",
      "CODTIPOIDTENEDOR": "'1'", "NUMIDTENEDOR": "'{cedula}'"}, {"NUMNITEMPRESATRANSPORTE"}),
    ("tipo 5, proc 12, NUMNITEMPRESATRANSPORTE(num) solo", 5, 12,
     {"NUMNITEMPRESATRANSPORTE": "{nit}"}, {"NUMNITEMPRESATRANSPORTE"}),
    ("tipo 6, proc 12, NUMNITEMPRESATRANSPORTE(num) + NUMPLACA(comillas)", 6, 12,
     {"NUMNITEMPRESATRANSPORTE": "{nit}", "NUMPLACA": "{placa}"}, {"NUMNITEMPRESATRANSPORTE"}),
]


def construir_xml(username: str, password: str, tipo: int, proceso: int, campos: dict, sin_comillas: set | None = None) -> bytes:
    root = ET.Element("root")
    acceso = ET.SubElement(root, "acceso")
    ET.SubElement(acceso, "username").text = username
    ET.SubElement(acceso, "password").text = password
    solicitud = ET.SubElement(root, "solicitud")
    ET.SubElement(solicitud, "tipo").text = str(tipo)
    ET.SubElement(solicitud, "procesoid").text = str(proceso)
    documento = ET.SubElement(root, "documento")
    sin_comillas = sin_comillas or set()
    for nombre, valor in campos.items():
        ET.SubElement(documento, nombre).text = valor if nombre in sin_comillas else f"'{valor}'"
    return ET.tostring(root, encoding="iso-8859-1", xml_declaration=True)


def main():
    username = os.getenv("RNDC_USERNAME", "")
    password = os.getenv("RNDC_PASSWORD", "")
    if not username or not password:
        print("ERROR: faltan RNDC_USERNAME/RNDC_PASSWORD en el .env")
        sys.exit(1)
    placa = (sys.argv[1] if len(sys.argv) > 1 else "QVK013").strip().upper()
    cedula = (sys.argv[2] if len(sys.argv) > 2 else "").strip()
    nit = os.getenv("RNDC_NIT_EMPRESA", "").strip()
    if not nit:
        print("AVISO: sin RNDC_NIT_EMPRESA en .env — los intentos que requieren NIT se envían vacíos.")
    print(f"=== Sonda RNDC proceso 12 (Vehículo) — placa {placa} cedula {cedula or '(ninguna)'} ===")
    print(f"Endpoint: {SOAP_URL}\n")

    cliente = httpx.Client(timeout=httpx.Timeout(45, connect=10))
    try:
        for etiqueta, tipo, proceso, campos, sin_comillas in INTENTOS:
            campos = {k: v.format(placa=placa, nit=nit, cedula=cedula) for k, v in campos.items()}
            inner = construir_xml(username, password, tipo, proceso, campos, sin_comillas=sin_comillas)
            body = construir_envolvente_soap(inner)
            headers = {"Content-Type": "text/xml; charset=utf-8", "SOAPAction": SOAP_ACTION}
            print(f"--- {etiqueta}")
            try:
                respuesta = cliente.post(SOAP_URL, content=body, headers=headers)
                respuesta.raise_for_status()
                documentos = interpretar_respuesta_soap(respuesta.content)
                if documentos:
                    print(f"    RESPUESTA OK: {len(documentos)} documento(s):")
                    for doc in documentos[:3]:
                        print("   ", json.dumps(doc, ensure_ascii=False)[:500])
                    print("    >>> ¡ESTA COMBINACIÓN FUNCIONA! <<<")
                    return
                print("    Respuesta sin documentos.")
            except SicetacError as exc:
                # Los ErrorMSG de RNDC son la guía para el siguiente intento.
                print(f"    RNDC dice: {str(exc)[:400]}")
            except httpx.HTTPError as exc:
                print(f"    Error de transporte: {exc}")
            time.sleep(1.2)
        print("\nNinguna combinación devolvió documentos.")
        if not nit:
            print("Siguiente paso: obtener el NIT de Integra (RNDC_NIT_EMPRESA) y reintentar;")
            print("el portal de test indica que el Nit de la Empresa es variable obligatoria y sin comillas.")
    finally:
        cliente.close()


if __name__ == "__main__":
    main()
