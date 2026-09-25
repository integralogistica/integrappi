# Funciones/bot_situacion_militar.py
"""
Bot de consulta del estado de situación militar (libreta militar, Ejército).

https://definicion.libretamilitar.mil.co/consulta-estado-situacion-militar

Portal PÚBLICO POR DISEÑO: el propio sitio declara "La información disponible
en esta consulta es de carácter público y no requiere autorización del
titular para su tratamiento" (art. 10, Ley 1581 de 2012) → la fuente
"situacion_militar" SÍ va en los defaults de empresa. Marco legal: Ley
1861/2017, Decreto 977/2018, Ley 1184/2008.

Descubrimiento (2026-09-25, dumps descargas_libreta/): SPA React cuyo form
(#tipoDocumento cc/ti + #numeroDocumento) hace un **GET directo SIN CAPTCHA**
al generador de certificados:
  GET /generator/v1/api/documento/consulta/estado-situacion-militar
      ?tipoDocumento=cc&numeroIdentificacion=<cedula>
  → 200 application/pdf: certificado oficial "REC_OR" con el estado; o
  → 400 "El tipoDocumento no coincide con el registrado para el ciudadano"
    (cédula no encontrada con CC = no_registra determinante).
El API responde a requests PURO (molde bot_rues, SIN navegador): ~1-2 s,
**costo $0**. El PDF se procesa EN MEMORIA con pdfplumber (como contraloría);
la evidencia es la 1ª hoja del certificado rasterizada
(`certificado_pdf_a_jpeg`, patrón CGR 2026-09-24) — no hay página que
fotografiar y la hoja del certificado ES el resultado.

Campos del certificado: Primer/Segundo Nombre, Primer/Segundo Apellido,
Tipo/Número Documento, **Estado Tarjeta Militar** (vistos: RESERVISTA - 1RA
CLASE, RESERVISTA - 2DA CLASE; otros posibles: APTO, APLAZADO, EXCLUIDO,
PENDIENTE/NO DEFINIDO…) y lugar/fecha de expedición.
"""
import logging
import os
import re
import threading
from datetime import datetime
from typing import Any, Dict, Optional

import pdfplumber
import requests

try:  # importado como paquete (orquestador) o standalone (CLI)
    from Funciones.captura_evidencia import certificado_pdf_a_jpeg
except ImportError:  # pragma: no cover - ejecución como script
    from captura_evidencia import certificado_pdf_a_jpeg

logger = logging.getLogger(__name__)

API_URL = os.getenv(
    "SEGURIDAD_SITUACION_MILITAR_URL",
    "https://definicion.libretamilitar.mil.co/generator/v1/api/documento/consulta/estado-situacion-militar")
TIMEOUT_S = float(os.getenv("SEGURIDAD_SITUACION_MILITAR_TIMEOUT_S", "30"))

# Bloqueo para serializar la sesión HTTP compartida (como bot_rues).
_LOCK = threading.Lock()

_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")

_MESES = {
    "ENERO": 1, "FEBRERO": 2, "MARZO": 3, "ABRIL": 4, "MAYO": 5, "JUNIO": 6,
    "JULIO": 7, "AGOSTO": 8, "SEPTIEMBRE": 9, "OCTUBRE": 10, "NOVIEMBRE": 11, "DICIEMBRE": 12,
}


class BotSituacionMilitarError(Exception):
    """Error del bot de situación militar (API caído / respuesta ilegible)."""


class BotSituacionMilitarSinResultado(BotSituacionMilitarError):
    """El certificado llegó sin estado legible (anti-envenenamiento)."""


def _campo(texto: str, etiqueta: str) -> str:
    m = re.search(rf"{etiqueta}:\s*([^\n]+)", texto)
    return m.group(1).strip() if m else ""


def _fecha_expedicion(texto: str) -> Optional[str]:
    """"a los 24 días del mes de SEPTIEMBRE de 2026" → '2026-09-24'."""
    m = re.search(r"a los (\d{1,2}) días? del mes de ([A-ZÁÉÍÓÚÑ]+) de (\d{4})", texto)
    if not m:
        return None
    mes = _MESES.get(m.group(2).upper())
    if not mes:
        return None
    try:
        return datetime(int(m.group(3)), mes, int(m.group(1))).date().isoformat()
    except ValueError:
        return None


def _parsear_certificado(pdf_bytes: bytes) -> Dict[str, Any]:
    """Texto del certificado → shape del doc (sin el PDF: minimización)."""
    with pdfplumber.open(__import__("io").BytesIO(pdf_bytes)) as pdf:
        texto = "\n".join(p.extract_text() or "" for p in pdf.pages[:2])
    estado = _campo(texto, "Estado Tarjeta Militar")
    # Anti-envenenamiento: sin estado legible no hay consulta válida (un PDF
    # truncado/dañado jamás se cachea como "limpio").
    if not estado:
        raise BotSituacionMilitarSinResultado(
            "El certificado llegó sin 'Estado Tarjeta Militar' legible"
        )
    nombres = " ".join(x for x in (_campo(texto, "Primer Nombre"), _campo(texto, "Segundo Nombre")) if x)
    apellidos = " ".join(x for x in (_campo(texto, "Primer Apellido"), _campo(texto, "Segundo Apellido")) if x)
    return {
        "nombres": nombres,
        "apellidos": apellidos,
        "nombre_completo": " ".join(x for x in (nombres, apellidos) if x),
        "tipo_documento": _campo(texto, "Tipo Documento"),
        "estado_tarjeta_militar": estado,
        "fecha_expedicion": _fecha_expedicion(texto),
        "texto_resultado": texto[:1500],
    }


def consultar_situacion_militar(cedula: str) -> Dict[str, Any]:
    """Consulta el estado de situación militar de una cédula (CC fijo).

    Retorna: cedula, no_registra (True = el ciudadano no existe con CC),
    mensaje, nombre_completo/nombres/apellidos según el certificado,
    estado_tarjeta_militar, fecha_expedicion (ISO), texto_resultado,
    pdf_bytes (el certificado — volátil, lo gestiona el orquestador) y
    captura_jpg (1ª hoja del certificado rasterizada: la evidencia ES el
    certificado, no hay navegador).
    """
    cedula_norm = re.sub(r"\D", "", cedula or "")
    if not (6 <= len(cedula_norm) <= 10):
        raise BotSituacionMilitarError("Cédula inválida")

    with _LOCK:
        try:
            respuesta = requests.get(
                API_URL,
                params={"tipoDocumento": "cc", "numeroIdentificacion": cedula_norm},
                headers={
                    "User-Agent": _UA,
                    "Referer": "https://definicion.libretamilitar.mil.co/consulta-estado-situacion-militar",
                    "Accept": "*/*",
                },
                timeout=TIMEOUT_S,
            )
        except requests.RequestException as exc:
            raise BotSituacionMilitarError(f"El API de situación militar no respondió: {exc}") from exc

    if respuesta.status_code == 400 and "no coincide" in respuesta.text:
        # Cédula no encontrada con CC: respuesta DETERMINANTE del API (el
        # mensaje culpa al tipo de documento, pero con cc fijo = no registra).
        return {
            "cedula": cedula_norm,
            "no_registra": True,
            "mensaje": "El ciudadano no registra situación militar con cédula de ciudadanía",
            "nombres": "", "apellidos": "", "nombre_completo": "",
            "estado_tarjeta_militar": "",
            "fecha_expedicion": None,
            "texto_resultado": "",
            "pdf_bytes": None,
            "pdf_ruta": None,
            "captura_jpg": None,
        }
    if respuesta.status_code != 200 or "pdf" not in (respuesta.headers.get("content-type") or ""):
        raise BotSituacionMilitarError(
            f"El API de situación militar respondió {respuesta.status_code} "
            f"({(respuesta.headers.get('content-type') or '')[:40]})"
        )

    pdf_bytes = respuesta.content
    leido = _parsear_certificado(pdf_bytes)
    return {
        "cedula": cedula_norm,
        "no_registra": False,
        "mensaje": "",
        **leido,
        "pdf_bytes": pdf_bytes,
        "pdf_ruta": None,
        # Evidencia: la hoja del certificado ES el resultado (patrón CGR).
        "captura_jpg": certificado_pdf_a_jpeg(pdf_bytes),
    }


def consultar_situacion_militar_sync(cedula: str) -> Dict[str, Any]:
    """Versión síncrona para asyncio.to_thread (ya es síncrona: identidad)."""
    return consultar_situacion_militar(cedula)


if __name__ == "__main__":
    import json
    import sys

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if len(sys.argv) < 2:
        print("Uso: python Funciones/bot_situacion_militar.py CEDULA")
        sys.exit(2)
    resultado = consultar_situacion_militar(sys.argv[1])
    resultado.pop("pdf_bytes", None)
    resultado.pop("texto_resultado", None)
    print(json.dumps(resultado, ensure_ascii=False, indent=2, default=str))
