"""Almacenamiento GCS privado para los estudios de seguridad.

Los PDF de los estudios contienen datos personales (cédula, antecedentes,
historial de viajes): se suben SIN hacerlos públicos y se sirven solo a través
del endpoint autenticado del backend (cada descarga queda auditada). La ruta
del blob NO contiene la cédula (minimización: las URLs llegan a logs de GCS y
proxies); la cédula viaja en los metadatos del blob y en el documento Mongo.

Patrón de rutas:  {CARPETA}/{empresa_id}/{AAAA}/{consulta_id}[_procuraduria].pdf
"""
from __future__ import annotations

import hashlib
import io
import logging
import os
from datetime import timedelta

os.environ.setdefault(
    "GOOGLE_APPLICATION_CREDENTIALS",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "credenciales.json"),
)

logger = logging.getLogger(__name__)

BUCKET_SEGURIDAD = os.getenv("SEGURIDAD_ESTUDIOS_BUCKET", "integrapp")
CARPETA_SEGURIDAD = os.getenv("SEGURIDAD_ESTUDIOS_CARPETA", "SeguridadEstudios")

_client = None


def _obtener_cliente():
    """Cliente GCS perezoso y compartido (crear uno por request es caro)."""
    global _client
    if _client is None:
        from google.cloud import storage

        _client = storage.Client()
    return _client


def ruta_blob(empresa_id: str, anio: int, consulta_id: str, sufijo: str = "") -> str:
    """Ruta del blob SIN cédula: SeguridadEstudios/{empresa_id}/{AAAA}/{consulta_id}{sufijo}.pdf"""
    return f"{CARPETA_SEGURIDAD}/{str(empresa_id)}/{anio}/{consulta_id}{sufijo}.pdf"


CARPETA_COBRO = os.getenv("SEGURIDAD_COBRO_CARPETA", "SeguridadCobro")


def ruta_blob_cuenta(empresa_id: str, periodo: str) -> str:
    """Ruta de la cuenta de cobro: SeguridadCobro/{empresa_id}/{YYYY-MM}/cuenta_cobro_{periodo}.pdf"""
    return f"{CARPETA_COBRO}/{str(empresa_id)}/{periodo}/cuenta_cobro_{periodo}.pdf"


def subir_pdf(contenido: bytes, ruta: str, cedula: str, content_type: str = "application/pdf") -> dict:
    """Sube (o pisa) un PDF privado. Retorna {gcs_ruta, sha256, tamano}."""
    blob = _obtener_cliente().bucket(BUCKET_SEGURIDAD).blob(ruta)
    blob.upload_from_file(
        io.BytesIO(contenido),
        content_type=content_type,
    )
    # La cédula no va en la ruta; queda como metadato del blob.
    blob.metadata = {"cedula": cedula, "modulo": "estudios_seguridad"}
    blob.patch()
    return {
        "gcs_ruta": ruta,
        "sha256": hashlib.sha256(contenido).hexdigest(),
        "tamano": len(contenido),
    }


def descargar_blob(ruta: str) -> bytes:
    """Descarga el contenido de un blob privado (lanza excepción si no existe)."""
    blob = _obtener_cliente().bucket(BUCKET_SEGURIDAD).blob(ruta)
    return blob.download_as_bytes()


def generar_url_firmada(ruta: str, minutos: int = 15) -> str:
    """URL firmada temporal para descarga directa del navegador."""
    blob = _obtener_cliente().bucket(BUCKET_SEGURIDAD).blob(ruta)
    return blob.generate_signed_url(
        version="v4",
        expiration=timedelta(minutes=minutos),
        method="GET",
    )
