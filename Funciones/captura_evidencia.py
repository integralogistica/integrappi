"""Captura de evidencia (pantallazo) de la página de resultado de un portal.

Anexo visual del estudio de seguridad (patrón TusDatos): cada bot Playwright
fotografía el viewport del portal en el momento de la consulta y la captura
viaja con el resultado (`captura_jpg`) hasta el PDF final del estudio.

La captura es SOLO evidencia de auditoría: si falla por cualquier motivo se
devuelve None y la consulta continúa normal — JAMÁS tumba una fuente.
"""
from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

CALIDAD_JPEG = int(os.getenv("SEGURIDAD_CAPTURA_CALIDAD", "70"))


async def capturar_viewport_jpeg(pagina, timeout_ms: int = 5000) -> bytes | None:
    """Fotografía el viewport (NO full_page) de la página en JPEG.

    Retorna los bytes de la imagen o None ante cualquier fallo (timeout,
    navegador cerrado, página en estado raro): la evidencia es best-effort.
    """
    try:
        return await pagina.screenshot(
            type="jpeg",
            quality=CALIDAD_JPEG,
            timeout=timeout_ms,
        )
    except Exception as exc:  # noqa: BLE001 — nunca tumba la consulta
        logger.warning("[CAPTURA EVIDENCIA] no se pudo capturar: %s", exc)
        return None


def certificado_pdf_a_jpeg(pdf_bytes: bytes, indice_pagina: int = 0) -> bytes | None:
    """Rasteriza una página de un PDF DESCARGADO (ej. certificado de la CGR).

    Evidencia de fuentes cuyo resultado llega como descarga (la página del
    portal no cambia tras el postback): la hoja del certificado ES el
    resultado — el pantallazo del form solo probaría que se abrió el portal.
    Best-effort: ante cualquier fallo devuelve None (el bot cae al viewport).
    """
    try:
        import io

        import pypdfium2 as pdfium

        documento = pdfium.PdfDocument(io.BytesIO(pdf_bytes))
        try:
            if len(documento) <= indice_pagina:
                return None
            bitmap = documento[indice_pagina].render(scale=2.0)
            imagen = bitmap.to_pil().convert("RGB")
            buffer = io.BytesIO()
            imagen.save(buffer, "JPEG", quality=CALIDAD_JPEG)
            return buffer.getvalue()
        finally:
            documento.close()
    except Exception as exc:  # noqa: BLE001 — nunca tumba la consulta
        logger.warning("[CAPTURA EVIDENCIA] PDF no rasterizable: %s", exc)
        return None
