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
