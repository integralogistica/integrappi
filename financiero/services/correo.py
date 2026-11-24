"""Envío de correos del Módulo Financiero con Resend.

Adaptación del app/services/email_service.py del módulo externo (que usaba
SMTP Office365 + aiosmtplib): aquí se usa Resend como el resto de integrappi
(rutas/baseusuarios.py, Funciones/whatsapp_utils_integra.py) — mismas vars
RESEND_API_KEY / MAIL_FROM, ya desplegadas en Render.

Reglas conservadas del original:
- Whitelist de dominios destinatarios (FIN_DOMINIOS_CORREO): se rechaza con
  403 un destino externo.
- Rate limit por usuario (services/rate_limit.limitar_envio).
- Si falta la configuración, NO lanza: registra y devuelve False (el flujo
  del módulo nunca se cae por un correo).
"""
import logging
import os
import re

from fastapi import HTTPException, status

from financiero.constantes import FIN_DOMINIOS_CORREO
from financiero.services.rate_limit import limitar_envio

logger = logging.getLogger("financiero.correo")

_RE_CORREO = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
PREFIJO_ASUNTO = "[Logística Integra]"


def _dominio_permitido(correo: str) -> bool:
    if not FIN_DOMINIOS_CORREO:
        return True  # sin whitelist configurada: abierto (no recomendado)
    dominio = correo.rsplit("@", 1)[-1].strip().lower()
    return dominio in FIN_DOMINIOS_CORREO


def enviar_correo(
    destinatarios: list[str],
    asunto: str,
    html: str,
    copia: list[str] | None = None,
    usuario_rate: str | None = None,
) -> bool:
    """Envía un correo vía Resend. Devuelve True si se despachó.

    Nunca lanza por fallos de la API (solo por validación de entrada: formato
    o dominio no permitido, que sí son error del llamador → 403).
    """
    if not isinstance(destinatarios, list) or not destinatarios:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "Se requiere al menos un destinatario.")
    for dest in list(destinatarios) + list(copia or []):
        if not _RE_CORREO.match(dest or ""):
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, f"Correo inválido: {dest}")
        if not _dominio_permitido(dest):
            raise HTTPException(
                status.HTTP_403_FORBIDDEN,
                f"No está permitido enviar correos a dominios externos ({dest}).",
            )
    if usuario_rate:
        limitar_envio(usuario_rate)

    if len(asunto) > 300:
        asunto = asunto[:297] + "..."
    if len(html) > 2_000_000:
        html = html[:2_000_000]

    try:
        import resend
        api_key = os.getenv("RESEND_API_KEY")
        if not api_key:
            logger.warning("[FIN] Correo omitido: falta RESEND_API_KEY.")
            return False
        resend.api_key = api_key
        desde = os.getenv("MAIL_FROM", "no-reply@integralogistica.com")
        params: dict = {
            "from": desde,
            "to": destinatarios,
            "subject": f"{PREFIJO_ASUNTO} {asunto}".strip(),
            "html": html,
        }
        if copia:
            params["cc"] = copia
        resend.Emails.send(params)
        return True
    except HTTPException:
        raise
    except Exception as exc:  # Resend caído, límite, etc.: el flujo sigue
        logger.warning("[FIN] Correo NO enviado (%s): %s", destinatarios, exc)
        return False
