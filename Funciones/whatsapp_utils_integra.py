# Funciones/whatsapp_utils_integra.py
import os
import secrets
import httpx
from typing import Dict, Any, Optional, Tuple
from datetime import datetime, timezone, timedelta
from passlib.context import CryptContext
from bd.bd_cliente import bd_cliente
import resend

PHONE_NUMBER_ID = os.getenv("WHATSAPP_PHONE_NUMBER_ID")
WHATSAPP_API_TOKEN = os.getenv("WHATSAPP_API_TOKEN")

GRAPH_URL = f"https://graph.facebook.com/v22.0/{PHONE_NUMBER_ID}/messages"


def _headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {WHATSAPP_API_TOKEN}",
        "Content-Type": "application/json",
    }


async def _post_graph(payload: Dict[str, Any], error_prefix: str = "WhatsApp"):
    if not WHATSAPP_API_TOKEN or not PHONE_NUMBER_ID:
        print("⚠️ Faltan WHATSAPP_API_TOKEN o WHATSAPP_PHONE_NUMBER_ID")
        return None

    async with httpx.AsyncClient() as client:
        resp = await client.post(GRAPH_URL, headers=_headers(), json=payload, timeout=20)

    if resp.status_code != 200:
        print(f"❌ {error_prefix}: {resp.status_code} - {resp.text}")
        return None

    return resp.json()


async def enviar_texto(to: str, texto: str):
    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "text",
        "text": {"preview_url": True, "body": texto},
    }
    return await _post_graph(payload, "Enviar texto")


async def enviar_template_con_parametros(
    to: str,
    template_name: str,
    language_code: str,
    body_params: list[str],
):
    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "template",
        "template": {
            "name": template_name,
            "language": {"code": language_code},
            "components": [
                {
                    "type": "body",
                    "parameters": [{"type": "text", "text": p} for p in body_params],
                }
            ],
        },
    }
    return await _post_graph(payload, f"Template {template_name}")


# =========================
# Funciones de Autenticación para Transportadores
# =========================

# Configuración de recuperación
CLAVE_SECRETA = os.getenv("JWT_SECRET", "cambia_esta_clave_por_una_bien_larga_y_aleatoria")
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
MAIL_FROM = os.getenv("MAIL_FROM", "no-reply@integralogistica.com")
FRONTEND_URL_RECUPERAR = os.getenv("FRONTEND_URL_RECUPERAR", "https://integralogistica.com/integrapp/recuperar-clave")
EXPIRE_MINUTOS_RECUPERACION = int(os.getenv("RESET_TOKEN_EXPIRE_MINUTES", "30"))

contexto_pwd = CryptContext(schemes=["bcrypt"], deprecated="auto")

if RESEND_API_KEY:
    resend.api_key = RESEND_API_KEY


def _crear_hash(clave: str) -> str:
    """Crea hash de una clave."""
    return contexto_pwd.hash(clave)


def _verificar_hash(clave: str, clave_hash: str) -> bool:
    """Verifica si una clave coincide con su hash."""
    return contexto_pwd.verify(clave, clave_hash)


def _utc_now() -> datetime:
    """Retorna datetime actual en UTC aware."""
    return datetime.now(timezone.utc)


def _to_utc_aware(dt: Optional[datetime]) -> Optional[datetime]:
    """Convierte datetime a UTC aware."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _guardar_token_recuperacion(usuario_doc: dict) -> str:
    """Genera token de recuperación, lo guarda y retorna el token plano."""
    token_plano = secrets.token_urlsafe(32)
    token_hash = _crear_hash(token_plano)
    exp = _utc_now() + timedelta(minutes=EXPIRE_MINUTOS_RECUPERACION)
    
    from bson import ObjectId
    base_datos = bd_cliente.integra
    base_datos.usuarios.update_one(
        {"_id": usuario_doc["_id"]},
        {"$set": {"reset_token_hash": token_hash, "reset_token_exp": exp}}
    )
    return token_plano


def _enviar_correo_resend_reset(email_destino: str, enlace: str):
    """Envía correo de recuperación usando Resend."""
    if not RESEND_API_KEY:
        print(f"[RESEND] RESEND_API_KEY no configurada. No se envió correo a {email_destino}.")
        return

    payload = {
        "from": MAIL_FROM,
        "to": [email_destino],
        "subject": "Recuperación de contraseña - Integra Soluciones Logísticas",
        "html": (
            "<p>Hola,</p>"
            "<p>Recibimos una solicitud para restablecer tu contraseña desde WhatsApp.</p>"
            f"<p>Puedes hacerlo ingresando al siguiente enlace (válido por {EXPIRE_MINUTOS_RECUPERACION} minutos):</p>"
            f'<p><a href="{enlace}" target="_blank">{enlace}</a></p>'
            "<p>Una vez restablezcas tu contraseña, vuelve a WhatsApp e ingresa tu cédula y nueva clave.</p>"
            "<p>Si no solicitaste este cambio, ignora este mensaje.</p>"
            "<p>Saludos,<br>Equipo Integra Soluciones Logísticas</p>"
        ),
    }
    
    try:
        resend.Emails.send(payload)
        print(f"[RESEND] Correo de recuperación enviado a {email_destino}")
    except Exception as e:
        print(f"[RESEND] Error enviando correo a {email_destino}: {e}")


def buscar_usuario_por_cedula(cedula: str) -> Optional[dict]:
    """
    Busca un usuario en la base de datos por su cédula (campo 'tenedor').
    Retorna el documento del usuario o None si no existe.
    """
    try:
        from bson import ObjectId
        base_datos = bd_cliente.integra
        return base_datos.usuarios.find_one({"tenedor": cedula})
    except Exception as e:
        print(f"[AUTH] Error buscando usuario por cédula {cedula}: {e}")
        return None


def verificar_credenciales_transportador(cedula: str, clave: str) -> Tuple[bool, Optional[dict]]:
    """
    Verifica si las credenciales son válidas.
    Retorna (es_valido, usuario_doc) donde usuario_doc es None si no es válido.
    """
    usuario = buscar_usuario_por_cedula(cedula)
    
    if not usuario:
        return (False, None)
    
    if not _verificar_hash(clave, usuario.get("clave", "")):
        return (False, None)
    
    return (True, usuario)


def solicitar_recuperacion_clave(cedula: str) -> Tuple[bool, str, Optional[str]]:
    """
    Solicita recuperación de clave para un usuario.
    Retorna (exito, mensaje, email_destino).
    """
    usuario = buscar_usuario_por_cedula(cedula)
    
    if not usuario:
        return (False, "No se encontró un usuario registrado con esa cédula.", None)
    
    email = usuario.get("email")
    if not email:
        return (False, "El usuario no tiene un correo electrónico registrado.", None)
    
    try:
        token_plano = _guardar_token_recuperacion(usuario)
        enlace = f"{FRONTEND_URL_RECUPERAR}?token={token_plano}"
        _enviar_correo_resend_reset(email, enlace)
        
        return (
            True,
            f"Hemos enviado un enlace de recuperación a tu correo electrónico ({email}). Sigue las instrucciones para cambiar tu contraseña. Cuando termines, vuelve aquí e ingresa tu cédula y nueva clave.",
            email
        )
    except Exception as e:
        print(f"[AUTH] Error en recuperación de clave: {e}")
        return (False, "Ocurrió un error al enviar el correo de recuperación. Inténtalo más tarde.", None)
