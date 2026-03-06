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
            "<p>Una vez restablezcas tu contraseña, vuelve a WhatsApp e ingresa tu cédula.</p>"
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
            f"Hemos enviado un enlace de recuperación a tu correo electrónico ({email}). Sigue las instrucciones para cambiar tu contraseña. Cuando termines, vuelve aquí e ingresa tu cédula.",
            email
        )
    except Exception as e:
        print(f"[AUTH] Error en recuperación de clave: {e}")
        return (False, "Ocurrió un error al enviar el correo de recuperación. Inténtalo más tarde.", None)


# =========================
# Funciones de Registro de Usuarios
# =========================

def _validar_email(email: str) -> Tuple[bool, str]:
    """Valida formato de email con regex."""
    import re
    email_regex = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
    if not email_regex.match(email):
        return (False, "El formato del correo electrónico no es válido.")
    return (True, "")


def _validar_telefono(telefono: str) -> Tuple[bool, str]:
    """Valida formato de teléfono (10 dígitos)."""
    import re
    tel_regex = re.compile(r"^\d{10}$")
    return bool(tel_regex.match(telefono.replace(" ", "").replace("-", "")))


def _validar_clave(clave: str) -> Tuple[bool, str]:
    """Valida que la clave cumpla requisitos mínimos."""
    if len(clave) < 6:
        return (False, "La clave debe tener mínimo 6 caracteres.")
    return (True, "")


def _generar_codigo_confirmacion() -> str:
    """Genera un código de 6 dígitos para confirmación de email."""
    return secrets.token_hex(3).upper()


def _guardar_codigo_confirmacion(usuario_id: str, email: str, codigo: str) -> datetime:
    """Guarda código de confirmación en BD con expiración de 1 hora."""
    from bson import ObjectId
    base_datos = bd_cliente.integra
    exp = _utc_now() + timedelta(hours=1)
    
    base_datos.usuarios.update_one(
        {"_id": ObjectId(usuario_id)},
        {"$set": {
            "email_confirmacion_codigo": codigo,
            "email_confirmacion_exp": exp,
            "email_temporal": email  # Guardar email temporal hasta confirmar
        }}
    )
    return exp


def _enviar_correo_confirmacion(email: str, codigo: str):
    """Envía correo de confirmación de registro usando Resend."""
    if not RESEND_API_KEY:
        print(f"[RESEND] RESEND_API_KEY no configurada. No se envió correo a {email}.")
        return

    payload = {
        "from": MAIL_FROM,
        "to": [email],
        "subject": "Confirma tu registro - Integra Soluciones Logísticas",
        "html": (
            "<p>Bienvenido a Integra Soluciones Logísticas.</p>"
            "<p>Tu código de confirmación es:</p>"
            f'<p style="font-size: 24px; font-weight: bold; color: #0066cc;">{codigo}</p>'
            "<p>Este código expira en 1 hora.</p>"
            "<p>Vuelve a WhatsApp e ingresa este código para completar tu registro.</p>"
        ),
    }
    
    try:
        resend.Emails.send(payload)
        print(f"[RESEND] Correo de confirmación enviado a {email}")
    except Exception as e:
        print(f"[RESEND] Error enviando correo a {email}: {e}")


def crear_usuario_transportador(
    cedula: str,
    nombre: str,
    email: str,
    telefono: str,
    clave: str
) -> Tuple[bool, str, Optional[str]]:
    """
    Crea un nuevo usuario transportador en la base de datos.
    Retorna (exito, mensaje, usuario_id).
    """
    try:
        from bson import ObjectId
        base_datos = bd_cliente.integra
        
        # Verificar si ya existe
        if base_datos.usuarios.find_one({"tenedor": cedula}):
            return (False, "Ya existe un usuario registrado con esa cédula.", None)
        
        # Verificar si el email ya está en uso
        if base_datos.usuarios.find_one({"email": email}):
            return (False, "El correo electrónico ya está registrado.", None)
        
        # Crear usuario
        clave_hash = _crear_hash(clave)
        nuevo_usuario = {
            "tenedor": cedula,
            "nombre": nombre,
            "email": email,
            "telefono": telefono,
            "clave": clave_hash,
            "rol": "transportador",
            "estado": "pendiente",  # Pendiente de confirmación de email
            "fecha_registro": _utc_now(),
        }
        
        resultado = base_datos.usuarios.insert_one(nuevo_usuario)
        usuario_id = str(resultado.inserted_id)
        
        return (True, "Usuario creado exitosamente.", usuario_id)
    
    except Exception as e:
        print(f"[AUTH] Error creando usuario: {e}")
        return (False, "Ocurrió un error al crear el usuario. Inténtalo más tarde.", None)


def verificar_codigo_confirmacion(usuario_id: str, codigo: str) -> Tuple[bool, str, Optional[dict]]:
    """
    Verifica el código de confirmación de email.
    Retorna (exito, mensaje, usuario_doc).
    """
    try:
        from bson import ObjectId
        base_datos = bd_cliente.integra
        
        usuario = base_datos.usuarios.find_one({"_id": ObjectId(usuario_id)})
        
        if not usuario:
            return (False, "Usuario no encontrado.", None)
        
        codigo_guardado = usuario.get("email_confirmacion_codigo")
        codigo_exp = usuario.get("email_confirmacion_exp")
        
        if not codigo_guardado:
            return (False, "No hay un código de confirmación pendiente.", None)
        
        if _utc_now() > _to_utc_aware(codigo_exp):
            return (False, "El código de confirmación ha expirado. Solicita un nuevo código.", None)
        
        if codigo_guardado != codigo.upper():
            return (False, "Código incorrecto.", None)
        
        # Activar usuario
        email_temporal = usuario.get("email_temporal", usuario.get("email"))
        base_datos.usuarios.update_one(
            {"_id": ObjectId(usuario_id)},
            {"$set": {
                "estado": "activo",
                "email": email_temporal,
                "email_confirmacion_codigo": None,
                "email_confirmacion_exp": None,
                "email_temporal": None
            }}
        )
        
        return (True, "Cuenta confirmada exitosamente.", usuario)
    
    except Exception as e:
        print(f"[AUTH] Error verificando código: {e}")
        return (False, "Ocurrió un error al verificar el código. Inténtalo más tarde.", None)
