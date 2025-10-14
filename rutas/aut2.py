# routers/usuarios.py
# ------------------------------------------------------------
# API de Usuarios (archivo único) con:
# - Login con JWT
# - CRUD básico
# - Recuperación de clave por correo usando Resend:
#     1) POST /usuarios/recuperar/solicitar   -> envía enlace con token
#     2) POST /usuarios/recuperar/confirmar   -> valida token y cambia clave
# - (Opcional) POST /usuarios/cambiar-clave   -> cambio autenticado
# - FIX: manejo correcto de zona horaria (naive vs aware) en expiración
#        y no lanzar excepciones desde BackgroundTasks.
# ------------------------------------------------------------

import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Depends, status, BackgroundTasks
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from bson import ObjectId
from passlib.context import CryptContext
from pydantic import BaseModel, EmailStr
import jwt
import resend
from dotenv import load_dotenv

from bd.bd_cliente import bd_cliente
from bd.models.usuario import modelo_usuario  # Debe normalizar _id -> id y ocultar 'clave'

# =========================
# Carga de variables de entorno
# =========================
load_dotenv()
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
MAIL_FROM = os.getenv("MAIL_FROM", "no-reply@integralogistica.com")  # remitente verificado en Resend
FRONTEND_URL_RECUPERAR = os.getenv("FRONTEND_URL_RECUPERAR", "https://integralogistica.com/integrapp/recuperar-clave")

if RESEND_API_KEY:
    resend.api_key = RESEND_API_KEY

# =========================
# Configuración base
# =========================
base_datos = bd_cliente.integra

ruta_usuario = APIRouter(
    prefix="/usuarios",
    tags=["Usuarios"],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}}
)

esquema_oauth2 = OAuth2PasswordBearer(tokenUrl="usuarios/token")

# ⚠️ Cambia por una clave segura y guárdala en variable de entorno en producción
CLAVE_SECRETA = os.getenv("JWT_SECRET", "cambia_esta_clave_por_una_bien_larga_y_aleatoria")
ALGORITMO = "HS256"
EXPIRE_MINUTOS_TOKEN = 20

# Duración del token de recuperación (enlace “olvidé mi contraseña”)
EXPIRE_MINUTOS_RECUPERACION = int(os.getenv("RESET_TOKEN_EXPIRE_MINUTES", "30"))

# Hash de contraseñas / tokens
contexto_pwd = CryptContext(schemes=["bcrypt"], deprecated="auto")

# =========================
# Modelos de datos
# =========================
class Usuario(BaseModel):
    nombre: str
    email: EmailStr
    tenedor: str
    telefono: str
    clave: str

class CambiarClaveIn(BaseModel):
    clave_actual: str
    clave_nueva: str

class RecuperarSolicitarIn(BaseModel):
    email: EmailStr

class RecuperarSolicitarOut(BaseModel):
    mensaje: str

class RecuperarConfirmarIn(BaseModel):
    token: str
    clave_nueva: str

# =========================
# Helpers de seguridad / tiempo
# =========================
def crear_hash(clave: str) -> str:
    return contexto_pwd.hash(clave)

def verificar_hash(clave: str, clave_hash: str) -> bool:
    return contexto_pwd.verify(clave, clave_hash)

def crear_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expira = datetime.now(timezone.utc) + (expires_delta or timedelta(minutes=15))
    to_encode.update({"exp": expira})
    return jwt.encode(to_encode, CLAVE_SECRETA, algorithm=ALGORITMO)

def decodificar_token(token: str) -> dict:
    return jwt.decode(token, CLAVE_SECRETA, algorithms=[ALGORITMO])

def _utc_now() -> datetime:
    """Siempre now en UTC 'aware'."""
    return datetime.now(timezone.utc)

def _to_utc_aware(dt: Optional[datetime]) -> Optional[datetime]:
    """
    Convierte un datetime a UTC aware para comparaciones seguras.
    - Si viene naive (sin tz), asumimos que es UTC y agregamos tz UTC.
    - Si ya es aware, lo normalizamos a UTC.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

# =========================
# Dependencia: usuario actual por JWT
# =========================
async def obtener_usuario_actual(token: str = Depends(esquema_oauth2)):
    cred_error = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="No se pudo validar las credenciales",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = decodificar_token(token)
        email = payload.get("sub")
        if email is None:
            raise cred_error
    except Exception:
        raise cred_error
    usuario = base_datos.usuarios.find_one({"email": email})
    if usuario is None:
        raise cred_error
    return modelo_usuario(usuario)

# =========================
# Login: devuelve JWT + datos útiles
# =========================
@ruta_usuario.post("/token")
async def iniciar_sesion(form_data: OAuth2PasswordRequestForm = Depends()):
    usuario = base_datos.usuarios.find_one({"email": form_data.username})
    if not usuario or not verificar_hash(form_data.password, usuario["clave"]):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Credenciales incorrectas")
    access_token = crear_token(data={"sub": usuario["email"]}, expires_delta=timedelta(minutes=EXPIRE_MINUTOS_TOKEN))
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "nombre": usuario.get("nombre", ""),
        "tenedor": usuario.get("tenedor", ""),
    }

# =========================
# Crear usuario
# =========================
@ruta_usuario.post("/", response_model=dict, status_code=status.HTTP_201_CREATED)
async def crear_usuario(usuario: Usuario):
    if base_datos.usuarios.find_one({"email": usuario.email}):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="El email ya está en uso")
    if base_datos.usuarios.find_one({"tenedor": usuario.tenedor}):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Esta cédula o NIT ya están en uso")

    nuevo_usuario = {
        "nombre": usuario.nombre,
        "email": usuario.email,
        "tenedor": usuario.tenedor,
        "telefono": usuario.telefono,
        "clave": crear_hash(usuario.clave),
        # Campos usados para recuperación de clave:
        "reset_token_hash": None,
        "reset_token_exp": None,
    }
    result = base_datos.usuarios.insert_one(nuevo_usuario)
    return modelo_usuario(base_datos.usuarios.find_one({"_id": result.inserted_id}))

# =========================
# Obtener usuario por id (requiere token)
# =========================
@ruta_usuario.get("/{usuario_id}", response_model=dict)
async def obtener_usuario(usuario_id: str, usuario_actual: dict = Depends(obtener_usuario_actual)):
    usuario = base_datos.usuarios.find_one({"_id": ObjectId(usuario_id)})
    if usuario is None:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    return modelo_usuario(usuario)

# =========================
# Actualizar usuario (requiere token)
# Nota: Si NO quieres que este endpoint cambie la clave,
# elimina la línea de 'clave'.
# =========================
@ruta_usuario.put("/{usuario_id}", response_model=dict)
async def actualizar_usuario(usuario_id: str, usuario: Usuario, usuario_actual: dict = Depends(obtener_usuario_actual)):
    usuario_actualizado = {
        "nombre": usuario.nombre,
        "email": usuario.email,
        "tenedor": usuario.tenedor,
        "telefono": usuario.telefono,
        "clave": crear_hash(usuario.clave)
    }
    result = base_datos.usuarios.update_one({"_id": ObjectId(usuario_id)}, {"$set": usuario_actualizado})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    return modelo_usuario(base_datos.usuarios.find_one({"_id": ObjectId(usuario_id)}))

# =========================
# Eliminar usuario (requiere token)
# =========================
@ruta_usuario.delete("/{usuario_id}", response_model=dict)
async def eliminar_usuario(usuario_id: str, usuario_actual: dict = Depends(obtener_usuario_actual)):
    result = base_datos.usuarios.delete_one({"_id": ObjectId(usuario_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    return {"mensaje": "Usuario eliminado"}

# ============================================================
# ------------ Recuperación de clave con Resend --------------
# Diseño: token aleatorio de un solo uso, almacenado como hash.
# FIX: normalización de datetime (naive vs aware) y no lanzar
#      excepciones desde BackgroundTasks.
# ============================================================

def _guardar_token_recuperacion(usuario_doc: dict) -> str:
    """Genera un token plano, guarda su hash + expiración en el usuario y retorna el token plano."""
    token_plano = secrets.token_urlsafe(32)  # lo que viaja en el enlace
    token_hash = contexto_pwd.hash(token_plano)
    exp = _utc_now() + timedelta(minutes=EXPIRE_MINUTOS_RECUPERACION)
    base_datos.usuarios.update_one(
        {"_id": usuario_doc["_id"]},
        {"$set": {"reset_token_hash": token_hash, "reset_token_exp": exp}}
    )
    return token_plano

def _verificar_token_recuperacion(usuario_doc: dict, token_plano: str) -> bool:
    """Verifica hash y expiración del token almacenado en el usuario con manejo de tz."""
    token_hash = usuario_doc.get("reset_token_hash")
    exp = _to_utc_aware(usuario_doc.get("reset_token_exp"))
    if not token_hash or not exp:
        return False
    if _utc_now() > exp:
        return False
    return contexto_pwd.verify(token_plano, token_hash)

def _invalidar_token_recuperacion(usuario_doc: dict):
    """Elimina el token almacenado para que no se pueda reutilizar."""
    base_datos.usuarios.update_one(
        {"_id": usuario_doc["_id"]},
        {"$set": {"reset_token_hash": None, "reset_token_exp": None}}
    )

def _enviar_correo_resend_reset(email_destino: str, enlace: str):
    """
    Envía correo usando Resend con el enlace de recuperación.
    IMPORTANTE: No lanzar HTTPException aquí (se ejecuta en background).
    """
    if not RESEND_API_KEY:
        print("[RESEND] RESEND_API_KEY no configurada. No se envió correo.")
        return

    payload = {
        "from": MAIL_FROM,
        "to": [email_destino],
        "subject": "Recuperación de contraseña",
        "html": (
            "<p>Recibimos una solicitud para restablecer tu contraseña.</p>"
            f"<p>Puedes hacerlo ingresando al siguiente enlace (válido por {EXPIRE_MINUTOS_RECUPERACION} minutos):</p>"
            f'<p><a href="{enlace}" target="_blank">{enlace}</a></p>'
            "<p>Si no solicitaste este cambio, ignora este mensaje.</p>"
        ),
    }
    try:
        resend.Emails.send(payload)  # SDK de Resend
        print(f"[RESEND] Correo de recuperación enviado a {email_destino}")
    except Exception as e:
        # Loguear, no lanzar (para evitar "response already started")
        print(f"[RESEND] Error enviando correo a {email_destino}: {e}")

@ruta_usuario.post("/recuperar/solicitar", response_model=RecuperarSolicitarOut)
async def recuperar_solicitar(data: RecuperarSolicitarIn, background: BackgroundTasks):
    """
    Paso 1: El usuario envía su email. Si existe, generamos un token de un solo uso,
    lo guardamos hasheado con expiración y enviamos un enlace por correo vía Resend.
    La respuesta es neutra (no revela si el email existe).
    """
    doc = base_datos.usuarios.find_one({"email": data.email})
    if doc:
        token_plano = _guardar_token_recuperacion(doc)
        enlace = f"{FRONTEND_URL_RECUPERAR}?token={token_plano}"
        # Enviar en background para no bloquear la solicitud (sin lanzar excepciones)
        background.add_task(_enviar_correo_resend_reset, data.email, enlace)

    return RecuperarSolicitarOut(
        mensaje="Si el email existe, se envió un enlace de recuperación."
    )

@ruta_usuario.post("/recuperar/confirmar", response_model=dict)
async def recuperar_confirmar(data: RecuperarConfirmarIn):
    """
    Paso 2: El frontend envía el token y la nueva clave.
    Buscamos un usuario con token válido, verificamos hash/expiración, cambiamos clave
    e invalidamos el token.
    """
    cursor = base_datos.usuarios.find({"reset_token_hash": {"$ne": None}})
    usuario_objetivo = None
    for doc in cursor:
        if _verificar_token_recuperacion(doc, data.token):
            usuario_objetivo = doc
            break

    if not usuario_objetivo:
        raise HTTPException(status_code=400, detail="Token inválido o expirado")

    nueva_hash = crear_hash(data.clave_nueva)
    base_datos.usuarios.update_one({"_id": usuario_objetivo["_id"]}, {"$set": {"clave": nueva_hash}})
    _invalidar_token_recuperacion(usuario_objetivo)
    return {"mensaje": "Tu clave ha sido restablecida exitosamente"}

# ============================================================
# (Opcional) Cambiar clave autenticado
# ============================================================
@ruta_usuario.post("/cambiar-clave", response_model=dict)
async def cambiar_clave(payload: CambiarClaveIn, usuario_actual: dict = Depends(obtener_usuario_actual)):
    doc = base_datos.usuarios.find_one({"email": usuario_actual["email"]})
    if not doc or not verificar_hash(payload.clave_actual, doc["clave"]):
        raise HTTPException(status_code=400, detail="La clave actual no es correcta")
    base_datos.usuarios.update_one({"_id": doc["_id"]}, {"$set": {"clave": crear_hash(payload.clave_nueva)}})
    return {"mensaje": "Clave actualizada correctamente"}
