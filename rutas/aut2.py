from fastapi import APIRouter, HTTPException, Depends, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pymongo import MongoClient
from bson import ObjectId
from passlib.context import CryptContext
import jwt
from datetime import datetime, timedelta, timezone
from typing import List
from pydantic import BaseModel
from bd.bd_cliente import bd_cliente
from bd.models.usuario import modelo_usuario, modelo_usuarios

# Configuración de la base de datos
base_datos = bd_cliente.integra

# Configuración de FastAPI y seguridad
ruta_usuario = APIRouter( 
    prefix="/usuarios",
    tags=['Usuarios'],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}}
)

esquema_oauth2 = OAuth2PasswordBearer(tokenUrl="usuarios/token")
CLAVE_SECRETA = "tu_clave_secreta"  # Cambia esto por una clave secreta más segura
ALGORITMO = "HS256"
EXPIRE_MINUTOS_TOKEN = 20

# Configuración de la contraseña
contexto_pwd = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Modelos de datos
class Usuario(BaseModel):
    nombre: str
    email: str
    tenedor: str
    telefono: str
    clave: str

def modelo_usuarios(usuarios) -> list:
    return [modelo_usuario(usuario) for usuario in usuarios]

# Funciones de seguridad
def crear_hash(clave: str) -> str:
    return contexto_pwd.hash(clave)

def verificar_hash(clave: str, clave_hash: str) -> bool:
    return contexto_pwd.verify(clave, clave_hash)

def crear_token(data: dict, expires_delta: timedelta = None) -> str:
    to_encode = data.copy()
    if expires_delta:
        expira = datetime.now(timezone.utc) + expires_delta
    else:
        expira = datetime.now(timezone.utc) + timedelta(minutes=15)
    to_encode.update({"exp": expira})
    return jwt.encode(to_encode, CLAVE_SECRETA, algorithm=ALGORITMO)

# Dependencias
async def obtener_usuario_actual(token: str = Depends(esquema_oauth2)):
    excepcion_credenciales = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="No se pudo validar las credenciales",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, CLAVE_SECRETA, algorithms=[ALGORITMO])
        email = payload.get("sub")
        if email is None:
            raise excepcion_credenciales
    except jwt.PyJWTError:
        raise excepcion_credenciales
    usuario = base_datos.usuarios.find_one({"email": email})
    if usuario is None:
        raise excepcion_credenciales
    return modelo_usuario(usuario)

# Rutas de la API
@ruta_usuario.post("/token")
async def iniciar_sesion(form_data: OAuth2PasswordRequestForm = Depends()):
    usuario = base_datos.usuarios.find_one({"email": form_data.username})
    if not usuario or not verificar_hash(form_data.password, usuario["clave"]):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Credenciales incorrectas")
    
    expires_access_token = timedelta(minutes=EXPIRE_MINUTOS_TOKEN)
    access_token = crear_token(data={"sub": usuario["email"]}, expires_delta=expires_access_token)
    return {"access_token": access_token, "token_type": "bearer"}

@ruta_usuario.post("/", response_model=dict)
async def crear_usuario(usuario: Usuario):
    # Verificar si el email ya existe
    if base_datos.usuarios.find_one({"email": usuario.email}):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="El email ya está en uso")
    
    usuario.clave = crear_hash(usuario.clave)  # Hash de la contraseña
    nuevo_usuario = {
        "nombre": usuario.nombre,
        "email": usuario.email,
        "tenedor": usuario.tenedor,
        "telefono": usuario.telefono,
        "clave": usuario.clave,
    }
    result = base_datos.usuarios.insert_one(nuevo_usuario)
    return modelo_usuario(base_datos.usuarios.find_one({"_id": result.inserted_id}))

@ruta_usuario.get("/{usuario_id}", response_model=dict)
async def obtener_usuario(usuario_id: str, usuario_actual: dict = Depends(obtener_usuario_actual)):
    usuario = base_datos.usuarios.find_one({"_id": ObjectId(usuario_id)})
    if usuario is None:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    return modelo_usuario(usuario)

@ruta_usuario.put("/{usuario_id}", response_model=dict)
async def actualizar_usuario(usuario_id: str, usuario: Usuario, usuario_actual: dict = Depends(obtener_usuario_actual)):
    usuario_actualizado = {
        "nombre": usuario.nombre,
        "email": usuario.email,
        "tenedor": usuario.tenedor,
        "telefono": usuario.telefono,
        "clave": crear_hash(usuario.clave)  # Actualiza la contraseña
    }
    result = base_datos.usuarios.update_one({"_id": ObjectId(usuario_id)}, {"$set": usuario_actualizado})
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    return modelo_usuario(base_datos.usuarios.find_one({"_id": ObjectId(usuario_id)}))

@ruta_usuario.delete("/{usuario_id}", response_model=dict)
async def eliminar_usuario(usuario_id: str, usuario_actual: dict = Depends(obtener_usuario_actual)):
    result = base_datos.usuarios.delete_one({"_id": ObjectId(usuario_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    return {"mensaje": "Usuario eliminado"}

# Ejecutar el servidor: `uvicorn main:app --reload`
