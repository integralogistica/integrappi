# archivo: rutas/baseusuarios.py

from fastapi import APIRouter, HTTPException, status, Body
from pymongo import MongoClient
from bson import ObjectId
from pydantic import BaseModel
from typing import List, Optional
import os

# ------------------------------
# 🔗 Conexión MongoDB
# ------------------------------
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
ConexionMongo = MongoClient(MONGO_URI)
base_datos = ConexionMongo["integra"]
coleccion_usuarios = base_datos["baseusuarios"]

# ------------------------------
# 🚦 Configuración Router
# ------------------------------
ruta_baseusuarios = APIRouter(
    prefix="/baseusuarios",
    tags=["BaseUsuarios"],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}},
)

# ------------------------------
# 📌 Esquema Pydantic
# ------------------------------
class BaseUsuario(BaseModel):
    nombre: str
    correo: Optional[str] = None
    regional: str
    celular: Optional[str] = None
    perfil: str
    usuario: str
    clave: str

# ------------------------------
# 📌 Modelo de salida
# ------------------------------
def modelo_usuario(u) -> dict:
    return {
        "id": str(u["_id"]),
        "nombre": u["nombre"],
        "correo": u.get("correo"),
        "regional": u["regional"],
        "celular": u.get("celular"),
        "perfil": u["perfil"],
        "usuario": u["usuario"],
        "clave": u["clave"],
    }

# ------------------------------
# ✅ Crear usuario
# ------------------------------
@ruta_baseusuarios.post("/", response_model=dict)
async def crear_baseusuario(data: BaseUsuario):
    if coleccion_usuarios.find_one({"usuario": data.usuario.upper()}):
        raise HTTPException(status_code=400, detail="El usuario ya existe")

    nuevo = {
        "nombre": data.nombre.upper(),
        "correo": data.correo.upper() if data.correo else None,
        "regional": data.regional.upper(),
        "celular": data.celular.upper() if data.celular else None,
        "perfil": data.perfil.upper(),
        "usuario": data.usuario.upper(),
        "clave": data.clave.upper(),
    }

    id_insertado = coleccion_usuarios.insert_one(nuevo).inserted_id
    usuario_insertado = coleccion_usuarios.find_one({"_id": id_insertado})
    return {"mensaje": "Usuario creado exitosamente", "usuario": modelo_usuario(usuario_insertado)}

# ------------------------------
# ✅ Obtener todos los usuarios
# ------------------------------
@ruta_baseusuarios.get("/", response_model=List[dict])
async def obtener_baseusuarios():
    usuarios = coleccion_usuarios.find()
    return [modelo_usuario(u) for u in usuarios]

# ------------------------------
# ✅ Obtener usuario por ID
# ------------------------------
@ruta_baseusuarios.get("/{usuario_id}", response_model=dict)
async def obtener_baseusuario(usuario_id: str):
    usuario = coleccion_usuarios.find_one({"_id": ObjectId(usuario_id)})
    if not usuario:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    return modelo_usuario(usuario)

# ------------------------------
# ✅ Actualizar usuario por ID
# ------------------------------
@ruta_baseusuarios.put("/{usuario_id}", response_model=dict)
async def actualizar_baseusuario(usuario_id: str, data: BaseUsuario):
    actualiza = {
        "nombre": data.nombre.upper(),
        "correo": data.correo.upper() if data.correo else None,
        "regional": data.regional.upper(),
        "celular": data.celular.upper() if data.celular else None,
        "perfil": data.perfil.upper(),
        "usuario": data.usuario.upper(),
        "clave": data.clave.upper(),
    }

    result = coleccion_usuarios.update_one({"_id": ObjectId(usuario_id)}, {"$set": actualiza})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")

    actualizado = coleccion_usuarios.find_one({"_id": ObjectId(usuario_id)})
    return {"mensaje": "Usuario actualizado", "usuario": modelo_usuario(actualizado)}

# ------------------------------
# ✅ Eliminar usuario por ID
# ------------------------------
@ruta_baseusuarios.delete("/{usuario_id}", response_model=dict)
async def eliminar_baseusuario(usuario_id: str):
    result = coleccion_usuarios.delete_one({"_id": ObjectId(usuario_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    return {"mensaje": "Usuario eliminado"}

# ------------------------------
# ✅ Login de usuario
# ------------------------------
@ruta_baseusuarios.post("/login", response_model=dict)
async def login_baseusuario(
    usuario: str = Body(..., embed=True),
    clave: str = Body(..., embed=True)
):
    usuario = usuario.upper()
    clave = clave.upper()

    encontrado = coleccion_usuarios.find_one({"usuario": usuario, "clave": clave})
    if not encontrado:
        raise HTTPException(status_code=401, detail="Usuario o clave incorrectos")

    return {
        "mensaje": "Login exitoso",
        "usuario": {
            "id": str(encontrado["_id"]),
            "usuario": encontrado["usuario"],
            "perfil": encontrado["perfil"],
            "regional": encontrado["regional"]
        }
    }
