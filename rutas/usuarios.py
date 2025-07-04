# ----------------------------------------
# 📁 Importaciones necesarias
# ----------------------------------------
from fastapi import APIRouter, HTTPException, status, Query
from bd.schemas.usuario import Usuario  # Esquema Pydantic para validar usuarios
from bd.bd_cliente import bd_cliente    # Conexión a la base de datos MongoDB
from bd.models.usuario import modelo_usuario, modelo_usuarios  # Funciones para formatear usuarios
from bson import ObjectId
from typing import List, Optional

# ----------------------------------------
# 🔗 Configuración del Router de Usuarios
# ----------------------------------------
ruta_usuario = APIRouter(
    prefix="/usuarios",  # Prefijo de las rutas para este módulo
    tags=['Usuarios'],   # Agrupa las rutas en la documentación de FastAPI
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}}  # Respuesta por defecto
)

# ----------------------------------------
# 🚀 GET /usuarios/
# 📌 Obtiene todos los usuarios registrados
# ----------------------------------------
@ruta_usuario.get("/", response_model=List[Usuario])
async def getUsuarios():
    """
    Retorna una lista con todos los usuarios en la base de datos.
    """
    return modelo_usuarios(bd_cliente.usuarios.find())

# ----------------------------------------
# 🚀 GET /usuarios/{id}
# 📌 Obtiene un usuario por su ID
# ----------------------------------------
@ruta_usuario.get("/{id}", response_model=Usuario)
async def getUsuario(id: str):
    """
    Busca un usuario por su ID de MongoDB y lo retorna.
    Si no existe, lanza un error 404.
    """
    try:
        usuario = bd_cliente.usuarios.find_one({"_id": ObjectId(id)})
        if not usuario:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Usuario no encontrado")
        return Usuario(**modelo_usuario(usuario))
    except:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No se encontró el usuario")

# ----------------------------------------
# 🚀 POST /usuarios/
# 📌 Crea un nuevo usuario
# ----------------------------------------
@ruta_usuario.post("/", status_code=status.HTTP_201_CREATED, response_model=Usuario)
async def crearUsuario(usuario: Usuario):
    """
    Crea un usuario nuevo en la base de datos.
    Si el email ya existe, lanza un error 409 (conflicto).
    """
    usuario_existente = buscarUsuario("email", usuario.email)
    if usuario_existente and "error" not in usuario_existente:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="El usuario ya existe")
    
    # Convierte el modelo Pydantic a dict y elimina el campo id si existe
    usuario_dict = usuario.model_dump(exclude_unset=True)
    usuario_dict.pop("id", None)
    
    # Inserta en MongoDB
    id = bd_cliente.usuarios.insert_one(usuario_dict).inserted_id
    nuevo_usuario = bd_cliente.usuarios.find_one({"_id": id})
    return Usuario(**modelo_usuario(nuevo_usuario))

# ----------------------------------------
# 🚀 PUT /usuarios/{id}
# 📌 Actualiza un usuario existente por su ID
# ----------------------------------------
@ruta_usuario.put("/{id}", response_model=Usuario)
async def actualizarUsuario(id: str, usuario: Usuario):
    """
    Actualiza la información de un usuario existente.
    Si no se encuentra el usuario, retorna error 404.
    """
    try:
        resultado = bd_cliente.usuarios.update_one(
            {"_id": ObjectId(id)},
            {"$set": usuario.model_dump(exclude_unset=True)}
        )
        if resultado.matched_count == 0:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Usuario no encontrado")
        
        usuario_actualizado = bd_cliente.usuarios.find_one({"_id": ObjectId(id)})
        return Usuario(**modelo_usuario(usuario_actualizado))
    except:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No se encontró usuario para actualizar")

# ----------------------------------------
# 🚀 DELETE /usuarios/{id}
# 📌 Elimina un usuario por su ID
# ----------------------------------------
@ruta_usuario.delete("/{id}")
async def eliminarUsuario(id: str):
    """
    Elimina un usuario de la base de datos por su ID.
    Si no se encuentra, retorna error 404.
    """
    try:
        resultado = bd_cliente.usuarios.delete_one({"_id": ObjectId(id)})
        if resultado.deleted_count == 0:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Usuario no encontrado")
        return {"message": "Usuario eliminado"}
    except:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No se encontró usuario para eliminar")

# ----------------------------------------
# 🔍 Función auxiliar: buscarUsuario
# 📌 Busca un usuario por un criterio (ejemplo: email)
# ----------------------------------------
def buscarUsuario(criterio: str, key):
    """
    Busca un usuario en la base de datos dado un campo y valor.
    Si no se encuentra, retorna un dict con 'error'.
    """
    try:
        usuario = bd_cliente.usuarios.find_one({criterio: key})
        return Usuario(**modelo_usuario(usuario))
    except:
        return {"error": "No se encontró usuario"}
