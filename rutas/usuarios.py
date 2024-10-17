from fastapi import APIRouter, HTTPException, status, Query
from bd.schemas.usuario import Usuario
from bd.bd_cliente import bd_cliente
from bd.models.usuario import modelo_usuario, modelo_usuarios
from bson import ObjectId
from typing import List, Optional

ruta_usuario = APIRouter(
    prefix="/usuarios",
    tags=['Usuarios'],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}}
)

@ruta_usuario.get("/", response_model=List[Usuario])
async def getUsuarios():
    return modelo_usuarios(bd_cliente.usuarios.find())

@ruta_usuario.get("/{id}", response_model=Usuario)
async def getUsuario(id: str):
    try:
        usuario = bd_cliente.usuarios.find_one({"_id": ObjectId(id)})
        if not usuario:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Usuario no encontrado")
        return Usuario(**modelo_usuario(usuario))
    except:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No se encontró el usuario")


@ruta_usuario.post("/", status_code=status.HTTP_201_CREATED, response_model=Usuario)
async def crearUsuario(usuario: Usuario):
    usuario_existente = buscarUsuario("email", usuario.email)
    if usuario_existente and "error" not in usuario_existente:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="El usuario ya existe")
    
    usuario_dict = usuario.model_dump(exclude_unset=True)
    usuario_dict.pop("id", None)
    
    id = bd_cliente.usuarios.insert_one(usuario_dict).inserted_id
    nuevo_usuario = bd_cliente.usuarios.find_one({"_id": id})
    return Usuario(**modelo_usuario(nuevo_usuario))

@ruta_usuario.put("/{id}", response_model=Usuario)
async def actualizarUsuario(id: str, usuario: Usuario):
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
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No se encotró usuario para actualizar")

@ruta_usuario.delete("/{id}")
async def eliminarUsuario(id: str):
    try:
        resultado = bd_cliente.usuarios.delete_one({"_id": ObjectId(id)})
        if resultado.deleted_count == 0:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Usuario no encontrado")
        return {"message": "Usuario eliminado"}
    except:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No se encontró usuario para eliminar")

def buscarUsuario(criterio: str, key):
    try:
        usuario = bd_cliente.usuarios.find_one({criterio: key})
        return Usuario(**modelo_usuario(usuario))
    except:
        return {"error": "No se encontró usuario"}