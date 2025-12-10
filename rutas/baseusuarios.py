from fastapi import APIRouter, HTTPException, status, Body, BackgroundTasks
from pymongo import MongoClient
from bson import ObjectId
from pydantic import BaseModel
from typing import List, Optional
import os
import random
import resend 
import requests
# ==============================================================================
# 🔗 CONFIGURACIÓN DE BASE DE DATOS
# ==============================================================================
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
ConexionMongo = MongoClient(MONGO_URI)
base_datos = ConexionMongo["integra"]
coleccion_usuarios = base_datos["baseusuarios"]

# Índices
try:
    coleccion_usuarios.create_index("usuario", unique=True)
    coleccion_usuarios.create_index("perfil")
except Exception:
    pass


# ==============================================================================
# 🚦 CONFIGURACIÓN DEL ROUTER
# ==============================================================================
ruta_baseusuarios = APIRouter(
    prefix="/baseusuarios",
    tags=["BaseUsuarios"],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}},
)


# ==============================================================================
# 🔑 CONFIGURACIÓN RESEND
# ==============================================================================
resend.api_key = os.getenv("RESEND_API_KEY", "re_TuApiKeyAqui...") 
MAIL_FROM = os.getenv("MAIL_FROM", "no-reply@integralogistica.com")


# ==============================================================================
# 📌 ESQUEMAS DE DATOS
# ==============================================================================
class BaseUsuario(BaseModel):
    nombre: str
    correo: Optional[str] = None
    regional: str
    celular: Optional[str] = None
    perfil: str
    usuario: str
    clave: str

class UsuarioLite(BaseModel):
    id: str
    nombre: str
    usuario: str

class VerificarInput(BaseModel):
    usuario: str
    perfil: str

class ValidarCodigoInput(BaseModel):
    usuario: str
    codigo: str
    perfil: str

class CambioClaveInput(BaseModel):
    usuario: str
    nuevaClave: str
    codigo: str
    perfil: str


# ==============================================================================
# 🛠️ FUNCIONES AUXILIARES
# ==============================================================================

def modelo_usuario(u) -> dict:
    return {
        "id": str(u["_id"]),
        "nombre": u["nombre"],
        "correo": u.get("correo"),
        "regional": u["regional"],
        "celular": u.get("celular"),
        "perfil": u["perfil"],
        "usuario": u["usuario"],
    }

def enviar_correo_codigo(destinatario: str, codigo: str):
    """Envía el código de verificación usando Resend de forma silenciosa."""
    
    if not resend.api_key or "TuApiKeyAqui" in resend.api_key:
         print("⚠️ ERROR: Falta API KEY de Resend.")
         return

    html_simple = f"""
    <p>Hola,</p>
    <p>Tu código de verificación es: <strong>{codigo}</strong></p>
    <p><small>Si no solicitaste este código, ignora este mensaje.</small></p>
    """

    try:
        params = {
            "from": MAIL_FROM,
            "to": [destinatario],
            "subject": f"Código de verificación: {codigo}",
            "html": html_simple,
        }
        resend.Emails.send(params)
        
    except Exception as e:
        print(f"❌ Error crítico enviando correo: {e}")


# ==============================================================================
# 🔐 RUTAS DE RECUPERACIÓN
# ==============================================================================

# 1. Verificar Usuario
@ruta_baseusuarios.post("/verificarRecuperacion")
async def verificar_recuperacion(data: VerificarInput, background_tasks: BackgroundTasks):
    usuario_norm = data.usuario.strip().upper()
    
    usuario = coleccion_usuarios.find_one({
        "usuario": usuario_norm,
        "perfil": data.perfil.upper()
    })
    
    if not usuario:
        return {"existe": False}

    codigo = str(random.randint(1000, 9999))
    
    coleccion_usuarios.update_one(
        {"_id": usuario["_id"]},
        {"$set": {"recovery_code": codigo}}
    )
    
    correo_destino = usuario.get("correo") or usuario_norm

    background_tasks.add_task(enviar_correo_codigo, correo_destino, codigo)

    return {"existe": True, "mensaje": "Código generado"}


# 2. Validar Código
@ruta_baseusuarios.post("/validarCodigoRecuperacion")
async def validar_codigo_recuperacion(data: ValidarCodigoInput):
    usuario_norm = data.usuario.strip().upper()
    
    usuario = coleccion_usuarios.find_one({
        "usuario": usuario_norm, 
        "perfil": data.perfil.upper()
    })
    
    if not usuario: 
        return {"valido": False}
    
    codigo_guardado = usuario.get("recovery_code")
    es_valido = (codigo_guardado is not None) and (codigo_guardado == data.codigo)
    
    return {"valido": es_valido}


# 3. Cambiar Contraseña
@ruta_baseusuarios.post("/cambiarClaveConductor")
async def cambiar_clave_conductor(data: CambioClaveInput):
    usuario_norm = data.usuario.strip().upper()
    
    usuario = coleccion_usuarios.find_one({
        "usuario": usuario_norm, 
        "perfil": data.perfil.upper()
    })
    
    if not usuario: 
        raise HTTPException(status_code=404, detail="Usuario no encontrado")

    codigo_guardado = usuario.get("recovery_code")
    if not codigo_guardado or codigo_guardado != data.codigo:
         raise HTTPException(status_code=403, detail="Código inválido o expirado")

    coleccion_usuarios.update_one(
        {"_id": usuario["_id"]},
        {
            "$set": {"clave": data.nuevaClave.strip()}, 
            "$unset": {"recovery_code": ""}
        }
    )
    return {"mensaje": "Clave actualizada correctamente"}


# ==============================================================================
# 👤 RUTAS CRUD DE USUARIOS
# ==============================================================================

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
        "clave": data.clave.strip(),         
    }

    id_insertado = coleccion_usuarios.insert_one(nuevo).inserted_id
    usuario_insertado = coleccion_usuarios.find_one({"_id": id_insertado})
    return {"mensaje": "Usuario creado", "usuario": modelo_usuario(usuario_insertado)}


@ruta_baseusuarios.get("/", response_model=List[dict])
async def obtener_baseusuarios():
    usuarios = coleccion_usuarios.find()
    return [modelo_usuario(u) for u in usuarios]


@ruta_baseusuarios.get("/despachadores", response_model=List[UsuarioLite])
async def listar_despachadores():
    cursor = coleccion_usuarios.find(
        {"perfil": "DESPACHADOR"}, 
        {"nombre": 1, "usuario": 1}
    ).sort("nombre", 1)
    
    return [{"id": str(u["_id"]), "nombre": u["nombre"], "usuario": u["usuario"]} for u in cursor]
    

@ruta_baseusuarios.get("/{usuario_id}", response_model=dict)
async def obtener_baseusuario(usuario_id: str):
    try:
        oid = ObjectId(usuario_id)
    except Exception:
        raise HTTPException(status_code=400, detail="ID inválido")
    
    usuario = coleccion_usuarios.find_one({"_id": oid})
    if not usuario:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
        
    return modelo_usuario(usuario)


@ruta_baseusuarios.put("/{usuario_id}", response_model=dict)
async def actualizar_baseusuario(usuario_id: str, data: BaseUsuario):
    try:
        oid = ObjectId(usuario_id)
    except Exception:
        raise HTTPException(status_code=400, detail="ID inválido")
        
    actualiza = {
        "nombre": data.nombre.upper(),
        "correo": data.correo.upper() if data.correo else None,
        "regional": data.regional.upper(),
        "celular": data.celular.upper() if data.celular else None,
        "perfil": data.perfil.upper(),
        "usuario": data.usuario.upper(),
        "clave": data.clave.strip(),
    }
    
    result = coleccion_usuarios.update_one({"_id": oid}, {"$set": actualiza})
    
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
        
    actualizado = coleccion_usuarios.find_one({"_id": oid})
    return {"mensaje": "Usuario actualizado", "usuario": modelo_usuario(actualizado)}


@ruta_baseusuarios.delete("/{usuario_id}", response_model=dict)
async def eliminar_baseusuario(usuario_id: str):
    try:
        oid = ObjectId(usuario_id)
    except Exception:
        raise HTTPException(status_code=400, detail="ID inválido")
        
    result = coleccion_usuarios.delete_one({"_id": oid})
    
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
        
    return {"mensaje": "Usuario eliminado"}


# ==============================================================================
# 🚪 RUTAS DE LOGIN
# ==============================================================================

@ruta_baseusuarios.post("/login", response_model=dict)
async def login_baseusuario(usuario: str = Body(..., embed=True), clave: str = Body(..., embed=True)):
    usuario_norm = usuario.strip().upper()
    clave_ingresada = clave.strip()
    
    encontrado = coleccion_usuarios.find_one({"usuario": usuario_norm})
    if not encontrado:
        raise HTTPException(status_code=401, detail="Usuario o clave incorrectos")
        
    clave_almacenada = str(encontrado.get("clave", "")).strip()
    
    if not (clave_almacenada == clave_ingresada or clave_almacenada == clave_ingresada.upper()):
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


@ruta_baseusuarios.post("/loginseguridad", response_model=dict)
async def login_seguridad(correo: str = Body(..., embed=True), clave: str = Body(..., embed=True)):

    correo_limpio = correo.strip()
    clave_ingresada = clave.strip()
    encontrado = coleccion_usuarios.find_one({
        "correo": {"$regex": f"^{correo_limpio}$", "$options": "i"}
    })
    
    if not encontrado:
        raise HTTPException(status_code=401, detail="Correo o clave incorrectos")
        
    # 3. Validar Clave
    clave_almacenada = str(encontrado.get("clave", "")).strip()
    if not (clave_almacenada == clave_ingresada or clave_almacenada == clave_ingresada.upper()):
        raise HTTPException(status_code=401, detail="Correo o clave incorrectos")
        
    # 4. Validar Perfil
    perfil = encontrado.get("perfil", "").strip().upper()
    if perfil not in ["SEGURIDAD", "ADMIN"]:
        raise HTTPException(status_code=403, detail="No tiene permisos de Seguridad")
        
    # 5. Retornar datos 
    return {
        "mensaje": "Login seguridad exitoso", 
        "usuario": {
            "id": str(encontrado["_id"]), 
            "nombre": encontrado.get("nombre", "Usuario"), 
            "usuario": encontrado.get("usuario", ""), 
            "correo": encontrado.get("correo", ""),
            "perfil": perfil
        }
    }

@ruta_baseusuarios.post("/loginConductor", response_model=dict)
async def login_Conductor(usuario: str = Body(..., embed=True), clave: str = Body(..., embed=True)):
    usuario_norm = usuario.strip().upper()
    clave_ingresada = clave.strip()
    
    encontrado = coleccion_usuarios.find_one({"usuario": usuario_norm})
    if not encontrado:
        raise HTTPException(status_code=401, detail="Usuario o clave incorrectos")
        
    clave_almacenada = str(encontrado.get("clave", "")).strip()
    if not (clave_almacenada == clave_ingresada or clave_almacenada == clave_ingresada.upper()):
        raise HTTPException(status_code=401, detail="Usuario o clave incorrectos")
        
    perfil = encontrado.get("perfil", "").strip().upper()
    if perfil not in ["CONDUCTOR", "ADMIN"]:
        raise HTTPException(status_code=403, detail="No tiene permisos de Seguridad")
        
    return {
        "mensaje": "Login Conductor exitoso", 
        "usuario": {
            "id": str(encontrado["_id"]), 
            "usuario": encontrado["usuario"], 
            "perfil": perfil
        }
    }