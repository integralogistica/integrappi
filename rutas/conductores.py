import os
import random
import re
from typing import Optional

import resend
from fastapi import APIRouter, BackgroundTasks, Body, HTTPException, status
from pydantic import BaseModel

from bd.bd_cliente import bd_cliente

# ==============================================================================
# 🔗 CONFIGURACIÓN DE BASE DE DATOS
# ==============================================================================
# Los conductores viven en `conductores`. `baseusuarios` solo se consulta para
# permitir el acceso de ADMIN al portal de conductores.
bd = bd_cliente["integra"]
coleccion_conductores = bd["conductores"]
coleccion_baseusuarios = bd["baseusuarios"]

# Índices únicos (correo y usuario se guardan en MAYÚSCULAS → unicidad case-insensitive).
try:
    coleccion_conductores.create_index("correo", unique=True)
    coleccion_conductores.create_index("usuario", unique=True)
except Exception:
    pass


# ==============================================================================
# 🚦 CONFIGURACIÓN DEL ROUTER
# ==============================================================================
ruta_conductores = APIRouter(
    prefix="/conductores",
    tags=["Conductores"],
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
class RegistrarConductorInput(BaseModel):
    nombre: str
    usuario: str
    correo: str
    clave: str
    celular: Optional[str] = None
    regional: Optional[str] = None
    perfil: Optional[str] = None  # se ignora (siempre CONDUCTOR)


class VerificarInput(BaseModel):
    usuario: str
    perfil: Optional[str] = None


class ValidarCodigoInput(BaseModel):
    usuario: str
    codigo: str
    perfil: Optional[str] = None


class CambioClaveInput(BaseModel):
    usuario: str
    nuevaClave: str
    codigo: str
    perfil: Optional[str] = None


# ==============================================================================
# 🛠️ HELPERS
# ==============================================================================
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
        resend.Emails.send({
            "from": MAIL_FROM,
            "to": [destinatario],
            "subject": f"Código de verificación: {codigo}",
            "html": html_simple,
        })
    except Exception as e:
        print(f"❌ Error crítico enviando correo: {e}")


def _existe_correo(correo: str) -> bool:
    if not correo:
        return False
    patron = {"$regex": f"^{re.escape(correo.strip())}$", "$options": "i"}
    return coleccion_conductores.find_one({"correo": patron}) is not None


def _existe_usuario(usuario_norm: str) -> bool:
    if not usuario_norm:
        return False
    return coleccion_conductores.find_one({"usuario": usuario_norm}) is not None


def _buscar_por_usuario(usuario_norm: str):
    """Conductor por campo `usuario` exacto (MAYÚSCULAS), para recuperación de clave."""
    if not usuario_norm:
        return None
    return coleccion_conductores.find_one({"usuario": usuario_norm})


# ==============================================================================
# 📝 REGISTRO
# ==============================================================================
@ruta_conductores.post("/registrar", response_model=dict)
async def registrar_conductor(data: RegistrarConductorInput):
    correo_norm = (data.correo or "").strip()
    usuario_norm = (data.usuario or "").strip().upper()

    if _existe_correo(correo_norm) or _existe_usuario(usuario_norm):
        raise HTTPException(status_code=400, detail="El usuario ya existe")

    nuevo = {
        "nombre": (data.nombre or "").upper(),
        "correo": correo_norm.upper() if correo_norm else None,
        "regional": (data.regional or "N/A").upper(),
        "celular": (data.celular or "").upper() if data.celular else None,
        "perfil": "CONDUCTOR",
        "usuario": usuario_norm,
        "clave": (data.clave or "").strip(),
        "clientes": [],
        "activo": True,
    }

    insertado = coleccion_conductores.insert_one(nuevo).inserted_id
    return {
        "mensaje": "Conductor registrado",
        "usuario": {"id": str(insertado), "usuario": usuario_norm, "perfil": "CONDUCTOR"},
    }


# ==============================================================================
# 🔓 LOGIN
# ==============================================================================
@ruta_conductores.post("/login", response_model=dict)
async def login_conductor(usuario: str = Body(..., embed=True), clave: str = Body(..., embed=True)):
    usuario_ingresado = usuario.strip()
    clave_ingresada = clave.strip()
    query_correo = {"correo": {"$regex": re.escape(usuario_ingresado), "$options": "i"}}

    # 1. Conductor en la colección conductores.
    encontrado = coleccion_conductores.find_one(query_correo)
    perfil = "CONDUCTOR"

    # 2. Si no es conductor, intentar ADMIN en baseusuarios (acceso de soporte al portal).
    if not encontrado:
        encontrado = coleccion_baseusuarios.find_one({**query_correo, "perfil": "ADMIN"})
        perfil = "ADMIN"

    if not encontrado:
        raise HTTPException(status_code=401, detail="Usuario o clave incorrectos")

    clave_almacenada = str(encontrado.get("clave", "")).strip()
    if not (clave_almacenada == clave_ingresada or clave_almacenada == clave_ingresada.upper()):
        raise HTTPException(status_code=401, detail="Usuario o clave incorrectos")

    nombre_completo = encontrado.get("nombre", "").strip()
    primer_nombre = nombre_completo.split(" ")[0]

    return {
        "mensaje": "Login Conductor exitoso",
        "usuario": {
            "id": str(encontrado["_id"]),
            "usuario": encontrado["usuario"],
            "perfil": perfil,
            "primerNombre": primer_nombre,
        },
    }


# ==============================================================================
# 🔐 RECUPERACIÓN DE CLAVE
# ==============================================================================
@ruta_conductores.post("/recuperar/verificar", response_model=dict)
async def recuperar_verificar(data: VerificarInput, background_tasks: BackgroundTasks):
    usuario_norm = data.usuario.strip().upper()

    doc = _buscar_por_usuario(usuario_norm)
    if not doc:
        return {"existe": False}

    codigo = str(random.randint(1000, 9999))
    coleccion_conductores.update_one({"_id": doc["_id"]}, {"$set": {"recovery_code": codigo}})

    correo_destino = doc.get("correo") or usuario_norm
    background_tasks.add_task(enviar_correo_codigo, correo_destino, codigo)

    return {"existe": True, "mensaje": "Código generado"}


@ruta_conductores.post("/recuperar/validar", response_model=dict)
async def recuperar_validar(data: ValidarCodigoInput):
    usuario_norm = data.usuario.strip().upper()

    doc = _buscar_por_usuario(usuario_norm)
    if not doc:
        return {"valido": False}

    codigo_guardado = doc.get("recovery_code")
    es_valido = (codigo_guardado is not None) and (codigo_guardado == data.codigo)
    return {"valido": es_valido}


@ruta_conductores.post("/recuperar/cambiar", response_model=dict)
async def recuperar_cambiar(data: CambioClaveInput):
    usuario_norm = data.usuario.strip().upper()

    doc = _buscar_por_usuario(usuario_norm)
    if not doc:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")

    codigo_guardado = doc.get("recovery_code")
    if not codigo_guardado or codigo_guardado != data.codigo:
        raise HTTPException(status_code=403, detail="Código inválido o expirado")

    coleccion_conductores.update_one(
        {"_id": doc["_id"]},
        {"$set": {"clave": data.nuevaClave.strip()}, "$unset": {"recovery_code": ""}},
    )
    return {"mensaje": "Clave actualizada correctamente"}
