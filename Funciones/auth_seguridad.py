"""Autenticación/autorización del módulo de Seguridad (estudios multi-tenant).

Reutiliza la identidad de `baseusuarios` (bcrypt dual-mode de Funciones/claves.py
y el secreto JWT de rutas/baseusuarios.py — única fuente de verdad) y añade:

  - Token propio del módulo (HS256, mismo secreto, claims extendidos con
    empresa_id y rol_seguridad; expiración SEGURIDAD_TOKEN_MINUTES, default 8h).
  - `actor_actual`: dependencia FastAPI que decodifica el token, RECARGA el
    usuario desde baseusuarios (fuente de verdad: desactivar al usuario o
    quitarle la empresa revoca el token al instante) y carga su empresa.
  - Roles derivados (no se guardan aparte para ADMIN_INTEGRA):
      ADMIN_INTEGRA   = perfil baseusuarios ADMIN      → todas las empresas
      ADMIN_EMPRESA   = SEGURIDAD + rol ADMIN_EMPRESA  → toda su empresa
      CONSULTADOR     = SEGURIDAD (rol ausente o CONSULTADOR) → su empresa
                        (o solo sus propios estudios si la empresa activa
                         config.aislamiento_usuario)

Nota consciente: al compartir secreto y auth_source con baseusuarios, un token
emitido aquí para un perfil ADMIN también pasa `obtener_baseusuario_actual`
(sicetac) — es el mismo usuario con la misma identidad, no elevación.

SIEMPRE fijar JWT_SECRET fuerte en el entorno: si sigue en el default
inseguro, se loguea CRITICAL en cada arranque y los códigos de verificación
de los estudios quedan predecibles.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

import jwt
from bson import ObjectId
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer

from bd.bd_cliente import bd_cliente
from rutas.baseusuarios import (
    BASEUSUARIOS_JWT_ALGORITHM,
    BASEUSUARIOS_JWT_SECRET,
    _buscar_baseusuario_activo,
)

logger = logging.getLogger(__name__)

SEGURIDAD_TOKEN_MINUTES = int(os.getenv("SEGURIDAD_TOKEN_MINUTES", "480"))

db = bd_cliente["integra"]
col_usuarios = db["baseusuarios"]
col_empresas = db["empresas_seguridad"]

# El login del módulo también acepta el flujo OAuth2 de Swagger.
oauth2_seguridad = OAuth2PasswordBearer(
    tokenUrl="/seguridad/estudios/login",
    scheme_name="SeguridadEstudiosOAuth2",
)

if BASEUSUARIOS_JWT_SECRET.startswith("cambia_esta_clave"):
    logger.critical(
        "JWT_SECRET sigue en el default inseguro: fijar un valor fuerte en el "
        "entorno (Render) antes de exponer el módulo de estudios de seguridad."
    )

ROL_ADMIN_INTEGRA = "ADMIN_INTEGRA"
ROL_ADMIN_EMPRESA = "ADMIN_EMPRESA"
ROL_CONSULTADOR = "CONSULTADOR"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def crear_token_estudios(usuario: dict, empresa_id: str | None, rol: str) -> str:
    """JWT del módulo con claims extendidos (empresa_id/rol solo como hint:
    la autorización SIEMPRE re-lee la BD en actor_actual)."""
    ahora = datetime.now(timezone.utc)
    payload = {
        "sub": str(usuario["_id"]),
        "usuario": usuario.get("usuario", ""),
        "perfil": str(usuario.get("perfil", "")).upper(),
        "auth_source": "baseusuarios",
        "modulo": "seguridad_estudios",
        "empresa_id": empresa_id,
        "rol_seguridad": rol,
        "iat": ahora,
        "exp": ahora + timedelta(minutes=SEGURIDAD_TOKEN_MINUTES),
    }
    return jwt.encode(payload, BASEUSUARIOS_JWT_SECRET, algorithm=BASEUSUARIOS_JWT_ALGORITHM)


def _derivar_rol(usuario_doc: dict) -> tuple[str, ObjectId | None]:
    """(rol del módulo, empresa_id) según perfil baseusuarios y rol_seguridad."""
    perfil = str(usuario_doc.get("perfil") or "").strip().upper()
    if perfil == "ADMIN":
        return ROL_ADMIN_INTEGRA, usuario_doc.get("empresa_id")
    if perfil == "SEGURIDAD":
        rol = str(usuario_doc.get("rol_seguridad") or "").strip().upper()
        if rol == ROL_ADMIN_EMPRESA:
            return ROL_ADMIN_EMPRESA, usuario_doc.get("empresa_id")
        return ROL_CONSULTADOR, usuario_doc.get("empresa_id")
    return "", usuario_doc.get("empresa_id")


def autenticar(correo: str, clave: str) -> tuple[dict, str, ObjectId | None, dict | None]:
    """Login del módulo: (usuario_doc, rol, empresa_id, empresa_doc o None).

    Errores: 401 credenciales inválidas; 403 inactivo / perfil sin permiso /
    usuario de empresa inexistente o inactiva.
    """
    encontrado = _buscar_baseusuario_activo(correo, clave)
    if not encontrado:
        raise HTTPException(status_code=401, detail="Correo o clave incorrectos")

    rol, empresa_id = _derivar_rol(encontrado)
    if not rol:
        raise HTTPException(
            status_code=403,
            detail=f"Su perfil ({encontrado.get('perfil')}) no tiene acceso al módulo de seguridad",
        )

    empresa_doc = None
    if rol != ROL_ADMIN_INTEGRA:
        if not empresa_id:
            raise HTTPException(
                status_code=403,
                detail="Su usuario no tiene empresa asignada. Contacte al administrador.",
            )
        empresa_doc = col_empresas.find_one({"_id": ObjectId(empresa_id)})
        if not empresa_doc or not empresa_doc.get("activo", True):
            raise HTTPException(
                status_code=403,
                detail="La empresa asignada no existe o está inactiva",
            )
    return encontrado, rol, empresa_id, empresa_doc


def _cargar_actor(usuario_id: str) -> dict:
    """Resuelve el actor desde BD: usuario activo + rol + empresa activa."""
    error = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="No se pudo validar las credenciales",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        usuario_doc = col_usuarios.find_one({"_id": ObjectId(usuario_id)})
    except Exception as exc:
        raise error from exc
    if not usuario_doc or not usuario_doc.get("activo", True):
        raise error

    rol, empresa_id = _derivar_rol(usuario_doc)
    if not rol:
        raise HTTPException(
            status_code=403,
            detail=f"Su perfil ({usuario_doc.get('perfil')}) no tiene acceso al módulo de seguridad",
        )

    empresa_doc = None
    if rol != ROL_ADMIN_INTEGRA:
        if not empresa_id:
            raise HTTPException(
                status_code=403,
                detail="Su usuario no tiene empresa asignada. Contacte al administrador.",
            )
        empresa_doc = col_empresas.find_one({"_id": ObjectId(empresa_id)})
        if not empresa_doc or not empresa_doc.get("activo", True):
            raise HTTPException(
                status_code=403,
                detail="La empresa asignada no existe o está inactiva",
            )

    return {
        "usuario_id": str(usuario_doc["_id"]),
        "usuario": usuario_doc.get("usuario", ""),
        "usuario_nombre": usuario_doc.get("nombre", ""),
        "usuario_correo": usuario_doc.get("correo", ""),
        "perfil": str(usuario_doc.get("perfil") or "").upper(),
        "rol": rol,
        "empresa_id": str(empresa_id) if empresa_id else None,
        "empresa_nombre": (empresa_doc or {}).get("nombre", ""),
        "empresa_config": (empresa_doc or {}).get("config", {}) or {},
    }


async def actor_actual(token: str = Depends(oauth2_seguridad)) -> dict:
    """Dependencia FastAPI: valida el token y devuelve el actor (recargado de BD)."""
    error = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="No se pudo validar las credenciales",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, BASEUSUARIOS_JWT_SECRET, algorithms=[BASEUSUARIOS_JWT_ALGORITHM])
        if payload.get("auth_source") != "baseusuarios":
            raise error
        usuario_id = payload.get("sub")
        if not usuario_id:
            raise error
    except HTTPException:
        raise
    except Exception as exc:
        raise error from exc
    return _cargar_actor(usuario_id)
