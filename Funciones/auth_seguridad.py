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

import hashlib
import logging
import os
import secrets
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
col_api_keys = db["api_keys_seguridad"]

# El flujo OAuth2 de Swagger usa el endpoint /token (formulario); el login
# JSON (/login) es el que consume el frontend.
oauth2_seguridad = OAuth2PasswordBearer(
    tokenUrl="/seguridad/estudios/token",
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


# ── API keys (integraciones de clientes, 2026-08-30) ──────────────────────────
# Canal de máquina a máquina para el servicio por API: el cliente manda
# `Authorization: Bearer sek_…` (mismo header que el JWT del portal). La clave
# se muestra UNA vez al crearla; en BD solo vive su SHA-256 (no reversible).
# El actor derivado es CONSULTADOR de la empresa (jamás admin) con
# canal="api": estudios, movimientos y eventos quedan marcados con el origen.

PREFIJO_API_KEY = "sek_"
# Throttle del update de ultimo_uso_en (evita 1 write por request).
_SEGUNDOS_THROTTLE_USO = 60


def generar_api_key(nombre: str, empresa_id, creado_por: str) -> tuple[str, dict]:
    """(clave_plana para mostrar UNA vez, doc listo para insert).

    Formato: sek_ + 40 hex (160 bits de entropía). Se persiste el hash
    SHA-256 y un prefijo visible (`sek_ab12cd…`) para identificarla en el
    panel sin exponer la clave.
    """
    clave = PREFIJO_API_KEY + secrets.token_hex(20)
    doc = {
        "empresa_id": empresa_id,
        "nombre": nombre.strip(),
        "prefijo": clave[:11] + "…",
        "hash_sha256": hashlib.sha256(clave.encode("utf-8")).hexdigest(),
        "activo": True,
        "scopes": ["estudios:crear", "estudios:leer"],
        "creado_por": creado_por,
        "creado_en": _utcnow(),
        "ultimo_uso_en": None,
        "revocada_en": None,
    }
    return clave, doc


def _actor_de_api_key(clave: str) -> dict:
    """Resuelve una API key en actor CONSULTADOR de su empresa.

    Anti-enumeración: clave inválida, revocada o empresa inactiva devuelven el
    MISMO 401 genérico del JWT (no revela cuál falló).
    """
    error = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="No se pudo validar las credenciales",
        headers={"WWW-Authenticate": "Bearer"},
    )
    doc = col_api_keys.find_one({
        "hash_sha256": hashlib.sha256(clave.encode("utf-8")).hexdigest(),
        "activo": True,
    })
    if not doc:
        raise error
    empresa = col_empresas.find_one({"_id": doc["empresa_id"]})
    if not empresa or not empresa.get("activo", True):
        raise error

    # ultimo_uso_en con throttle: máx 1 write/min por key (best-effort).
    ultimo = doc.get("ultimo_uso_en")
    if not ultimo or (_utcnow() - ultimo).total_seconds() >= _SEGUNDOS_THROTTLE_USO:
        try:
            col_api_keys.update_one({"_id": doc["_id"]}, {"$set": {"ultimo_uso_en": _utcnow()}})
        except Exception as exc:
            logger.warning("ultimo_uso_en de la API key %s no se actualizó: %s", doc.get("prefijo"), exc)

    return {
        "usuario_id": None,  # no hay usuario humano detrás
        "usuario": f"API:{doc.get('nombre', '')}",
        "usuario_nombre": f"API {doc.get('nombre', '')}",
        "usuario_correo": "",
        "perfil": "CLIENTE_ESTUDIOS",
        "rol": ROL_CONSULTADOR,  # una API key NUNCA es admin
        "empresa_id": str(doc["empresa_id"]),
        "empresa_nombre": empresa.get("nombre", ""),
        "empresa_config": empresa.get("config", {}) or {},
        "canal": "api",
        "api_key_id": str(doc["_id"]),
        "api_key_nombre": doc.get("nombre", ""),
    }


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
    """(rol del módulo, empresa_id) según perfil baseusuarios y rol_seguridad.

    CLIENTE_ESTUDIOS = cliente externo del portal de Estudios de Seguridad.
    SEGURIDAD = personal interno de Integra (histórico; la vía actual para
    clientes es CLIENTE_ESTUDIOS).
    """
    perfil = str(usuario_doc.get("perfil") or "").strip().upper()
    if perfil == "ADMIN":
        return ROL_ADMIN_INTEGRA, usuario_doc.get("empresa_id")
    if perfil in ("SEGURIDAD", "CLIENTE_ESTUDIOS"):
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
        "canal": "portal",  # sesión humana (portal/Swagger); "api" = API key
        "api_key_id": None,
        "api_key_nombre": None,
    }


async def actor_actual(token: str = Depends(oauth2_seguridad)) -> dict:
    """Dependencia FastAPI: valida el token y devuelve el actor (recargado de BD).

    Acepta DOS credenciales en el mismo header `Authorization: Bearer …`:
      - JWT del login del portal (recarga el usuario de baseusuarios), o
      - API key de integración (`sek_…`) → actor CONSULTADOR con canal="api".
    """
    error = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="No se pudo validar las credenciales",
        headers={"WWW-Authenticate": "Bearer"},
    )
    if token.startswith(PREFIJO_API_KEY):
        return _actor_de_api_key(token)
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
