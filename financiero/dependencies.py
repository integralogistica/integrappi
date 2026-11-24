"""Autenticación y autorización del Módulo Financiero.

Adaptación de app/dependencies.py + app/services/auth_service.py del módulo
externo. Al vivir DENTRO de integrappi desaparece el autollamado HTTP que
hacía el externo para resolver sub→correo: aquí se lee Mongo `baseusuarios`
directamente (bd_cliente).

Dos tokens aceptados por get_usuario_fin:
1. El JWT del login de IntegrApp (auth_source="baseusuarios", sin claim
   `modulo` — filtro anti-suplantación del original): se resuelve el sub
   (ObjectId) a correo consultando baseusuarios.
2. El token de sesión del módulo (auth_source="fin_modulo", sub=correo,
   12 h) que emite POST /fin/auth/sesion — evita el re-login cada 20 min.

En ambos casos el ROL lo define fin.usuarios (por correo), y se revalida
`activo` en cada request: desactivar a alguien corta su acceso al instante.
"""
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import jwt
from bson import ObjectId
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import func
from sqlalchemy.orm import Session

from financiero.constantes import AUTH_SOURCE_MODULO, AUTH_SOURCE_PORTAL, SESION_HORAS
from financiero.database import get_db_fin
from financiero.models.usuario import RolUsuario, Usuario

logger = logging.getLogger("financiero.auth")

FIN_JWT_SECRET = os.getenv("JWT_SECRET", "")
FIN_JWT_ALGORITHM = "HS256"

_bearer = HTTPBearer(auto_error=False)

_error_401 = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Token no válido o expirado. Vuelve a iniciar sesión en IntegrApp.",
    headers={"WWW-Authenticate": "Bearer"},
)


# ── Resolución de identidad ─────────────────────────────────────────────

def _correo_desde_baseusuarios(usuario_id: str) -> Optional[dict]:
    """sub (ObjectId) → {correo, activo, perfil} leyendo Mongo directamente."""
    try:
        from bd.bd_cliente import bd_cliente
        col = bd_cliente["integra"]["baseusuarios"]
        doc = col.find_one({"_id": ObjectId(usuario_id)})
    except Exception as exc:  # Mongo caído / sub inválido
        logger.warning("[FIN] No se pudo resolver el usuario de IntegrApp: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="No se pudo validar tu usuario con IntegrApp. Intenta de nuevo en un momento.",
        ) from exc
    if not doc:
        return None
    return {
        "correo": (doc.get("correo") or "").strip().lower(),
        "activo": bool(doc.get("activo", True)),
        "perfil": doc.get("perfil", ""),
        "nombre": doc.get("nombre", ""),
    }


def decodificar_token(token: str) -> dict:
    """Decodifica y valida el JWT (HS256, mismo JWT_SECRET del login)."""
    if not FIN_JWT_SECRET:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="El Módulo Financiero no está configurado (falta JWT_SECRET).",
        )
    try:
        return jwt.decode(token, FIN_JWT_SECRET, algorithms=[FIN_JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Tu sesión de IntegrApp expiró. Vuelve a iniciar sesión.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except Exception as exc:
        raise _error_401 from exc


def crear_token_sesion(usuario: Usuario) -> str:
    """Token de sesión del módulo (12 h): evita el re-login cada 20 min."""
    ahora = datetime.now(timezone.utc)
    payload = {
        "sub": (usuario.correo or "").strip().lower(),
        "usuario": usuario.username,
        "rol": usuario.rol.value,
        "auth_source": AUTH_SOURCE_MODULO,
        "iat": ahora,
        "exp": ahora + timedelta(hours=SESION_HORAS),
    }
    return jwt.encode(payload, FIN_JWT_SECRET, algorithm=FIN_JWT_ALGORITHM)


# ── Dependencia principal ───────────────────────────────────────────────

def get_usuario_fin(
    credenciales: Optional[HTTPAuthorizationCredentials] = Depends(_bearer),
    db: Session = Depends(get_db_fin),
) -> Usuario:
    if credenciales is None:
        raise _error_401
    payload = decodificar_token(credenciales.credentials)

    auth_source = payload.get("auth_source")
    perfil_integrapp: Optional[str] = None

    if auth_source == AUTH_SOURCE_PORTAL:
        # Filtro anti-suplantación del original: tokens del login normal de
        # IntegrApp (otros tokens firmados con el mismo secreto, p. ej. el de
        # registro público, traen claim `modulo` y se rechazan).
        if "modulo" in payload:
            raise _error_401
        info = _correo_desde_baseusuarios(payload.get("sub", ""))
        if info is None:
            raise _error_401
        if not info["activo"]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Tu usuario de IntegrApp está inactivo.",
            )
        correo = info["correo"]
        perfil_integrapp = info["perfil"]
        if not correo:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Tu usuario de IntegrApp no tiene correo; el Módulo Financiero identifica por correo.",
            )
    elif auth_source == AUTH_SOURCE_MODULO:
        correo = str(payload.get("sub", "")).strip().lower()
        if not correo:
            raise _error_401
    else:
        raise _error_401

    usuario = db.query(Usuario).filter(func.lower(Usuario.correo) == correo).first()
    if usuario is None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="No tienes acceso al Módulo Financiero. Pide al administrador que te dé de alta con tu correo de IntegrApp.",
        )
    if not usuario.activo:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Tu acceso al Módulo Financiero está desactivado.",
        )
    # Perfil IntegrApp como atributo dinámico (informativo, no autoriza)
    try:
        usuario.perfil_integrapp = perfil_integrapp
    except Exception:
        pass
    return usuario


# ── Fábricas de dependencias por rol ────────────────────────────────────

def require_not_consulta(usuario: Usuario = Depends(get_usuario_fin)) -> Usuario:
    if usuario.rol == RolUsuario.CONSULTA and not (usuario.rol == RolUsuario.ADMIN or usuario.es_admin_adicional):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Tu rol es de solo consulta.",
        )
    return usuario


def require_admin(usuario: Usuario = Depends(get_usuario_fin)) -> Usuario:
    if not (usuario.rol == RolUsuario.ADMIN or usuario.es_admin_adicional):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Solo el administrador del módulo puede hacer esto.",
        )
    return usuario


def require_roles(*roles: RolUsuario):
    """El admin (o admin adicional) pasa siempre, como en el original."""
    def _verificador(usuario: Usuario = Depends(get_usuario_fin)) -> Usuario:
        if usuario.rol == RolUsuario.ADMIN or usuario.es_admin_adicional:
            return usuario
        if usuario.rol not in roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="No tienes permisos para esta acción.",
            )
        return usuario
    return _verificador


def require_seccion(seccion: str):
    """Permisos finos por usuario: la sección debe estar habilitada para el
    rol (por defecto) o expresamente para la persona (fin.usuarios.permisos).
    El admin siempre pasa."""
    def _verificador(usuario: Usuario = Depends(get_usuario_fin)) -> Usuario:
        from financiero.services.permisos import puede_ver_seccion
        if not puede_ver_seccion(usuario, seccion):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"No tienes acceso a la sección «{seccion}» del Módulo Financiero. "
                       "Pide al administrador que te la habilite.",
            )
        return usuario
    return _verificador
