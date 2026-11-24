"""Autenticación del Módulo Financiero.

# puerto de app/routers/auth.py del módulo externo, reducido al modo portal:
- POST /fin/auth/sesion — canjea el JWT de IntegrApp (20 min) por un token de
  sesión del módulo (12 h). Lo llama el front una vez al entrar.
- GET  /fin/auth/me     — identidad completa: usuario del módulo + perfil de
  IntegrApp (informativo).

No hay login propio ni cambio de clave: la contraseña es la de IntegrApp.
"""
import logging

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import object_session

from financiero.dependencies import crear_token_sesion, get_usuario_fin
from financiero.models.usuario import Usuario
from financiero.services.audit_service import AuditService
from financiero.services.permisos import es_admin

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/fin/auth", tags=["Financiero · Autenticación"])


class RespuestaSesion(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expira_horas: int
    usuario: dict


def _usuario_dict(usuario: Usuario) -> dict:
    return {
        "id": str(usuario.id),
        "username": usuario.username,
        "nombre": usuario.nombre,
        "correo": usuario.correo,
        "rol": usuario.rol.value,
        "area": usuario.area,
        "activo": usuario.activo,
        "es_aprobador_pagos": usuario.es_aprobador_pagos,
        "es_admin_adicional": usuario.es_admin_adicional,
        "es_autorizador_viajes": usuario.es_autorizador_viajes,
    }


@router.post("/sesion", response_model=RespuestaSesion)
def crear_sesion(usuario: Usuario = Depends(get_usuario_fin)):
    """Canjea el token de IntegrApp (o uno de sesión vigente) por uno de 12 h.

    Acepta también un token de sesión aún válido: el front puede llamarlo al
    arrancar sin saber si su token baseusuarios ya expiró.
    """
    from financiero.constantes import SESION_HORAS
    token = crear_token_sesion(usuario)
    try:
        AuditService.registrar(
            db=object_session(usuario),
            modulo="auth",
            accion="SESION",
            entidad_id=str(usuario.id),
            usuario=usuario,
            detalles={"expira_horas": SESION_HORAS},
        )
    except Exception as exc:
        logger.warning("[FIN] Auditoría de sesión falló (no bloquea): %s", exc)
    return RespuestaSesion(
        access_token=token,
        expira_horas=SESION_HORAS,
        usuario={**_usuario_dict(usuario), "perfil_integrapp": getattr(usuario, "perfil_integrapp", None)},
    )


@router.get("/me")
def perfil_actual(usuario: Usuario = Depends(get_usuario_fin)):
    """Identidad del módulo + perfil de IntegrApp (informativo, no autoriza)."""
    return {
        **_usuario_dict(usuario),
        "perfil_integrapp": getattr(usuario, "perfil_integrapp", None),
        "es_admin": es_admin(usuario),
    }
