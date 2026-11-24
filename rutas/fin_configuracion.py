"""Configuración editable del Módulo Financiero (administrador).

# puerto LITERAL de app/routers/configuracion.py del módulo externo
(solo cambian prefix y dependencias).
"""
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel
from sqlalchemy.orm import Session

from financiero.database import get_db_fin
from financiero.dependencies import get_usuario_fin, require_admin
from financiero.models.configuracion import Configuracion
from financiero.models.usuario import Usuario
from financiero.services import configuracion as cfg
from financiero.services.audit_service import AuditService

from rutas.fin_traslados import get_client_ip

router = APIRouter(prefix="/fin/configuracion", tags=["Financiero · Configuración"])


class ValorConfig(BaseModel):
    valor: Any
    version: Optional[int] = None


def _validar(clave: str, valor: Any):
    if clave not in cfg.CLAVES:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Parámetro desconocido: {clave}. Permitidos: {', '.join(cfg.CLAVES)}")
    tipo = cfg.CLAVES[clave]["tipo"]
    if isinstance(valor, bool) or not isinstance(valor, tipo):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, f"'{clave}' tiene un tipo inválido.")
    if clave == "limite_controller" and valor <= 0:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "El límite debe ser mayor a cero.")
    if clave in ("areas", "correos_cc") and not all(isinstance(x, str) and x.strip() for x in valor):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, f"'{clave}' debe ser una lista de textos no vacíos.")
    if clave == "areas" and len(set(x.strip().lower() for x in valor)) != len(valor):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "Hay áreas repetidas.")


@router.get("")
def leer_todo(db: Session = Depends(get_db_fin), _: Usuario = Depends(get_usuario_fin)):
    """Parámetros vigentes (los que el administrador no ha cambiado traen el valor por defecto)."""
    return {k: cfg.obtener(db, k) for k in cfg.CLAVES}


@router.get("/_estado/todo")
def estado(db: Session = Depends(get_db_fin), _: Usuario = Depends(get_usuario_fin)):
    """Cada parámetro con su versión (0 = nunca lo ha cambiado el administrador, vale el valor por defecto)."""
    out = {}
    for k in cfg.CLAVES:
        fila = db.get(Configuracion, k)
        out[k] = {"valor": fila.valor.get("v") if fila else cfg.CLAVES[k]["defecto"], "version": fila.version if fila else 0}
    return out


@router.get("/{clave}")
def leer(clave: str, db: Session = Depends(get_db_fin), _: Usuario = Depends(get_usuario_fin)):
    if clave not in cfg.CLAVES:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Parámetro desconocido.")
    fila = db.get(Configuracion, clave)
    return {"clave": clave, "valor": cfg.obtener(db, clave), "version": fila.version if fila else 0}


@router.put("/{clave}")
def guardar(clave: str, req: ValorConfig, request: Request, db: Session = Depends(get_db_fin), admin: Usuario = Depends(require_admin)):
    _validar(clave, req.valor)
    fila = db.get(Configuracion, clave)
    anterior = fila.valor.get("v") if fila else cfg.CLAVES[clave]["defecto"]
    if fila:
        if req.version is not None and req.version != fila.version:
            raise HTTPException(status.HTTP_409_CONFLICT, f"Otro administrador cambió este parámetro (versión {fila.version}).")
        fila.valor, fila.version = {"v": req.valor}, fila.version + 1
        fila.updated_at, fila.updated_by = datetime.now(timezone.utc), admin.username
    else:
        fila = Configuracion(clave=clave, valor={"v": req.valor}, version=1, updated_by=admin.username)
        db.add(fila)
    db.commit()
    AuditService.registrar(db=db, modulo="configuracion", accion="CAMBIAR", entidad_id=clave, usuario=admin,
                           ip_cliente=get_client_ip(request), detalles={"antes": anterior, "despues": req.valor})
    return {"clave": clave, "valor": req.valor, "version": fila.version}
