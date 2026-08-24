from __future__ import annotations

import asyncio
import os
import threading
import uuid
from datetime import datetime, timezone

from bson import Decimal128
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, status

from bd.bd_cliente import bd_cliente
from rutas.aut2 import obtener_usuario_actual
from sicetac.config import Settings
from sicetac.errors import ConfigurationError
from sicetac.models import ConsultaRequest
from sicetac.repository import SicetacRepository
from sicetac.rndc_client import RNDCClient
from sicetac.service import SicetacService

router = APIRouter(prefix="/sicetac", tags=["SICE-TAC"])
_jobs: dict[str, dict] = {}
_lock = threading.Lock()


def _shared_client(settings):
    existing_uri = os.getenv("MONGO_URI", "").strip()
    return bd_cliente if existing_uri and existing_uri == settings.mongodb_uri else None


def _admin(user=Depends(obtener_usuario_actual)):
    role = str(user.get("rol") or user.get("perfil") or "").upper()
    if role not in {"ADMIN", "ADMINISTRADOR"}:
        raise HTTPException(status_code=403, detail="Se requiere permiso administrativo")
    return user


def _json(value):
    if isinstance(value, Decimal128): return str(value.to_decimal())
    if isinstance(value, datetime): return value.isoformat()
    if isinstance(value, dict): return {k: _json(v) for k, v in value.items()}
    if isinstance(value, list): return [_json(v) for v in value]
    return value


def _run(job_id, payload):
    client = None
    try:
        _jobs[job_id].update(estado="ejecutando", iniciado_en=datetime.now(timezone.utc).isoformat())
        settings = Settings.from_env()
        repository = SicetacRepository(settings.mongodb_uri, settings.mongodb_database, settings.mongodb_collection, shared_client=_shared_client(settings))
        client = RNDCClient(settings.soap_url, settings.username, settings.password)
        service = SicetacService(client, repository)
        result = service.ejecutar(payload.periodo, payload.dry_run, lambda done, summary: _jobs[job_id].update(progreso=done, resumen=summary))
        _jobs[job_id].update(estado="completada", resumen=result)
    except Exception as exc:
        _jobs[job_id].update(estado="fallida", error={"tipo": type(exc).__name__, "mensaje": str(exc)})
    finally:
        if client: client.close()
        _jobs[job_id]["finalizado_en"] = datetime.now(timezone.utc).isoformat()
        _lock.release()


@router.post("/consultas", status_code=status.HTTP_202_ACCEPTED)
async def crear_consulta(payload: ConsultaRequest, background: BackgroundTasks, _=Depends(_admin)):
    try: Settings.from_env()
    except ConfigurationError as exc: raise HTTPException(status_code=503, detail=str(exc)) from exc
    if not _lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="Ya existe una ejecución SICE-TAC activa")
    job_id = uuid.uuid4().hex
    _jobs[job_id] = {"ejecucion_id": job_id, "estado": "pendiente", "progreso": 0, "dry_run": payload.dry_run, "creado_en": datetime.now(timezone.utc).isoformat()}
    background.add_task(_run, job_id, payload)
    return _jobs[job_id]


@router.get("/consultas/{ejecucion_id}")
async def estado_consulta(ejecucion_id: str, _=Depends(_admin)):
    if ejecucion_id not in _jobs: raise HTTPException(status_code=404, detail="Ejecución no encontrada")
    return _jobs[ejecucion_id]


@router.get("/resultados")
async def resultados(periodo: str | None = None, limit: int = Query(100, ge=1, le=1000), _=Depends(_admin)):
    settings = Settings.from_env()
    repository = SicetacRepository(settings.mongodb_uri, settings.mongodb_database, settings.mongodb_collection, shared_client=_shared_client(settings))
    return _json(await asyncio.to_thread(repository.listar, periodo, limit))
