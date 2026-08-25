from __future__ import annotations

import asyncio
import os
import threading
import uuid
from datetime import datetime, timezone

from bson import Decimal128
from fastapi import (APIRouter, BackgroundTasks, Depends, File, HTTPException,
                     Query, UploadFile, status)
from fastapi.responses import StreamingResponse

from bd.bd_cliente import bd_cliente
from rutas.baseusuarios import obtener_baseusuario_actual
from sicetac.batch_jobs import SicetacBatchJobs
from sicetac.config import Settings
from sicetac.errors import (ConfigurationError, RNDCBusinessError,
                            RNDCCredentialsError, RNDCNoDataError,
                            RNDCResponseParseError, RNDCSoapFaultError,
                            RNDCTransportError)
from sicetac.excel_service import crear_plantilla, procesar_excel, resumir_rutas as _resumir_rutas
from sicetac.execution import RNDC_EXECUTION_LOCK
from sicetac.models import ConsultaRequest, ExploracionRutaRequest
from sicetac.repository import SicetacRepository
from sicetac.rndc_client import RNDCClient
from sicetac.service import SicetacService, normalizar

router = APIRouter(prefix="/sicetac", tags=["SICE-TAC"])
_jobs: dict[str, dict] = {}
_lock = RNDC_EXECUTION_LOCK
XLSX_MEDIA_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
MAX_EXCEL_BYTES = 5 * 1024 * 1024
MAX_JOB_EXCEL_BYTES = 20 * 1024 * 1024
_batch_manager = None
_batch_manager_lock = threading.Lock()


def _shared_client(settings):
    existing_uri = os.getenv("MONGO_URI", "").strip()
    return bd_cliente if existing_uri and existing_uri == settings.mongodb_uri else None


def _get_batch_manager():
    global _batch_manager
    if _batch_manager is None:
        with _batch_manager_lock:
            if _batch_manager is None:
                settings = Settings.from_env()
                _batch_manager = SicetacBatchJobs(settings, shared_client=_shared_client(settings))
    return _batch_manager


def reanudar_jobs_excel():
    _get_batch_manager().recuperar()


def _admin(user=Depends(obtener_baseusuario_actual)):
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
        result = service.ejecutar(
            payload.periodo, payload.dry_run,
            lambda done, summary: _jobs[job_id].update(progreso=done, resumen=summary),
            payload.horas_totales_cargue, payload.horas_totales_descargue
        )
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


@router.post("/rutas-disponibles")
async def rutas_disponibles(
    payload: ExploracionRutaRequest,
    _=Depends(_admin),
):
    periodo, configuracion = payload.periodo, payload.configuracion
    origen, destino = payload.origen, payload.destino
    condicion_carga, limit = payload.condicion_carga, payload.limit
    client = None
    try:
        settings = Settings.from_env()
        client = RNDCClient(settings.soap_url, settings.username, settings.password)
        documentos = await asyncio.to_thread(
            client.explorar, periodo, configuracion, origen, destino, condicion_carga
        )
        rutas, total_unicas = _resumir_rutas(
            documentos, limit, payload.unidad_transporte_nombre,
            payload.tipo_carga_nombre, payload.horas_totales_cargue,
            payload.horas_totales_descargue
        )
        return {
            "periodo": periodo,
            "configuracion": configuracion,
            "origen_consultado": origen,
            "destino_consultado": destino,
            "condicion_carga_consultada": condicion_carga,
            "unidad_transporte_filtrada": payload.unidad_transporte_nombre,
            "tipo_carga_filtrado": payload.tipo_carga_nombre,
            "horas_totales_cargue": str(payload.horas_totales_cargue),
            "horas_totales_descargue": str(payload.horas_totales_descargue),
            "documentos_recibidos": len(documentos),
            "rutas_unicas": total_unicas,
            "rutas_mostradas": len(rutas),
            "rutas": rutas,
        }
    except RNDCNoDataError:
        return {
            "periodo": periodo, "configuracion": configuracion,
            "origen_consultado": origen, "destino_consultado": destino,
            "condicion_carga_consultada": condicion_carga, "documentos_recibidos": 0,
            "rutas_unicas": 0, "rutas_mostradas": 0, "rutas": [],
            "mensaje": "RNDC no publicó registros para estos filtros",
        }
    except RNDCBusinessError as exc:
        if "RNDC13" in str(exc).upper():
            return {
                "periodo": periodo, "configuracion": configuracion,
                "origen_consultado": origen, "destino_consultado": destino,
                "condicion_carga_consultada": condicion_carga, "documentos_recibidos": 0,
                "rutas_unicas": 0, "rutas_mostradas": 0, "rutas": [],
                "mensaje": "RNDC no reconoció esta combinación; pruebe el mes anterior, otra configuración o revise los códigos DIVIPOLA",
            }
        raise HTTPException(502, detail=f"RNDC rechazó la consulta: {exc}") from exc
    except RNDCCredentialsError as exc:
        raise HTTPException(502, detail="RNDC rechazó las credenciales configuradas") from exc
    except (RNDCTransportError, RNDCSoapFaultError, RNDCResponseParseError) as exc:
        raise HTTPException(502, detail=f"No fue posible consultar RNDC: {exc}") from exc
    except ConfigurationError as exc:
        raise HTTPException(503, detail=str(exc)) from exc
    finally:
        if client:
            client.close()


@router.get("/plantilla-excel")
async def plantilla_excel(_=Depends(_admin)):
    return StreamingResponse(
        crear_plantilla(), media_type=XLSX_MEDIA_TYPE,
        headers={"Content-Disposition": 'attachment; filename="plantilla_sicetac.xlsx"'},
    )


@router.post("/consultas-excel")
async def consultas_excel(archivo: UploadFile = File(...), _=Depends(_admin)):
    filename = (archivo.filename or "").lower()
    if not filename.endswith(".xlsx"):
        raise HTTPException(422, detail="El archivo debe tener extensión .xlsx")
    content = await archivo.read(MAX_EXCEL_BYTES + 1)
    if len(content) > MAX_EXCEL_BYTES:
        raise HTTPException(413, detail="El archivo supera el máximo de 5 MB")
    if not _lock.acquire(blocking=False):
        raise HTTPException(409, detail="Ya existe una ejecución SICE-TAC activa")
    client = None
    try:
        settings = Settings.from_env()
        client = RNDCClient(settings.soap_url, settings.username, settings.password)
        output = await asyncio.to_thread(procesar_excel, content, client, _resumir_rutas)
        return StreamingResponse(
            output, media_type=XLSX_MEDIA_TYPE,
            headers={"Content-Disposition": 'attachment; filename="resultados_sicetac.xlsx"'},
        )
    except ValueError as exc:
        raise HTTPException(422, detail=str(exc)) from exc
    except ConfigurationError as exc:
        raise HTTPException(503, detail=str(exc)) from exc
    finally:
        if client:
            client.close()
        _lock.release()


@router.post("/consultas-excel/jobs", status_code=status.HTTP_202_ACCEPTED)
async def crear_job_excel(archivo: UploadFile = File(...), user=Depends(_admin)):
    filename = archivo.filename or ""
    if not filename.lower().endswith(".xlsx"):
        raise HTTPException(422, detail="El archivo debe tener extensión .xlsx")
    content = await archivo.read(MAX_JOB_EXCEL_BYTES + 1)
    if len(content) > MAX_JOB_EXCEL_BYTES:
        raise HTTPException(413, detail="El archivo supera el máximo de 20 MB")
    creado_por = str(user.get("usuario") or user.get("username") or user.get("email") or "")
    try:
        manager = _get_batch_manager()
        return await asyncio.to_thread(manager.crear, content, filename, creado_por)
    except ValueError as exc:
        raise HTTPException(422, detail=str(exc)) from exc
    except ConfigurationError as exc:
        raise HTTPException(503, detail=str(exc)) from exc


@router.get("/consultas-excel/jobs")
async def listar_jobs_excel(
    limit: int = Query(10, ge=1, le=50),
    _=Depends(_admin),
):
    return await asyncio.to_thread(_get_batch_manager().listar, limit)


@router.get("/consultas-excel/jobs/{ejecucion_id}")
async def estado_job_excel(ejecucion_id: str, _=Depends(_admin)):
    job = await asyncio.to_thread(_get_batch_manager().obtener, ejecucion_id)
    if not job:
        raise HTTPException(404, detail="Ejecución Excel no encontrada")
    return job


@router.get("/consultas-excel/jobs/{ejecucion_id}/resultado")
async def resultado_job_excel(ejecucion_id: str, _=Depends(_admin)):
    manager = _get_batch_manager()
    job = await asyncio.to_thread(manager.obtener, ejecucion_id)
    if not job:
        raise HTTPException(404, detail="Ejecución Excel no encontrada")
    if job["estado"] != "completada":
        raise HTTPException(409, detail=f"La ejecución está en estado {job['estado']}")
    output = await asyncio.to_thread(manager.generar_excel, ejecucion_id)
    return StreamingResponse(
        output, media_type=XLSX_MEDIA_TYPE,
        headers={"Content-Disposition": f'attachment; filename="resultados_sicetac_{ejecucion_id}.xlsx"'},
    )
