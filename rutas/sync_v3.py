"""
Endpoints para controlar la sincronización programada V3.
"""
from fastapi import APIRouter, HTTPException
from typing import List
from Funciones.sync_api_v3 import ejecutar_sync_v3, archivar_mes_v3
from bd.bd_cliente import bd_cliente
import logging

logger = logging.getLogger(__name__)

_bd = bd_cliente['integra']
_cache = _bd['cache_cruce_mc']
_config_collection = _bd['config_v3']

router = APIRouter(prefix="/sync-v3", tags=["Sync V3"])


def _obtener_config_desde_db():
    """Obtiene la configuración desde MongoDB. Si no existe, crea la default."""
    config_doc = _config_collection.find_one({'tipo': 'sync_config'})
    if config_doc:
        return {
            'horarios': config_doc.get('horarios', ['08:00', '14:00']),
            'activo': config_doc.get('activo', True),
        }
    else:
        # Crear config default
        default_config = {
            'tipo': 'sync_config',
            'horarios': ['08:00', '14:00'],
            'activo': True,
        }
        _config_collection.insert_one(default_config)
        logger.info("Config V3 default creada en MongoDB")
        return default_config


def _guardar_config_en_db(horarios: List[str] = None, activo: bool = None):
    """Actualiza la configuración en MongoDB."""
    update_doc = {}
    if horarios is not None:
        update_doc['horarios'] = horarios
    if activo is not None:
        update_doc['activo'] = activo

    _config_collection.update_one(
        {'tipo': 'sync_config'},
        {'$set': update_doc},
        upsert=True
    )
    logger.info(f"Config V3 actualizada en MongoDB: {update_doc}")
    return _obtener_config_desde_db()


_ultimo_resultado: dict = {
    "ok": None,
    "timestamp": None,
    "exitosos": 0,
    "errores": [],
    "total": 0,
    "segundos": 0,
}


def actualizar_ultimo_resultado(resultado: dict):
    _ultimo_resultado.update(resultado)


# ── Endpoints ────────────────────────────────────────────────────────────────

@router.get("/config")
def obtener_config():
    """Devuelve la configuración actual del sync desde MongoDB."""
    config_db = _obtener_config_desde_db()
    return {
        **config_db,
        "fuente": "API Siscore V3",
        "endpoint": "https://integra-wms.appsiscore.com/app/ws/informe_v3.php"
    }


@router.post("/config")
def actualizar_config(horarios: List[str] = None, activo: bool = None):
    """
    Actualiza la configuración del sync y la guarda en MongoDB.
    - horarios: lista de horas en formato HH:MM, ej: ["05:00","10:30","19:00"]
    - activo: true/false para activar o pausar
    """
    if horarios is not None:
        for h in horarios:
            try:
                hh, mm = h.split(":")
                assert 0 <= int(hh) <= 23 and 0 <= int(mm) <= 59
            except Exception:
                raise HTTPException(status_code=400, detail=f"Hora inválida: '{h}'. Usa formato HH:MM")
        config_actualizada = _guardar_config_en_db(horarios=horarios)
        return {"mensaje": "Horarios actualizados", **config_actualizada}
    if activo is not None:
        config_actualizada = _guardar_config_en_db(activo=activo)
        return {"mensaje": "Estado actualizado", **config_actualizada}
    return obtener_config()


@router.post("/horarios")
def agregar_horario(horario: str):
    """
    Agrega un nuevo horario a la lista.
    Ejemplo: POST /sync-v3/horarios?horario=16:30
    """
    try:
        hh, mm = horario.split(":")
        assert 0 <= int(hh) <= 23 and 0 <= int(mm) <= 59
    except Exception:
        raise HTTPException(status_code=400, detail=f"Hora inválida: '{horario}'. Usa formato HH:MM")

    config_actual = _obtener_config_desde_db()
    if horario in config_actual['horarios']:
        raise HTTPException(status_code=400, detail=f"El horario {horario} ya existe")

    nuevos_horarios = config_actual['horarios'] + [horario]
    nuevos_horarios.sort()
    config_actualizada = _guardar_config_en_db(horarios=nuevos_horarios)
    return {"mensaje": f"Horario {horario} agregado", **config_actualizada}


@router.delete("/horarios/{horario}")
def eliminar_horario(horario: str):
    """
    Elimina un horario de la lista.
    Ejemplo: DELETE /sync-v3/horarios/08:00
    """
    config_actual = _obtener_config_desde_db()
    if horario not in config_actual['horarios']:
        raise HTTPException(status_code=404, detail=f"El horario {horario} no existe")

    nuevos_horarios = [h for h in config_actual['horarios'] if h != horario]
    if not nuevos_horarios:
        raise HTTPException(status_code=400, detail="No se pueden eliminar todos los horarios. Debe haber al menos uno.")

    config_actualizada = _guardar_config_en_db(horarios=nuevos_horarios)
    return {"mensaje": f"Horario {horario} eliminado", **config_actualizada}


@router.post("/ejecutar")
async def ejecutar_manual():
    """Dispara el sync manualmente ahora mismo."""
    resultado = await ejecutar_sync_v3()
    actualizar_ultimo_resultado(resultado)
    return resultado


@router.post("/archivar")
def archivar_manual():
    """
    Dispara el corte mensual manualmente.
    Guarda una copia de v3 + cruce en v3_historico para el mes actual.
    """
    return archivar_mes_v3()


@router.get("/historico")
def obtener_historico():
    """
    Lista los meses archivados (sin incluir los registros completos).
    Devuelve anio, mes, fecha_corte y total de registros de cada corte.
    """
    _historico = _bd['v3_historico']
    meses = list(_historico.find(
        {},
        {'_id': 0, 'registros': 0, 'cruce': 0}
    ).sort([('anio', -1), ('mes', -1)]))
    return {'meses': meses}


@router.get("/historico/{anio}/{mes}")
def obtener_historico_mes(anio: int, mes: int):
    """
    Devuelve el cruce archivado de un mes específico.
    """
    _historico = _bd['v3_historico']
    doc = _historico.find_one({'anio': anio, 'mes': mes}, {'_id': 0, 'registros': 0})
    if not doc:
        raise HTTPException(status_code=404, detail=f"No hay archivo para {mes}/{anio}")
    cruce = doc.get('cruce') or {}
    return {
        'anio':              doc['anio'],
        'mes':               doc['mes'],
        'fecha_corte':       doc.get('fecha_corte'),
        'total':             doc.get('total', 0),
        'rutas':             cruce.get('ocupacion_rutas', []),
        'v3_sin_paciente':   cruce.get('v3_sin_paciente', []),
        'total_sin_paciente': cruce.get('total_sin_paciente', 0),
    }


@router.get("/estado")
def obtener_estado():
    """Devuelve el resultado del último sync ejecutado.
    Si el servidor acaba de arrancar y aún no ha corrido un sync,
    recupera el timestamp del último cruce guardado en MongoDB."""
    config_db = _obtener_config_desde_db()
    resultado = {**_ultimo_resultado}
    if not resultado.get("timestamp"):
        doc = _cache.find_one({'tipo': 'cruce_completo'}, {'fecha_calculo': 1, '_id': 0})
        if doc and doc.get('fecha_calculo'):
            resultado["timestamp"] = doc['fecha_calculo']
    return {**resultado, "config": config_db}
