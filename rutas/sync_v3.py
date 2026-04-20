"""
Endpoints para controlar la sincronización programada V3.
"""
from fastapi import APIRouter, HTTPException
from typing import List
from Funciones.sync_api_v3 import ejecutar_sync_v3, archivar_mes_v3, EXCEL_PATH
from bd.bd_cliente import bd_cliente

_bd = bd_cliente['integra']
_cache = _bd['cache_cruce_mc']

router = APIRouter(prefix="/sync-v3", tags=["Sync V3"])

# ── Configuración ────────────────────────────────────────────────────────────
# horarios: lista de strings "HH:MM" — el loop ejecuta el sync cuando la hora coincide
config = {
    "horarios": ["20:00"],  # ← modifica aquí o via POST /sync-v3/config
    "activo": True,
}

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
    """Devuelve la configuración actual del sync."""
    return {**config, "excel_path": EXCEL_PATH}


@router.post("/config")
def actualizar_config(horarios: List[str] = None, activo: bool = None):
    """
    Actualiza la configuración del sync.
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
        config["horarios"] = horarios
    if activo is not None:
        config["activo"] = activo
    return {"mensaje": "Configuración actualizada", **config}


@router.post("/ejecutar")
def ejecutar_manual():
    """Dispara el sync manualmente ahora mismo."""
    resultado = ejecutar_sync_v3()
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
    resultado = {**_ultimo_resultado}
    if not resultado.get("timestamp"):
        doc = _cache.find_one({'tipo': 'cruce_completo'}, {'fecha_calculo': 1, '_id': 0})
        if doc and doc.get('fecha_calculo'):
            resultado["timestamp"] = doc['fecha_calculo']
    return {**resultado, "config": config}
