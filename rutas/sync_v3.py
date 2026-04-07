"""
Endpoints para controlar la sincronización programada V3.
"""
from fastapi import APIRouter, HTTPException
from typing import List
from Funciones.sync_api_v3 import ejecutar_sync_v3, EXCEL_PATH

router = APIRouter(prefix="/sync-v3", tags=["Sync V3"])

# ── Configuración ────────────────────────────────────────────────────────────
# horarios: lista de strings "HH:MM" — el loop ejecuta el sync cuando la hora coincide
config = {
    "horarios": ["05:00", "10:30", "18:32"],  # ← modifica aquí o via POST /sync-v3/config
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


@router.get("/estado")
def obtener_estado():
    """Devuelve el resultado del último sync ejecutado."""
    return {**_ultimo_resultado, "config": config}
