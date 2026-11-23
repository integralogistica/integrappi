# archivo: rutas/historial_tarifas.py
# Auditoría de cambios en los tarifarios (`tarifas` y `fletes_rutas_fmc`).
# Un evento = un documento en `historial_tarifas` (colección de un único
# propósito, regla de oro del README). Sobrevive a eliminaciones de tarifas:
# las eliminaciones guardan snapshot completo del doc borrado y las cargas
# masivas de `tarifas` (que hacen delete_many({}) + insert_many) guardan
# snapshot de todo lo reemplazado — permite responder ante auditoría y
# restaurar si hace falta.

from fastapi import APIRouter, HTTPException, status
from datetime import datetime, timezone
from typing import Optional
from bson import ObjectId

from bd.bd_cliente import bd_cliente
client = bd_cliente
db = client["integra"]
col_historial = db["historial_tarifas"]
col_usuarios = db["baseusuarios"]

# Perfiles que pueden editar tarifas y ver el historial de cambios
PERFILES_EDICION_TARIFAS = {"ADMIN", "CONTROL"}

# Tarifarios auditados (filtro del listado)
TARIFARIOS_VALIDOS = {"fletes", "fletes_rutas_fmc"}


# ── Resolución de identidad / autorización ──────────────────────────────────
# Patrón copiado de otros_costos.py (_resolver_usuario/_requiere): el backend
# NO confía en el perfil que envía el frontend, lo deriva de baseusuarios.
def _resolver_usuario(usuario: str) -> dict:
    """Devuelve el perfil/nombre REALES del usuario desde baseusuarios.
    Lanza 401 si no llega usuario / no existe, 403 si está inactivo.
    """
    if not usuario or not str(usuario).strip():
        raise HTTPException(status_code=401, detail="No autenticado")
    u = str(usuario).strip().upper()
    doc = col_usuarios.find_one({"usuario": u})
    if not doc:
        raise HTTPException(status_code=401, detail="Usuario no válido")
    if not doc.get("activo", True):
        raise HTTPException(status_code=403, detail="Usuario inactivo")
    return {
        "usuario": doc["usuario"],
        "perfil": (doc.get("perfil") or "").strip().upper(),
        "regional": doc.get("regional", ""),
        "nombre": doc.get("nombre", ""),
        "id": str(doc["_id"]),
    }


def _requiere(info: dict, perfiles: set, accion: str):
    if info["perfil"] not in perfiles:
        raise HTTPException(
            status_code=403,
            detail=f"Su perfil ({info['perfil']}) no tiene permiso para {accion}.",
        )


# ── Diff de campos (antes → después) ────────────────────────────────────────
def _limpiar_valor(v):
    """Normaliza un valor para comparación/almacenamiento (ObjectId → str,
    floats enteros → int) para que el diff y el JSON del historial salgan limpios."""
    if isinstance(v, ObjectId):
        return str(v)
    if isinstance(v, dict):
        return {k: _limpiar_valor(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_limpiar_valor(x) for x in v]
    if isinstance(v, float) and v == int(v):
        return int(v)  # 850000.0 == 850000 → no reportar como cambio
    return v


def _diff_campos(anterior: dict, nuevo: dict) -> list:
    """Compara campo a campo la unión de claves. Devuelve
    [{campo, valor_anterior, valor_nuevo}] SOLO de lo que cambió."""
    cambios = []
    claves = sorted(set(list(anterior.keys()) + list(nuevo.keys())))
    for campo in claves:
        va = _limpiar_valor(anterior.get(campo))
        vn = _limpiar_valor(nuevo.get(campo))
        if campo not in anterior:
            cambios.append({"campo": campo, "valor_anterior": None, "valor_nuevo": vn})
        elif campo not in nuevo:
            cambios.append({"campo": campo, "valor_anterior": va, "valor_nuevo": None})
        elif va != vn:
            cambios.append({"campo": campo, "valor_anterior": va, "valor_nuevo": vn})
    return cambios


# ── Registro de eventos ──────────────────────────────────────────────────────
def _ahora_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _registrar_evento(
    accion: str,
    tarifario: str,
    registro: str,
    info: dict,
    registro_id: Optional[str] = None,
    campos_modificados: Optional[list] = None,
    snapshot: Optional[dict] = None,
    resumen: Optional[dict] = None,
    ip: Optional[str] = None,
) -> None:
    """Inserta un documento de auditoría en `historial_tarifas`.

    NUNCA rompe el flujo de negocio: si el insert falla, sólo queda en log
    (la operación de tarifa ya se realizó).
    """
    evento = {
        "accion": accion,          # creacion | edicion | eliminacion | carga_masiva
        "tarifario": tarifario,    # fletes | fletes_rutas_fmc
        "registro": registro,
        "registro_id": registro_id,
        "usuario": info.get("usuario", ""),
        "nombre_usuario": info.get("nombre", ""),
        "rol": info.get("perfil", ""),
        "fecha": _ahora_utc(),
        "ip": ip or "",
    }
    if campos_modificados is not None:
        evento["campos_modificados"] = campos_modificados
    # snapshot (eliminaciones) y resumen (carga masiva) se guardan tal cual.
    # El snapshot de una carga masiva de `tarifas` son cientos de filas:
    # muy por debajo del límite de 16 MB de MongoDB; se guarda completo
    # para permitir restauración.
    if snapshot is not None:
        evento["snapshot"] = snapshot
    if resumen is not None:
        evento["resumen"] = resumen
    try:
        col_historial.insert_one(evento)
    except Exception as e:  # pragma: no cover - red de Atlas
        print(f"[HISTORIAL TARIFAS] No se pudo registrar evento {accion}/{tarifario}: {e}")


# ── Configuración Router ─────────────────────────────────────────────────────
ruta_historial_tarifas = APIRouter(
    prefix="/historial-tarifas",
    tags=["Historial Tarifas"],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}},
)


def _modelo_evento(e: dict) -> dict:
    return {
        "id": str(e.get("_id", "")),
        "accion": e.get("accion", ""),
        "tarifario": e.get("tarifario", ""),
        "registro": e.get("registro", ""),
        "registro_id": e.get("registro_id"),
        "usuario": e.get("usuario", ""),
        "nombre_usuario": e.get("nombre_usuario", ""),
        "rol": e.get("rol", ""),
        "fecha": e.get("fecha", ""),
        "campos_modificados": e.get("campos_modificados", []),
        "snapshot": e.get("snapshot"),
        "resumen": e.get("resumen"),
        "ip": e.get("ip", ""),
    }


# ------------------------------
# ✅ Listar historial de cambios de tarifas (ADMIN/CONTROL)
# ------------------------------
@ruta_historial_tarifas.get("/", response_model=list)
async def obtener_historial(usuario: str, tarifario: Optional[str] = None, limite: int = 500):
    info = _resolver_usuario(usuario)
    _requiere(info, PERFILES_EDICION_TARIFAS, "ver el historial de tarifas")

    filtro = {}
    if tarifario:
        if tarifario not in TARIFARIOS_VALIDOS:
            raise HTTPException(
                status_code=400,
                detail=f"Tarifario inválido: '{tarifario}'. Válidos: {', '.join(sorted(TARIFARIOS_VALIDOS))}",
            )
        filtro["tarifario"] = tarifario

    limite = max(1, min(limite, 2000))
    docs = col_historial.find(filtro).sort("fecha", -1).limit(limite)
    return [_modelo_evento(e) for e in docs]
