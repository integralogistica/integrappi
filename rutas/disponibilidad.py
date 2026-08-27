import json
from datetime import datetime
from typing import Optional

import pytz
from bson import ObjectId
from fastapi import APIRouter, Form, HTTPException, status
from fastapi.responses import JSONResponse

from bd.bd_cliente import bd_cliente

# ==========================================
# Configuración base de datos
# ==========================================
bd = bd_cliente['integra']
coleccion_disponibilidades = bd['disponibilidades']
coleccion_vehiculos = bd['vehiculos']
# Control de vehículos tomados de la bolsa por la operación (usar/devolver).
coleccion_asignaciones = bd['asignaciones_flota']

# ==========================================
# Constantes
# ==========================================
_tz_bogota = pytz.timezone("America/Bogota")

# Bodegas operativas (origen del vehículo). Coincide con REGIONES del frontend.
BODEGAS_VALIDAS = ["JUAN MINA", "YUMBO", "GIRARDOTA", "BUCARAMANGA", "FUNZA"]

# Departamentos de Colombia (destino). Para validación.
DEPARTAMENTOS_COLOMBIA = [
    "AMAZONAS", "ANTIOQUIA", "ARAUCA",
    "ARCHIPIÉLAGO DE SAN ANDRÉS, PROVIDENCIA Y SANTA CATALINA",
    "ATLÁNTICO", "BOLÍVAR", "BOYACÁ", "CALDAS", "CAQUETÁ", "CASANARE", "CAUCA",
    "CESAR", "CHOCÓ", "CÓRDOBA", "CUNDINAMARCA", "BOGOTÁ D.C.", "GUAINÍA",
    "GUAVIARE", "HUILA", "LA GUAJIRA", "MAGDALENA", "META", "NARIÑO",
    "NORTE DE SANTANDER", "PUTUMAYO", "QUINDÍO", "RISARALDA", "SANTANDER",
    "SUCRE", "TOLIMA", "VALLE DEL CAUCA", "VAUPÉS", "VICHADA"
]


# ==========================================
# Helpers de fecha (zona Bogotá)
# ==========================================
def _ahora_bogota() -> datetime:
    return datetime.now(_tz_bogota)


def _fecha_hoy_str() -> str:
    """Día actual en zona Colombia (YYYY-MM-DD). Clave de la disponibilidad diaria."""
    return _ahora_bogota().strftime("%Y-%m-%d")


def _serializar(doc: dict) -> dict:
    """Convierte ObjectId/datetime a tipos serializables a JSON."""
    out = {}
    for k, v in doc.items():
        if isinstance(v, ObjectId):
            out[k] = str(v)
        elif isinstance(v, datetime):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


# Router
ruta_disponibilidad = APIRouter(
    prefix="/disponibilidad",
    tags=['Disponibilidad'],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}}
)


# ==========================================
# ENDPOINTS
# ==========================================

@ruta_disponibilidad.post("/checkin")
async def checkin(
    id_usuario: str = Form(...),
    placa: str = Form(...),
    origen: str = Form(...),
    destinos_json: str = Form(...)
):
    """
    Crea o actualiza el check-in de disponibilidad de HOY para una placa.

    - El vehículo debe existir, pertenecer al conductor y estar aprobado.
    - origen: bodega de origen (JUAN MINA / YUMBO / GIRARDOTA / ...).
    - destinos_json: JSON string con lista de departamentos destino,
      ej: '["ANTIOQUIA","VALLE DEL CAUCA"]'.
    """
    placa_limpia = placa.strip().upper()
    origen_limpio = origen.strip().upper()

    # 1. Validar origen
    if origen_limpio not in BODEGAS_VALIDAS:
        raise HTTPException(
            status_code=400,
            detail=f"Origen '{origen}' no válido. Bodegas: {', '.join(BODEGAS_VALIDAS)}"
        )

    # 2. Validar vehículo
    vehiculo = coleccion_vehiculos.find_one({"placa": placa_limpia})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")
    # Propiedad: dueño de la ficha (idUsuario = tenedor o conductor legacy) o
    # conductor invitado vinculado (idConductor).
    es_dueno = str(vehiculo.get("idUsuario", "")) == str(id_usuario)
    es_conductor_vinculado = str(vehiculo.get("idConductor") or "") == str(id_usuario)
    if not (es_dueno or es_conductor_vinculado):
        raise HTTPException(status_code=403, detail="El vehículo no pertenece a este conductor.")
    if vehiculo.get("estadoIntegra") != "aprobado":
        raise HTTPException(
            status_code=400,
            detail="El vehículo no está aprobado, no puede ofrecerse como disponible."
        )

    # 3. Validar destinos
    try:
        destinos = json.loads(destinos_json)
        if not isinstance(destinos, list):
            raise ValueError
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="destinos_json debe ser un JSON con la lista de departamentos."
        )
    destinos = [d.strip().upper() for d in destinos if isinstance(d, str) and d.strip()]
    if not destinos:
        raise HTTPException(status_code=400, detail="Debe indicar al menos un departamento destino.")
    invalidos = [d for d in destinos if d not in DEPARTAMENTOS_COLOMBIA]
    if invalidos:
        raise HTTPException(status_code=400, detail=f"Departamentos no válidos: {', '.join(invalidos)}")

    # 4. Upsert de hoy (un check-in por placa+fecha). Expira solo por fecha (lazy).
    hoy = _fecha_hoy_str()
    ahora = _ahora_bogota()
    coleccion_disponibilidades.update_one(
        {"placa": placa_limpia, "fecha": hoy},
        {
            "$set": {
                "idUsuario": str(id_usuario),
                "origen": origen_limpio,
                "departamentos_destino": destinos,
                "estado": "activa",
                "actualizado_en": ahora
            },
            "$setOnInsert": {"creado_en": ahora}
        },
        upsert=True
    )

    # Índice único (placa + fecha). Idempotente.
    try:
        coleccion_disponibilidades.create_index([("placa", 1), ("fecha", 1)], unique=True)
    except Exception:
        pass

    return JSONResponse(status_code=status.HTTP_200_OK, content={
        "message": "Disponibilidad registrada",
        "fecha": hoy,
        "placa": placa_limpia,
        "origen": origen_limpio,
        "departamentos_destino": destinos
    })


@ruta_disponibilidad.get("/mia")
def mia(id_usuario: str):
    """
    Devuelve los check-ins activos de HOY del conductor + la lista de placas aprobadas
    que puede marcar como disponibles (para pre-poblar la UI).
    """
    hoy = _fecha_hoy_str()

    # Placas aprobadas del usuario: propias (idUsuario) o como conductor
    # invitado (idConductor).
    aprobados = list(coleccion_vehiculos.find(
        {"$or": [{"idUsuario": id_usuario}, {"idConductor": id_usuario}], "estadoIntegra": "aprobado"},
        {"_id": 0, "placa": 1, "vehMarca": 1, "tipo_veh_sicetac": 1}
    ))

    # Se incluyen las ASIGNADAS (tomadas por la operación): siguen siendo
    # check-ins de hoy, pero fuera de la bolsa hasta que las devuelvan.
    checkins_hoy = [
        _serializar(c) for c in coleccion_disponibilidades.find(
            {"idUsuario": str(id_usuario), "fecha": hoy, "estado": {"$in": ["activa", "asignada"]}},
            {"_id": 0}
        )
    ]

    return JSONResponse(status_code=status.HTTP_200_OK, content={
        "message": "OK",
        "fecha": hoy,
        "vehiculos_aprobados": aprobados,
        "disponibles_hoy": checkins_hoy
    })


@ruta_disponibilidad.get("/bolsa")
def bolsa(
    origen: Optional[str] = None,
    destino: Optional[str] = None,
    tipo_veh_sicetac: Optional[str] = None
):
    """
    Flota disponible HOY. Cada ítem = un check-in activo + datos del vehículo y conductor.
    Filtros opcionales: origen (bodega), destino (departamento), tipo_veh_sicetac.
    """
    hoy = _fecha_hoy_str()

    query = {"fecha": hoy, "estado": "activa"}
    if origen:
        query["origen"] = origen.strip().upper()

    destino_buscar = destino.strip().upper() if destino else None
    tipo_buscar = tipo_veh_sicetac.strip().upper() if tipo_veh_sicetac else None

    resultado = []
    for c in coleccion_disponibilidades.find(query, {"_id": 0}):
        destinos_c = [d.upper() for d in c.get("departamentos_destino", [])]
        if destino_buscar and destino_buscar not in destinos_c:
            continue

        veh = coleccion_vehiculos.find_one({"placa": c.get("placa")}, {"_id": 0}) or {}
        # Segunda barrera: un vehículo inactivado con check-in activo (si la
        # cancelación falló) NO aparece en la bolsa — solo los aprobados operan.
        if veh.get("estadoIntegra") != "aprobado":
            continue
        tipo_veh = (veh.get("tipo_veh_sicetac") or "")
        if tipo_buscar and tipo_veh.strip().upper() != tipo_buscar:
            continue

        actualizado = c.get("actualizado_en")
        resultado.append({
            "placa": c.get("placa"),
            "origen": c.get("origen"),
            "departamentos_destino": c.get("departamentos_destino", []),
            "estado": c.get("estado"),
            "actualizado_en": actualizado.isoformat() if isinstance(actualizado, datetime) else actualizado,
            "conductor": {
                "nombre": veh.get("condNombres") or veh.get("condNombre"),
                "celular": veh.get("condCelular"),
                "correo": veh.get("condCorreo"),
                "cedula": veh.get("condCedulaCiudadania")
            },
            "vehiculo": {
                "linea": veh.get("vehLinea"),
                "tipo_veh_sicetac": tipo_veh or None,
                "toneladas": veh.get("vehCapacidadToneladas") or veh.get("toneladas") or veh.get("vehTonelaje")
            }
        })

    return JSONResponse(status_code=status.HTTP_200_OK, content={
        "message": "OK",
        "fecha": hoy,
        "total": len(resultado),
        "flota": resultado
    })


@ruta_disponibilidad.put("/cancelar")
async def cancelar(id_usuario: str = Form(...), placa: str = Form(...)):
    """Marca como cancelada la disponibilidad de HOY de una placa (el conductor deja de estar disponible)."""
    placa_limpia = placa.strip().upper()
    hoy = _fecha_hoy_str()

    res = coleccion_disponibilidades.update_one(
        {"placa": placa_limpia, "fecha": hoy, "idUsuario": str(id_usuario)},
        {"$set": {"estado": "cancelada", "actualizado_en": _ahora_bogota()}}
    )
    if res.matched_count == 0:
        raise HTTPException(
            status_code=404,
            detail="No hay disponibilidad activa hoy para esta placa."
        )

    return JSONResponse(status_code=status.HTTP_200_OK, content={
        "message": "Disponibilidad cancelada",
        "placa": placa_limpia,
        "fecha": hoy
    })


# ==========================================
# ASIGNACIÓN DE FLOTA (operación toma un vehículo de la bolsa)
# ==========================================

@ruta_disponibilidad.put("/asignar")
async def asignar(
    placa: str = Form(...),
    asignado_por: str = Form(...),
    nombre_asignado: Optional[str] = Form(None),
    observacion: Optional[str] = Form(None)
):
    """
    La operación TOMA un vehículo de la bolsa de hoy:

    - Su check-in pasa de `activa` a `asignada` (desaparece de la bolsa).
    - Queda registrado en `asignaciones_flota` (control de vehículos usados,
      con quién lo tomó y cuándo).
    """
    placa_limpia = placa.strip().upper()
    hoy = _fecha_hoy_str()
    ahora = _ahora_bogota()

    # Un vehículo no se asigna dos veces el mismo día sin devolverse antes.
    if coleccion_asignaciones.find_one({"placa": placa_limpia, "fecha": hoy, "estado": "asignada"}):
        raise HTTPException(
            status_code=400,
            detail="Este vehículo ya está asignado hoy (mira la lista de vehículos en uso)."
        )

    disp = coleccion_disponibilidades.find_one_and_update(
        {"placa": placa_limpia, "fecha": hoy, "estado": "activa"},
        {"$set": {"estado": "asignada", "actualizado_en": ahora}}
    )
    if not disp:
        raise HTTPException(
            status_code=404,
            detail="No hay disponibilidad activa hoy para esta placa."
        )

    doc = {
        "placa": placa_limpia,
        "fecha": hoy,
        "origen": disp.get("origen"),
        "departamentos_destino": disp.get("departamentos_destino", []),
        "idUsuario": disp.get("idUsuario"),
        "asignado_por": str(asignado_por).strip(),
        "nombre_asignado_por": (nombre_asignado or "").strip() or None,
        "observacion": (observacion or "").strip() or None,
        "asignado_en": ahora,
        "estado": "asignada"
    }
    insertado = coleccion_asignaciones.insert_one(doc)
    doc["_id"] = str(insertado.inserted_id)

    return JSONResponse(status_code=status.HTTP_200_OK, content={
        "message": "Vehículo asignado",
        "asignacion": _serializar(doc)
    })


@ruta_disponibilidad.put("/devolver")
async def devolver(placa: str = Form(...), devuelto_por: str = Form(...)):
    """
    Devuelve un vehículo asignado (no se va a usar):

    - Cierra la asignación abierta más reciente (queda `devuelta` con fecha/quién).
    - Si la asignación era de HOY, su check-in vuelve a `activa` y reaparece
      en la bolsa. Si era de otro día, solo queda el registro (el check-in
      ya expiró).
    """
    placa_limpia = placa.strip().upper()
    ahora = _ahora_bogota()
    hoy = _fecha_hoy_str()

    asig = coleccion_asignaciones.find_one_and_update(
        {"placa": placa_limpia, "estado": "asignada"},
        {"$set": {
            "estado": "devuelta",
            "devuelto_en": ahora,
            "devuelto_por": str(devuelto_por).strip()
        }},
        sort=[("asignado_en", -1)]
    )
    if not asig:
        raise HTTPException(
            status_code=404,
            detail="Esta placa no tiene una asignación abierta."
        )

    # Reactivar el check-in SOLO si la asignación era de hoy.
    reactivada = False
    if asig.get("fecha") == hoy:
        res = coleccion_disponibilidades.update_one(
            {"placa": placa_limpia, "fecha": hoy, "estado": "asignada"},
            {"$set": {"estado": "activa", "actualizado_en": ahora}}
        )
        reactivada = res.matched_count > 0

    return JSONResponse(status_code=status.HTTP_200_OK, content={
        "message": "Vehículo devuelto",
        "placa": placa_limpia,
        "reactivada": reactivada,
        "asignacion": _serializar(asig)
    })


@ruta_disponibilidad.get("/asignadas")
def asignadas(solo_abiertas: bool = False, limit: int = 100):
    """
    Asignaciones de flota (control de vehículos usados), la más reciente primero.
    `solo_abiertas=true` → solo las que están en uso (sin devolver).
    """
    query = {"estado": "asignada"} if solo_abiertas else {}
    cursor = coleccion_asignaciones.find(query).sort("asignado_en", -1).limit(max(1, min(limit, 500)))
    lista = [_serializar(d) for d in cursor]

    return JSONResponse(status_code=status.HTTP_200_OK, content={
        "message": "OK",
        "total": len(lista),
        "asignaciones": lista
    })
