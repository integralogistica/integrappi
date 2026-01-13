# archivo: rutas/ruta_ciudades_general.py

from fastapi import APIRouter, HTTPException, status, UploadFile, File
from pymongo import MongoClient
from bson import ObjectId
from pydantic import BaseModel
from typing import List, Optional
import os
import pandas as pd
import re

# ------------------------------
# 🔗 Conexión MongoDB
# ------------------------------
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
client = MongoClient(MONGO_URI)
db = client["integra"]
coleccion_ciudades_general = db["ciudades_general"]

# ------------------------------
# 🚦 Configuración Router
# ------------------------------
ruta_ciudades_general = APIRouter(
    prefix="/ciudades-general",
    tags=["Ciudades General"],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}},
)

# ------------------------------
# 🧰 Helpers
# ------------------------------
def limpiar_texto(valor) -> str:
    if valor is None:
        return ""
    return str(valor).strip()

def normalizar_columna(col: str) -> str:
    # Normaliza encabezados típicos de Excel
    c = limpiar_texto(col).lower().replace(" ", "_")
    # Variantes comunes
    c = c.replace("municipío", "municipio")
    c = c.replace("latitud", "latitud")
    c = c.replace("longitud", "longitud")
    return c

def normalizar_municipio(valor) -> str:
    # Para búsquedas consistentes
    return limpiar_texto(valor).upper()

def to_float(valor) -> Optional[float]:
    try:
        return float(valor)
    except Exception:
        return None

# ------------------------------
# 📌 Esquema Pydantic
# ------------------------------
class CiudadGeneral(BaseModel):
    municipio: str
    departamento: str
    latitud: Optional[float] = None
    longitud: Optional[float] = None
    ubicacion: str

# ------------------------------
# 📌 Modelo de salida
# ------------------------------
def modelo_ciudad_general(c: dict) -> dict:
    return {
        "id": str(c["_id"]),
        "municipio": c.get("municipio", ""),
        "departamento": c.get("departamento", ""),
        "latitud": c.get("latitud"),
        "longitud": c.get("longitud"),
        "ubicacion": c.get("ubicacion", ""),
    }

# ============================================================
# ✅ NUEVO: Obtener UBICACION por MUNICIPIO
# Ejemplo: GET /ciudades-general/ubicacion-por-municipio/MEDELLIN
# ============================================================
@ruta_ciudades_general.get("/ubicacion-por-municipio/{municipio}", response_model=dict)
async def obtener_ubicacion_por_municipio(municipio: str):
    muni_norm = normalizar_municipio(municipio)
    if not muni_norm:
        raise HTTPException(status_code=400, detail="El MUNICIPIO es obligatorio")

    doc = coleccion_ciudades_general.find_one({"municipio_norm": muni_norm})
    if not doc:
        # fallback: contiene
        doc = coleccion_ciudades_general.find_one({"municipio": {"$regex": muni_norm, "$options": "i"}})

    if not doc:
        raise HTTPException(status_code=404, detail="Municipio no encontrado en ciudades_general")

    return {
        "municipio": doc.get("municipio", ""),
        "ubicacion": doc.get("ubicacion", ""),
    }

# ============================================================
# 🔎 Obtener registro por MUNICIPIO (similar a endpoints de consulta)
# Ejemplo: GET /ciudades-general/por-municipio/MEDELLIN
# ============================================================
@ruta_ciudades_general.get("/por-municipio/{municipio}", response_model=dict)
async def obtener_por_municipio(municipio: str):
    muni_norm = normalizar_municipio(municipio)
    if not muni_norm:
        raise HTTPException(status_code=400, detail="El MUNICIPIO es obligatorio")

    doc = coleccion_ciudades_general.find_one({"municipio_norm": muni_norm})
    if not doc:
        doc = coleccion_ciudades_general.find_one({"municipio": {"$regex": f"^{muni_norm}$", "$options": "i"}})

    if not doc:
        raise HTTPException(status_code=404, detail="Municipio no encontrado")

    return modelo_ciudad_general(doc)

# ------------------------------
# ✅ Crear ciudad
# ------------------------------
@ruta_ciudades_general.post("/", response_model=dict, status_code=status.HTTP_201_CREATED)
async def crear_ciudad_general(data: CiudadGeneral):
    muni_norm = normalizar_municipio(data.municipio)
    if not muni_norm:
        raise HTTPException(status_code=400, detail="El MUNICIPIO es obligatorio")

    # Evitar duplicados por municipio (normalizado)
    if coleccion_ciudades_general.find_one({"municipio_norm": muni_norm}):
        raise HTTPException(status_code=400, detail="El MUNICIPIO ya existe en ciudades_general")

    nuevo = {
        "municipio": limpiar_texto(data.municipio),
        "municipio_norm": muni_norm,
        "departamento": limpiar_texto(data.departamento),
        "latitud": data.latitud,
        "longitud": data.longitud,
        "ubicacion": limpiar_texto(data.ubicacion),
    }

    inserted_id = coleccion_ciudades_general.insert_one(nuevo).inserted_id
    doc = coleccion_ciudades_general.find_one({"_id": inserted_id})

    return {"mensaje": "Ciudad creada exitosamente", "ciudad": modelo_ciudad_general(doc)}

# ------------------------------
# ✅ Listar todas
# ------------------------------
@ruta_ciudades_general.get("/", response_model=List[dict])
async def listar_ciudades_general():
    return [modelo_ciudad_general(c) for c in coleccion_ciudades_general.find()]

# ------------------------------
# ✅ Obtener por ID
# ------------------------------
@ruta_ciudades_general.get("/{ciudad_id}", response_model=dict)
async def obtener_ciudad_general(ciudad_id: str):
    try:
        oid = ObjectId(ciudad_id)
    except Exception:
        raise HTTPException(status_code=400, detail="ID de ciudad inválido")

    doc = coleccion_ciudades_general.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="Ciudad no encontrada")

    return modelo_ciudad_general(doc)

# ------------------------------
# ✅ Actualizar por ID
# ------------------------------
@ruta_ciudades_general.put("/{ciudad_id}", response_model=dict)
async def actualizar_ciudad_general(ciudad_id: str, data: CiudadGeneral):
    try:
        oid = ObjectId(ciudad_id)
    except Exception:
        raise HTTPException(status_code=400, detail="ID de ciudad inválido")

    existente = coleccion_ciudades_general.find_one({"_id": oid})
    if not existente:
        raise HTTPException(status_code=404, detail="Ciudad no encontrada")

    muni_norm = normalizar_municipio(data.municipio)
    if not muni_norm:
        raise HTTPException(status_code=400, detail="El MUNICIPIO es obligatorio")

    # Evitar duplicados al cambiar municipio
    otro = coleccion_ciudades_general.find_one({"municipio_norm": muni_norm, "_id": {"$ne": oid}})
    if otro:
        raise HTTPException(status_code=400, detail="Ya existe otra ciudad con ese MUNICIPIO")

    actualiza = {
        "municipio": limpiar_texto(data.municipio),
        "municipio_norm": muni_norm,
        "departamento": limpiar_texto(data.departamento),
        "latitud": data.latitud,
        "longitud": data.longitud,
        "ubicacion": limpiar_texto(data.ubicacion),
    }

    coleccion_ciudades_general.update_one({"_id": oid}, {"$set": actualiza})
    actualizado = coleccion_ciudades_general.find_one({"_id": oid})

    return {"mensaje": "Ciudad actualizada", "ciudad": modelo_ciudad_general(actualizado)}

# ------------------------------
# ❌ Eliminar por ID
# ------------------------------
@ruta_ciudades_general.delete("/{ciudad_id}", response_model=dict)
async def eliminar_ciudad_general(ciudad_id: str):
    try:
        oid = ObjectId(ciudad_id)
    except Exception:
        raise HTTPException(status_code=400, detail="ID de ciudad inválido")

    result = coleccion_ciudades_general.delete_one({"_id": oid})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Ciudad no encontrada")

    return {"mensaje": "Ciudad eliminada exitosamente"}

# ------------------------------
# 📦 Cargar masivo desde Excel (formato de la imagen)
# Requiere columnas:
# MUNICIPIO | DEPARTAMENTO | LATITUD | LONGITUD | UBICACION
# ------------------------------
@ruta_ciudades_general.post("/cargar-masivo", response_model=dict)
async def cargar_ciudades_general_masivo(archivo: UploadFile = File(...)):
    try:
        df = pd.read_excel(archivo.file)
        df = df.fillna("")

        df.columns = [normalizar_columna(c) for c in df.columns]

        requeridas = {"municipio", "departamento", "latitud", "longitud", "ubicacion"}
        if not requeridas.issubset(df.columns):
            faltantes = requeridas - set(df.columns)
            raise HTTPException(status_code=400, detail=f"Columnas faltantes: {faltantes}")

        # Limpieza total previa (mismo patrón)
        coleccion_ciudades_general.delete_many({})

        registros = []
        vistos = set()  # municipio_norm único

        for _, row in df.iterrows():
            municipio_raw = limpiar_texto(row.get("municipio"))
            muni_norm = normalizar_municipio(municipio_raw)

            if not muni_norm:
                continue
            if muni_norm in vistos:
                continue
            vistos.add(muni_norm)

            registros.append({
                "municipio": municipio_raw,
                "municipio_norm": muni_norm,
                "departamento": limpiar_texto(row.get("departamento")),
                "latitud": to_float(row.get("latitud")),
                "longitud": to_float(row.get("longitud")),
                "ubicacion": limpiar_texto(row.get("ubicacion")),
            })

        if registros:
            coleccion_ciudades_general.insert_many(registros)

        return {"mensaje": f"{len(registros)} ciudades_general cargadas exitosamente"}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
