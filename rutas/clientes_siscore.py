# archivo: rutas/ruta_clientes_siscore.py

from fastapi import APIRouter, HTTPException, status, UploadFile, File
from pymongo import MongoClient
from bson import ObjectId
from pydantic import BaseModel
from typing import List
import os
import pandas as pd

# ------------------------------
# 🔗 Conexión MongoDB
# ------------------------------
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
client = MongoClient(MONGO_URI)
db = client["integra"]
coleccion_clientes_siscore = db["clientes_siscore"]

# ------------------------------
# 🚦 Configuración Router
# ------------------------------
ruta_clientes_siscore = APIRouter(
    prefix="/clientes-siscore",
    tags=["Clientes Siscore"],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}},
)

# ------------------------------
# 🧰 Helpers
# ------------------------------
def limpiar_texto(valor) -> str:
    """Convierte a str y limpia espacios. Si viene None, retorna ''."""
    if valor is None:
        return ""
    return str(valor).strip()

def limpiar_nit(valor) -> str:
    return limpiar_texto(valor).upper()

def normalizar_entidad(valor) -> str:
    # Para búsquedas consistentes por entidad
    return limpiar_texto(valor).upper()

# ------------------------------
# 📌 Esquema Pydantic
# ------------------------------
class ClienteSiscore(BaseModel):
    entidad: str
    razon: str
    nit: str

# ------------------------------
# 📌 Modelo de salida
# ------------------------------
def modelo_cliente_siscore(c: dict) -> dict:
    return {
        "id": str(c["_id"]),
        "entidad": c.get("entidad"),
        "razon": c.get("razon"),
        "nit": c.get("nit"),
    }

# ------------------------------
# ✅ Crear cliente siscore
# ------------------------------
@ruta_clientes_siscore.post("/", response_model=dict, status_code=status.HTTP_201_CREATED)
async def crear_cliente_siscore(data: ClienteSiscore):
    nit_up = limpiar_nit(data.nit)
    if not nit_up:
        raise HTTPException(status_code=400, detail="El NIT es obligatorio")

    if coleccion_clientes_siscore.find_one({"nit": nit_up}):
        raise HTTPException(status_code=400, detail="El NIT ya existe en clientes_siscore")

    nuevo = {
        "entidad": limpiar_texto(data.entidad),
        "razon": limpiar_texto(data.razon),
        "nit": nit_up,
    }

    inserted_id = coleccion_clientes_siscore.insert_one(nuevo).inserted_id
    doc = coleccion_clientes_siscore.find_one({"_id": inserted_id})
    return {"mensaje": "Cliente Siscore creado exitosamente", "cliente": modelo_cliente_siscore(doc)}

# ------------------------------
# ✅ Listar todos los clientes siscore
# ------------------------------
@ruta_clientes_siscore.get("/", response_model=List[dict])
async def obtener_clientes_siscore():
    docs = coleccion_clientes_siscore.find()
    return [modelo_cliente_siscore(c) for c in docs]

# ------------------------------
# ✅ Obtener cliente siscore por ID
# ------------------------------
@ruta_clientes_siscore.get("/{cliente_id}", response_model=dict)
async def obtener_cliente_siscore(cliente_id: str):
    try:
        oid = ObjectId(cliente_id)
    except Exception:
        raise HTTPException(status_code=400, detail="ID de cliente inválido")

    cliente = coleccion_clientes_siscore.find_one({"_id": oid})
    if not cliente:
        raise HTTPException(status_code=404, detail="Cliente Siscore no encontrado")

    return modelo_cliente_siscore(cliente)

# ------------------------------
# ✅ NUEVO: Obtener NIT por entidad
# Ejemplo: GET /clientes-siscore/nit-por-entidad/EXABYTE%20IT%20SAS
# ------------------------------
@ruta_clientes_siscore.get("/nit-por-entidad/{entidad}", response_model=dict)
async def obtener_nit_por_entidad(entidad: str):
    entidad_norm = normalizar_entidad(entidad)
    if not entidad_norm:
        raise HTTPException(status_code=400, detail="La entidad es obligatoria")

    # Primero intenta match exacto (case-insensitive por normalización)
    doc = coleccion_clientes_siscore.find_one({"entidad": {"$regex": f"^{entidad_norm}$", "$options": "i"}})

    # Fallback: contiene (por si vienen diferencias leves)
    if not doc:
        doc = coleccion_clientes_siscore.find_one({"entidad": {"$regex": entidad_norm, "$options": "i"}})

    if not doc:
        raise HTTPException(status_code=404, detail="Entidad no encontrada en clientes_siscore")

    return {"entidad": doc.get("entidad"), "nit": doc.get("nit")}

# ------------------------------
# ✅ Actualizar cliente siscore por ID
# ------------------------------
@ruta_clientes_siscore.put("/{cliente_id}", response_model=dict)
async def actualizar_cliente_siscore(cliente_id: str, data: ClienteSiscore):
    try:
        oid = ObjectId(cliente_id)
    except Exception:
        raise HTTPException(status_code=400, detail="ID de cliente inválido")

    existente = coleccion_clientes_siscore.find_one({"_id": oid})
    if not existente:
        raise HTTPException(status_code=404, detail="Cliente Siscore no encontrado")

    nit_up = limpiar_nit(data.nit)
    if not nit_up:
        raise HTTPException(status_code=400, detail="El NIT es obligatorio")

    # Evitar duplicados al cambiar NIT
    otro = coleccion_clientes_siscore.find_one({"nit": nit_up, "_id": {"$ne": oid}})
    if otro:
        raise HTTPException(status_code=400, detail="Ya existe otro registro con ese NIT")

    actualiza = {
        "entidad": limpiar_texto(data.entidad),
        "razon": limpiar_texto(data.razon),
        "nit": nit_up,
    }

    coleccion_clientes_siscore.update_one({"_id": oid}, {"$set": actualiza})
    actualizado = coleccion_clientes_siscore.find_one({"_id": oid})

    return {"mensaje": "Cliente Siscore actualizado", "cliente": modelo_cliente_siscore(actualizado)}

# ------------------------------
# ❌ Eliminar cliente siscore por ID
# ------------------------------
@ruta_clientes_siscore.delete("/{cliente_id}", response_model=dict)
async def eliminar_cliente_siscore(cliente_id: str):
    try:
        oid = ObjectId(cliente_id)
    except Exception:
        raise HTTPException(status_code=400, detail="ID de cliente inválido")

    result = coleccion_clientes_siscore.delete_one({"_id": oid})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Cliente Siscore no encontrado")

    return {"mensaje": "Cliente Siscore eliminado exitosamente"}
# ------------------------------
# 📦 Cargar clientes siscore masivamente desde Excel
# Requiere columnas: entidad, razon, nit
# ------------------------------
@ruta_clientes_siscore.post("/cargar-masivo", response_model=dict)
async def cargar_clientes_siscore_masivo(archivo: UploadFile = File(...)):
    try:
        df = pd.read_excel(archivo.file)
        df = df.fillna("").astype(str)

        # Normalizar nombres de columna
        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

        requeridas = {"entidad", "razon", "nit"}
        if not requeridas.issubset(df.columns):
            faltantes = requeridas - set(df.columns)
            raise HTTPException(status_code=400, detail=f"Columnas faltantes: {faltantes}")

        # Eliminar todos los registros existentes antes de insertar
        coleccion_clientes_siscore.delete_many({})

        registros = []
        claves_vistas = set()  # evita duplicados exactos por (entidad, nit)

        for _, row in df.iterrows():
            entidad = limpiar_texto(row.get("entidad"))
            razon = limpiar_texto(row.get("razon"))
            nit = limpiar_nit(row.get("nit"))

            # Validación mínima
            if not entidad and not razon and not nit:
                continue
            if not nit:
                continue  # si quieres permitir nit vacío, quita esta línea

            clave = (entidad.upper(), nit)
            if clave in claves_vistas:
                continue
            claves_vistas.add(clave)

            registros.append({
                "entidad": entidad,
                "razon": razon,
                "nit": nit
            })

        if registros:
            coleccion_clientes_siscore.insert_many(registros)

        return {
            "mensaje": f"{len(registros)} clientes_siscore cargados exitosamente, anteriores eliminados"
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
