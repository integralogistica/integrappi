# archivo: rutas/ruta_clientes.py

from fastapi import APIRouter, HTTPException, status, UploadFile, File
from pymongo import MongoClient
from bson import ObjectId
from pydantic import BaseModel, EmailStr
from typing import List
import os
import pandas as pd
from typing import Optional

# ------------------------------
# 🔗 Conexión MongoDB
# ------------------------------
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
client = MongoClient(MONGO_URI)
db = client["integra"]
coleccion_clientes = db["clientes"]

# ------------------------------
# 🚦 Configuración Router
# ------------------------------
ruta_clientes = APIRouter(
    prefix="/clientes",
    tags=["Clientes"],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}},
)

# ------------------------------
# 📌 Esquema Pydantic
# ------------------------------
class Cliente(BaseModel):
    nit: str
    nombre: str
    ubicacion: Optional[str] = None
    contacto: Optional[str] = None
    cargo: Optional[str] = None
    telefono: Optional[str] = None
    fax: Optional[str] = None
    email: Optional[EmailStr] = None
    direccion: Optional[str] = None
    forma_pago: Optional[str] = None
    equivalencia_centro_costo: Optional[str] = None
    

# ------------------------------
# 📌 Modelo de salida
# ------------------------------
def modelo_cliente(c: dict) -> dict:
    return {
        "id": str(c["_id"]),
        "nit": c["nit"],
        "nombre": c["nombre"],
        "ubicacion": c["ubicacion"],
        "contacto": c["contacto"],
        "cargo": c["cargo"],
        "telefono": c["telefono"],
        "fax": c["fax"],
        "email": c["email"],
        "direccion": c["direccion"],
        "forma_pago": c["forma_pago"],
        "equivalencia_centro_costo": c["equivalencia_centro_costo"],        
    }

# ------------------------------
# ✅ Crear cliente
# ------------------------------
@ruta_clientes.post("/", response_model=dict, status_code=status.HTTP_201_CREATED)
async def crear_cliente(data: Cliente):
    nit_up = data.nit.upper().strip()
    if coleccion_clientes.find_one({"nit": nit_up}):
        raise HTTPException(status_code=400, detail="El NIT ya existe")
    nuevo = {
        "nit": nit_up,
        "nombre": data.nombre.strip(),
        "ubicacion": data.ubicacion.strip(),
        "contacto": data.contacto.strip(),
        "cargo": data.cargo.strip(),
        "telefono": data.telefono.strip(),
        "fax": data.fax.strip(),
        "email": data.email.strip(),
        "direccion": data.direccion.strip(),
        "forma_pago": data.forma_pago.strip(),
        "equivalencia_centro_costo": data.equivalencia_centro_costo.strip(),
    }
    inserted_id = coleccion_clientes.insert_one(nuevo).inserted_id
    cliente = coleccion_clientes.find_one({"_id": inserted_id})
    return {"mensaje": "Cliente creado exitosamente", "cliente": modelo_cliente(cliente)}

# ------------------------------
# ✅ Listar todos los clientes
# ------------------------------
@ruta_clientes.get("/", response_model=List[dict])
async def obtener_clientes():
    docs = coleccion_clientes.find()
    return [modelo_cliente(c) for c in docs]

# ------------------------------
# ✅ Obtener cliente por ID
# ------------------------------
@ruta_clientes.get("/{cliente_id}", response_model=dict)
async def obtener_cliente(cliente_id: str):
    try:
        oid = ObjectId(cliente_id)
    except:
        raise HTTPException(status_code=400, detail="ID de cliente inválido")
    cliente = coleccion_clientes.find_one({"_id": oid})
    if not cliente:
        raise HTTPException(status_code=404, detail="Cliente no encontrado")
    return modelo_cliente(cliente)

# ------------------------------
# ✅ Actualizar cliente por ID
# ------------------------------
@ruta_clientes.put("/{cliente_id}", response_model=dict)
async def actualizar_cliente(cliente_id: str, data: Cliente):
    try:
        oid = ObjectId(cliente_id)
    except:
        raise HTTPException(status_code=400, detail="ID de cliente inválido")
    actualiza = {
        "nit": data.nit.upper().strip(),
        "nombre": data.nombre.strip(),
        "ubicacion": data.ubicacion.strip(),
        "contacto": data.contacto.strip(),
        "cargo": data.cargo.strip(),
        "telefono": data.telefono.strip(),
        "fax": data.fax.strip(),
        "email": data.email.strip(),
        "direccion": data.direccion.strip(),
        "forma_pago": data.forma_pago.strip(),
        "equivalencia_centro_costo     ": data.equivalencia_centro_costo     .strip(),        
    }
    result = coleccion_clientes.update_one({"_id": oid}, {"$set": actualiza})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Cliente no encontrado")
    actualizado = coleccion_clientes.find_one({"_id": oid})
    return {"mensaje": "Cliente actualizado", "cliente": modelo_cliente(actualizado)}

# ------------------------------
# ❌ Eliminar cliente por ID
# ------------------------------
@ruta_clientes.delete("/{cliente_id}", response_model=dict)
async def eliminar_cliente(cliente_id: str):
    try:
        oid = ObjectId(cliente_id)
    except:
        raise HTTPException(status_code=400, detail="ID de cliente inválido")
    result = coleccion_clientes.delete_one({"_id": oid})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Cliente no encontrado")
    return {"mensaje": "Cliente eliminado exitosamente"}

# ------------------------------
# 📦 Cargar clientes masivamente desde Excel
# ------------------------------
@ruta_clientes.post("/cargar-masivo", response_model=dict)
async def cargar_clientes_masivo(archivo: UploadFile = File(...)):
    try:
        df = pd.read_excel(archivo.file)
        df = df.fillna("").astype(str)   
        # Normalizar nombres de columna
        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
        required = {"nit", "nombre", "ubicacion", "contacto", "cargo", "telefono", "fax", "email", "direccion", "forma_pago","equivalencia_centro_costo"}
        if not required.issubset(df.columns):
            faltantes = required - set(df.columns)
            raise HTTPException(status_code=400, detail=f"Columnas faltantes: {faltantes}")

        # Eliminar todos los clientes existentes antes de insertar
        coleccion_clientes.delete_many({})

        registros = []
        for _, row in df.iterrows():
            registros.append({
                "nit": row["nit"].upper().strip(),
                "nombre": row["nombre"].strip(),
                "ubicacion":    row["ubicacion"].strip()    or None,
                "contacto":     row["contacto"].strip()     or None,
                "cargo":        row["cargo"].strip()        or None,
                "telefono":     row["telefono"].strip()     or None,
                "fax":          row["fax"].strip()          or None,
                "email":        row["email"].strip()        or None,
                "direccion":    row["direccion"].strip()    or None,
                "forma_pago":    row["forma_pago"].strip()    or None,                
                "equivalencia_centro_costo":    row["equivalencia_centro_costo"].strip()    or None, 
            })

        if registros:
            coleccion_clientes.insert_many(registros)

        return {"mensaje": f"{len(registros)} clientes cargados exitosamente, anteriores eliminados"}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
