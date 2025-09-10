# archivo: rutas/ruta_fletes.py

from fastapi import APIRouter, HTTPException, status, UploadFile, File
from pymongo import MongoClient
from pydantic import BaseModel
from typing import List, Dict
import os
import pandas as pd

# ------------------------------
# 🔗 Conexión MongoDB
# ------------------------------
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
client = MongoClient(MONGO_URI)
db = client["integra"]
coleccion_fletes = db["tarifas"]
coleccion_otros_costos = db["otros_costos"]

# ------------------------------
# 🚦 Configuración Router
# ------------------------------
ruta_fletes = APIRouter(
    prefix="/fletes",
    tags=["Fletes"],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}},
)

# ------------------------------
# 📌 Esquema Pydantic
# ------------------------------
class Flete(BaseModel):
    origen: str
    destino: str
    ruta: str
    tipo: str
    equivalencia_centro_costo: str
    tarifas: Dict[str, float]

# ------------------------------
# 📌 Modelo de salida
# ------------------------------
def modelo_flete(f: dict) -> dict:
    return {
        "origen": f["origen"],
        "destino": f["destino"],
        "ruta": f["ruta"],
        "tipo": f["tipo"],
        "equivalencia_centro_costo": f["equivalencia_centro_costo"],        
        "tarifas": f["tarifas"],
    }

# ------------------------------
# ✅ Crear flete/tarifa individual
# ------------------------------
@ruta_fletes.post("/", response_model=dict, status_code=status.HTTP_201_CREATED)
async def crear_flete(data: Flete):
    origen = data.origen.upper().strip()
    destino = data.destino.upper().strip()
    # Verificar duplicado
    if coleccion_fletes.find_one({"origen": origen, "destino": destino}):
        raise HTTPException(status_code=409, detail="Flete ya existe para ese origen y destino")
    nuevo = {
        "origen": origen,
        "destino": destino,
        "ruta": data.ruta.upper().strip(),
        "tipo": data.tipo.upper().strip(),
        "equivalencia_centro_costo": data.equivalencia_centro_costo.upper().strip(),    
        "pago_cargue_desc": data.tipo.upper().strip(),
        "tarifas": {k.upper().strip(): v for k, v in data.tarifas.items()},
    }
    coleccion_fletes.insert_one(nuevo)
    return {"mensaje": "Flete creado exitosamente", "flete": modelo_flete(nuevo)}

# ------------------------------
# ✅ Carga masiva desde Excel (reemplaza todo)
# ------------------------------
# Carga masiva corregida con campo TIPO
@ruta_fletes.post("/cargar-masivo", response_model=dict)
async def cargar_fletes_masivo(archivo: UploadFile = File(...)):
    try:
        df = pd.read_excel(archivo.file)
        df.columns = [col.strip().upper().replace(" ", "_") for col in df.columns]
        if not {"ORIGEN", "DESTINO", "RUTA", "TIPO","EQUIVALENCIA_CENTRO_COSTO"}.issubset(df.columns):
            raise HTTPException(status_code=400, detail="El archivo debe tener ORIGEN, DESTINO, RUTA,  TIPO y EQUIVALENCIA_CENTRO_COSTO")
        registros = []
        for _, row in df.iterrows():
            origen = str(row["ORIGEN"]).strip().upper()
            destino = str(row["DESTINO"]).strip().upper()
            ruta = str(row["RUTA"]).strip().upper()
            tipo = str(row["TIPO"]).strip().upper()
            pago_cargue_desc = str(row["PAGO_CARGUE_DESC"]).strip().upper()
            equivalencia_centro_costo  = str(row["EQUIVALENCIA_CENTRO_COSTO"]).strip().upper()
            tarifas = {}
            for col in df.columns:
                if col not in {"ORIGEN", "DESTINO", "RUTA", "TIPO","PAGO_CARGUE_DESC","EQUIVALENCIA_CENTRO_COSTO"}:
                    tarifas[col] = float(row[col])
            registros.append({"origen": origen, "destino": destino, "ruta": ruta, "tipo": tipo, "pago_cargue_desc": pago_cargue_desc,"equivalencia_centro_costo": equivalencia_centro_costo, "tarifas": tarifas})
        coleccion_fletes.delete_many({})
        if registros:
            coleccion_fletes.insert_many(registros)
        return {"mensaje": f"{len(registros)} tarifas cargadas con TIPO, anteriores eliminadas"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ------------------------------
# ✅ Obtener valor de tarifa específica
# ------------------------------
@ruta_fletes.get("/buscar-tarifa", response_model=dict)
async def obtener_tarifa_especifica(origen: str, destino: str, tipo_vehiculo: str):
    o = origen.upper().strip()
    d = destino.upper().strip()
    t = tipo_vehiculo.upper().strip()
    flete = coleccion_fletes.find_one({"origen": o, "destino": d})
    if not flete:
        raise HTTPException(status_code=404, detail="No se encontró flete para ese origen y destino")
    valor = flete["tarifas"].get(t)
    if valor is None:
        raise HTTPException(status_code=404, detail=f"Tarifa '{t}' no encontrada")
    return {"origen": o, "destino": d, "tipo_vehiculo": t, "valor": valor}

# ------------------------------
# ✅ Listar todos los fletes
# ------------------------------
@ruta_fletes.get("/", response_model=List[dict])
async def obtener_fletes():
    docs = coleccion_fletes.find()
    return [modelo_flete(f) for f in docs]

# ------------------------------
# ✅ Obtener flete por origen y destino
# ------------------------------
@ruta_fletes.get("/{origen}/{destino}", response_model=dict)
async def get_flete(origen: str, destino: str):
    o = origen.upper().strip()
    d = destino.upper().strip()
    flete = coleccion_fletes.find_one({"origen": o, "destino": d})
    if not flete:
        raise HTTPException(status_code=404, detail="Flete no encontrado")
    return modelo_flete(flete)

# ------------------------------
# ✅ Actualizar flete por origen y destino
# ------------------------------
@ruta_fletes.put("/{origen}/{destino}", response_model=dict)
async def actualizar_flete(origen: str, destino: str, data: Flete):
    o = origen.upper().strip()
    d = destino.upper().strip()
    actualiza = {
        "origen": o,
        "destino": d,
        "ruta": data.ruta.upper().strip(),
        "tipo": data.tipo.upper().strip(),
        "equivalencia_centro_costo": data.equivalencia_centro_costo.upper().strip(),        
        "tarifas": {k.upper().strip(): v for k, v in data.tarifas.items()},
    }
    result = coleccion_fletes.update_one({"origen": o, "destino": d}, {"$set": actualiza})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Flete no encontrado para actualizar")
    return {"mensaje": "Flete actualizado", "flete": actualiza}

# ------------------------------
# ✅ Eliminar flete por origen y destino
# ------------------------------
@ruta_fletes.delete("/{origen}/{destino}", response_model=dict)
async def eliminar_flete(origen: str, destino: str):
    o = origen.upper().strip()
    d = destino.upper().strip()
    result = coleccion_fletes.delete_one({"origen": o, "destino": d})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Flete no encontrado para eliminar")
    return {"mensaje": "Flete eliminado exitosamente"}



# ------------------------------
# ✅ Carga masiva de otros costos
# ------------------------------
@ruta_fletes.post("/cargar-otros-costos", response_model=dict)
async def cargar_otros_costos(archivo: UploadFile = File(...)):
    try:
        df = pd.read_excel(archivo.file)
        df.columns = [col.strip().upper().replace(" ", "_") for col in df.columns]

        columnas_requeridas = {"TIPO_VEHICULO", "MAX_PUNTOS", "VALOR_PUNTO_ADICIONAL", "CARGUE_DESCARGUE"}
        if not columnas_requeridas.issubset(df.columns):
            raise HTTPException(
                status_code=400,
                detail=f"El archivo debe contener las columnas: {', '.join(columnas_requeridas)}"
            )

        registros = []
        for _, row in df.iterrows():
            registro = {
                "tipo_vehiculo": str(row["TIPO_VEHICULO"]).strip().upper(),
                "max_puntos": int(row["MAX_PUNTOS"]),
                "valor_punto_adicional": float(row["VALOR_PUNTO_ADICIONAL"]),
                "cargue_descargue": float(row["CARGUE_DESCARGUE"]),
            }
            registros.append(registro)

        coleccion_otros_costos.delete_many({})  # Borra registros anteriores
        if registros:
            coleccion_otros_costos.insert_many(registros)

        return {"mensaje": f"{len(registros)} registros de otros costos cargados exitosamente"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))