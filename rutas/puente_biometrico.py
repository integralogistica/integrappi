from fastapi import APIRouter, HTTPException, status
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from pydantic import BaseModel
from typing import List, Optional

# ─── CARGAR CONFIG ─────────────────────────────────────────────────────────────
load_dotenv()
mongo_uri = os.getenv("MONGO_URI")
if not mongo_uri:
    raise ValueError("La variable de entorno MONGO_URI no está configurada.")
client = MongoClient(mongo_uri)
db = client["integra"]
collection = db["biometria"]

# ─── ROUTER ─────────────────────────────────────────────────────────────────────
ruta_biometria = APIRouter(
    prefix="/biometria",
    tags=["Biometría"],
    responses={status.HTTP_404_NOT_FOUND: {"description": "No encontrado"}}
)

# ─── MODELOS ────────────────────────────────────────────────────────────────────
class HuellaResponse(BaseModel):
    huella: str

class GuardarHuellasRequest(BaseModel):
    usuario_id: str
    huellas: List[Optional[str]]   # Debe tener longitud=10

class GuardarHuellaRequest(BaseModel):
    usuario_id: str
    indice: int                     # 0–9
    huella: str

# ─── ENDPOINTS ─────────────────────────────────────────────────────────────────
@ruta_biometria.get("/capturar", response_model=HuellaResponse)
async def capturar_huella():
    """
    Simula captura de huella y devuelve base64.
    """
    try:
        fake = b"1234567890FAKEHUELLADATA"
        return HuellaResponse(huella=fake.hex())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@ruta_biometria.post("/guardar_todas", status_code=status.HTTP_201_CREATED)
async def guardar_todas_huellas(data: GuardarHuellasRequest):
    """
    Guarda o actualiza un documento con las 10 huellas de una vez.
    """
    if len(data.huellas) != 10:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Se requieren exactamente 10 huellas."
        )
    try:
        result = collection.update_one(
            {"usuario_id": data.usuario_id},
            {"$set": {"huellas": data.huellas}},
            upsert=True
        )
        return {
            "mensaje": "Huellas guardadas correctamente",
            "upserted_id": str(result.upserted_id) if result.upserted_id else None,
            "modified_count": result.modified_count
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@ruta_biometria.post("/guardar", status_code=status.HTTP_201_CREATED)
async def guardar_huella(req: GuardarHuellaRequest):
    """
    Guarda o actualiza una sola huella en el array por índice (0–9).
    """
    if req.indice < 0 or req.indice > 9:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Índice de dedo inválido. Debe estar entre 0 y 9."
        )
    try:
        result = collection.update_one(
            {"usuario_id": req.usuario_id},
            {"$set": {f"huellas.{req.indice}": req.huella}},
            upsert=True
        )
        return {
            "mensaje": f"Huella índice {req.indice} guardada correctamente",
            "modified_count": result.modified_count
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
