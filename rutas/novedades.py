from fastapi import APIRouter, FastAPI, HTTPException, status
from pymongo import MongoClient
from bson import ObjectId
from typing import List, Dict, Any

# Conexión a la base de datos MongoDB
client = MongoClient("mongodb+srv://integra:integra2025@integrappi.agvcg.mongodb.net/?retryWrites=true&w=majority&appName=integrappi")
db = client["integra"]
collection = db["novedades"]

ruta_novedades = APIRouter(
    prefix="/Novedades",
    tags=['Novedades'],
    responses={status.HTTP_404_NOT_FOUND: {"description": "No encontrado"}}  # Descripción para respuestas 404
)

@ruta_novedades.get("/", response_model=List[Dict[str, Any]])
async def read_manifiestos():
    try:
        manifiestos = list(collection.find())
        # Convertir el ObjectId a string para que sea JSON serializable
        for manifiesto in manifiestos:
            manifiesto["_id"] = str(manifiesto["_id"])
        return manifiestos
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

