from fastapi import APIRouter, FastAPI, HTTPException, status
from pymongo import MongoClient
from bson import ObjectId
from typing import List, Dict, Any

# Conexión a la base de datos MongoDB
client = MongoClient("mongodb+srv://integra:integra2025@integrappi.agvcg.mongodb.net/?retryWrites=true&w=majority&appName=integrappi")
db = client["integra"]
collection = db["manifiestos_pagos"]

ruta_manifiestos = APIRouter(
    prefix="/manifiestos",
    tags=['Manifiestos'],
    responses={status.HTTP_404_NOT_FOUND: {"description": "No encontrado"}}  # Descripción para respuestas 404
)

@ruta_manifiestos.get("/", response_model=List[Dict[str, Any]])
async def read_manifiestos():
    try:
        manifiestos = list(collection.find())
        # Convertir el ObjectId a string para que sea JSON serializable
        for manifiesto in manifiestos:
            manifiesto["_id"] = str(manifiesto["_id"])
        return manifiestos
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@ruta_manifiestos.get("/{manifiesto_id}", response_model=Dict[str, Any])
async def read_manifiesto(manifiesto_id: str):
    try:
        manifiesto = collection.find_one({"_id": ObjectId(manifiesto_id)})
        if manifiesto is None:
            raise HTTPException(status_code=404, detail="Manifiesto no encontrado")
        manifiesto["_id"] = str(manifiesto["_id"])
        return manifiesto
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@ruta_manifiestos.get("/tenedor/{tenedor}", response_model=List[Dict[str, Any]])
async def read_manifiestos_by_tenedor(tenedor: str):
    try:
        # Buscar todos los manifiestos que tengan el campo "Tenedor" con el valor proporcionado
        manifiestos = list(collection.find({"Tenedor": tenedor}))
        # Convertir el ObjectId a string para que sea JSON serializable
        for manifiesto in manifiestos:
            manifiesto["_id"] = str(manifiesto["_id"])
        return manifiestos
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
