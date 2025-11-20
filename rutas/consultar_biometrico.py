from fastapi import APIRouter, Body, HTTPException, status
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from pydantic import BaseModel
from typing import List, Optional

load_dotenv()

mongo_uri = os.getenv("MONGO_URI")
if not mongo_uri:
    raise ValueError("La variable de entorno MONGO_URI no está configurada.")

cliente = MongoClient(mongo_uri)
bd = cliente["integra"]
coleccion_verificacion = bd["biometria"]

ruta_verificacion = APIRouter(
    prefix="/verificacion",
    tags=["Cedula y Biometría"],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}}
)

class biometriaData(BaseModel):
    tenedor: str
    imagen_url: Optional[List[str]] = None
    fecha_registro: Optional[str] = None

class VerificartenedorResponse(BaseModel):
    existe: bool
    data: Optional[biometriaData] = None
    mensaje: str


@ruta_verificacion.post("/verificar", response_model=VerificartenedorResponse)
async def verificar_biometria(tenedor: str = Body(..., embed=True)):
    try:
        if not tenedor:
            raise HTTPException(status_code=400, detail="La cédula no puede estar vacía")

        documento = coleccion_verificacion.find_one({"tenedor": str(tenedor)})

        if not documento:
            return VerificartenedorResponse(
                existe=False,
                mensaje="No existe usuario con esa cédula"
            )

        imagenes = []
        huellas = documento.get("huellas", {})

        # Recorremos las claves numéricas "0","1","2",...
        for key, h in huellas.items():
            if "imagen_url" in h:
                imagenes.append(h["imagen_url"])

        data = biometriaData(
            tenedor=documento.get("tenedor"),
            imagen_url=imagenes,
            fecha_registro=documento.get("fecha_registro")
        )

        return VerificartenedorResponse(
            existe=True,
            data=data,
            mensaje="Usuario biométrico encontrado"
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error del servidor: {str(e)}")
