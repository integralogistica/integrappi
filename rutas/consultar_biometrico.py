from fastapi import APIRouter, Body, HTTPException, status
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from pydantic import BaseModel
from typing import List, Optional
import base64
from PIL import Image
import io
import requests


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

@ruta_verificacion.get("/obtener-huellas-pdf/{cedula}")
def obtener_huellas_pdf(cedula: str):
    try:
        biometria = coleccion_verificacion.find_one({"tenedor": str(cedula)})
        
        if not biometria:
            print(f"No se encontró biometría para: {cedula}")
            return {"encontrado": False, "huellas": []}

        raw_huellas = biometria.get("huellas", {})
        lista_imagenes_base64 = []

        for i in range(10):
            key = str(i)
            url = None
            if key in raw_huellas and "imagen_url" in raw_huellas[key]:
                url = raw_huellas[key]["imagen_url"]
            imagen_final = "" 

            if url:
                try:
                    respuesta_img = requests.get(url, timeout=5)
                    
                    if respuesta_img.status_code == 200:
                        # Proceso de conversión a Base64
                        imagen_pil = Image.open(io.BytesIO(respuesta_img.content))
                        buffer = io.BytesIO()
                        # Guardamos la imagen en formato PNG para asegurar la compatibilidad
                        imagen_pil.save(buffer, format="PNG")
                        b64_data = base64.b64encode(buffer.getvalue()).decode('utf-8')
                        # Se añade el prefijo MIME necesario para react-pdf/renderer
                        imagen_final = f"data:image/png;base64,{b64_data}"
                    else:
                        print(f"Advertencia: URL {url} devolvió status {respuesta_img.status_code}")
                
                except requests.exceptions.Timeout:
                    print(f"Advertencia: Tiempo de espera agotado para {url}")
                except Exception as ex:
                    print(f"Error al procesar la imagen de huella {url}: {ex}")
            
            # Se añade la imagen Base64 (si se obtuvo) o la cadena vacía ("") (si falló o no había URL)
            lista_imagenes_base64.append(imagen_final)

        return {
            "encontrado": True, 
            "huellas": lista_imagenes_base64 
        }

    except Exception as e:
        print(f"Error general en obtener-huellas-pdf: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")