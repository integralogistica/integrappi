from fastapi import APIRouter, Body, HTTPException, status
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from pydantic import BaseModel
from typing import List, Optional
import base64
from PIL import Image
import io


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
            return {"encontrado": False, "huellas": []}

        raw_huellas = biometria.get("huellas", {})
        lista_imagenes_base64 = []

        for i in range(10):
            key = str(i)
            url = None
            
            if key in raw_huellas and "imagen_url" in raw_huellas[key]:
                url = raw_huellas[key]["imagen_url"]

            if url:
                try:
                    # 1. Descargamos la imagen
                    respuesta_img = requests.get(url, timeout=5)
                    
                    if respuesta_img.status_code == 200:
                        # 2. Abrimos la imagen con Pillow (detecta si es webp, jpg, etc)
                        imagen_pil = Image.open(io.BytesIO(respuesta_img.content))
                        
                        # 3. La guardamos en memoria como PNG (Formato soportado por React-PDF)
                        buffer = io.BytesIO()
                        imagen_pil.save(buffer, format="PNG")
                        
                        # 4. Obtenemos los bytes del PNG y convertimos a Base64
                        b64_data = base64.b64encode(buffer.getvalue()).decode('utf-8')
                        
                        # 5. Enviamos el string listo con cabecera PNG
                        imagen_final = f"data:image/png;base64,{b64_data}"
                        
                        lista_imagenes_base64.append(imagen_final)
                    else:
                        print(f"Error status {url}")
                        lista_imagenes_base64.append(None)
                except Exception as e:
                    print(f"Error convirtiendo imagen {url}: {e}")
                    lista_imagenes_base64.append(None)
            else:
                lista_imagenes_base64.append(None)

        return {
            "encontrado": True, 
            "huellas": lista_imagenes_base64 
        }

    except Exception as e:
        print(f"Error general: {e}")
        raise HTTPException(status_code=500, detail="Error interno")