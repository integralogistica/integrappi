from fastapi import APIRouter, HTTPException, status, UploadFile, Form
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from typing import Dict, Optional
from google.cloud import storage
from io import BytesIO
from PIL import Image
import certifi
import requests


# ─── CONFIG ────────────────────────────────────────────────────────────────────
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise ValueError("La variable de entorno MONGO_URI no está configurada.")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if not GOOGLE_APPLICATION_CREDENTIALS:
    raise ValueError("La variable de entorno GOOGLE_APPLICATION_CREDENTIALS no está configurada.")
BUCKET_NAME = os.getenv("BUCKET_NAME", "integrapp")  # Ajusta si tu bucket se llama distinto
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS

# ─── MONGO ─────────────────────────────────────────────────────────────────────
from bd.bd_cliente import bd_cliente
client = bd_cliente
db = client["integra"]
collection = db["biometria"]

# ─── ROUTER ────────────────────────────────────────────────────────────────────
ruta_biometria = APIRouter(
    prefix="/biometria",
    tags=["Biometría"],
    responses={status.HTTP_404_NOT_FOUND: {"description": "No encontrado"}}
)

# ─── MODELOS ────────────────────────────────────────────────────────────────────
class HuellaResponse(BaseModel):
    huella: str

class DedoHuella(BaseModel):
    plantilla: Optional[str] = None
    imagen_url: Optional[str] = None

class GuardarHuellasFullRequest(BaseModel):
    tenedor: str
    huellas: Dict[int, DedoHuella] = Field(..., description="Keys 0–9, cada uno con plantilla y/o imagen_url")

class VerificarHuellaRequest(BaseModel):
    tenedor: str

class VerificarHuellaResponse(BaseModel):
    match: bool
    plantillas: Dict[int, str]

# ─── UTIL ───────────────────────────────────────────────────────────────────────
def optimizar_imagen(archivo: UploadFile, formato: str = "WEBP", max_width: int = 400, max_height: int = 400) -> BytesIO:
    try:
        # Convertimos archivo.file en bytes, ya que no se puede leer dos veces directamente
        contenido = archivo.file.read()
        stream = BytesIO(contenido)

        imagen = Image.open(stream)
        imagen.thumbnail((max_width, max_height), Image.Resampling.LANCZOS)

        buffer = BytesIO()
        imagen.save(buffer, format=formato, optimize=True, quality=80)
        buffer.seek(0)
        return buffer
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error al optimizar imagen: {str(e)}")

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

@ruta_biometria.post("/guardar_completo", status_code=status.HTTP_201_CREATED)
async def guardar_huellas_completas(data: GuardarHuellasFullRequest):
    """
    Guarda o actualiza un documento con el diccionario de dedos,
    cada uno con plantilla y/o URL de imagen.
    """
    try:
        # Convertir índices a cadenas para la clave en Mongo
        doc_huellas = { str(idx): dedo.dict() for idx, dedo in data.huellas.items() }
        result = collection.update_one(
            {"tenedor": data.tenedor},
            {"$set": {"huellas": doc_huellas}},
            upsert=True
        )
        return {
            "mensaje": "Huellas completas guardadas correctamente",
            "modified_count": result.modified_count
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@ruta_biometria.post("/verificar")
async def verificar_huella(req: VerificarHuellaRequest):
    # 1. Find document for this ID
    doc = collection.find_one(
        {"tenedor": req.tenedor},
        {"_id": 0, "huellas": 1}
    )
    if not doc or "huellas" not in doc:
        raise HTTPException(404, "No hay huellas registradas para esta cédula.")

    # 2. Extract all available templates
    plantillas = {
        idx: info.get("plantilla")
        for idx, info in doc["huellas"].items()
        if info.get("plantilla")
    }
    
    if not plantillas:
        raise HTTPException(404, "No se encontraron plantillas válidas para esta cédula.")

    # 3. Return templates
    return {"plantillas": plantillas}

    

@ruta_biometria.post("/subir-imagen", status_code=status.HTTP_201_CREATED)
async def subir_imagen_huella(
    archivo: UploadFile,
    tenedor: str = Form(...),
    indice: int = Form(...)
):
    """
    Optimiza y sube la imagen de la huella al bucket de GCS,
    luego guarda la URL dentro del campo huellas[indice].imagen_url.
    """
    if indice < 0 or indice > 9:
        raise HTTPException(status_code=400, detail="Índice de dedo inválido (debe ser 0–9).")
    
    try:
        print(f"📥 Recibido archivo: {archivo.filename}, tenedor: {tenedor}, índice: {indice}")

        cliente = storage.Client()
        print("✅ Cliente GCS creado")

        bucket = cliente.bucket(BUCKET_NAME)
        print(f"✅ Acceso al bucket: {BUCKET_NAME}")

        nombre_archivo = f"Huellas/huella_{tenedor}_{indice}.webp"
        blob = bucket.blob(nombre_archivo)
        print(f"📂 Archivo a subir: {nombre_archivo}")

        imagen_buf = optimizar_imagen(archivo)
        print("🖼 Imagen optimizada correctamente")

        blob.upload_from_file(imagen_buf, content_type="image/webp")
        print("📤 Imagen subida exitosamente a GCS")

        url = f"https://storage.googleapis.com/{BUCKET_NAME}/{nombre_archivo}"
        print(f"🔗 URL generada: {url}")

        result = collection.update_one(
            {"tenedor": tenedor},
            {"$set": {f"huellas.{indice}.imagen_url": url}},
            upsert=True
        )
        print(f"📦 Mongo actualizado: modified_count = {result.modified_count}")

        return {
            "mensaje": "Imagen subida correctamente",
            "url": url,
            "modified_count": result.modified_count
        }

    except Exception as e:
        print("❌ ERROR en /subir-imagen:", str(e))
        raise HTTPException(status_code=500, detail=str(e))
