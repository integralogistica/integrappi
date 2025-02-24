from fastapi import APIRouter, UploadFile, HTTPException, status, Form
from fastapi.responses import JSONResponse
from pymongo import MongoClient
from google.cloud import storage
from uuid import uuid4
from io import BytesIO
from PIL import Image
import os
from typing import List

# Configuración de MongoDB
mongo_uri = os.getenv("MONGO_URI")
if not mongo_uri:
    raise ValueError("La variable de entorno MONGO_URI no está configurada.")
bd_cliente = MongoClient(mongo_uri)
bd = bd_cliente['integra']
coleccion_vehiculos = bd['vehiculos']

# Configuración de Google Cloud Storage
BUCKET_NAME = "integrapp"
CARPETA_STORAGE = "Vehiculos"

google_credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if not google_credentials_path:
    raise ValueError("La variable de entorno GOOGLE_APPLICATION_CREDENTIALS no está configurada.")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = google_credentials_path

# APIRouter para la ruta
ruta_vehiculos = APIRouter(
    prefix="/vehiculos",
    tags=['Vehiculos'],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}}
)

# Función para optimizar imágenes y convertirlas a WebP
def optimizar_imagen(archivo: UploadFile, formato: str = "WEBP", max_width: int = 1200, max_height: int = 800) -> BytesIO:
    try:
        imagen = Image.open(archivo.file)
        imagen.thumbnail((max_width, max_height), Image.Resampling.LANCZOS)
        buffer = BytesIO()
        imagen.save(buffer, format=formato, optimize=True, quality=75)
        buffer.seek(0)
        return buffer
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error al optimizar la imagen: {str(e)}")

# Función para subir archivos a Google Cloud Storage
def subir_a_google_storage(archivo: UploadFile, nombre_archivo: str) -> str:
    try:
        cliente = storage.Client()
        bucket = cliente.bucket(BUCKET_NAME)
        ruta_archivo = f"{CARPETA_STORAGE}/{nombre_archivo}"

        if archivo.content_type.startswith("image/"):
            archivo_optimizado = optimizar_imagen(archivo)
            blob = bucket.blob(ruta_archivo)
            blob.upload_from_file(archivo_optimizado, content_type="image/webp")
        else:
            blob = bucket.blob(ruta_archivo)
            blob.upload_from_file(archivo.file, content_type="application/pdf")

        return f"https://storage.googleapis.com/{BUCKET_NAME}/{ruta_archivo}"
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al subir el archivo a Google Storage: {str(e)}")

# Función para eliminar un archivo de Google Cloud Storage
def eliminar_de_google_storage(url: str):
    try:
        cliente = storage.Client()
        bucket = cliente.bucket(BUCKET_NAME)
        nombre_archivo = url.split(f"https://storage.googleapis.com/{BUCKET_NAME}/")[-1]
        blob = bucket.blob(nombre_archivo)
        blob.delete()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al eliminar el archivo: {str(e)}")

# 1️⃣ Crear vehículo en MongoDB
@ruta_vehiculos.post("/crear")
async def crear_vehiculo(id_usuario: str = Form(...), placa: str = Form(...)):
    if coleccion_vehiculos.find_one({"placa": placa}):
        raise HTTPException(status_code=400, detail="La placa ya está registrada.")

    nuevo_vehiculo = {
        "id_usuario": id_usuario,
        "placa": placa,
        "fotos": [],
        "tarjeta_propiedad": None,
        "soat": None,
        "revision_tecnomecanica": None,
        "tarjeta_remolque": None,
        "poliza_responsabilidad": None,
        "documento_identidad": None,
        "licencia": None,
        "planilla_eps": None,
        "planilla_arl": None,
        "documento_identidad_tenedor": None,
        "certificacion_bancaria": None,
        "documento_acreditacion_tenedor": None,
        "rut_tenedor": None,
        "documento_identidad_propietario": None,
        "rut_propietario": None
    }

    coleccion_vehiculos.insert_one(nuevo_vehiculo)
    return JSONResponse(status_code=status.HTTP_201_CREATED, content={"message": "Vehículo registrado exitosamente"})

# 2️⃣ Subir documentos generales (Tarjeta de Propiedad, SOAT, etc.)
@ruta_vehiculos.put("/subir-documento")
async def subir_documento(archivo: UploadFile, placa: str = Form(...), tipo: str = Form(...)):
    if tipo not in [
        "tarjeta_propiedad", "soat", "revision_tecnomecanica", "tarjeta_remolque",
        "poliza_responsabilidad", "documento_identidad", "licencia",
        "planilla_eps", "planilla_arl", "documento_identidad_tenedor",
        "certificacion_bancaria", "documento_acreditacion_tenedor",
        "rut_tenedor", "documento_identidad_propietario", "rut_propietario"
    ]:
        raise HTTPException(status_code=400, detail="Tipo de documento no válido.")

    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")

    nombre_archivo = f"{tipo}_{placa}.webp"
    url_archivo = subir_a_google_storage(archivo, nombre_archivo)

    coleccion_vehiculos.update_one({"placa": placa}, {"$set": {tipo: url_archivo}})
    return JSONResponse(status_code=status.HTTP_200_OK, content={"message": f"{tipo} subido correctamente", "url": url_archivo})

# 3️⃣ Subir varias fotos
@ruta_vehiculos.put("/subir-fotos")
async def subir_fotos(archivos: List[UploadFile], placa: str = Form(...)):
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")

    urls_fotos = []
    for archivo in archivos:
        nombre_archivo = f"Foto_{placa}_{uuid4().hex}.webp"
        url_archivo = subir_a_google_storage(archivo, nombre_archivo)
        urls_fotos.append(url_archivo)

    coleccion_vehiculos.update_one({"placa": placa}, {"$push": {"fotos": {"$each": urls_fotos}}})
    return JSONResponse(status_code=status.HTTP_200_OK, content={"message": "Fotos subidas correctamente", "urls": urls_fotos})

# 4️⃣ Eliminar documento
@ruta_vehiculos.delete("/eliminar-documento")
async def eliminar_documento(placa: str, tipo: str):
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo or not vehiculo.get(tipo):
        raise HTTPException(status_code=404, detail="Documento no encontrado.")

    eliminar_de_google_storage(vehiculo[tipo])
    coleccion_vehiculos.update_one({"placa": placa}, {"$set": {tipo: None}})
    return JSONResponse(status_code=status.HTTP_200_OK, content={"message": f"{tipo} eliminado correctamente"})

# 5️⃣ Eliminar una foto específica
@ruta_vehiculos.delete("/eliminar-foto")
async def eliminar_foto(placa: str, url: str):
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo or url not in vehiculo["fotos"]:
        raise HTTPException(status_code=404, detail="Foto no encontrada.")

    eliminar_de_google_storage(url)
    coleccion_vehiculos.update_one({"placa": placa}, {"$pull": {"fotos": url}})
    return JSONResponse(status_code=status.HTTP_200_OK, content={"message": "Foto eliminada correctamente"})
