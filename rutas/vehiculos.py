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

# 1️⃣ Endpoint para crear un registro de vehículo en MongoDB
@ruta_vehiculos.post("/crear")
async def crear_vehiculo(id_usuario: str = Form(...), placa: str = Form(...)):
    if coleccion_vehiculos.find_one({"placa": placa}):
        raise HTTPException(status_code=400, detail="La placa ya está registrada.")

    nuevo_vehiculo = {
    "id_usuario": id_usuario,
    "placa": placa,
    "tarjeta_propiedad": None,
    "soat": None,
    "revision_tecnomecanica": None,
    "tarjeta_remolque": None,
    "fotos": [],  # ✅ Se inicializa como un array vacío de strings para almacenar URLs de fotos
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
    
    return JSONResponse(
        status_code=status.HTTP_201_CREATED,
        content={"message": "Vehículo registrado exitosamente", "placa": placa, "id_usuario": id_usuario}
    )

# 2 Endpoint para subir la Tarjeta de Propiedad
@ruta_vehiculos.put("/subir-tarjeta")
async def subir_tarjeta(archivo: UploadFile, placa: str = Form(...)):
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")

    nombre_archivo = f"TarjetaPropiedad_{placa}.webp"
    url_archivo = subir_a_google_storage(archivo, nombre_archivo)

    coleccion_vehiculos.update_one({"placa": placa}, {"$set": {"tarjeta_propiedad": url_archivo}})

    return JSONResponse(status_code=status.HTTP_200_OK, content={"message": "Tarjeta de Propiedad subida", "url": url_archivo})

# 3️ Endpoint para subir el SOAT
@ruta_vehiculos.put("/subir-soat")
async def subir_soat(archivo: UploadFile, placa: str = Form(...)):
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")

    nombre_archivo = f"Soat_{placa}.webp"
    url_archivo = subir_a_google_storage(archivo, nombre_archivo)

    coleccion_vehiculos.update_one({"placa": placa}, {"$set": {"soat": url_archivo}})

    return JSONResponse(status_code=status.HTTP_200_OK, content={"message": "SOAT subido", "url": url_archivo})

# 4️ Endpoint para subir la Revisión Técnico-Mecánica
@ruta_vehiculos.put("/subir-revision")
async def subir_revision(archivo: UploadFile, placa: str = Form(...)):
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")

    nombre_archivo = f"Revision_{placa}.webp"
    url_archivo = subir_a_google_storage(archivo, nombre_archivo)

    coleccion_vehiculos.update_one({"placa": placa}, {"$set": {"revision_tecnomecanica": url_archivo}})

    return JSONResponse(status_code=status.HTTP_200_OK, content={"message": "Revisión Técnico-Mecánica subida", "url": url_archivo})


# 5 Endpoint para subir la Revisión Tarjeta de Remolque
@ruta_vehiculos.put("/subir-tarjeta-remolque")
async def subir_revision(archivo: UploadFile, placa: str = Form(...)):
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")

    nombre_archivo = f"Revision_{placa}.webp"
    url_archivo = subir_a_google_storage(archivo, nombre_archivo)

    coleccion_vehiculos.update_one({"placa": placa}, {"$set": {"tarjeta_remolque": url_archivo}})

    return JSONResponse(status_code=status.HTTP_200_OK, content={"message": "Tarjeta remolque subida", "url": url_archivo})

# 6 Endpoint para subir varias fotos de un vehículo
@ruta_vehiculos.put("/subir-fotos")
async def subir_fotos(archivos: List[UploadFile], placa: str = Form(...)):
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")

    urls_fotos = []

    for archivo in archivos:
        # Verifica que sea un archivo de imagen válido
        if not archivo.content_type.startswith("image/"):
            raise HTTPException(status_code=400, detail=f"Formato de archivo no permitido: {archivo.filename}")

        nombre_archivo = f"Foto_{placa}_{uuid4().hex}.webp"
        url_archivo = subir_a_google_storage(archivo, nombre_archivo)
        urls_fotos.append(url_archivo)

    # Agregar las nuevas URLs al array de fotos en la base de datos
    coleccion_vehiculos.update_one({"placa": placa}, {"$push": {"fotos": {"$each": urls_fotos}}})

    return JSONResponse(status_code=status.HTTP_200_OK, content={"message": "Fotos subidas correctamente", "urls": urls_fotos})


# 7 Endpoint para consultar vehículos por id_usuario
@ruta_vehiculos.get("/consultar-por-id-usuario/{id_usuario}")
async def consultar_por_id_usuario(id_usuario: str):
    try:
        resultados = list(coleccion_vehiculos.find({"id_usuario": id_usuario}, {"_id": 0}))

        if not resultados:
            raise HTTPException(status_code=404, detail=f"No se encontraron registros para el id_usuario: {id_usuario}")

        return JSONResponse(status_code=status.HTTP_200_OK, content={"message": "Registros encontrados", "data": resultados})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al consultar los datos: {str(e)}")

