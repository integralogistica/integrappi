from fastapi import APIRouter, UploadFile, HTTPException, status, Form
from fastapi.responses import JSONResponse
from pymongo import MongoClient
from google.cloud import storage
from uuid import uuid4
from io import BytesIO
from PIL import Image
import os

# Configuración de MongoDB
mongo_uri = os.getenv("MONGO_URI")
if not mongo_uri:
    raise ValueError("La variable de entorno MONGO_URI no está configurada.")
bd_cliente = MongoClient(mongo_uri)
bd = bd_cliente['integra']
coleccion_vehiculos = bd['vehiculos']

# Configuración de Google Cloud Storage
BUCKET_NAME = "integrapp"
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
def subir_a_google_storage(archivo: UploadFile, carpeta: str = "TarjetasPropiedad") -> str:
    try:
        cliente = storage.Client()
        bucket = cliente.bucket(BUCKET_NAME)
        if archivo.content_type.startswith("image/"):
            archivo_optimizado = optimizar_imagen(archivo)
            nombre_archivo = f"{carpeta}/{uuid4().hex}.webp"
            blob = bucket.blob(nombre_archivo)
            blob.upload_from_file(archivo_optimizado, content_type="image/webp")
        else:  # Si es PDF, súbelo directamente
            nombre_archivo = f"{carpeta}/{uuid4().hex}.pdf"
            blob = bucket.blob(nombre_archivo)
            blob.upload_from_file(archivo.file, content_type="application/pdf")
        
        return f"https://storage.googleapis.com/{BUCKET_NAME}/{nombre_archivo}"
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al subir el archivo a Google Storage: {str(e)}")

# Ruta para subir la tarjeta de conducir con placa e id_usuario
@ruta_vehiculos.post("/subir-tarjeta")
async def subir_tarjeta(
    archivo: UploadFile,
    placa: str = Form(...),
    id_usuario: str = Form(...),
):
    if archivo.content_type not in ["image/jpeg", "image/png", "image/webp", "application/pdf"]:
        raise HTTPException(
            status_code=400,
            detail="Solo se permiten archivos de tipo imagen (JPEG, PNG, WEBP) o PDF"
        )

    try:
        # Subir a Google Cloud Storage
        url_archivo = subir_a_google_storage(archivo)

        # Guardar la información en MongoDB
        nueva_tarjeta = {
            "placa": placa,
            "id_usuario": id_usuario,
            "nombre_archivo": archivo.filename,
            "tipo": archivo.content_type,
            "url": url_archivo
        }
        coleccion_vehiculos.insert_one(nueva_tarjeta)

        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content={
                "message": "Archivo y datos subidos exitosamente",
                "url": url_archivo,
                "placa": placa,
                "id_usuario": id_usuario
            }
        )
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al procesar el archivo: {str(e)}")

# Ruta para consultar vehículos por id_usuario
@ruta_vehiculos.get("/consultar-por-id-usuario/{id_usuario}")
async def consultar_por_id_usuario(id_usuario: str):
    try:
        # Consultar en MongoDB
        resultados = list(coleccion_vehiculos.find({"id_usuario": id_usuario}, {"_id": 0}))

        if not resultados:
            raise HTTPException(
                status_code=404, 
                detail=f"No se encontraron registros para el id_usuario: {id_usuario}"
            )

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"message": "Registros encontrados", "data": resultados}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al consultar los datos: {str(e)}")
