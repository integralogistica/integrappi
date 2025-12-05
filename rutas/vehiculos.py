import os
from io import BytesIO
from typing import List, Optional
from uuid import uuid4

from dotenv import load_dotenv
from fastapi import APIRouter, File, Form, HTTPException, UploadFile, status
from fastapi.responses import JSONResponse
from google.cloud import storage
from PIL import Image
from pymongo import MongoClient

# Cargar variables de entorno
load_dotenv()

# ==========================================
# 1. CONFIGURACIÓN
# ==========================================

# MongoDB
mongo_uri = os.getenv("MONGO_URI")
if not mongo_uri:
    raise ValueError("La variable de entorno MONGO_URI no está configurada.")

bd_cliente = MongoClient(mongo_uri)
bd = bd_cliente['integra']
coleccion_vehiculos = bd['vehiculos']

# Google Cloud Storage
BUCKET_NAME = "integrapp"
CARPETA_STORAGE = "Vehiculos"

google_credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if google_credentials_path:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = google_credentials_path

# Router
ruta_vehiculos = APIRouter(
    prefix="/vehiculos",
    tags=['Vehiculos'],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}}
)


# ==========================================
# 2. FUNCIONES AUXILIARES
# ==========================================

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

def subir_a_google_storage(archivo: UploadFile, nombre_archivo: str) -> str:
    try:
        cliente = storage.Client()
        bucket = cliente.bucket(BUCKET_NAME)
        ruta_archivo = f"{CARPETA_STORAGE}/{nombre_archivo}"

        # Detectar si es imagen para optimizar, sino subir tal cual (ej. PDF)
        if archivo.content_type.startswith("image/"):
            archivo_optimizado = optimizar_imagen(archivo)
            blob = bucket.blob(ruta_archivo)
            blob.upload_from_file(archivo_optimizado, content_type="image/webp")
        else:
            blob = bucket.blob(ruta_archivo)
            archivo.file.seek(0)
            blob.upload_from_file(archivo.file, content_type=archivo.content_type)

        return f"https://storage.googleapis.com/{BUCKET_NAME}/{ruta_archivo}"
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al subir el archivo a Google Storage: {str(e)}")

def eliminar_de_google_storage(url: str):
    try:
        cliente = storage.Client()
        bucket = cliente.bucket(BUCKET_NAME)
        nombre_archivo = url.split(f"https://storage.googleapis.com/{BUCKET_NAME}/")[-1]
        blob = bucket.blob(nombre_archivo)
        blob.delete()
    except Exception as e:
        # Solo imprimimos el error para no detener el flujo si el archivo ya no existe
        print(f"Advertencia al eliminar archivo: {str(e)}")


# ==========================================
# 3. ENDPOINTS
# ==========================================

@ruta_vehiculos.post("/crear")
async def crear_vehiculo(id_usuario: str = Form(...), placa: str = Form(...)):
    placa_limpia = placa.strip().upper()
    if coleccion_vehiculos.find_one({"placa": placa_limpia}):
        raise HTTPException(status_code=400, detail="La placa ya está registrada.")
    
    nuevo_vehiculo = {
        "idUsuario": id_usuario,
        "placa": placa_limpia,
        "estadoIntegra": "registro_incompleto",
        "estudioSeguridad": None,
        "usuarioIntegra": None,
        "fotos": [],
        "tarjetaPropiedad": None,
        "soat": None,
        "revisionTecnomecanica": None,
        "tarjetaRemolque": None,
        "polizaResponsabilidad": None,
        "documentoIdentidadConductor": None,
        "condFoto": None,
        "licencia": None,
        "planillaEpsArl": None,
        "documentoIdentidadTenedor": None,
        "condCertificacionBancaria": None,
        "propCertificacionBancaria": None,
        "tenedCertificacionBancaria": None,
        "documentoAcreditacionTenedor": None,
        "rutTenedor": None,
        "documentoIdentidadPropietario": None,
        "rutPropietario": None,
        "vehMarca": None,
    }
    
    coleccion_vehiculos.insert_one(nuevo_vehiculo)
    print(f" Vehículo creado: {placa_limpia} para usuario {id_usuario} con estado registro_incompleto")
    return JSONResponse(status_code=status.HTTP_201_CREATED, content={"message": "Vehículo registrado exitosamente"})


@ruta_vehiculos.get("/obtener-vehiculos")
def obtener_vehiculos(id_usuario: str, estadoIntegra: Optional[str] = None):
    print(f" Buscando vehículos para ID: {id_usuario} - Estado: {estadoIntegra}")
    
    filtro = {"idUsuario": id_usuario}
    if estadoIntegra:
        filtro["estadoIntegra"] = estadoIntegra

    vehiculos = list(coleccion_vehiculos.find(filtro, {"_id": 0}))
    
    print(f" Se encontraron {len(vehiculos)} vehículos")
    
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"message": "Búsqueda finalizada", "vehiculos": vehiculos}
    )


@ruta_vehiculos.get("/obtener-vehiculo/{placa}")
async def obtener_vehiculo(placa: str):
    vehiculo = coleccion_vehiculos.find_one({"placa": placa}, {"_id": 0})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")

    return JSONResponse(status_code=status.HTTP_200_OK, content={"message": "Vehículo encontrado", "data": vehiculo})


@ruta_vehiculos.put("/actualizar-estado")
async def actualizar_estado(
    placa: str = Form(...),
    nuevo_estado: str = Form(...),
    usuario_id: str = Form(...),
    observaciones: Optional[str] = Form(None) 
):
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")
    datos_actualizar = {
        "estadoIntegra": nuevo_estado,
        "usuarioIntegra": usuario_id
    }

    if observaciones:
        datos_actualizar["observaciones"] = observaciones

    coleccion_vehiculos.update_one(
        {"placa": placa},
        {"$set": datos_actualizar}
    )

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "message": f"Estado actualizado a '{nuevo_estado}' por el usuario '{usuario_id}'"
        }
    )


@ruta_vehiculos.put("/actualizar-informacion/{placa}")
async def actualizar_informacion_vehiculo(placa: str, datos: dict):
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")
    
    coleccion_vehiculos.update_one({"placa": placa}, {"$set": datos})
    return JSONResponse(status_code=200, content={"message": "Información actualizada"})


@ruta_vehiculos.put("/subir-estudio-seguridad")
async def subir_estudio_seguridad(
    archivo: UploadFile = File(...),
    placa: str = Form(...)
):
    placa_limpia = placa.strip().upper()
    print(f"🚀 Iniciando subida de estudio para placa: '{placa_limpia}'")

    # Validar existencia
    vehiculo = coleccion_vehiculos.find_one({"placa": placa_limpia})
    if not vehiculo:
        print(f"❌ Error: No se encontró el vehículo con placa {placa_limpia}")
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")

    # Validar tipo de archivo
    print(f"Tipo de archivo recibido: {archivo.content_type}")
    if archivo.content_type == "application/pdf":
        extension = "pdf"
    elif archivo.content_type.startswith("image/"):
        extension = "webp"
    else:
        raise HTTPException(status_code=400, detail="Solo se permiten archivos PDF o Imágenes.")

    nombre_archivo = f"EstudioSeguridad_{placa_limpia}_{uuid4().hex[:8]}.{extension}"

    try:
        url_archivo = subir_a_google_storage(archivo, nombre_archivo)
        print(f"Archivo subido a Cloud Storage: {url_archivo}")
        
        resultado = coleccion_vehiculos.update_one(
            {"placa": placa_limpia},
            {"$set": {"estudioSeguridad": url_archivo}}
        )

        if resultado.modified_count == 0:
            print(" Advertencia: MongoDB encontró el vehículo pero no modificó nada.")
        else:
            print(" Éxito: Base de datos actualizada con el campo estudioSeguridad")

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": "Estudio de seguridad subido correctamente",
                "url": url_archivo
            }
        )
    except Exception as e:
        print(f"🔥 Error crítico: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al procesar el archivo: {str(e)}")


@ruta_vehiculos.put("/subir-documento")
async def subir_documento(archivo: UploadFile, placa: str = Form(...), tipo: str = Form(...)):
    tipos_validos = [
        "tarjetaPropiedad", "soat", "revisionTecnomecanica", "tarjetaRemolque",
        "polizaResponsabilidad", "documentoIdentidadConductor", "documentoIdentidadPropietario",
        "documentoIdentidadTenedor", "licencia", "planillaEpsArl", "condFoto",
        "condCertificacionBancaria", "propCertificacionBancaria", "tenedCertificacionBancaria",
        "documentoAcreditacionTenedor", "rutTenedor", "rutPropietario"
    ]
    
    if tipo not in tipos_validos:
        raise HTTPException(status_code=400, detail="Tipo de documento no válido.")

    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")

    if archivo.content_type.startswith("image/"):
        extension = "webp"
    elif archivo.content_type == "application/pdf":
        extension = "pdf"
    else:
        raise HTTPException(status_code=400, detail="Solo se permiten archivos de imagen o PDF.")
    
    nombre_archivo = f"{tipo}_{placa}.{extension}"
    url_archivo = subir_a_google_storage(archivo, nombre_archivo)
    
    coleccion_vehiculos.update_one({"placa": placa}, {"$set": {tipo: url_archivo}})
    
    return JSONResponse(
        status_code=status.HTTP_200_OK, 
        content={"message": f"{tipo} subido correctamente", "url": url_archivo}
    )


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


@ruta_vehiculos.delete("/eliminar-documento")
async def eliminar_documento(placa: str, tipo: str):
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo or not vehiculo.get(tipo):
        raise HTTPException(status_code=404, detail="Documento no encontrado.")
    
    eliminar_de_google_storage(vehiculo[tipo])
    coleccion_vehiculos.update_one({"placa": placa}, {"$set": {tipo: None}})
    
    return JSONResponse(status_code=status.HTTP_200_OK, content={"message": f"{tipo} eliminado correctamente"})


@ruta_vehiculos.delete("/eliminar-foto")
async def eliminar_foto(placa: str, url: str):
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo or url not in vehiculo["fotos"]:
        raise HTTPException(status_code=404, detail="Foto no encontrada.")
    
    eliminar_de_google_storage(url)
    coleccion_vehiculos.update_one({"placa": placa}, {"$pull": {"fotos": url}})
    
    return JSONResponse(status_code=status.HTTP_200_OK, content={"message": "Foto eliminada correctamente"})

@ruta_vehiculos.get("/obtener-vehiculos-incompletos")
def obtener_vehiculos_incompletos(id_usuario: Optional[str] = None):
    print(f"🔍 [Panel Revisión] Buscando vehículos para revisar. Usuario: {id_usuario}")
    filtro = {
        "estadoIntegra": {
            "$in": ["registro_incompleto", "completado_revision", "aprobado", "rechazado"]
        }
    }
    
    if id_usuario:
        pass 

    vehiculos_raw = list(coleccion_vehiculos.find(filtro))

    if not vehiculos_raw:
        return JSONResponse(
            status_code=status.HTTP_200_OK, 
            content={"message": "No hay vehículos", "vehicles": []}
        )

    vehiculos_final = []
    for veh in vehiculos_raw:
        # Convertir ObjectId a string para que no falle el JSON
        veh["_id"] = str(veh["_id"])
        # Agrupar documentos en un sub-diccionario para mejor organización
        documentos = {
            k: v for k, v in veh.items()
            if isinstance(v, str) and v.startswith("https://storage.googleapis.com") and k != "estudioSeguridad"
        }
        veh["documentos"] = documentos
        vehiculos_final.append(veh)

    print(f"✅ Se enviaron {len(vehiculos_final)} vehículos al panel.")

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"message": "Vehículos encontrados", "vehicles": vehiculos_final}
    )


@ruta_vehiculos.get("/obtener-aprobados-paginados")
def obtener_aprobados_paginados(
    search: Optional[str] = None, 
    limit: int = 10
):
    print(f"🔍 [Backend] Buscando aprobados. Query: '{search}', Limite: {limit}")

    # 1. Filtro base: Solo los aprobados
    filtro = {"estadoIntegra": "aprobado"}
    # 2. Filtro de búsqueda (si aplica)
    if search and search.strip():
        search_regex = {"$regex": search.strip(), "$options": "i"} 
        filtro["$or"] = [
            {"placa": search_regex},
            {"condCedulaCiudadania": search_regex}
        ]
    # 3. Consulta con paginación
    vehiculos_cursor = coleccion_vehiculos.find(filtro).sort("_id", -1).limit(limit)
    vehiculos_final = []
    
    # Procesamos solo los 10 (o los que coincidan)
    for veh in vehiculos_cursor:
        veh["_id"] = str(veh["_id"])
        
        # Agrupar documentos (Lógica visual)
        documentos = {
            k: v for k, v in veh.items()
            if isinstance(v, str) and v.startswith("https://storage.googleapis.com") and k != "estudioSeguridad"
        }
        veh["documentos"] = documentos
        
        vehiculos_final.append(veh)

    return JSONResponse(
        status_code=status.HTTP_200_OK, 
        content={"vehiculos": vehiculos_final}
    )