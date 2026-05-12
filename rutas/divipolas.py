# archivo: rutas/divipolas.py

from fastapi import APIRouter, HTTPException, status, UploadFile, File
from pymongo import MongoClient
from pydantic import BaseModel
from typing import List
import os
import pandas as pd
from io import StringIO, BytesIO

# Conexión MongoDB
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
client = MongoClient(MONGO_URI)
db = client["integra"]
coleccion_divipolas = db["divipolas"]

# Crear índice único para evitar duplicados (divipola)
try:
    coleccion_divipolas.create_index(
        [("divipola", 1)],
        unique=True,
        name="unique_divipola"
    )
except Exception as e:
    print(f"Advertencia: No se pudo crear índice único: {e}")

# Router
ruta_divipolas = APIRouter(
    prefix="/divipolas",
    tags=["Divipolas"],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}},
)

# Esquema Pydantic
class Divipola(BaseModel):
    divipola: str
    ruta: str

# Modelo de salida
def modelo_divipola(d: dict) -> dict:
    return {
        "id": str(d.get("_id", "")),
        "divipola": d.get("divipola", ""),
        "ruta": d.get("ruta", ""),
    }

# Listar todas las divipolas
@ruta_divipolas.get("/", response_model=List[dict])
async def obtener_divipolas():
    docs = coleccion_divipolas.find().sort("divipola", 1)
    return [modelo_divipola(d) for d in docs]

# Crear divipola individual
@ruta_divipolas.post("/", response_model=dict, status_code=status.HTTP_201_CREATED)
async def crear_divipola(data: Divipola):
    divipola = data.divipola.strip()

    if coleccion_divipolas.find_one({"divipola": divipola}):
        raise HTTPException(
            status_code=409,
            detail=f"Ya existe una divipola con el código '{divipola}'. No se permiten duplicados."
        )

    nuevo = {
        "divipola": divipola,
        "ruta": data.ruta.strip()
    }

    result = coleccion_divipolas.insert_one(nuevo)
    nuevo["_id"] = result.inserted_id
    return {"mensaje": "Divipola creada exitosamente", "divipola": modelo_divipola(nuevo)}

# Actualizar divipola
@ruta_divipolas.put("/{divipola_id}", response_model=dict)
async def actualizar_divipola(divipola_id: str, data: Divipola):
    from bson import ObjectId

    if not ObjectId.is_valid(divipola_id):
        raise HTTPException(status_code=400, detail="ID inválido")

    actualiza = {
        "divipola": data.divipola.strip(),
        "ruta": data.ruta.strip()
    }

    result = coleccion_divipolas.update_one(
        {"_id": ObjectId(divipola_id)},
        {"$set": actualiza}
    )

    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Divipola no encontrada")

    return {"mensaje": "Divipola actualizada", "divipola": actualiza}

# Eliminar divipola
@ruta_divipolas.delete("/{divipola_id}", response_model=dict)
async def eliminar_divipola(divipola_id: str):
    from bson import ObjectId

    if not ObjectId.is_valid(divipola_id):
        raise HTTPException(status_code=400, detail="ID inválido")

    result = coleccion_divipolas.delete_one({"_id": ObjectId(divipola_id)})

    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Divipola no encontrada")

    return {"mensaje": "Divipola eliminada exitosamente"}

# Cargar masivamente desde Excel
@ruta_divipolas.post("/cargar-masivo", response_model=dict)
async def cargar_divipolas_masivo(archivo: UploadFile = File(...)):
    logger.info(f"=== INICIO CARGA MASIVA DIVIPOLAS ===")
    logger.info(f"Archivo recibido: {archivo.filename}")

    try:
        nombre_archivo = archivo.filename.lower()
        logger.info(f"Tipo de archivo: {nombre_archivo}")

        if nombre_archivo.endswith(('.xlsx', '.xls')):
            logger.info("Leyendo archivo Excel...")
            df = pd.read_excel(archivo.file, engine='openpyxl' if nombre_archivo.endswith('.xlsx') else 'xlrd')
        else:
            logger.info("Leyendo archivo CSV...")
            contenido = await archivo.read()
            df = pd.read_csv(StringIO(contenido.decode('utf-8')), sep=',', on_bad_lines='skip')

        logger.info(f"Filas leídas: {len(df)}")
        logger.info(f"Columnas: {list(df.columns)}")

        # Normalizar columnas
        df.columns = [col.strip().upper().replace(" ", "_") for col in df.columns]
        logger.info(f"Columnas normalizadas: {list(df.columns)}")

        # Verificar columnas requeridas
        columnas_requeridas = {"DIVIPOLA", "RUTA"}

        if not columnas_requeridas.issubset(df.columns):
            faltantes = columnas_requeridas - set(df.columns)
            logger.error(f"Faltan columnas: {faltantes}")
            raise HTTPException(
                status_code=400,
                detail=f"El archivo debe tener las columnas: {', '.join(columnas_requeridas)}. Faltan: {', '.join(faltantes)}"
            )

        # Verificar duplicados dentro del archivo
        duplicados_archivo = []
        divipolas_vistas = set()
        for idx, row in df.iterrows():
            divipola_key = str(row["DIVIPOLA"]).strip()
            if divipola_key in divipolas_vistas:
                duplicados_archivo.append(f"Fila {idx + 2}: Divipola '{row['DIVIPOLA']}' está duplicada en el archivo")
            divipolas_vistas.add(divipola_key)

        if duplicados_archivo:
            logger.warning(f"Duplicados en archivo: {duplicados_archivo}")
            raise HTTPException(
                status_code=400,
                detail=f"El archivo contiene divipolas duplicadas:\n" + "\n".join(duplicados_archivo[:5])
            )

        # Verificar duplicados contra la base de datos
        duplicados_db = []
        for divipola in divipolas_vistas:
            existe = coleccion_divipolas.find_one({"divipola": divipola})
            if existe:
                duplicados_db.append(f"Divipola '{divipola}' ya existe en la base de datos")

        if duplicados_db:
            logger.warning(f"Duplicados en BD: {duplicados_db}")
            raise HTTPException(
                status_code=409,
                detail=f"Las siguientes divipolas ya existen en el sistema:\n" + "\n".join(duplicados_db[:10])
            )

        registros_exitosos = 0
        registros_errores = 0

        logger.info(f"Insertando {len(df)} registros...")
        for _, row in df.iterrows():
            try:
                registro = {
                    "divipola": str(row["DIVIPOLA"]).strip(),
                    "ruta": str(row["RUTA"]).strip()
                }

                coleccion_divipolas.insert_one(registro)
                registros_exitosos += 1

            except Exception as e:
                logger.error(f"Error insertando registro: {e}")
                if "duplicate key" in str(e):
                    registros_errores += 1
                else:
                    registros_errores += 1

        logger.info(f"Carga finalizada: {registros_exitosos} exitosos, {registros_errores} errores")

        return {
            "mensaje": f"Carga completada. {registros_exitosos} registros procesados exitosamente, {registros_errores} con errores",
            "exitosos": registros_exitosos,
            "errores": registros_errores
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error general en carga masiva: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al procesar el archivo: {str(e)}")

# Descargar plantilla de Excel
@ruta_divipolas.get("/descargar-plantilla")
async def descargar_plantilla():
    from fastapi.responses import Response
    from datetime import datetime
    import io

    columnas = {
        "DIVIPOLA": [],
        "RUTA": []
    }

    df = pd.DataFrame(columnas)
    buffer = io.BytesIO()

    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Divipolas')
        worksheet = writer.sheets['Divipolas']
        for idx, col in enumerate(df.columns, 1):
            columna_letra = chr(64 + idx)
            worksheet.column_dimensions[columna_letra].width = 18

    buffer.seek(0)
    excel_data = buffer.getvalue()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nombre_archivo = f"plantilla_divipolas_{timestamp}.xlsx"

    return Response(
        content=excel_data,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename={nombre_archivo}",
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0"
        }
    )

# Descargar todas las divipolas en Excel
@ruta_divipolas.get("/descargar-excel")
async def descargar_excel():
    from fastapi.responses import Response
    from datetime import datetime
    import io

    docs = list(coleccion_divipolas.find().sort("divipola", 1))

    datos = []
    for doc in docs:
        datos.append({
            "DIVIPOLA": doc.get("divipola", ""),
            "RUTA": doc.get("ruta", "")
        })

    df = pd.DataFrame(datos)

    buffer = io.BytesIO()

    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Divipolas')
        worksheet = writer.sheets['Divipolas']
        for idx, col in enumerate(df.columns, 1):
            columna_letra = chr(64 + idx)
            worksheet.column_dimensions[columna_letra].width = 18

    buffer.seek(0)
    excel_data = buffer.getvalue()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nombre_archivo = f"divipolas_{timestamp}.xlsx"

    return Response(
        content=excel_data,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename={nombre_archivo}",
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0"
        }
    )
