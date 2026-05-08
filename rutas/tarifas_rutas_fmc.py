# archivo: rutas/tarifas_rutas_fmc.py

from fastapi import APIRouter, HTTPException, status, UploadFile, File
from pymongo import MongoClient
from pydantic import BaseModel
from typing import List
import os
import pandas as pd
from io import StringIO, BytesIO

# ------------------------------
# 🔗 Conexión MongoDB
# ------------------------------
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
client = MongoClient(MONGO_URI)
db = client["integra"]
coleccion_tarifas = db["fletes_rutas_fmc"]

# Crear índice único para evitar duplicados (centro_costo + ruta)
try:
    coleccion_tarifas.create_index(
        [("centro_costo", 1), ("ruta", 1)],
        unique=True,
        name="unique_centro_costo_ruta"
    )
except Exception as e:
    print(f"Advertencia: No se pudo crear índice único: {e}")

# ------------------------------
# 🚦 Configuración Router
# ------------------------------
ruta_tarifas_rutas_fmc = APIRouter(
    prefix="/fletes-rutas-fmc",
    tags=["Tarifas Rutas FMC"],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}},
)

# ------------------------------
# 📌 Esquema Pydantic
# ------------------------------
class TarifaRutaFmc(BaseModel):
    centro_costo: str
    ruta: str
    carry: float
    nhr: float
    turbo: float
    nies: float
    sencillo: float
    patineta: float
    tractomula: float
    requiere_descargue: str

# ------------------------------
# 📌 Modelo de salida
# ------------------------------
def modelo_tarifa(t: dict) -> dict:
    return {
        "id": str(t.get("_id", "")),
        "centro_costo": t.get("centro_costo", ""),
        "ruta": t.get("ruta", ""),
        "carry": t.get("carry", 0),
        "nhr": t.get("nhr", 0),
        "turbo": t.get("turbo", 0),
        "nies": t.get("nies", 0),
        "sencillo": t.get("sencillo", 0),
        "patineta": t.get("patineta", 0),
        "tractomula": t.get("tractomula", 0),
        "requiere_descargue": t.get("requiere_descargue", "NO"),
    }

# ------------------------------
# ✅ Listar todas las tarifas
# ------------------------------
@ruta_tarifas_rutas_fmc.get("/", response_model=List[dict])
async def obtener_tarifas():
    docs = coleccion_tarifas.find()
    return [modelo_tarifa(t) for t in docs]

# ------------------------------
# ✅ Crear tarifa individual
# ------------------------------
@ruta_tarifas_rutas_fmc.post("/", response_model=dict, status_code=status.HTTP_201_CREATED)
async def crear_tarifa(data: TarifaRutaFmc):
    # Verificar duplicado por centro_costo y ruta
    cc = data.centro_costo.upper().strip()
    ruta = data.ruta.upper().strip()

    if coleccion_tarifas.find_one({"centro_costo": cc, "ruta": ruta}):
        raise HTTPException(
            status_code=409,
            detail=f"Ya existe una tarifa para la RUTA '{ruta}' con CENTRO DE COSTO '{cc}'. No se permiten rutas duplicadas."
        )

    nuevo = {
        "centro_costo": cc,
        "ruta": ruta,
        "carry": float(data.carry),
        "nhr": float(data.nhr),
        "turbo": float(data.turbo),
        "nies": float(data.nies),
        "sencillo": float(data.sencillo),
        "patineta": float(data.patineta),
        "tractomula": float(data.tractomula),
        "requiere_descargue": data.requiere_descargue.upper().strip(),
    }

    result = coleccion_tarifas.insert_one(nuevo)
    nuevo["_id"] = result.inserted_id
    return {"mensaje": "Tarifa creada exitosamente", "tarifa": modelo_tarifa(nuevo)}

# ------------------------------
# ✅ Actualizar tarifa
# ------------------------------
@ruta_tarifas_rutas_fmc.put("/{tarifa_id}", response_model=dict)
async def actualizar_tarifa(tarifa_id: str, data: TarifaRutaFmc):
    from bson import ObjectId

    if not ObjectId.is_valid(tarifa_id):
        raise HTTPException(status_code=400, detail="ID inválido")

    actualiza = {
        "centro_costo": data.centro_costo.upper().strip(),
        "ruta": data.ruta.upper().strip(),
        "carry": float(data.carry),
        "nhr": float(data.nhr),
        "turbo": float(data.turbo),
        "nies": float(data.nies),
        "sencillo": float(data.sencillo),
        "patineta": float(data.patineta),
        "tractomula": float(data.tractomula),
        "requiere_descargue": data.requiere_descargue.upper().strip(),
    }

    result = coleccion_tarifas.update_one(
        {"_id": ObjectId(tarifa_id)},
        {"$set": actualiza}
    )

    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Tarifa no encontrada")

    return {"mensaje": "Tarifa actualizada", "tarifa": actualiza}

# ------------------------------
# ✅ Eliminar tarifa
# ------------------------------
@ruta_tarifas_rutas_fmc.delete("/{tarifa_id}", response_model=dict)
async def eliminar_tarifa(tarifa_id: str):
    from bson import ObjectId

    if not ObjectId.is_valid(tarifa_id):
        raise HTTPException(status_code=400, detail="ID inválido")

    result = coleccion_tarifas.delete_one({"_id": ObjectId(tarifa_id)})

    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Tarifa no encontrada")

    return {"mensaje": "Tarifa eliminada exitosamente"}

# ------------------------------
# ✅ Cargar tarifas masivamente desde archivo (Excel o plano)
# ------------------------------
@ruta_tarifas_rutas_fmc.post("/cargar-masivo", response_model=dict)
async def cargar_tarifas_masivo(archivo: UploadFile = File(...)):
    try:
        nombre_archivo = archivo.filename.lower()

        # Leer según el tipo de archivo
        if nombre_archivo.endswith(('.xlsx', '.xls')):
            # Archivo Excel
            df = pd.read_excel(archivo.file, engine='openpyxl' if nombre_archivo.endswith('.xlsx') else 'xlrd')
        else:
            # Archivo de texto (CSV, TSV, etc.)
            contenido = await archivo.read()

            # Intentar con tabulador
            df = pd.read_csv(StringIO(contenido.decode('utf-8')), sep='\t', on_bad_lines='skip')

            # Si no funciona, intentar con coma
            if df.empty:
                df = pd.read_csv(StringIO(contenido.decode('utf-8')), sep=',', on_bad_lines='skip')

            # Si aún está vacío, detectar automáticamente
            if df.empty:
                df = pd.read_csv(StringIO(contenido.decode('utf-8')), sep=None, engine='python', on_bad_lines='skip')

        # Normalizar columnas
        df.columns = [col.strip().upper().replace(" ", "_") for col in df.columns]

        # Verificar columnas requeridas
        columnas_requeridas = {"CENTRO_COSTO", "RUTA", "CARRY", "NHR", "TURBO", "NIES", "SENCILLO", "PATINETA", "TRACTOMULA", "REQUIERE_DESCARGUE"}

        if not columnas_requeridas.issubset(df.columns):
            faltantes = columnas_requeridas - set(df.columns)
            raise HTTPException(
                status_code=400,
                detail=f"El archivo debe tener las columnas: {', '.join(columnas_requeridas)}. Faltan: {', '.join(faltantes)}"
            )

        # Verificar duplicados dentro del archivo primero
        duplicados_archivo = []
        rutas_vistas = set()
        for idx, row in df.iterrows():
            ruta_key = (str(row["CENTRO_COSTO"]).strip().upper(), str(row["RUTA"]).strip().upper())
            if ruta_key in rutas_vistas:
                duplicados_archivo.append(f"Fila {idx + 2}: Ruta '{row['RUTA']}' con centro de costo '{row['CENTRO_COSTO']}' está duplicada en el archivo")
            rutas_vistas.add(ruta_key)

        if duplicados_archivo:
            raise HTTPException(
                status_code=400,
                detail=f"El archivo contiene rutas duplicadas:\n" + "\n".join(duplicados_archivo[:5]) +
                       ("\n... y más" if len(duplicados_archivo) > 5 else "")
            )

        # Verificar duplicados contra la base de datos
        duplicados_db = []
        for cc, ruta in rutas_vistas:
            existe = coleccion_tarifas.find_one({"centro_costo": cc, "ruta": ruta})
            if existe:
                duplicados_db.append(f"Ruta '{ruta}' con centro de costo '{cc}' ya existe en la base de datos")

        if duplicados_db:
            raise HTTPException(
                status_code=409,
                detail=f"Las siguientes rutas ya existen en el sistema:\n" + "\n".join(duplicados_db[:10]) +
                       ("\n... y más" if len(duplicados_db) > 10 else "") +
                       "\n\nElimine o modifique estos registros del archivo e intente nuevamente."
            )

        registros_exitosos = 0
        registros_errores = 0
        detalles = []

        for _, row in df.iterrows():
            try:
                registro = {
                    "centro_costo": str(row["CENTRO_COSTO"]).strip().upper(),
                    "ruta": str(row["RUTA"]).strip().upper(),
                    "carry": float(row["CARRY"]) if pd.notna(row["CARRY"]) else 0,
                    "nhr": float(row["NHR"]) if pd.notna(row["NHR"]) else 0,
                    "turbo": float(row["TURBO"]) if pd.notna(row["TURBO"]) else 0,
                    "nies": float(row["NIES"]) if pd.notna(row["NIES"]) else 0,
                    "sencillo": float(row["SENCILLO"]) if pd.notna(row["SENCILLO"]) else 0,
                    "patineta": float(row["PATINETA"]) if pd.notna(row["PATINETA"]) else 0,
                    "tractomula": float(row["TRACTOMULA"]) if pd.notna(row["TRACTOMULA"]) else 0,
                    "requiere_descargue": str(row["REQUIERE_DESCARGUE"]).strip().upper() if pd.notna(row["REQUIERE_DESCARGUE"]) else "NO",
                }

                # Insertar nuevo registro (no actualiza si existe)
                coleccion_tarifas.insert_one(registro)

                registros_exitosos += 1
                detalles.append({"estado": "ok", "ruta": registro["ruta"], "centro_costo": registro["centro_costo"]})

            except Exception as e:
                # Si hay error de duplicado de MongoDB, continuar
                if "duplicate key" in str(e):
                    registros_errores += 1
                    detalles.append({
                        "estado": "error",
                        "ruta": str(row.get("RUTA", "N/A")),
                        "error": "Ruta duplicada"
                    })
                else:
                    registros_errores += 1
                    detalles.append({
                        "estado": "error",
                        "ruta": str(row.get("RUTA", "N/A")),
                        "error": str(e)
                    })

        return {
            "mensaje": f"Carga completada. {registros_exitosos} registros procesados exitosamente, {registros_errores} con errores",
            "exitosos": registros_exitosos,
            "errores": registros_errores,
            "detalles": detalles if registros_errores > 0 else []
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al procesar el archivo: {str(e)}")

# ------------------------------
# ✅ Descargar plantilla de tarifas en Excel (solo encabezados)
# ------------------------------
@ruta_tarifas_rutas_fmc.get("/descargar-plantilla")
async def descargar_plantilla():
    from fastapi.responses import Response
    from datetime import datetime
    import io

    # Crear DataFrame solo con los encabezados (sin datos de ejemplo)
    columnas = {
        "CENTRO_COSTO": [],
        "RUTA": [],
        "CARRY": [],
        "NHR": [],
        "TURBO": [],
        "NIES": [],
        "SENCILLO": [],
        "PATINETA": [],
        "TRACTOMULA": [],
        "REQUIERE_DESCARGUE": []
    }

    df = pd.DataFrame(columnas)

    # Crear buffer en memoria
    buffer = io.BytesIO()

    # Escribir el Excel directamente en el buffer
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Tarifas')

        # Dar formato a los encabezados
        worksheet = writer.sheets['Tarifas']
        for idx, col in enumerate(df.columns, 1):
            columna_letra = chr(64 + idx)
            worksheet.column_dimensions[columna_letra].width = 18

    # Obtener el valor del buffer
    buffer.seek(0)
    excel_data = buffer.getvalue()

    # Generar nombre con timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nombre_archivo = f"plantilla_tarifas_rutas_{timestamp}.xlsx"

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
