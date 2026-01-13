from fastapi import APIRouter, HTTPException, status, UploadFile, File
from pymongo import MongoClient
from bson import ObjectId
from pydantic import BaseModel
from typing import List, Optional
import os
import pandas as pd
import re

# ------------------------------
# 🔗 Conexión MongoDB
# ------------------------------
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
client = MongoClient(MONGO_URI)
db = client["integra"]
coleccion_clientes_general = db["clientes_general"]

# ------------------------------
# 🚦 Configuración Router
# ------------------------------
ruta_clientes_general = APIRouter(
    prefix="/clientes-general",
    tags=["Clientes General"],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}},
)

# ------------------------------
# 🧰 Helpers
# ------------------------------
def limpiar_texto(valor) -> str:
    if valor is None:
        return ""
    return str(valor).strip()

def normalizar_columna(col: str) -> str:
    return (
        limpiar_texto(col)
        .lower()
        .replace(" ", "_")
        .replace("dirección", "direccion")
    )

def extraer_destinatario(cliente_destinatario: str) -> str:
    """
    Extrae el número al final del string (permite ruido al final):
    Ej:
      FKC_HOSPITAL CIVIL DE IPIALES E.S.E_57701136        -> 57701136
      FKC_HOSPITAL CIVIL DE IPIALES E.S.E_57701136        -> 57701136
      FKC_..._57701136-1                                  -> 57701136
      FKC_..._57701136   (espacios)                       -> 57701136
    """
    s = limpiar_texto(cliente_destinatario)
    if not s:
        return ""

    # Captura dígitos después de "_" hasta antes de cualquier no-dígito final
    match = re.search(r"_(\d+)\D*$", s)
    return match.group(1) if match else ""

def normalizar_destinatario(valor: str) -> str:
    """
    Normaliza el valor que llega por path param o desde Excel:
    - trim
    - deja solo dígitos (si hay puntos, espacios, guiones)
    """
    s = limpiar_texto(valor)
    if not s:
        return ""
    solo_digitos = re.sub(r"\D", "", s)
    return solo_digitos if solo_digitos else s  # fallback si no eran dígitos

def to_float(valor) -> Optional[float]:
    try:
        if valor is None or str(valor).strip() == "":
            return None
        return float(valor)
    except Exception:
        return None

# ------------------------------
# 📌 Esquema Pydantic
# ------------------------------
class ClienteGeneral(BaseModel):
    nombre: str
    cliente_destinatario: str
    direccion: str
    ciudad: str
    departamento: str
    lat: Optional[float] = None
    lon: Optional[float] = None

# ------------------------------
# 📌 Modelo de salida
# ------------------------------
def modelo_cliente_general(c: dict) -> dict:
    return {
        "id": str(c["_id"]),
        "nombre": c.get("nombre", ""),
        "cliente_destinatario": c.get("cliente_destinatario", ""),
        "destinatario": c.get("destinatario", ""),
        "direccion": c.get("direccion", ""),
        "ciudad": c.get("ciudad", ""),
        "departamento": c.get("departamento", ""),
        "lat": c.get("lat"),
        "lon": c.get("lon"),
    }

# ============================================================
# 🔎 Obtener CLIENTE_DESTINATARIO + GEO por DESTINATARIO (CÉDULA)
# ============================================================
@ruta_clientes_general.get("/por-destinatario/{destinatario}", response_model=dict)
async def obtener_por_destinatario(destinatario: str):
    # Normaliza (por si llega con puntos/espacios)
    destinatario_norm = normalizar_destinatario(destinatario)

    if not destinatario_norm:
        raise HTTPException(status_code=400, detail="Destinatario vacío o inválido")

    # Búsqueda robusta por si existen registros viejos con tipo numérico
    filtros = [{"destinatario": destinatario_norm}]
    if destinatario_norm.isdigit():
        try:
            filtros.append({"destinatario": int(destinatario_norm)})
        except Exception:
            pass

    doc = coleccion_clientes_general.find_one({"$or": filtros})
    if not doc:
        raise HTTPException(status_code=404, detail="Destinatario no encontrado")

    return {
        "cliente_destinatario": doc.get("cliente_destinatario", ""),
        "direccion": doc.get("direccion", ""),
        "ciudad": doc.get("ciudad", ""),
        "departamento": doc.get("departamento", ""),
        "lat": doc.get("lat"),
        "lon": doc.get("lon"),
    }

# ============================================================
# 🔎 Obtener por CLIENTE_DESTINATARIO exacto (case-insensitive)
# ============================================================
@ruta_clientes_general.get("/por-cliente-destinatario/{cliente_destinatario}", response_model=dict)
async def obtener_por_cliente_destinatario(cliente_destinatario: str):
    cliente_destinatario = limpiar_texto(cliente_destinatario)

    doc = coleccion_clientes_general.find_one({
        "cliente_destinatario": {
            "$regex": f"^{re.escape(cliente_destinatario)}$",
            "$options": "i"
        }
    })

    if not doc:
        raise HTTPException(status_code=404, detail="Cliente destinatario no encontrado")

    return modelo_cliente_general(doc)

# ------------------------------
# ✅ Listar todos
# ------------------------------
@ruta_clientes_general.get("/", response_model=List[dict])
async def listar_clientes_general():
    return [modelo_cliente_general(c) for c in coleccion_clientes_general.find()]

# ------------------------------
# ❌ Eliminar por ID
# ------------------------------
@ruta_clientes_general.delete("/{cliente_id}", response_model=dict)
async def eliminar_cliente_general(cliente_id: str):
    try:
        oid = ObjectId(cliente_id)
    except Exception:
        raise HTTPException(status_code=400, detail="ID inválido")

    result = coleccion_clientes_general.delete_one({"_id": oid})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Registro no encontrado")

    return {"mensaje": "Cliente eliminado"}

# ============================================================
# 📦 Carga masiva desde Excel (formato REAL del archivo)
# Columnas requeridas:
# NOMBRE | CLIENTE_DESTINATARIO | DIRECCION | CIUDAD | DEPARTAMENTO | LAT | LON
# ============================================================
@ruta_clientes_general.post("/cargar-masivo", response_model=dict)
async def cargar_clientes_general_masivo(archivo: UploadFile = File(...)):
    try:
        df = pd.read_excel(archivo.file)
        df = df.fillna("")
        df.columns = [normalizar_columna(c) for c in df.columns]

        requeridas = {
            "nombre",
            "cliente_destinatario",
            "direccion",
            "ciudad",
            "departamento",
            "lat",
            "lon",
        }

        if not requeridas.issubset(df.columns):
            faltantes = requeridas - set(df.columns)
            raise HTTPException(status_code=400, detail=f"Columnas faltantes: {faltantes}")

        # Limpieza total previa
        coleccion_clientes_general.delete_many({})

        registros = []
        vistos = set()

        for _, row in df.iterrows():
            cliente_dest = limpiar_texto(row.get("cliente_destinatario", ""))
            cliente_dest = re.sub(r"\s+", " ", cliente_dest).strip()

            destinatario = extraer_destinatario(cliente_dest)
            destinatario = normalizar_destinatario(destinatario)

            if not cliente_dest or not destinatario:
                continue

            # evitar duplicar por destinatario (cédula)
            if destinatario in vistos:
                continue
            vistos.add(destinatario)

            registros.append({
                "nombre": limpiar_texto(row.get("nombre", "")),
                "cliente_destinatario": cliente_dest,
                # GUARDA SIEMPRE COMO STRING NORMALIZADO
                "destinatario": str(destinatario),
                "direccion": limpiar_texto(row.get("direccion", "")),
                "ciudad": limpiar_texto(row.get("ciudad", "")),
                "departamento": limpiar_texto(row.get("departamento", "")),
                "lat": to_float(row.get("lat")),
                "lon": to_float(row.get("lon")),
            })

        if registros:
            coleccion_clientes_general.insert_many(registros)

        return {"mensaje": f"{len(registros)} clientes_general cargados exitosamente"}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
