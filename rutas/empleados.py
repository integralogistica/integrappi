import os
import math
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from typing import Optional, List
from pymongo import MongoClient

# ——— Configuración de MongoDB ———
mongo_uri = os.getenv("MONGO_URI")
if not mongo_uri:
    raise ValueError("La variable de entorno MONGO_URI no está configurada.")

client = MongoClient(mongo_uri)
db = client["integra"]
coleccion_empleados = db["empleados"]

# ——— Modelo Pydantic actualizado ———
class Empleado(BaseModel):
    id: Optional[str]
    codigoVulcano: Optional[str]
    identificacion: str
    nombre: Optional[str]
    cargo: Optional[str]
    tipoContrato: Optional[str]
    fechaIngreso: Optional[str]
    basico: Optional[float]
    auxilioVivienda: Optional[float]
    auxilioAlimentacion: Optional[float]
    auxilioMovilidad: Optional[float]
    auxilioRodamiento: Optional[float]
    auxilioProductividad: Optional[float]
    auxilioComunic: Optional[float]
    correo: Optional[str]

    class Config:
        orm_mode = True

# ——— Función de transformación adaptada ———
def transformar_empleado(doc: dict) -> Empleado:
    # Helper para obtener el primer valor no None de varias claves
    get = lambda *keys: next((doc.get(k) for k in keys if k in doc and doc.get(k) is not None), None)
    # Para convertir a float o devolver None
    def get_float(*keys):
        val = get(*keys)
        try:
            return float(val)
        except:
            return None

    # Manejo de fecha
    fecha_raw = get('FECHA INGRESO', 'FECHA_INGRESO')
    if fecha_raw:
        fecha_ing = fecha_raw.isoformat() if hasattr(fecha_raw, 'isoformat') else str(fecha_raw)
    else:
        fecha_ing = None

    return Empleado(
        id=str(doc.get('_id')),
        codigoVulcano=str(get('CODIGO VULCANO', 'CODIGO_VULCANO')) if get('CODIGO VULCANO', 'CODIGO_VULCANO') else None,
        identificacion=str(get('IDENTIFICACIÓN', 'IDENTIFICACION') or ""),
        nombre=get('NOMBRE', 'nombre'),
        cargo=get('CARGO', 'cargo'),
        tipoContrato=get('TIPO DE CONTRATO', 'TIPO_DE_CONTRATO', 'tipo_contrato'),
        fechaIngreso=fecha_ing,
        basico=get_float('BASICO ', 'BASICO'),
        auxilioVivienda=get_float('AUXILIO VIVIENDA ', 'AUXILIO_VIVIENDA'),
        auxilioAlimentacion=get_float('AUXILIO ALIMENTA', 'AUXILIO_ALIMENTA'),
        auxilioMovilidad=get_float('AUXILIO DE MOVILIDAD', 'AUXILIO_DE_MOVILIDAD'),
        auxilioRodamiento=get_float('AUXILIO RODAMIENTO ', 'AUXILIO_RODAMIENTO'),
        auxilioProductividad=get_float('AUXILIO DE PRODUCTIVIDAD', 'AUXILIO_DE_PRODUCTIVIDAD'),
        auxilioComunic=get_float('AUXILIO COMUNIC', 'AUXILIO_COMUNIC'),
        correo=get('CORREO', 'correo')
    )

# ——— Router de empleados ———
ruta_empleado = APIRouter(
    prefix="/empleados",
    tags=["Empleados"],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}},
)

@ruta_empleado.get("/", response_model=List[Empleado])
async def get_empleados():
    docs = coleccion_empleados.find()
    return [transformar_empleado(doc) for doc in docs]

@ruta_empleado.get("/buscar", response_model=Empleado)
async def get_empleado_por_identificacion(identificacion: str):
    # Intentar como número y como cadena
    try:
        num = float(identificacion)
    except:
        num = None
    doc = None
    if num is not None:
        doc = coleccion_empleados.find_one({"IDENTIFICACIÓN": num})
    if not doc:
        doc = coleccion_empleados.find_one({"IDENTIFICACIÓN": identificacion})
    if not doc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Empleado no encontrado")
    return transformar_empleado(doc)
