from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from typing import Optional, List
from bson import ObjectId
from bd.bd_cliente import bd_cliente

# Accedemos correctamente a la colección
coleccion_empleados = bd_cliente["empleados"]

# Modelo de respuesta para empleados
class Empleado(BaseModel):
    id: Optional[str]
    nombre: Optional[str]
    identificacion: str
    cargo: Optional[str]
    salario: Optional[int]
    fechaIngreso: Optional[str]
    tipoContrato: Optional[str]

    class Config:
        orm_mode = True

# Función para transformar un documento de Mongo a un Empleado válido
def transformar_empleado(doc):
    return Empleado(
        id=str(doc.get("_id")),
        nombre=doc.get("nombre"),
        identificacion=doc.get("identificacion"),
        cargo=doc.get("cargo"),
        salario=doc.get("salario"),
        fechaIngreso=doc.get("fechaIngreso"),
        tipoContrato=doc.get("tipoContrato")
    )

# Ruta y configuración del router
ruta_empleado = APIRouter(
    prefix="/empleados",
    tags=["Empleados"],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}}
)

# Endpoint: Obtener todos los empleados
@ruta_empleado.get("/", response_model=List[Empleado])
async def getEmpleados():
    empleados_cursor = coleccion_empleados.find()
    empleados = [transformar_empleado(doc) for doc in empleados_cursor]
    return empleados

# Endpoint: Buscar empleado por identificación
@ruta_empleado.get("/buscar", response_model=Empleado)
async def getEmpleadoPorIdentificacion(identificacion: str):
    empleado = coleccion_empleados.find_one({"identificacion": identificacion})
    if not empleado:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Empleado no encontrado")
    return transformar_empleado(empleado)
