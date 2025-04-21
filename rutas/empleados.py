from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from typing import Optional
from bd.bd_cliente import bd_cliente

# Modelo directo
class Empleado(BaseModel):
    id: Optional[str]
    nombre: Optional[str]
    identificacion: str
    cargo: Optional[str]
    salario: Optional[int]
    fechaIngreso: Optional[str]
    tipoContrato: Optional[str]

# Funciones de conversión
def modelo_empleado(doc: dict) -> dict:
    doc["id"] = str(doc["_id"])
    doc.pop("_id", None)
    return doc

def modelo_empleados(cursor) -> list[dict]:
    return [modelo_empleado(doc) for doc in cursor]

# Rutas
ruta_empleado = APIRouter(
    prefix="/empleados",
    tags=["Empleados"],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}}
)

@ruta_empleado.get("/", response_model=list[Empleado])
async def getEmpleados():
    empleados_cursor = bd_cliente.empleados.find()  # ✅ NO LO LLAMES COMO FUNCIÓN
    return modelo_empleados(empleados_cursor)

@ruta_empleado.get("/buscar", response_model=Empleado)
async def getEmpleadoPorIdentificacion(identificacion: str):
    empleado = bd_cliente.empleados.find_one({"identificacion": identificacion})
    if not empleado:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Empleado no encontrado")
    return Empleado(**modelo_empleado(empleado))
