from fastapi import APIRouter, HTTPException, status
from pymongo.collection import Collection
from pydantic import BaseModel
from typing import Optional

from bd.bd_cliente import bd_cliente
from bd.models.empleado import modelo_empleado, modelo_empleados

# Modelo Pydantic directamente aquí
class Empleado(BaseModel):
    id: Optional[str]
    nombre: Optional[str]
    identificacion: str
    cargo: Optional[str]
    salario: Optional[int]
    fechaIngreso: Optional[str]
    tipoContrato: Optional[str]

# API Router
ruta_empleado = APIRouter(
    prefix="/empleados",
    tags=["Empleados"],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}}
)

@ruta_empleado.get("/", response_model=list[Empleado])
async def getEmpleados():
    return modelo_empleados(bd_cliente.empleados.find())

@ruta_empleado.get("/buscar", response_model=Empleado)
async def getEmpleadoPorIdentificacion(identificacion: str):
    empleado = bd_cliente.empleados.find_one({"identificacion": identificacion})
    if not empleado:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Empleado no encontrado")
    return Empleado(**modelo_empleado(empleado))
