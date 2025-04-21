# rutas/empleados.py

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from typing import Optional, List
from bd.bd_cliente import bd_cliente

# Accede SIN paréntesis a la colección
coleccion_empleados = bd_cliente["empleados"]

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

def transformar_empleado(doc: dict) -> Empleado:
    return Empleado(
        id=str(doc.get("_id")),
        nombre=doc.get("nombre"),
        identificacion=doc.get("identificacion"),
        cargo=doc.get("cargo"),
        salario=doc.get("salario"),
        fechaIngreso=doc.get("fechaIngreso"),
        tipoContrato=doc.get("tipoContrato"),
    )

ruta_empleado = APIRouter(
    prefix="/empleados",
    tags=["Empleados"],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}}
)

@ruta_empleado.get("/", response_model=List[Empleado])
async def getEmpleados():
    cursor = coleccion_empleados.find()          # → método, no colección()
    return [transformar_empleado(doc) for doc in cursor]

@ruta_empleado.get("/buscar", response_model=Empleado)
async def getEmpleadoPorIdentificacion(identificacion: str):
    doc = coleccion_empleados.find_one({"identificacion": identificacion})
    if not doc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Empleado no encontrado")
    return transformar_empleado(doc)
