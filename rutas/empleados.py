from fastapi import APIRouter, HTTPException, status
from bd.schemas.empleado import Empleado
from bd.bd_cliente import bd_cliente
from bd.models.empleado import modelo_empleado, modelo_empleados

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
