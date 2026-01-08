from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from rutas.aut2 import ruta_usuario
from rutas.pagoSaldos import ruta_manifiestos
from rutas.novedades import ruta_novedades
from rutas.vehiculos import ruta_vehiculos
from rutas.empleados import ruta_empleado
from rutas.revision import ruta_revision
from rutas.puente_biometrico import ruta_biometria
from rutas.baseusuarios import ruta_baseusuarios
from rutas.clientes import ruta_clientes
from rutas.clientes_siscore import ruta_clientes_siscore
from rutas.fletes import ruta_fletes
from rutas.pedidos import ruta_pedidos
from rutas.consultar_biometrico import ruta_verificacion

app = FastAPI()
app.title = "integra"
app.version = "1"

# Configuración de CORS
# Se incluyen dominios de producción y localhost para desarrollo
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",       
        "http://127.0.0.1:5173", 
        "https://integralogistica.com",   
        "https://www.integralogistica.com", 
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
)

app.include_router(ruta_usuario)
app.include_router(ruta_manifiestos)
app.include_router(ruta_novedades)
app.include_router(ruta_vehiculos)
app.include_router(ruta_empleado)
app.include_router(ruta_biometria)
app.include_router(ruta_baseusuarios)
app.include_router(ruta_clientes)
app.include_router(ruta_clientes_siscore)
app.include_router(ruta_fletes)
app.include_router(ruta_pedidos)
app.include_router(ruta_verificacion)
app.include_router(ruta_revision)

@app.get("/", tags=['Home'])
async def root():
    return {"message": "Hello integra"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)