from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Importación de rutas
from rutas.aut2 import ruta_usuario
from rutas.pagoSaldos import ruta_manifiestos
from rutas.novedades import ruta_novedades
from rutas.vehiculos import ruta_vehiculos
from rutas.empleados import ruta_empleado
from rutas.puente_biometrico import ruta_biometria
from rutas.baseusuarios import ruta_baseusuarios
from rutas.clientes import ruta_clientes
from rutas.fletes import ruta_fletes
from rutas.pedidos import ruta_pedidos


app = FastAPI()
app.title = "integra"
app.version = "1"

# Configuración de CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permite todas las orígenes. Cambia esto para permitir solo orígenes específicos
    allow_credentials=True,
    allow_methods=["*"],  # Permite todos los métodos (GET, POST, PUT, DELETE, etc.)
    allow_headers=["*"],  # Permite todos los encabezados
)

app.include_router(ruta_usuario)
app.include_router(ruta_manifiestos)
app.include_router(ruta_novedades)
app.include_router(ruta_vehiculos)
app.include_router(ruta_empleado)
app.include_router(ruta_biometria)
app.include_router(ruta_baseusuarios)
app.include_router(ruta_clientes)
app.include_router(ruta_fletes)
app.include_router(ruta_pedidos)


@app.get("/", tags=['Home'])
async def root():
    return {"message": "Hello integra"}

# Ejecuta el servidor
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
