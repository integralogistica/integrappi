from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Importación de rutas
from rutas.usuarios import ruta_usuario

app = FastAPI()
app.title = "Glamping"
app.version = "0.0.1"

# Configuración de CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permite todas las orígenes. Cambia esto para permitir solo orígenes específicos
    allow_credentials=True,
    allow_methods=["*"],  # Permite todos los métodos (GET, POST, PUT, DELETE, etc.)
    allow_headers=["*"],  # Permite todos los encabezados
)

app.include_router(ruta_usuario)

@app.get("/", tags=['Home'])
async def root():
    return {"message": "Hello glamping"}

# Ejecuta el servidor
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
