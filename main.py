from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Importación de rutas
from rutas.aut2 import ruta_usuario
# from rutas.aut import ruta_aut
# from rutas.aut2 import ruta_edwin


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
# # app.include_router(ruta_aut)
# app.include_router(ruta_edwin)

@app.get("/", tags=['Home'])
async def root():
    return {"message": "Hello integra"}

# Ejecuta el servidor
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
