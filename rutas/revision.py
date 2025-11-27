import os
from fastapi import APIRouter, HTTPException, status, Query, Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from pymongo import MongoClient
from dotenv import load_dotenv
import resend

load_dotenv()

# ---------------------------------------------------------
# Routers
# ---------------------------------------------------------
ruta_revision = APIRouter(prefix="/revision", tags=["Revisión de Vehículos"])

# ---------------------------------------------------------
# MongoDB
# ---------------------------------------------------------
mongo_uri = os.getenv("MONGO_URI")
if not mongo_uri:
    raise ValueError("La variable de entorno MONGO_URI no está configurada.")

client = MongoClient(mongo_uri)
db = client["integra"]
coleccion_vehiculos = db["vehiculos"]

# ---------------------------------------------------------
# Resend
# ---------------------------------------------------------
resend_api_key = os.getenv("RESEND_API_KEY")
if not resend_api_key:
    raise ValueError("La variable de entorno RESEND_API_KEY no está configurada.")
resend.api_key = resend_api_key

# ---------------------------------------------------------
# Modelo
# ---------------------------------------------------------
class ObservacionRequest(BaseModel):
    observaciones: str

# ---------------------------------------------------------
# Enviar correo al tenedor con observaciones
# ---------------------------------------------------------
@ruta_revision.post("/enviar-observaciones")
async def enviar_revision(
    tenedor: str = Query(..., description="Cédula del tenedor"),
    req: ObservacionRequest = Body(...)
):
    # Buscar por idUsuario o por campo tenedor
    consulta = {"$or": [{"idUsuario": tenedor}, {"tenedor": tenedor}]}
    veh = coleccion_vehiculos.find_one(consulta)

    if not veh:
        raise HTTPException(
            status_code=404,
            detail="No existe ningún vehículo registrado con esta cédula (ni en idUsuario ni en tenedor)"
        )

    # Obtener correo desde campo tenedCorreo
    correo = veh.get("tenedCorreo")
    if not correo:
        raise HTTPException(
            status_code=400,
            detail="Este tenedor no tiene un correo registrado en el campo 'tenedCorreo'"
        )

    mensaje_html = f"""
        <h3>Observaciones sobre la información registrada</h3>
        <p>Hola,</p>
        <p>Se han detectado observaciones en los datos asociados a tu(s) vehículo(s).</p>
        <p><strong>Observaciones registradas:</strong></p>
        <p>{req.observaciones}</p>
        <br>
        <p>Por favor comunícate con la empresa para realizar las correcciones necesarias.</p>
    """

    payload = {
        "from": "no-reply@integralogistica.com",
        "to": [correo],
        "subject": "Observaciones sobre tu registro de vehículo",
        "html": mensaje_html
    }

    try:
        resend.Emails.send(payload)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error enviando correo: {e}")

    return JSONResponse(status_code=200, content={"message": "Correo enviado correctamente"})
