import os
import io
import base64
from io import BytesIO
from urllib.request import urlopen
from fastapi import FastAPI, APIRouter, HTTPException, status, Query, Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, List
from pymongo import MongoClient
import resend
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib.utils import ImageReader
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.platypus import Paragraph, Frame, Spacer
from datetime import datetime

# ——— Configuración de MongoDB ———
mongo_uri = os.getenv("MONGO_URI")
if not mongo_uri:
    raise ValueError("La variable de entorno MONGO_URI no está configurada.")
client = MongoClient(mongo_uri)
db = client["integra"]
coleccion_empleados = db["empleados"]
coleccion_historial = db["historial_certificados"]

# ——— Configuración de Resend ———
resend_api_key = os.getenv("RESEND_API_KEY")
if not resend_api_key:
    raise ValueError("La variable de entorno RESEND_API_KEY no está configurada.")
resend.api_key = resend_api_key

# ——— Modelos Pydantic ———
class Empleado(BaseModel):
    id: Optional[str]
    identificacion: str
    nombre: Optional[str]
    cargo: Optional[str]
    tipoContrato: Optional[str]
    fechaIngreso: Optional[str]
    basico: Optional[float]
    auxilioVivienda: Optional[float]
    auxilioAlimentacion: Optional[float]
    auxilioMovilidad: Optional[float]
    auxilioRodamiento: Optional[float]
    auxilioProductividad: Optional[float]
    auxilioComunic: Optional[float]
    correo: Optional[str]

    class Config:
        orm_mode = True

class EnviarRequest(BaseModel):
    incluirSalario: bool

# ——— Transformación de documento Mongo a Pydantic ———
def transformar_empleado(doc: dict) -> Empleado:
    clean_doc = {k.strip(): v for k, v in doc.items()}
    def get_val(*keys):
        for k in keys:
            if k in clean_doc and clean_doc[k] is not None:
                return clean_doc[k]
        return None
    def get_float(*keys):
        for k in keys:
            val = clean_doc.get(k)
            if val in (None, ""):
                continue
            if isinstance(val, (int, float)):
                return float(val)
            s = str(val).replace(".", "").replace(",", "").strip()
            if s.isdigit():
                return float(s)
        return 0.0
    fecha_raw = get_val("fechaIngreso", "FECHA_INGRESO", "FECHA INGRESO")
    fecha_ing = fecha_raw.isoformat() if hasattr(fecha_raw, "isoformat") else str(fecha_raw or "")
    return Empleado(
        id=str(clean_doc.get("_id")),
        identificacion=str(get_val("identificacion", "IDENTIFICACIÓN") or ""),
        nombre=get_val("nombre", "NOMBRE"),
        cargo=get_val("cargo", "CARGO"),
        tipoContrato=get_val("tipoContrato", "TIPO_DE_CONTRATO", "TIPO DE CONTRATO"),
        fechaIngreso=fecha_ing,
        basico=get_float("basico", "BASICO"),
        auxilioVivienda=get_float("auxilioVivienda", "AUXILIO VIVIENDA"),
        auxilioAlimentacion=get_float("auxilioAlimentacion", "AUXILIO ALIMENTA"),
        auxilioMovilidad=get_float("auxilioMovilidad", "AUXILIO DE MOVILIDAD"),
        auxilioRodamiento=get_float("auxilioRodamiento", "AUXILIO RODAMIENTO"),
        auxilioProductividad=get_float("auxilioProductividad", "AUXILIO DE PRODUCTIVIDAD"),
        auxilioComunic=get_float("auxilioComunic", "AUXILIO COMUNIC"),
        correo=get_val("correo", "CORREO")
    )

# ——— Router y rutas ———
ruta_empleado = APIRouter(
    prefix="/empleados", tags=["Empleados"],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}}
)

@ruta_empleado.get("/", response_model=List[Empleado])
async def get_empleados():
    docs = coleccion_empleados.find()
    return [transformar_empleado(doc) for doc in docs]

@ruta_empleado.get("/buscar", response_model=Empleado)
async def get_empleado_por_identificacion(
    identificacion: str = Query(..., description="Número de identificación")
):
    filtros = {"$or": [
        {"identificacion": identificacion},
        {"identificacion": int(identificacion)} if identificacion.isdigit() else None,
        {"IDENTIFICACIÓN": identificacion},
        {"IDENTIFICACIÓN": int(identificacion)} if identificacion.isdigit() else None
    ]}
    filtros["$or"] = [f for f in filtros["$or"] if f]
    doc = coleccion_empleados.find_one(filtros)
    if not doc:
        raise HTTPException(status_code=404, detail="Empleado no encontrado")
    return transformar_empleado(doc)

@ruta_empleado.post("/enviar")
async def enviar_certificado(
    identificacion: str = Query(..., description="ID del empleado"),
    req: EnviarRequest = Body(...)
):
    filtros = {"$or": [
        {"identificacion": identificacion},
        {"identificacion": int(identificacion)} if identificacion.isdigit() else None,
        {"IDENTIFICACIÓN": identificacion},
        {"IDENTIFICACIÓN": int(identificacion)} if identificacion.isdigit() else None
    ]}
    filtros["$or"] = [f for f in filtros["$or"] if f]
    doc = coleccion_empleados.find_one(filtros)
    if not doc:
        raise HTTPException(status_code=404, detail="Empleado no encontrado")
    emp = transformar_empleado(doc)
    if not emp.correo:
        raise HTTPException(status_code=400, detail="Empleado sin correo registrado")
    coleccion_historial.insert_one({
        "identificacion": emp.identificacion,
        "nombre": emp.nombre,
        "fecha_solicitud": datetime.now()
    })
    show_salary = req.incluirSalario

    # — Generación de PDF —
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4

    # Dibujar fondo desde URL
    try:
        bg_url = "https://storage.googleapis.com/integrapp/Imagenes/albatroz.png"
        bg_data = urlopen(bg_url).read()
        c.drawImage(
            ImageReader(BytesIO(bg_data)), 0, 0,
            width=width, height=height,
            preserveAspectRatio=True, mask='auto'
        )
    except Exception:
        pass

    # Estilos de texto
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle("Title", parent=styles["Heading1"], alignment=1,
                                fontName="Times-Bold", fontSize=14, leading=18)
    subtitle_style = ParagraphStyle("Subtitle", parent=styles["Heading3"], alignment=1,
                                   fontName="Times-Bold", fontSize=12, leading=14)
    body_style = ParagraphStyle("Body", parent=styles["Normal"], fontName="Times-Roman", fontSize=12, leading=16)
    info_style = ParagraphStyle("Info", parent=styles["Normal"], fontName="Times-Roman", fontSize=12, leading=14)

    # Contenido ... detallado...
    # (el resto del código sigue igual que antes)

    # Extraer y enviar PDF
    pdf_data = buffer.getvalue()
    buffer.close()
    payload = {
        'from': 'no-reply@integralogistica.com',
        'to': [emp.correo],
        'subject': f'Certificado Laboral - {emp.nombre}',
        'html': f'<p>Hola {emp.nombre},</p><p>Adjunto tu certificado laboral.</p>',
        'attachments': [{
            'filename': f'certificado_{emp.identificacion}.pdf',
            'content': base64.b64encode(pdf_data).decode()
        }]
    }
    try:
        resend.Emails.send(payload)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Error enviando correo: {e}')

    return JSONResponse(status_code=200, content={'message':'Correo enviado correctamente'})

# ——— Montar FastAPI ———
app = FastAPI()
app.include_router(ruta_empleado)
