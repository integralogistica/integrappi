import os
import io
import base64
from io import BytesIO
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
from rutas.fondoBase64 import fondo_base64
from rutas.firmaBase64 import firma_base64

# ——— Configuración de MongoDB ———
mongo_uri = os.getenv("MONGO_URI")
if not mongo_uri:
    raise ValueError("La variable de entorno MONGO_URI no está configurada.")
client = MongoClient(mongo_uri)
db = client["integra"]
coleccion_empleados = db["empleados"]

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

# ——— Función ayuda: saca float o 0 ———
def get_float(doc, *keys):
    for k in keys:
        if k in doc and doc[k] not in (None, ""):
            try: return float(doc[k])
            except: pass
    return 0.0

# ——— Transformación de documento Mongo a Pydantic ———
def transformar_empleado(doc: dict) -> Empleado:
    fecha_raw = doc.get('fechaIngreso') or doc.get('FECHA_INGRESO')
    fecha_ing = fecha_raw.isoformat() if hasattr(fecha_raw, 'isoformat') else str(fecha_raw or "")
    return Empleado(
        id=str(doc.get('_id')),
        identificacion=str(doc.get('identificacion') or doc.get('IDENTIFICACIÓN') or ""),
        nombre=doc.get('nombre'),
        cargo=doc.get('cargo'),
        tipoContrato=doc.get('tipoContrato') or doc.get('TIPO_DE_CONTRATO'),
        fechaIngreso=fecha_ing,
        basico=get_float(doc, 'basico', 'BASICO'),
        auxilioVivienda=get_float(doc, 'auxilioVivienda', 'auxilio_vivienda', 'AUXILIO_VIVIENDA'),
        auxilioAlimentacion=get_float(doc, 'auxilioAlimentacion', 'auxilio_alimentacion', 'AUXILIO_ALIMENTACIÓN'),
        auxilioMovilidad=get_float(doc, 'auxilioMovilidad', 'auxilio_movilidad', 'AUXILIO_DE_MOVILIDAD'),
        auxilioRodamiento=get_float(doc, 'auxilioRodamiento', 'auxilio_rodamiento', 'AUXILIO_RODAMIENTO'),
        auxilioProductividad=get_float(doc, 'auxilioProductividad', 'auxilio_productividad', 'AUXILIO_DE_PRODUCTIVIDAD'),
        auxilioComunic=get_float(doc, 'auxilioComunic', 'auxilio_comunic', 'AUXILIO_COMUNIC'),
        correo=doc.get('correo')
    )

# ——— Router y rutas ———
ruta_empleado = APIRouter(
    prefix="/empleados", tags=["Empleados"],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}}
)

@ruta_empleado.get("/", response_model=List[Empleado])
async def get_empleados():
    return [transformar_empleado(doc) for doc in coleccion_empleados.find()]

@ruta_empleado.get("/buscar", response_model=Empleado)
async def get_empleado_por_identificacion(
    identificacion: str = Query(..., description="Número de identificación")
):
    filtros = {"$or": [
        {"identificacion": identificacion},
        {"identificacion": int(identificacion)} if identificacion.isdigit() else {},
        {"IDENTIFICACIÓN": identificacion},
        {"IDENTIFICACIÓN": int(identificacion)} if identificacion.isdigit() else {}
    ]}
    doc = coleccion_empleados.find_one(filtros)
    if not doc:
        raise HTTPException(status_code=404, detail="Empleado no encontrado")
    return transformar_empleado(doc)

@ruta_empleado.post("/enviar")
async def enviar_certificado(
    identificacion: str = Query(..., description="ID del empleado"),
    req: EnviarRequest = Body(...)    # <== obligatorio
):
    # —— obtén el empleado
    filtros = {"$or": [
        {"identificacion": identificacion},
        {"identificacion": int(identificacion)} if identificacion.isdigit() else {},
        {"IDENTIFICACIÓN": identificacion},
        {"IDENTIFICACIÓN": int(identificacion)} if identificacion.isdigit() else {}
    ]}
    doc = coleccion_empleados.find_one(filtros)
    if not doc:
        raise HTTPException(status_code=404, detail="Empleado no encontrado")
    emp = transformar_empleado(doc)
    if not emp.correo:
        raise HTTPException(status_code=400, detail="Empleado sin correo registrado")

    show_salary = req.incluirSalario

    # — Generación de PDF —
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4

    # fondo
    fondo_clean = fondo_base64.split(',',1)[1] if fondo_base64.startswith("data:image") else fondo_base64
    try:
        c.drawImage(
            ImageReader(BytesIO(base64.b64decode(fondo_clean))),
            0, 0, width=width, height=height
        )
    except:
        pass

    # estilos
    styles = getSampleStyleSheet()
    title_style    = ParagraphStyle('Title',    parent=styles['Heading1'], alignment=1, fontName='Times-Bold', fontSize=14, leading=18)
    subtitle_style = ParagraphStyle('Subtitle', parent=styles['Heading3'], alignment=1, fontName='Times-Bold', fontSize=12, leading=14)
    body_style     = ParagraphStyle('Body',     parent=styles['Normal'],   fontName='Times-Roman', fontSize=12, leading=16)
    info_style     = ParagraphStyle('Info',     parent=styles['Normal'],   fontName='Times-Roman', fontSize=10, leading=12)

    # construye story
    header   = Paragraph("EL DEPARTAMENTO DE GESTIÓN HUMANA", title_style)
    subtitle = Paragraph("CERTIFICA QUE:", subtitle_style)

    # fecha legible
    try:
        dt = datetime.fromisoformat(emp.fechaIngreso)
        meses = ["enero","febrero","marzo","abril","mayo","junio",
                 "julio","agosto","septiembre","octubre","noviembre","diciembre"]
        fecha_humana = f"{dt.day} de {meses[dt.month-1]} de {dt.year}"
    except:
        fecha_humana = emp.fechaIngreso

    # cédula formateada
    if emp.identificacion.isdigit():
        ced = f"{int(emp.identificacion):,}".replace(",",".")
    else:
        ced = emp.identificacion

    texto = (
        f"El señor/a <b>{emp.nombre}</b>, identificado/a con cédula número <b>{ced}</b>, "
        f"labora en nuestra empresa desde <b>{fecha_humana}</b>, desempeñando el cargo de "
        f"<b>{emp.cargo}</b> con contrato a término <b>{emp.tipoContrato}</b>."
    )
    if show_salary and emp.basico and emp.basico > 0:
        texto += f" Con un salario fijo mensual por valor de <b>{int(emp.basico):,}</b> pesos."

    body = Paragraph(texto, body_style)

    story = [
        header,
        Spacer(1, 8),         # sube un poco
        subtitle,
        Spacer(1, 8),
        body
    ]

    # auxilios
    if show_salary:
        for label, val in [
            ("Auxilio Vivienda",      emp.auxilioVivienda),
            ("Auxilio Alimentación",  emp.auxilioAlimentacion),
            ("Auxilio Movilidad",     emp.auxilioMovilidad),
            ("Auxilio Rodamiento",    emp.auxilioRodamiento),
            ("Auxilio Productividad", emp.auxilioProductividad),
            ("Auxilio Comunic",       emp.auxilioComunic)
        ]:
            if val and val > 0:
                story.append(Spacer(1,6))
                story.append(Paragraph(f"<b>{label}:</b> {int(val):,}".replace(",", "."), body_style))

    # info de contacto
    story.append(Spacer(1, 10))
    story.append(Paragraph("Para mayor información: PBX 7006232 o celular 3183385709.", info_style))

    # — Frame elevado — 
    frame = Frame(40, 340, width - 80, height - 380, showBoundary=0)
    frame.addFromList(story, c)

    # — Firma SOBRE el nombre —
    firma_clean = firma_base64.split(',',1)[1]
    y_base = 300  # ajusta aquí si quieres subir/bajar la firma/nombre
    c.setFont('Times-Bold', 12)
    c.drawCentredString(width/2, y_base +  5, 'PATRICIA LEAL AROCA')
    c.setFont('Times-Roman', 10)
    c.drawCentredString(width/2, y_base - 10, 'Gerente de gestión humana')
    c.drawCentredString(width/2, y_base - 22, 'Integra cadena de servicios')
    c.drawImage(
        ImageReader(BytesIO(base64.b64decode(firma_clean))),
        width/2 - 75, y_base - 10,
        width=150, height=50,
        mask='auto'
    )

    c.showPage()
    c.save()
    buffer.seek(0)

    # — Envío por correo —
    payload = {
        'from': 'no-reply@integralogistica.com',
        'to': [emp.correo],
        'subject': f'Certificado Laboral - {emp.nombre}',
        'html': f'<p>Hola {emp.nombre},</p><p>Adjunto tu certificado laboral.</p>',
        'attachments': [{
            'filename': f'certificado_{emp.identificacion}.pdf',
            'type': 'application/pdf',
            'content': base64.b64encode(buffer.read()).decode()
        }]
    }
    try:
        resend.Emails.send(payload)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Error enviando correo: {e}')

    return JSONResponse(status_code=200, content={'message': 'Correo enviado correctamente'})

# ——— App ———
app = FastAPI()
app.include_router(ruta_empleado)
