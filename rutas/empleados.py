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

# ——— Transformación de documento Mongo a Pydantic ———
def transformar_empleado(doc: dict) -> Empleado:
    get = lambda *keys: next((doc.get(k) for k in keys if k in doc), None)
    def get_float(*keys):
        try:
            v = get(*keys)
            return float(v) if v is not None else 0
        except:
            return 0

    fecha_raw = get('fechaIngreso', 'FECHA_INGRESO', 'FECHA INGRESO')
    fecha_ing = fecha_raw.isoformat() if hasattr(fecha_raw, 'isoformat') else str(fecha_raw or "")
    return Empleado(
        id=str(doc.get('_id')),
        identificacion=str(get('identificacion', 'IDENTIFICACIÓN') or ""),
        nombre=get('nombre', 'NOMBRE'),
        cargo=get('cargo', 'CARGO'),
        tipoContrato=get('tipoContrato', 'TIPO_DE_CONTRATO', 'TIPO DE CONTRATO'),
        fechaIngreso=fecha_ing,
        basico=get_float('basico', 'BASICO'),
        auxilioVivienda=get_float('auxilioVivienda', 'AUXILIO_VIVIENDA'),
        auxilioAlimentacion=get_float('auxilioAlimentacion', 'AUXILIO_ALIMENTACIÓN'),
        auxilioMovilidad=get_float('auxilioMovilidad', 'AUXILIO_DE_MOVILIDAD'),
        auxilioRodamiento=get_float('auxilioRodamiento', 'AUXILIO_RODAMIENTO'),
        auxilioProductividad=get_float('auxilioProductividad', 'AUXILIO_DE_PRODUCTIVIDAD'),
        auxilioComunic=get_float('auxilioComunic', 'AUXILIO_COMUNIC'),
        correo=get('correo', 'CORREO')
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
    req: EnviarRequest = Body(None)
):
    # Busca empleado
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

    show_salary = req.incluirSalario if req is not None else True

    # — Generación de PDF —
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4

    # (opcional) cuadrícula de ayuda...
    # ...

    # Fondo
    fondo_clean = fondo_base64.split(',',1)[1] if fondo_base64.startswith("data:image") else fondo_base64
    try:
        c.drawImage(
            ImageReader(BytesIO(base64.b64decode(fondo_clean))),
            0, 0, width=width, height=height
        )
    except:
        pass

    # Estilos
    styles = getSampleStyleSheet()
    body_style = ParagraphStyle('Body',
        parent=styles['Normal'], fontName='Times-Roman', fontSize=12, leading=16
    )
    info_style = ParagraphStyle('Info',
        parent=styles['Normal'], fontName='Times-Roman', fontSize=12, leading=12
    )
    title_style = ParagraphStyle('Title',
        parent=styles['Heading1'], alignment=1,
        fontName='Times-Bold', fontSize=14, leading=18
    )
    subtitle_style = ParagraphStyle('Subtitle',
        parent=styles['Heading3'], alignment=1,
        fontName='Times-Bold', fontSize=12, leading=14
    )

    # Header + subtitle
    header   = Paragraph("EL DEPARTAMENTO DE GESTIÓN HUMANA", title_style)
    subtitle = Paragraph("CERTIFICA QUE:", subtitle_style)

    # Fecha en español
    try:
        dt = datetime.fromisoformat(emp.fechaIngreso)
        meses = ["enero","febrero","marzo","abril","mayo","junio",
                 "julio","agosto","septiembre","octubre","noviembre","diciembre"]
        fecha_humana = f"{dt.day} de {meses[dt.month-1]} de {dt.year}"
    except:
        fecha_humana = emp.fechaIngreso

    # Cédula con puntos
    if emp.identificacion.isdigit():
        ced = f"{int(emp.identificacion):,}".replace(",",".")
    else:
        ced = emp.identificacion

    # Texto principal + salario
    texto = (
        f"El señor/a <b>{emp.nombre}</b>, identificado/a con cédula número <b>{ced}</b>, "
        f"labora en nuestra empresa desde <b>{fecha_humana}</b>, desempeñando el cargo de "
        f"<b>{emp.cargo}</b> con contrato a término <b>{emp.tipoContrato}</b>."
    )
    if show_salary and emp.basico and emp.basico > 0:
        sal = f"{int(emp.basico):,}".replace(",",".")
        texto += f" Con un salario fijo mensual por valor de <b>{sal} pesos</b>."
    body = Paragraph(texto, body_style)

    # Construir el story (incluye párrafos y luego el “Para mayor información”)
    story = [
        header,
        Spacer(1, 12),
        subtitle,
        Spacer(1, 12),
        body
    ]

    # Auxilios (si aplica), agregados como párrafos independientes
    if show_salary:
        aux_labels = [
            ('Auxilio Vivienda', emp.auxilioVivienda),
            ('Auxilio Alimentación', emp.auxilioAlimentacion),
            ('Auxilio Movilidad', emp.auxilioMovilidad),
            ('Auxilio Rodamiento', emp.auxilioRodamiento),
            ('Auxilio Productividad', emp.auxilioProductividad),
            ('Auxilio Comunic', emp.auxilioComunic),
        ]
        for label, val in aux_labels:
            if val and val > 0:
                story.append(Spacer(1,8))
                story.append(Paragraph(f"<b>{label}:</b> {int(val):,}".replace(",", "."),
                                       body_style))

    # “Para mayor información...” al final del frame
    story.append(Spacer(1,12))
    story.append(Paragraph(
        "Para mayor información: PBX 7006232 o celular 3183385709.",
        info_style
    ))

    # Dibujo en frame
    frame = Frame(40, 250, width-80, height-430, showBoundary=0)
    frame.addFromList(story, c)

    # — Firma sobre el nombre —
    firma_clean = firma_base64.split(',',1)[1] if firma_base64.startswith("data:image") else firma_base64
    try:
        # Coordenada base para la firma
        y_base = 200
        # Primero escribimos el nombre y cargo
        c.setFont('Times-Bold', 12)
        c.drawCentredString(width/2, y_base +  5, 'PATRICIA LEAL AROCA')
        c.setFont('Times-Roman', 10)
        c.drawCentredString(width/2, y_base - 10, 'Gerente de gestión humana')
        c.drawCentredString(width/2, y_base - 22, 'Integra cadena de servicios')

        # Luego superponemos la imagen para que “tape” parcialmente el texto
        c.drawImage(
            ImageReader(BytesIO(base64.b64decode(firma_clean))),
            width/2 - 75, y_base - 10,
            width=150, height=50,
            mask='auto'
        )
    except:
        pass

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

# ——— FastAPI app ———
app = FastAPI()
app.include_router(ruta_empleado)
