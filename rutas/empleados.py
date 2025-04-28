import os
import io
from io import BytesIO
from fastapi import FastAPI, APIRouter, HTTPException, status, Query, Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import base64
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

# Para linearizar PDF y compatibilidad móvil
try:
    from pikepdf import Pdf
    _HAVE_PIKEPDF = True
except ImportError:
    _HAVE_PIKEPDF = False

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
        for key in keys:
            if key in clean_doc and clean_doc[key] is not None:
                return clean_doc[key]
        return None
    def get_float(*keys):
        for key in keys:
            val = clean_doc.get(key)
            if not val:
                continue
            if isinstance(val, (int, float)):
                return float(val)
            s = str(val).replace('.', '').replace(',', '').strip()
            if s.isdigit():
                return float(s)
        return 0.0

    fecha_raw = get_val('fechaIngreso', 'FECHA_INGRESO', 'FECHA INGRESO')
    if hasattr(fecha_raw, 'isoformat'):
        fecha_ing = fecha_raw.isoformat()
    else:
        fecha_ing = str(fecha_raw or '')

    return Empleado(
        id=str(clean_doc.get('_id')),
        identificacion=str(get_val('identificacion', 'IDENTIFICACIÓN') or ''),
        nombre=get_val('nombre', 'NOMBRE'),
        cargo=get_val('cargo', 'CARGO'),
        tipoContrato=get_val('tipoContrato', 'TIPO_DE_CONTRATO', 'TIPO DE CONTRATO'),
        fechaIngreso=fecha_ing,
        basico=get_float('basico', 'BASICO'),
        auxilioVivienda=get_float('auxilioVivienda', 'AUXILIO VIVIENDA'),
        auxilioAlimentacion=get_float('auxilioAlimentacion', 'AUXILIO ALIMENTA'),
        auxilioMovilidad=get_float('auxilioMovilidad', 'AUXILIO DE MOVILIDAD'),
        auxilioRodamiento=get_float('auxilioRodamiento', 'AUXILIO RODAMIENTO'),
        auxilioProductividad=get_float('auxilioProductividad', 'AUXILIO DE PRODUCTIVIDAD'),
        auxilioComunic=get_float('auxilioComunic', 'AUXILIO COMUNIC'),
        correo=get_val('correo', 'CORREO')
    )

# ——— Rutas y aplicación ———
ruta_empleado = APIRouter(
    prefix='/empleados',
    tags=['Empleados'],
    responses={status.HTTP_404_NOT_FOUND: {'message': 'No encontrado'}}
)

@ruta_empleado.get('/', response_model=List[Empleado])
async def get_empleados():
    return [transformar_empleado(doc) for doc in coleccion_empleados.find()]

@ruta_empleado.get('/buscar', response_model=Empleado)
async def get_empleado_por_identificacion(
    identificacion: str = Query(..., description='Número de identificación')
):
    condiciones = (
        [{'identificacion': identificacion}, {'IDENTIFICACIÓN': identificacion}]
        if not identificacion.isdigit()
        else [
            {'identificacion': identificacion},
            {'identificacion': int(identificacion)},
            {'IDENTIFICACIÓN': identificacion},
            {'IDENTIFICACIÓN': int(identificacion)}
        ]
    )
    doc = coleccion_empleados.find_one({'$or': condiciones})
    if not doc:
        raise HTTPException(status_code=404, detail='Empleado no encontrado')
    return transformar_empleado(doc)

@ruta_empleado.post('/enviar')
async def enviar_certificado(
    identificacion: str = Query(..., description='Identificación del empleado'),
    req: EnviarRequest = Body(...)
):
    # Buscar empleado
    condiciones = (
        [{'identificacion': identificacion}, {'IDENTIFICACIÓN': identificacion}]
        if not identificacion.isdigit()
        else [
            {'identificacion': identificacion},
            {'identificacion': int(identificacion)},
            {'IDENTIFICACIÓN': identificacion},
            {'IDENTIFICACIÓN': int(identificacion)}
        ]
    )
    doc = coleccion_empleados.find_one({'$or': condiciones})
    if not doc:
        raise HTTPException(status_code=404, detail='Empleado no encontrado')

    emp = transformar_empleado(doc)
    if not emp.correo:
        raise HTTPException(status_code=400, detail='Empleado sin correo registrado')

    # Guardar en historial
    coleccion_historial.insert_one({
        'identificacion': emp.identificacion,
        'nombre': emp.nombre,
        'fecha_solicitud': datetime.now()
    })

    incluir_salario = req.incluirSalario

    # ——— Generar PDF ———
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    c.setTitle('Certificado Laboral')
    c.setAuthor('Integra Logística')
    width, height = A4

    # Fondo
    try:
        img_data = fondo_base64.split(',', 1)[1] if fondo_base64.startswith('data:image') else fondo_base64
        c.drawImage(
            ImageReader(BytesIO(base64.b64decode(img_data))),
            0, 0, width=width, height=height
        )
    except:
        pass

    # Estilos y contenido
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('Title', parent=styles['Heading1'], alignment=1,
                                 fontName='Times-Bold', fontSize=14, leading=18)
    subtitle_style = ParagraphStyle('Subtitle', parent=styles['Heading3'], alignment=1,
                                    fontName='Times-Bold', fontSize=12, leading=14)
    body_style = ParagraphStyle('Body', parent=styles['Normal'],
                                fontName='Times-Roman', fontSize=12, leading=16)
    info_style = ParagraphStyle('Info', parent=styles['Normal'],
                                fontName='Times-Roman', fontSize=12, leading=14)

    header = Paragraph('EL DEPARTAMENTO DE GESTIÓN HUMANA', title_style)
    subtitle = Paragraph('CERTIFICA QUE:', subtitle_style)

    # Fecha legible de ingreso
    try:
        dt = datetime.fromisoformat(emp.fechaIngreso)
        meses = [
            'enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio',
            'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'
        ]
        fecha_leg = f"{dt.day} de {meses[dt.month-1]} de {dt.year}"
    except:
        fecha_leg = emp.fechaIngreso or ''

    ced = (f"{int(emp.identificacion):,}".replace(',', '.')
           if emp.identificacion.isdigit() else emp.identificacion)

    texto = (
        f"El señor/a <b>{emp.nombre}</b>, identificado/a con cédula "
        f"número <b>{ced}</b>, labora en nuestra empresa desde "
        f"<b>{fecha_leg}</b>, desempeñando el cargo de <b>{emp.cargo}</b> "
        f"con contrato a término <b>{emp.tipoContrato}</b>."
    )
    if incluir_salario and emp.basico > 0:
        texto += f" Con un salario fijo mensual por valor de <b>${int(emp.basico):,}</b> pesos."

    body = Paragraph(texto, body_style)

    story = [Spacer(1, 50), header, Spacer(1, 16), subtitle, Spacer(1, 16), body]

    auxs = [
        ('Auxilio Vivienda', emp.auxilioVivienda),
        ('Auxilio Alimentación', emp.auxilioAlimentacion),
        ('Auxilio Movilidad', emp.auxilioMovilidad),
        ('Auxilio Rodamiento', emp.auxilioRodamiento),
        ('Auxilio Productividad', emp.auxilioProductividad),
        ('Auxilio Comunic', emp.auxilioComunic),
    ]

    if incluir_salario and any(v > 0 for _, v in auxs):
        story.append(Spacer(1, 6))
        story.append(Paragraph(
            'Más un auxilio no salarial de mera liberalidad por concepto de:',
            body_style
        ))
        for lab, val in auxs:
            if val > 0:
                story.append(Spacer(1, 6))
                story.append(Paragraph(f"<b>{lab}:</b> ${int(val):,}".replace(',', '.'), body_style))

    story.append(Spacer(1, 20))
    story.append(Paragraph(
        'Para mayor información: PBX 7006232 o celular 3183385709.',
        info_style
    ))

    # Fecha de expedición
    now = datetime.now()
    cert_date = f"{now.day} de {meses[now.month-1]} de {now.year}"
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        f"La presente certificación se expide a solicitud del interesado, "
        f"dado a {cert_date} en Bogotá.",
        info_style
    ))
    story.append(Spacer(1, 6))
    story.append(Paragraph('Cordialmente,', info_style))

    Frame(40, 340, width-80, height-380, showBoundary=0).addFromList(story, c)

    # Firma
    try:
        sig = firma_base64.split(',', 1)[1]
        c.drawImage(
            ImageReader(BytesIO(base64.b64decode(sig))),
            width/2 - 75, 300 - 10, width=150, height=50, mask='auto'
        )
    except:
        pass

    c.setFont('Times-Bold', 12)
    c.drawCentredString(width/2, 305, 'PATRICIA LEAL AROCA')
    c.setFont('Times-Roman', 10)
    c.drawCentredString(width/2, 290, 'Certificado laboral')
    c.drawCentredString(width/2, 278, 'Gerente de gestión humana')
    c.drawCentredString(width/2, 266, 'Integra cadena de servicios')

    c.showPage()
    c.save()

    # ——— Preparar bytes ———
    buffer.seek(0)
    pdf_bytes = buffer.getvalue()
    final_bytes = pdf_bytes
    if _HAVE_PIKEPDF:
        linear_io = io.BytesIO()
        Pdf.open(io.BytesIO(pdf_bytes)).save(linear_io, linearize=True)
        final_bytes = linear_io.getvalue()

    # ——— Construir y enviar payload ———

    attachment_b64 = base64.b64encode(final_bytes).decode('ascii')

    payload = {
        'from': 'no-reply@integralogistica.com',
        'to': [emp.correo],
        'subject': f'Certificado Laboral - {emp.nombre}',
        'html': f'<p>Hola {emp.nombre},</p><p>Adjunto tu certificado laboral generado automáticamente.</p>',
        'attachments': [{
            'filename': f'certificado_{emp.identificacion}.pdf',
            'content': attachment_b64,             # ← string JSON-serializable
            'content_type': 'application/pdf',
            'encoding': 'base64'  
        }]
    }

    try:
        resend.Emails.send(payload)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Error enviando correo: {e}')

    return JSONResponse(status_code=200, content={'message': 'Correo enviado correctamente'})

app = FastAPI()
app.include_router(ruta_empleado)
