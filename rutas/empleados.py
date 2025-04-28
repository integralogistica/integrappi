import os
import io
import logging
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
from PIL import Image, ImageFile
from rutas.fondoBase64 import fondo_base64
from rutas.firmaBase64 import firma_base64

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
ImageFile.LOAD_TRUNCATED_IMAGES = True

# Para linearizar PDF y compatibilidad móvil
try:
    from pikepdf import Pdf, PdfError
    _HAVE_PIKEPDF = True
except ImportError:
    _HAVE_PIKEPDF = False

# Configuración de MongoDB
mongo_uri = os.getenv("MONGO_URI")
if not mongo_uri:
    raise ValueError("La variable de entorno MONGO_URI no está configurada.")

client = MongoClient(mongo_uri)
db = client["integra"]
coleccion_empleados = db["empleados"]
coleccion_historial = db["historial_certificados"]

# Configuración de Resend
resend_api_key = os.getenv("RESEND_API_KEY")
if not resend_api_key:
    raise ValueError("La variable de entorno RESEND_API_KEY no está configurada.")

resend.api_key = resend_api_key

# Modelos Pydantic
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

# Funciones de validación
def validar_imagen_base64(b64_data: str) -> bool:
    try:
        if ',' in b64_data:
            header, data = b64_data.split(',', 1)
            if 'image/' not in header:
                return False
        else:
            data = b64_data
            
        img_data = base64.b64decode(data)
        img = Image.open(BytesIO(img_data))
        img.verify()
        img.close()
        return True
    except Exception as e:
        logger.error(f"Error validando imagen base64: {str(e)}")
        return False

def validar_pdf(pdf_bytes: bytes) -> bool:
    if not pdf_bytes:
        return False
    try:
        Pdf.open(BytesIO(pdf_bytes))
        return True
    except Exception as e:
        logger.error(f"Error validando PDF: {str(e)}")
        return False

# Transformación de documento Mongo a Pydantic
def transformar_empleado(doc: dict) -> Empleado:
    clean_doc = {k.strip(): v for k, v in doc.items()}
    
    def get_val(*keys):
        for key in keys:
            val = clean_doc.get(key)
            if val is not None:
                return val
        return None

    def get_float(*keys):
        for key in keys:
            val = clean_doc.get(key)
            if val is None:
                continue
            try:
                return float(str(val).replace('.', '').replace(',', '').strip())
            except ValueError:
                continue
        return 0.0

    fecha_raw = get_val('fechaIngreso', 'FECHA_INGRESO')
    fecha_ing = ""
    try:
        fecha_ing = fecha_raw.isoformat() if hasattr(fecha_raw, 'isoformat') else str(fecha_raw)
    except AttributeError:
        fecha_ing = ""

    return Empleado(
        id=str(clean_doc.get('_id', '')),
        identificacion=str(get_val('identificacion', 'IDENTIFICACIÓN') or ''),
        nombre=get_val('nombre', 'NOMBRE'),
        cargo=get_val('cargo', 'CARGO'),
        tipoContrato=get_val('tipoContrato', 'TIPO_CONTRATO'),
        fechaIngreso=fecha_ing,
        basico=get_float('basico', 'BASICO'),
        auxilioVivienda=get_float('auxilioVivienda', 'AUX_VIVIENDA'),
        auxilioAlimentacion=get_float('auxilioAlimentacion', 'AUX_ALIMENTACION'),
        auxilioMovilidad=get_float('auxilioMovilidad', 'AUX_MOVILIDAD'),
        auxilioRodamiento=get_float('auxilioRodamiento', 'AUX_RODAMIENTO'),
        auxilioProductividad=get_float('auxilioProductividad', 'AUX_PRODUCTIVIDAD'),
        auxilioComunic=get_float('auxilioComunic', 'AUX_COMUNICACION'),
        correo=get_val('correo', 'EMAIL')
    )

# Rutas y aplicación
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
    try:
        query = {'$or': [
            {'identificacion': identificacion},
            {'IDENTIFICACIÓN': identificacion}
        ]}
        
        if identificacion.isdigit():
            query['$or'].extend([
                {'identificacion': int(identificacion)},
                {'IDENTIFICACIÓN': int(identificacion)}
            ])
        
        doc = coleccion_empleados.find_one(query)
        if not doc:
            raise HTTPException(status_code=404, detail='Empleado no encontrado')
        return transformar_empleado(doc)
    except Exception as e:
        logger.error(f"Error buscando empleado: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno en la búsqueda")

@ruta_empleado.post('/enviar')
async def enviar_certificado(
    identificacion: str = Query(..., description='Identificación del empleado'),
    req: EnviarRequest = Body(...)
):
    try:
        # Buscar empleado
        empleado = await get_empleado_por_identificacion(identificacion)
        
        if not empleado.correo:
            raise HTTPException(status_code=400, detail='El empleado no tiene correo registrado')

        # Validar imágenes
        if not validar_imagen_base64(fondo_base64):
            logger.warning("Imagen de fondo inválida, se generará sin fondo")
        
        if not validar_imagen_base64(firma_base64):
            logger.warning("Imagen de firma inválida, se generará sin firma")

        # Generar PDF
        buffer = BytesIO()
        c = canvas.Canvas(buffer, pagesize=A4)
        width, height = A4

        # Agregar fondo
        try:
            if validar_imagen_base64(fondo_base64):
                img_part = fondo_base64.split(',')
                img_data = base64.b64decode(img_part[1] if len(img_part) > 1 else img_part[0])
                c.drawImage(ImageReader(BytesIO(img_data)), 0, 0, width=width, height=height)
        except Exception as e:
            logger.error(f"Error al agregar fondo: {str(e)}")

        # Contenido del PDF
        styles = getSampleStyleSheet()
        content = [
            Paragraph('EL DEPARTAMENTO DE GESTIÓN HUMANA', styles['Heading1']),
            Spacer(1, 24),
            Paragraph('CERTIFICA QUE:', styles['Heading2']),
            Spacer(1, 24),
            Paragraph(
                f"El señor/a <b>{empleado.nombre}</b>, identificado con cédula No. <b>{empleado.identificacion}</b>...", 
                styles['BodyText']
            )
        ]

        # Agregar firma
        try:
            if validar_imagen_base64(firma_base64):
                firma_part = firma_base64.split(',')
                firma_data = base64.b64decode(firma_part[1] if len(firma_part) > 1 else firma_part[0])
                c.drawImage(ImageReader(BytesIO(firma_data)), width/2-75, 100, width=150, height=50)
        except Exception as e:
            logger.error(f"Error al agregar firma: {str(e)}")

        Frame(40, 40, width-80, height-80).addFromList(content, c)
        c.save()

        # Validar y optimizar PDF
        buffer.seek(0)
        pdf_bytes = buffer.getvalue()
        
        if not validar_pdf(pdf_bytes):
            raise HTTPException(status_code=500, detail="El PDF generado es inválido")

        # Linearizar PDF
        if _HAVE_PIKEPDF:
            try:
                with Pdf.open(BytesIO(pdf_bytes)) as pdf:
                    optimized = BytesIO()
                    pdf.save(optimized, linearize=True)
                    pdf_bytes = optimized.getvalue()
            except PdfError as e:
                logger.warning(f"No se pudo linearizar el PDF: {str(e)}")

        # Enviar correo
        try:
            resend.Emails.send({
                'from': 'no-reply@integralogistica.com',
                'to': [empleado.correo],
                'subject': f'Certificado Laboral - {empleado.nombre}',
                'html': '<p>Adjunto su certificado laboral</p>',
                'attachments': [{
                    'filename': f'certificado_{empleado.identificacion}.pdf',
                    'content': base64.b64encode(pdf_bytes).decode('utf-8'),
                    'type': 'application/pdf'
                }]
            })
        except Exception as e:
            logger.error(f"Error enviando correo: {str(e)}")
            raise HTTPException(status_code=500, detail="Error al enviar el correo")

        return JSONResponse(status_code=200, content={'message': 'Certificado enviado correctamente'})

    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error general: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error interno al procesar la solicitud")

app = FastAPI()
app.include_router(ruta_empleado)