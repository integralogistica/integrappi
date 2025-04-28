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
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph, Frame, Spacer
from datetime import datetime
from PIL import Image, ImageFile
from rutas.fondoBase64 import fondo_base64
from rutas.firmaBase64 import firma_base64

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
ImageFile.LOAD_TRUNCATED_IMAGES = True

# Intentar importar pikepdf para validación y linearización de PDF
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

# Validar que un string Base64 sea una imagen
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
        logger.error(f"Error validando imagen base64: {e}")
        return False

# Validar PDF solo si pikepdf está disponible
def validar_pdf(pdf_bytes: bytes) -> bool:
    if not pdf_bytes:
        return False
    if not _HAVE_PIKEPDF:
        # No podemos validar sin pikepdf: asumimos válido
        return True
    try:
        Pdf.open(BytesIO(pdf_bytes))
        return True
    except Exception as e:
        logger.error(f"Error validando PDF: {e}")
        return False

# Transformar doc de Mongo a Pydantic
def transformar_empleado(doc: dict) -> Empleado:
    field_mapping = {
        'identificacion': ['IDENTIFICACIÓN', 'identificacion'],
        'nombre': ['NOMBRE'],
        'cargo': ['CARGO'],
        'tipoContrato': ['TIPO DE CONTRATO', 'TIPO_CONTRATO'],
        'fechaIngreso': ['FECHA INGRESO', 'FECHA_INGRESO'],
        'basico': ['BASICO ', 'BASICO'],
        'auxilioVivienda': ['AUXILIO VIVIENDA ', 'AUX_VIVIENDA'],
        'auxilioAlimentacion': ['AUXILIO ALIMENTA'],
        'auxilioMovilidad': ['AUXILIO DE MOVILIDAD'],
        'auxilioRodamiento': ['AUXILIO RODAMIENTO '],
        'auxilioProductividad': ['AUXILIO DE PRODUCTIVIDAD'],
        'auxilioComunic': ['AUXILIO COMUNIC'],
        'correo': ['CORREO']
    }

    def get_value(keys):
        for k in keys:
            v = doc.get(k)
            if v is not None:
                if isinstance(v, dict):
                    if '$numberInt' in v:
                        return float(v['$numberInt'])
                    if '$numberDouble' in v:
                        return float(v['$numberDouble'])
                return v
        return None

    fecha_iso = None
    try:
        raw = get_value(field_mapping['fechaIngreso'])
        if isinstance(raw, datetime):
            fecha_iso = raw.isoformat()
        elif raw:
            fecha_iso = datetime.fromisoformat(str(raw)).isoformat()
    except Exception as e:
        logger.error(f"Error procesando fecha: {e}")

    return Empleado(
        id=str(doc.get('_id', '')),
        identificacion=str(get_value(field_mapping['identificacion']) or ''),
        nombre=get_value(field_mapping['nombre']),
        cargo=get_value(field_mapping['cargo']),
        tipoContrato=get_value(field_mapping['tipoContrato']),
        fechaIngreso=fecha_iso,
        basico=float(get_value(field_mapping['basico']) or 0.0),
        auxilioVivienda=float(get_value(field_mapping['auxilioVivienda']) or 0.0),
        auxilioAlimentacion=float(get_value(field_mapping['auxilioAlimentacion']) or 0.0),
        auxilioMovilidad=float(get_value(field_mapping['auxilioMovilidad']) or 0.0),
        auxilioRodamiento=float(get_value(field_mapping['auxilioRodamiento']) or 0.0),
        auxilioProductividad=float(get_value(field_mapping['auxilioProductividad']) or 0.0),
        auxilioComunic=float(get_value(field_mapping['auxilioComunic']) or 0.0),
        correo=get_value(field_mapping['correo'])
    )

# Definición de rutas
ruta_empleado = APIRouter(
    prefix='/empleados',
    tags=['Empleados'],
    responses={status.HTTP_404_NOT_FOUND: {'message': 'No encontrado'}}
)

@ruta_empleado.get('/', response_model=List[Empleado])
async def get_empleados():
    try:
        docs = list(coleccion_empleados.find())
        return [transformar_empleado(d) for d in docs]
    except Exception as e:
        logger.error(f"Error obteniendo empleados: {e}")
        raise HTTPException(status_code=500, detail="Error al obtener empleados")

@ruta_empleado.get('/buscar', response_model=Empleado)
async def get_empleado_por_identificacion(
    identificacion: str = Query(..., description='Número de identificación')
):
    try:
        query = {'$or': []}
        campos = ['IDENTIFICACIÓN', 'identificacion']
        for c in campos:
            query['$or'].append({c: identificacion})
            if identificacion.isdigit():
                query['$or'].append({c: int(identificacion)})
        doc = coleccion_empleados.find_one(query)
        if not doc:
            raise HTTPException(status_code=404, detail='Empleado no encontrado')
        return transformar_empleado(doc)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error buscando empleado: {e}")
        raise HTTPException(status_code=500, detail="Error interno en la búsqueda")

@ruta_empleado.post('/enviar')
async def enviar_certificado(
    identificacion: str = Query(..., description='Identificación del empleado'),
    req: EnviarRequest = Body(...)
):
    try:
        empleado = await get_empleado_por_identificacion(identificacion)
        if not empleado.correo:
            raise HTTPException(status_code=400, detail='El empleado no tiene correo registrado')

        # Generar PDF
        buffer = BytesIO()
        c = canvas.Canvas(buffer, pagesize=A4)
        width, height = A4

        # Agregar fondo si es válido
        if validar_imagen_base64(fondo_base64):
            data_b64 = fondo_base64.split(',',1)[1] if ',' in fondo_base64 else fondo_base64
            try:
                img_bytes = base64.b64decode(data_b64)
                c.drawImage(ImageReader(BytesIO(img_bytes)), 0, 0, width=width, height=height)
            except Exception as e:
                logger.error(f"Error al agregar fondo: {e}")

        styles = getSampleStyleSheet()
        contenido = [
            Paragraph('EL DEPARTAMENTO DE GESTIÓN HUMANA', styles['Heading1']),
            Spacer(1, 24),
            Paragraph('CERTIFICA QUE:', styles['Heading2']),
            Spacer(1, 24),
            Paragraph(
                f"El señor/a <b>{empleado.nombre}</b>, identificado con cédula No. "
                f"<b>{empleado.identificacion}</b>, labora en nuestra empresa desde "
                f"<b>{empleado.fechaIngreso}</b>, desempeñando el cargo de <b>{empleado.cargo}</b> "
                f"con contrato <b>{empleado.tipoContrato}</b>.",
                styles['BodyText']
            )
        ]

        if req.incluirSalario:
            contenido.extend([
                Spacer(1, 24),
                Paragraph("Detalle salarial:", styles['Heading3']),
                Paragraph(f"Salario base: ${empleado.basico:,.2f}", styles['BodyText']),
            ])

        # Agregar firma si es válida
        if validar_imagen_base64(firma_base64):
            data_b64 = firma_base64.split(',',1)[1] if ',' in firma_base64 else firma_base64
            try:
                img_bytes = base64.b64decode(data_b64)
                c.drawImage(ImageReader(BytesIO(img_bytes)), width/2-75, 100, width=150, height=50)
            except Exception as e:
                logger.error(f"Error al agregar firma: {e}")

        Frame(40, 40, width-80, height-80).addFromList(contenido, c)
        c.save()

        # Validar PDF antes de enviar
        buffer.seek(0)
        pdf_bytes = buffer.getvalue()
        if not validar_pdf(pdf_bytes):
            raise HTTPException(status_code=500, detail="El PDF generado es inválido")

        # Linearizar si es posible
        if _HAVE_PIKEPDF:
            try:
                with Pdf.open(BytesIO(pdf_bytes)) as pdf:
                    opt = BytesIO()
                    pdf.save(opt, linearize=True)
                    pdf_bytes = opt.getvalue()
            except PdfError as e:
                logger.warning(f"No se pudo linearizar el PDF: {e}")

        # Enviar correo
        try:
            resend.Emails.send({
                'from': 'no-reply@integralogistica.com',
                'to': [empleado.correo],
                'subject': f'Certificado Laboral - {empleado.nombre}',
                'html': (
                    f"<p>Estimado/a {empleado.nombre},</p>"
                    "<p>Adjunto encontrará su certificado laboral actualizado.</p>"
                    "<p>Atentamente,<br>Recursos Humanos</p>"
                ),
                'attachments': [{
                    'filename': f'certificado_{empleado.identificacion}.pdf',
                    'content': base64.b64encode(pdf_bytes).decode('utf-8'),
                    'type': 'application/pdf'
                }]
            })
        except Exception as e:
            logger.error(f"Error enviando correo: {e}")
            raise HTTPException(status_code=500, detail="Error al enviar el correo")

        # Guardar en historial
        coleccion_historial.insert_one({
            'identificacion': empleado.identificacion,
            'nombre': empleado.nombre,
            'fecha_envio': datetime.now(),
            'incluyo_salario': req.incluirSalario
        })

        return JSONResponse(status_code=200, content={'message': 'Certificado enviado correctamente'})

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error general: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error interno al procesar la solicitud")

app = FastAPI()
app.include_router(ruta_empleado)
