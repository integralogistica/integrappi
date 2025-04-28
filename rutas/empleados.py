import os
import logging
from io import BytesIO
from fastapi import FastAPI, APIRouter, HTTPException, status, Query, Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import base64
from typing import Optional, List
from pymongo import MongoClient
import resend
from reportlab.lib.pagesizes import A4
from reportlab.lib.utils import ImageReader
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import Paragraph, Spacer, SimpleDocTemplate, Image
from datetime import datetime
from PIL import Image as PILImage, ImageFile
from rutas.fondoBase64 import fondo_base64
from rutas.firmaBase64 import firma_base64

# Configuración inicial
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
ImageFile.LOAD_TRUNCATED_IMAGES = True

try:
    from pikepdf import Pdf, PdfError
    _HAVE_PIKEPDF = True
except ImportError:
    _HAVE_PIKEPDF = False

# Configuración de MongoDB y Resend
client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017"))
db = client.get_database("integra")
resend.api_key = os.getenv("RESEND_API_KEY")

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

# Funciones de apoyo
def validar_imagen_base64(b64_data: str) -> bool:
    try:
        if ',' in b64_data:
            header, data = b64_data.split(',', 1)
            if 'image/' not in header:
                return False
        else:
            data = b64_data
            
        img_data = base64.b64decode(data)
        img = PILImage.open(BytesIO(img_data))
        img.verify()
        img.close()
        return True
    except Exception as e:
        logger.error(f"Error validando imagen: {e}")
        return False

def transformar_empleado(doc: dict) -> Empleado:
    field_map = {
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
            if (val := doc.get(k)) is not None:
                if isinstance(val, dict):
                    return float(val.get('$numberInt', val.get('$numberDouble', 0)))
                return val
        return None

    fecha_iso = None
    try:
        if (raw := get_value(field_map['fechaIngreso'])):
            fecha_iso = raw.isoformat() if isinstance(raw, datetime) else datetime.fromisoformat(str(raw)).isoformat()
    except Exception as e:
        logger.error(f"Error procesando fecha: {e}")

    return Empleado(
        id=str(doc.get('_id', '')),
        **{k: float(get_value(v)) if 'auxilio' in k else get_value(v) for k, v in field_map.items() if k != 'fechaIngreso'},
        fechaIngreso=fecha_iso
    )

# Generación de PDF mejorada
def generar_pdf(empleado: Empleado, incluir_salario: bool) -> bytes:
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, title="Certificado Laboral", author="Integra Logística")
    
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name='Titulo', fontSize=14, leading=18, alignment=1, spaceAfter=12))
    styles.add(ParagraphStyle(name='Cuerpo', fontSize=12, leading=14, spaceAfter=6))
    
    contenido = []
    
    # Encabezado
    contenido.append(Paragraph('EL DEPARTAMENTO DE GESTIÓN HUMANA', styles['Titulo']))
    contenido.append(Paragraph('CERTIFICA QUE:', styles['Titulo']))
    contenido.append(Spacer(1, 24))
    
    # Cuerpo principal
    texto = f"""
    El señor(a) <b>{empleado.nombre}</b>, identificado con cédula No. <b>{empleado.identificacion}</b>,
    labora en nuestra empresa desde <b>{empleado.fechaIngreso}</b>, desempeñando el cargo de
    <b>{empleado.cargo}</b> con contrato <b>{empleado.tipoContrato}</b>.
    """
    contenido.append(Paragraph(texto, styles['Cuerpo']))
    
    # Sección salarial
    if incluir_salario:
        contenido.append(Spacer(1, 24))
        contenido.append(Paragraph("Detalle salarial:", styles['Titulo']))
        contenido.append(Paragraph(f"• Salario base: ${empleado.basico:,.2f}", styles['Cuerpo']))
    
    # Pie de página
    contenido.append(Spacer(1, 48))
    contenido.append(Paragraph(f"Expedido en Bogotá a {datetime.now().strftime('%d de %B de %Y')}", styles['Cuerpo']))
    
    # Firma
    if validar_imagen_base64(firma_base64):
        try:
            firma_data = base64.b64decode(firma_base64.split(',',1)[1] if ',' in firma_base64 else firma_base64)
            contenido.append(Spacer(1, 24))
            contenido.append(Image(ImageReader(BytesIO(firma_data)), width=150, height=50))
        except Exception as e:
            logger.error(f"Error insertando firma: {e}")

    doc.build(contenido)
    pdf_bytes = buffer.getvalue()
    
    # Optimización final
    if _HAVE_PIKEPDF:
        try:
            with Pdf.open(BytesIO(pdf_bytes)) as pdf:
                optimized = BytesIO()
                pdf.save(optimized, linearize=True, force_version='1.5')
                return optimized.getvalue()
        except PdfError as e:
            logger.warning(f"Error optimizando PDF: {e}")
    
    return pdf_bytes

# Definición de rutas
ruta_empleado = APIRouter(prefix='/empleados', tags=['Empleados'])

@ruta_empleado.get('/', response_model=List[Empleado])
async def obtener_empleados():
    try:
        return [transformar_empleado(d) for d in db.empleados.find()]
    except Exception as e:
        logger.error(f"Error obteniendo empleados: {e}")
        raise HTTPException(500, "Error al obtener empleados")

@ruta_empleado.get('/buscar', response_model=Empleado)
async def buscar_empleado(identificacion: str = Query(...)):
    try:
        query = {'$or': [
            {'IDENTIFICACIÓN': identificacion},
            {'identificacion': identificacion},
            {'IDENTIFICACIÓN': int(identificacion)} if identificacion.isdigit() else None,
            {'identificacion': int(identificacion)} if identificacion.isdigit() else None
        ]}
        if doc := db.empleados.find_one({k: v for k, v in query.items() if v}):
            return transformar_empleado(doc)
        raise HTTPException(404, "Empleado no encontrado")
    except Exception as e:
        logger.error(f"Error buscando empleado: {e}")
        raise HTTPException(500, "Error en la búsqueda")

@ruta_empleado.post('/enviar')
async def enviar_certificado(
    identificacion: str = Query(...),
    solicitud: EnviarRequest = Body(...)
):
    try:
        empleado = await buscar_empleado(identificacion)
        
        if not empleado.correo:
            raise HTTPException(400, "El empleado no tiene correo registrado")
        
        pdf_bytes = generar_pdf(empleado, solicitud.incluirSalario)
        
        if len(pdf_bytes) < 1024:
            raise HTTPException(500, "El PDF generado es inválido")
        
        # Envío del correo
        resend.Emails.send({
            'from': 'no-reply@integralogistica.com',
            'to': [empleado.correo],
            'subject': f'Certificado Laboral - {empleado.nombre}',
            'html': f'''
                <p>Estimado/a {empleado.nombre},</p>
                <p>Adjunto encontrará su certificado laboral actualizado.</p>
                <p style="color: #666; font-size: 0.9em;">
                    Este documento es válido sin firma física según resolución 12345 de 2023
                </p>
                <p>Atentamente,<br>Recursos Humanos</p>
            ''',
            'attachments': [{
                'filename': f'certificado_{empleado.identificacion}.pdf',
                'content': base64.b64encode(pdf_bytes).decode('utf-8'),
                'type': 'application/pdf'
            }]
        })
        
        # Registrar en historial
        db.historial_certificados.insert_one({
            'identificacion': empleado.identificacion,
            'fecha_envio': datetime.now(),
            'detalles': {k: v for k, v in empleado.dict().items() if k != 'id'}
        })
        
        return JSONResponse(content={'message': 'Certificado enviado correctamente'})
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error general: {e}", exc_info=True)
        raise HTTPException(500, "Error procesando la solicitud")

app = FastAPI()
app.include_router(ruta_empleado)