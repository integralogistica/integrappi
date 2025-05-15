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
from reportlab.lib.enums import TA_JUSTIFY 
from dotenv import load_dotenv
load_dotenv()

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
            num = str(val).replace(".", "").replace(",", "").strip()
            if num.isdigit():
                return float(num)
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
    return [transformar_empleado(doc) for doc in coleccion_empleados.find()]

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

    # Fondo de página completa desde nueva URL
    try:
        bg_url = "https://storage.googleapis.com/integrapp/Imagenes/FONDO%20INTEGRA%20CORPORATIVO.png"
        bg_data = urlopen(bg_url).read()
        img = ImageReader(BytesIO(bg_data))
        c.saveState()
        c.setFillAlpha(0.6)
        c.drawImage(img, 0, 0, width=width, height=height, mask='auto')
        c.restoreState()
    except Exception:
        pass

    # Estilos y contenido
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle("Title", parent=styles["Heading1"], alignment=1, fontName="Times-Bold", fontSize=16, leading=18)
    subtitle_style = ParagraphStyle("Subtitle", parent=styles["Heading3"], alignment=1, fontName="Times-Bold", fontSize=14, leading=14)
    body_style = ParagraphStyle("Body", parent=styles["Normal"], fontName="Times-Roman", fontSize=14, leading=16, alignment=TA_JUSTIFY)
    info_style = ParagraphStyle("Info", parent=styles["Normal"], fontName="Times-Roman", fontSize=14, leading=14, alignment=TA_JUSTIFY)

    header = Paragraph("EL DEPARTAMENTO DE GESTIÓN HUMANA", title_style)
    subtitle = Paragraph("CERTIFICA QUE:", subtitle_style)

    try:
        dt_ing = datetime.fromisoformat(emp.fechaIngreso)
        meses_esp = ["enero","febrero","marzo","abril","mayo","junio","julio","agosto","septiembre","octubre","noviembre","diciembre"]
        fecha_humana = f"{dt_ing.day} de {meses_esp[dt_ing.month-1]} de {dt_ing.year}"
    except Exception:
        fecha_humana = emp.fechaIngreso or ""

    ced = (f"{int(emp.identificacion):,}".replace(",", ".") if emp.identificacion.isdigit() else emp.identificacion)
    texto = (
        f"El señor/a <b>{emp.nombre}</b>, identificado/a con cédula número <b>{ced}</b>, "
        f"labora en nuestra empresa desde <b>{fecha_humana}</b>, desempeñando el cargo de <b>{emp.cargo}</b> con contrato a término <b>{emp.tipoContrato}</b>,"
    )
    if show_salary and emp.basico > 0:
        texto += f" con un salario fijo mensual por valor de $<b>{int(emp.basico):,}</b> pesos"
    body = Paragraph(texto, body_style)

    now = datetime.now()
    fecha_cert = f"{now.day} de {meses_esp[now.month-1]} de {now.year}"
    story = [Spacer(1, 75), header, Spacer(1, 16), subtitle, Spacer(1, 16), body]
    aux_items = [("Auxilio Vivienda", emp.auxilioVivienda), ("Auxilio Alimentación", emp.auxilioAlimentacion), ("Auxilio Movilidad", emp.auxilioMovilidad), ("Auxilio Rodamiento", emp.auxilioRodamiento), ("Auxilio Productividad", emp.auxilioProductividad), ("Auxilio Comunic", emp.auxilioComunic)]
    if show_salary and any(v>0 for _, v in aux_items):
        story.append(Spacer(1,6))
        story.append(Paragraph("Más un auxilio no salarial de mera liberalidad por concepto de:", body_style))
        for label, v in aux_items:
            if v>0:
                story.append(Spacer(1,6))
                story.append(Paragraph(f"<b>{label}:</b> ${int(v):,}".replace(",","."), body_style))
    story.append(Spacer(1,10))
    story.append(Paragraph("Para mayor información de ser necesario: PBX 7006232 o celular 3183385709.", info_style))
    story.append(Spacer(1,6))
    story.append(Paragraph(f"La presente certificación se expide a solicitud del interesado el {fecha_cert} en la ciudad de Bogotá.", info_style))
    story.append(Spacer(1,6))
    story.append(Paragraph("Cordialmente,", info_style))

    frame = Frame(85, 340, width-85*2, height-380, showBoundary=0)
    frame.addFromList(story, c)

    # Firma y textos adicionales debajo
    y_base = 300
    c.setFont("Times-Bold", 12)
    c.drawCentredString(width/2, y_base + 5, "PATRICIA LEAL AROCA")
    c.drawCentredString(width/2, y_base - 10, "Certificado laboral")
    c.drawCentredString(width/2, y_base - 22, "Gerente de gestión humana")
    c.drawCentredString(width/2, y_base - 34, "Integra cadena de servicios")
    try:
        sig_url = "https://storage.googleapis.com/integrapp/Imagenes/firma%20patricia.png"
        sig_data = urlopen(sig_url).read()
        c.drawImage(ImageReader(BytesIO(sig_data)), x=width/2-75, y=y_base-10, width=150, height=50, mask='auto')
    except Exception:
        pass

    c.showPage()
    c.save()

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
