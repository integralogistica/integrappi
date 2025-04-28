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
coleccion_historial = db["historial_certificados"]  # Nueva colección para historial

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
    """
    Normaliza las claves del documento eliminando espacios en los nombres de campo,
    y extrae valores de texto y numéricos correctamente.
    """
    # Limpiar claves: quitar espacios al inicio y final
    clean_doc = {k.strip(): v for k, v in doc.items()}

    # Función para obtener valor de múltiples posibles claves
    def get_val(*keys):
        for k in keys:
            if k in clean_doc and clean_doc[k] is not None:
                return clean_doc[k]
        return None

    # Función para obtener floater
    def get_float(*keys):
        for k in keys:
            val = clean_doc.get(k)
            if val in (None, ""):
                continue
            if isinstance(val, (int, float)):
                return float(val)
            # Convertir cadenas numéricas
            s = str(val).replace(".", "").replace(",", "").strip()
            if s.isdigit():
                return float(s)
        return 0.0

    # Manejo de fecha de ingreso
    fecha_raw = get_val("fechaIngreso", "FECHA_INGRESO", "FECHA INGRESO")
    if hasattr(fecha_raw, "isoformat"):
        fecha_ing = fecha_raw.isoformat()
    else:
        fecha_ing = str(fecha_raw or "")

    # Construcción del modelo Empleado
    empleado = Empleado(
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
    return empleado

# ——— Router y rutas ———
ruta_empleado = APIRouter(
    prefix="/empleados", tags=["Empleados"],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}}
)

@ruta_empleado.get("/", response_model=List[Empleado])
async def get_empleados():
    """Devuelve la lista de todos los empleados."""
    docs = coleccion_empleados.find()
    return [transformar_empleado(doc) for doc in docs]

@ruta_empleado.get("/buscar", response_model=Empleado)
async def get_empleado_por_identificacion(
    identificacion: str = Query(..., description="Número de identificación")
):
    """Busca un empleado por su identificación."""
    filtros = {"$or": [
        {"identificacion": identificacion},
        {"identificacion": int(identificacion)} if identificacion.isdigit() else None,
        {"IDENTIFICACIÓN": identificacion},
        {"IDENTIFICACIÓN": int(identificacion)} if identificacion.isdigit() else None
    ]}
    # Filtrar None
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
    """Genera y envía por correo el certificado laboral. Registra la solicitud en historial."""
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

    # Registrar en historial
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

    # Dibujar fondo si existe
    fondo_clean = fondo_base64.split(",", 1)[1] if fondo_base64.startswith("data:image") else fondo_base64
    try:
        c.drawImage(
            ImageReader(BytesIO(base64.b64decode(fondo_clean))),
            0, 0, width=width, height=height
        )
    except Exception:
        pass

    # Estilos de texto
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "Title", parent=styles["Heading1"], alignment=1,
        fontName="Times-Bold", fontSize=14, leading=18
    )
    subtitle_style = ParagraphStyle(
        "Subtitle", parent=styles["Heading3"], alignment=1,
        fontName="Times-Bold", fontSize=12, leading=14
    )
    body_style = ParagraphStyle(
        "Body", parent=styles["Normal"],
        fontName="Times-Roman", fontSize=12, leading=16
    )
    info_style = ParagraphStyle(
        "Info", parent=styles["Normal"],
        fontName="Times-Roman", fontSize=12, leading=14
    )

    # Construcción del certificado
    header = Paragraph("EL DEPARTAMENTO DE GESTIÓN HUMANA", title_style)
    subtitle = Paragraph("CERTIFICA QUE:", subtitle_style)

    # Fecha de ingreso legible
    try:
        dt_ing = datetime.fromisoformat(emp.fechaIngreso)
        meses_esp = [
            "enero","febrero","marzo","abril","mayo","junio",
            "julio","agosto","septiembre","octubre","noviembre","diciembre"
        ]
        fecha_humana = f"{dt_ing.day} de {meses_esp[dt_ing.month-1]} de {dt_ing.year}"
    except Exception:
        fecha_humana = emp.fechaIngreso or ""

    # Texto principal
    ced = (
        f"{int(emp.identificacion):,}".replace(",", ".")
        if emp.identificacion.isdigit() else emp.identificacion
    )
    texto = (
        f"El señor/a <b>{emp.nombre}</b>, identificado/a con cédula número <b>{ced}</b>, "
        f"labora en nuestra empresa desde <b>{fecha_humana}</b>, desempeñando el cargo de "
        f"<b>{emp.cargo}</b> con contrato a término <b>{emp.tipoContrato}</b>."
    )
    if show_salary and emp.basico > 0:
        texto += f" Con un salario fijo mensual por valor de <b>{int(emp.basico):,}</b> pesos."
    body = Paragraph(texto, body_style)

    # Fecha de certificación
    now = datetime.now()
    fecha_cert = f"{now.day} de {meses_esp[now.month-1]} de {now.year}"

    story = [
        Spacer(1, 50), header, Spacer(1, 16), subtitle, Spacer(1, 16), body
    ]

    # Auxilios
    aux_items = [
        ("Auxilio Vivienda", emp.auxilioVivienda),
        ("Auxilio Alimentación", emp.auxilioAlimentacion),
        ("Auxilio Movilidad", emp.auxilioMovilidad),
        ("Auxilio Rodamiento", emp.auxilioRodamiento),
        ("Auxilio Productividad", emp.auxilioProductividad),
        ("Auxilio Comunic", emp.auxilioComunic)
    ]
    if show_salary and any(val > 0 for _, val in aux_items):
        story.append(Spacer(1, 6))
        story.append(Paragraph(
            "más un auxilio no salarial de mera liberalidad por concepto de:",
            body_style
        ))
        for label, val in aux_items:
            if val > 0:
                story.append(Spacer(1, 6))
                story.append(Paragraph(
                    f"<b>{label}:</b> {int(val):,}".replace(",", "."),
                    body_style
                ))

    # Pie y contacto
    story.append(Spacer(1, 10))
    story.append(Paragraph(
        "Para mayor información de ser necesario: PBX 7006232 o celular 3183385709.",
        info_style
    ))
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        f"La presente certificación se expide a solicitud del interesado, dado a {fecha_cert} en la ciudad de Bogotá.",
        info_style
    ))
    story.append(Spacer(1, 6))
    story.append(Paragraph("Cordialmente,", info_style))

    # Renderizar contenido
    frame = Frame(40, 340, width - 80, height - 380, showBoundary=0)
    frame.addFromList(story, c)

    # Firma digital
    firma_clean = firma_base64.split(",", 1)[1]
    y_base = 300
    c.setFont("Times-Bold", 12)
    c.drawCentredString(width/2, y_base + 5, "PATRICIA LEAL AROCA")
    c.setFont("Times-Roman", 10)
    c.drawCentredString(width/2, y_base - 10, "Certificado laboral")
    c.drawCentredString(width/2, y_base - 22, "Gerente de gestión humana")
    c.drawCentredString(width/2, y_base - 34, "Integra cadena de servicios")
    c.drawImage(
        ImageReader(BytesIO(base64.b64decode(firma_clean))),
        width/2 - 75, y_base - 10,
        width=150, height=50, mask='auto'
    )

    c.showPage()
    c.save()
    buffer.seek(0)

    # Enviar correo
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

# ——— Montar FastAPI ———
app = FastAPI()
app.include_router(ruta_empleado)
