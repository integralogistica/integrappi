# Funciones/whatsapp_certificado_integra.py
import os
import io
import base64
import math
from io import BytesIO
from urllib.request import urlopen
from datetime import datetime
from typing import Optional, Dict, Any, Tuple

from pymongo import MongoClient
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib.utils import ImageReader
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.platypus import Paragraph, Frame, Spacer
from reportlab.lib.enums import TA_JUSTIFY
import resend

mongo_uri = os.getenv("MONGO_URI")
if not mongo_uri:
    raise ValueError("La variable de entorno MONGO_URI no está configurada.")

client = MongoClient(mongo_uri)
db = client["integra"]
coleccion_empleados = db["empleados"]
coleccion_historial = db["historial_certificados"]

resend_api_key = os.getenv("RESEND_API_KEY")
if not resend_api_key:
    raise ValueError("La variable de entorno RESEND_API_KEY no está configurada.")
resend.api_key = resend_api_key


def _get_val(clean_doc: dict, *keys):
    for k in keys:
        val = clean_doc.get(k)
        if val is not None:
            return val
    return None


def _get_float(clean_doc: dict, *keys) -> Optional[float]:
    for k in keys:
        val = clean_doc.get(k)
        if val in (None, "", " ", "NaN", "nan"):
            continue
        if isinstance(val, (int, float)):
            if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
                continue
            return float(val)
        s = str(val).strip()
        if s.lower() in ("nan", "inf", "+inf", "-inf"):
            continue
        num = s.replace(".", "").replace(",", "")
        if num.isdigit():
            return float(num)
    return None


def _transformar_empleado(doc: dict) -> Dict[str, Any]:
    clean_doc = {k.strip(): v for k, v in doc.items()}

    fecha_raw = _get_val(clean_doc, "fechaIngreso", "fecha_ingreso", "FECHA_INGRESO", "FECHA INGRESO")
    if hasattr(fecha_raw, "isoformat"):
        fecha_ing = fecha_raw.isoformat()
    else:
        fecha_ing = str(fecha_raw or "")

    nombre = " ".join(
        filter(
            None,
            [
                _get_val(clean_doc, "primer_nombre"),
                _get_val(clean_doc, "segundo_nombre"),
                _get_val(clean_doc, "primer_apellido"),
                _get_val(clean_doc, "segundo_apellido"),
            ],
        )
    ).strip()

    return {
        "identificacion": str(_get_val(clean_doc, "identificacion", "IDENTIFICACIÓN") or ""),
        "nombre": nombre or _get_val(clean_doc, "nombre", "NOMBRE") or "",
        "cargo": _get_val(clean_doc, "cargo", "cargo_laboral", "CARGO") or "",
        "tipoContrato": _get_val(clean_doc, "tipoContrato", "tipo_contrato", "TIPO_DE_CONTRATO", "TIPO DE CONTRATO") or "",
        "fechaIngreso": fecha_ing,
        "basico": _get_float(clean_doc, "basico", "salario_mes", "BASICO"),
        "auxilioVivienda": _get_float(clean_doc, "auxilioVivienda", "auxilio_transporte", "AUXILIO VIVIENDA"),
        "auxilioAlimentacion": _get_float(clean_doc, "auxilioAlimentacion", "auxilio_alimentacion", "AUXILIO ALIMENTA"),
        "auxilioMovilidad": _get_float(clean_doc, "auxilioMovilidad", "auxilio_transporte", "AUXILIO DE MOVILIDAD"),
        "auxilioRodamiento": _get_float(clean_doc, "auxilioRodamiento", "auxilio_rodamiento", "AUXILIO RODAMIENTO"),
        "auxilioProductividad": _get_float(clean_doc, "auxilioProductividad", "auxilio_productividad", "AUXILIO DE PRODUCTIVIDAD"),
        "auxilioComunic": _get_float(clean_doc, "auxilioComunic", "auxilio_comunic", "AUXILIO COMUNIC"),
        "correo": _get_val(clean_doc, "correo", "email", "CORREO"),
    }


def _buscar_empleado_por_cedula(identificacion: str) -> Optional[Dict[str, Any]]:
    filtros = {
        "$or": [
            {"identificacion": identificacion},
            {"identificacion": int(identificacion)} if identificacion.isdigit() else None,
            {"IDENTIFICACIÓN": identificacion},
            {"IDENTIFICACIÓN": int(identificacion)} if identificacion.isdigit() else None,
        ]
    }
    filtros["$or"] = [f for f in filtros["$or"] if f]
    doc = coleccion_empleados.find_one(filtros)
    if not doc:
        return None
    return _transformar_empleado(doc)


def generar_pdf_certificado(emp: Dict[str, Any], incluir_salario: bool) -> bytes:
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4

    # Fondo
    try:
        bg_url = "https://storage.googleapis.com/integrapp/Imagenes/FONDO%20INTEGRA%20CORPORATIVO.png"
        bg_data = urlopen(bg_url).read()
        img = ImageReader(BytesIO(bg_data))
        c.saveState()
        c.setFillAlpha(0.6)
        c.drawImage(img, 0, 0, width=width, height=height, mask="auto")
        c.restoreState()
    except Exception:
        pass

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle("Title", parent=styles["Heading1"], alignment=1, fontName="Times-Bold", fontSize=16, leading=18)
    subtitle_style = ParagraphStyle("Subtitle", parent=styles["Heading3"], alignment=1, fontName="Times-Bold", fontSize=14, leading=14)
    body_style = ParagraphStyle("Body", parent=styles["Normal"], fontName="Times-Roman", fontSize=14, leading=16, alignment=TA_JUSTIFY)
    info_style = ParagraphStyle("Info", parent=styles["Normal"], fontName="Times-Roman", fontSize=14, leading=14, alignment=TA_JUSTIFY)

    header = Paragraph("EL DEPARTAMENTO DE GESTIÓN HUMANA", title_style)
    subtitle = Paragraph("CERTIFICA QUE:", subtitle_style)

    meses_esp = ["enero","febrero","marzo","abril","mayo","junio","julio","agosto","septiembre","octubre","noviembre","diciembre"]

    try:
        dt_ing = datetime.fromisoformat(emp.get("fechaIngreso") or "")
        fecha_humana = f"{dt_ing.day} de {meses_esp[dt_ing.month-1]} de {dt_ing.year}"
    except Exception:
        fecha_humana = emp.get("fechaIngreso") or ""

    ced_raw = emp.get("identificacion") or ""
    ced = f"{int(ced_raw):,}".replace(",", ".") if ced_raw.isdigit() else ced_raw

    texto = (
        f"El señor/a <b>{emp.get('nombre','')}</b>, identificado/a con cédula número <b>{ced}</b>, "
        f"labora en nuestra empresa desde <b>{fecha_humana}</b>, desempeñando el cargo de <b>{emp.get('cargo','')}</b> "
        f"con contrato a término <b>{emp.get('tipoContrato','')}</b>,"
    )

    if incluir_salario and emp.get("basico") and emp["basico"] > 0:
        texto += f" con un salario fijo mensual por valor de $<b>{int(emp['basico']):,}</b> pesos".replace(",", ".")

    body = Paragraph(texto, body_style)

    now = datetime.now()
    fecha_cert = f"{now.day} de {meses_esp[now.month-1]} de {now.year}"

    story = [Spacer(1, 75), header, Spacer(1, 16), subtitle, Spacer(1, 16), body]

    aux_items = [
        ("Auxilio Vivienda", emp.get("auxilioVivienda")),
        ("Auxilio Alimentación", emp.get("auxilioAlimentacion")),
        ("Auxilio Movilidad", emp.get("auxilioMovilidad")),
        ("Auxilio Rodamiento", emp.get("auxilioRodamiento")),
        ("Auxilio Productividad", emp.get("auxilioProductividad")),
        ("Auxilio Comunic", emp.get("auxilioComunic")),
    ]

    if incluir_salario and any(v and v > 0 for _, v in aux_items):
        story.append(Spacer(1, 6))
        story.append(Paragraph("Más un auxilio no salarial de mera liberalidad por concepto de:", body_style))
        for label, v in aux_items:
            if v and v > 0:
                story.append(Spacer(1, 6))
                story.append(Paragraph(f"<b>{label}:</b> ${int(v):,}".replace(",", "."), body_style))

    story.append(Spacer(1, 10))
    story.append(Paragraph("Para mayor información de ser necesario: PBX 7006232 o celular 3183385709.", info_style))
    story.append(Spacer(1, 6))
    story.append(Paragraph(f"La presente certificación se expide a solicitud del interesado el {fecha_cert} en la ciudad de Bogotá.", info_style))
    story.append(Spacer(1, 6))
    story.append(Paragraph("Cordialmente,", info_style))

    frame = Frame(85, 340, width - 85 * 2, height - 380, showBoundary=0)
    frame.addFromList(story, c)

    # Firma
    y_base = 300
    c.setFont("Times-Bold", 12)
    c.drawCentredString(width / 2, y_base + 5, "PATRICIA LEAL AROCA")
    c.drawCentredString(width / 2, y_base - 10, "Certificado laboral")
    c.drawCentredString(width / 2, y_base - 22, "Gerente de gestión humana")
    c.drawCentredString(width / 2, y_base - 34, "Integra cadena de servicios")

    try:
        sig_url = "https://storage.googleapis.com/integrapp/Imagenes/firma%20patricia.png"
        sig_data = urlopen(sig_url).read()
        c.drawImage(ImageReader(BytesIO(sig_data)), x=width / 2 - 75, y=y_base - 10, width=150, height=50, mask="auto")
    except Exception:
        pass

    c.showPage()
    c.save()

    pdf_data = buffer.getvalue()
    buffer.close()
    return pdf_data


def enviar_correo_certificado(emp: Dict[str, Any], pdf_data: bytes) -> None:
    payload = {
        "from": "no-reply@integralogistica.com",
        "to": [emp["correo"]],
        "subject": f"Certificado Laboral - {emp.get('nombre','')}",
        "html": f"<p>Hola {emp.get('nombre','')},</p><p>Adjunto tu certificado laboral.</p>",
        "attachments": [
            {
                "filename": f"certificado_{emp.get('identificacion','')}.pdf",
                "content": base64.b64encode(pdf_data).decode(),
            }
        ],
    }
    resend.Emails.send(payload)


def generar_y_enviar_certificado_por_cedula(cedula: str, incluir_salario: bool = False) -> Tuple[bool, str, Optional[str]]:
    """
    Returns:
      (ok, mensaje, correo_destino)
    """
    emp = _buscar_empleado_por_cedula(cedula)
    if not emp:
        return (False, "Empleado no encontrado.", None)

    if not emp.get("correo"):
        return (False, "El empleado no tiene correo registrado.", None)

    # Historial
    try:
        coleccion_historial.insert_one(
            {
                "identificacion": emp["identificacion"],
                "nombre": emp.get("nombre"),
                "fecha_solicitud": datetime.now(),
                "canal": "whatsapp",
            }
        )
    except Exception:
        pass

    pdf_data = generar_pdf_certificado(emp, incluir_salario=incluir_salario)
    enviar_correo_certificado(emp, pdf_data)
    return (True, "Correo enviado correctamente.", emp["correo"])
