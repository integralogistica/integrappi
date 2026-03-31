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
        "basico": _get_float(clean_doc, "basico", "salario_mensual", "BASICO"),
        "auxilioVivienda": _get_float(clean_doc, "auxilioVivienda", "auxilio_transporte", "AUXILIO VIVIENDA"),
        "auxilioAlimentacion": _get_float(clean_doc, "auxilioAlimentacion", "auxilio_alimentacion", "AUXILIO ALIMENTACION"),
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


def _dibujar_franjas_footer(c, width: float, height: float) -> None:
    """Dibuja las dos franjas verdes curvas en la parte inferior de la página."""
    # Franja 1 – verde lima oscuro (más grande, detrás) — semitransparente
    c.saveState()
    c.setFillColorRGB(0.44, 0.66, 0.18)
    c.setFillAlpha(0.20)
    p = c.beginPath()
    p.moveTo(0, 0)
    p.lineTo(width, 0)
    p.lineTo(width, 42)
    p.curveTo(width * 0.75, 58, width * 0.35, 50, 0, 60)
    p.close()
    c.drawPath(p, fill=1, stroke=0)
    c.restoreState()

    # Franja 2 – verde lima claro (más pequeña, encima)
    c.saveState()
    c.setFillColorRGB(0.55, 0.78, 0.25)
    c.setFillAlpha(0.20)
    p = c.beginPath()
    p.moveTo(0, 0)
    p.lineTo(width, 0)
    p.lineTo(width, 26)
    p.curveTo(width * 0.65, 38, width * 0.30, 32, 0, 40)
    p.close()
    c.drawPath(p, fill=1, stroke=0)
    c.restoreState()


def generar_pdf_certificado(emp: Dict[str, Any], incluir_salario: bool) -> bytes:
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4

    imagenes_dir = os.path.normpath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "imagenes")
    )

    # ── 1. Marca de agua: albatros centrado, muy tenue ──────────────────────
    albatros_path = os.path.join(imagenes_dir, "albatros.png")
    if os.path.exists(albatros_path):
        wm_size = 340
        c.saveState()
        c.setFillAlpha(0.07)
        c.drawImage(
            albatros_path,
            x=(width - wm_size) / 2 + 60,
            y=(height - wm_size) / 2 - 30,
            width=wm_size,
            height=wm_size,
            preserveAspectRatio=True,
            mask="auto",
        )
        c.restoreState()

    # ── 2. Franjas verdes inferiores ────────────────────────────────────────
    _dibujar_franjas_footer(c, width, height)

    # ── 3. Texto de pie de página ───────────────────────────────────────────
    c.saveState()
    c.setFont("Helvetica", 7.5)
    c.setFillColorRGB(0.2, 0.2, 0.2)
    c.drawCentredString(
        width / 2, 85,
        "Oficina Principal: Carrera 27a No. 49a - 36 Bogotá D.C., Colombia - Teléfono: 7006232"
    )
    c.drawCentredString(width / 2, 75, "www.integraenvios.com")
    c.restoreState()

    # ── 4. Encabezado: recuadro punteado dividido en dos celdas ─────────────
    box_x, box_y = 40, height - 90
    box_w, box_h = width - 80, 74
    mid_x = box_x + box_w * 0.42   # divisor vertical, celda izquierda ~42 %

    c.saveState()
    c.setDash(4, 4)
    c.setLineWidth(0.8)
    c.setStrokeColorRGB(0.5, 0.5, 0.5)
    c.rect(box_x, box_y, box_w, box_h)          # caja exterior
    c.line(mid_x, box_y, mid_x, box_y + box_h)  # separador vertical
    c.restoreState()

    # Celda izquierda – logo Integra
    logo_integra = os.path.join(imagenes_dir, "logo_integra.png")
    if os.path.exists(logo_integra):
        cell_w = mid_x - box_x
        logo_h = 54
        logo_w = logo_h * 2.5
        lx = box_x + (cell_w - logo_w) / 2
        ly = box_y + (box_h - logo_h) / 2
        c.drawImage(logo_integra, lx, ly, width=logo_w, height=logo_h,
                    preserveAspectRatio=True, mask="auto")

    # Celda derecha – ISO + BASC lado a lado
    right_x = mid_x
    right_w = (box_x + box_w) - mid_x
    cert_h = 46
    logos_cert = [("iso.png", cert_h * 1.0), ("basc.jpg", cert_h * 1.0)]
    total_cert_w = sum(w for _, w in logos_cert) + 16
    cx = right_x + (right_w - total_cert_w) / 2
    for nombre, lw in logos_cert:
        ruta = os.path.join(imagenes_dir, nombre)
        if os.path.exists(ruta):
            ly = box_y + (box_h - cert_h) / 2
            c.drawImage(ruta, cx, ly, width=lw, height=cert_h,
                        preserveAspectRatio=True, mask="auto")
        cx += lw + 16

    # ── 5. Contenido del certificado ─────────────────────────────────────────
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "Title", parent=styles["Heading1"],
        alignment=1, fontName="Times-Bold", fontSize=16, leading=20
    )
    subtitle_style = ParagraphStyle(
        "Subtitle", parent=styles["Heading3"],
        alignment=1, fontName="Times-Bold", fontSize=14, leading=16
    )
    body_style = ParagraphStyle(
        "Body", parent=styles["Normal"],
        fontName="Times-Roman", fontSize=14, leading=18, alignment=TA_JUSTIFY
    )
    info_style = ParagraphStyle(
        "Info", parent=styles["Normal"],
        fontName="Times-Roman", fontSize=14, leading=16, alignment=TA_JUSTIFY
    )

    meses_espanol = [
        "enero", "febrero", "marzo", "abril", "mayo", "junio",
        "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"
    ]

    try:
        dt_ing = datetime.fromisoformat(emp.get("fechaIngreso") or "")
        fecha_humana = f"{dt_ing.day} de {meses_espanol[dt_ing.month - 1]} de {dt_ing.year}"
    except Exception:
        fecha_humana = emp.get("fechaIngreso") or ""

    cedula_raw = emp.get("identificacion") or ""
    ced = f"{int(cedula_raw):,}".replace(",", ".") if cedula_raw.isdigit() else cedula_raw

    texto = (
        f"El señor/a <b>{emp.get('nombre', '')}</b>, identificado/a con cédula número <b>{ced}</b>, "
        f"labora en nuestra empresa desde <b>{fecha_humana}</b>, desempeñando el cargo de "
        f"<b>{emp.get('cargo', '')}</b> con contrato a término <b>{emp.get('tipoContrato', '')}</b>,"
    )

    if incluir_salario and emp.get("basico") and emp["basico"] > 0:
        texto += (
            f" con un salario fijo mensual por valor de "
            f"<b>${int(emp['basico']):,}</b> pesos".replace(",", ".")
        )

    now = datetime.now()
    fecha_cert = f"{now.day} de {meses_espanol[now.month - 1]} de {now.year}"

    story = [
        Spacer(1, 80),
        Paragraph("EL DEPARTAMENTO DE GESTIÓN HUMANA", title_style),
        Spacer(1, 16),
        Paragraph("CERTIFICA QUE:", subtitle_style),
        Spacer(1, 18),
        Paragraph(texto, body_style),
    ]

    aux_items = [
        ("Auxilio Vivienda", emp.get("auxilioVivienda")),
        ("Auxilio Alimentación", emp.get("auxilioAlimentacion")),
        ("Auxilio Movilidad", emp.get("auxilioMovilidad")),
        ("Auxilio Rodamiento", emp.get("auxilioRodamiento")),
        ("Auxilio Productividad", emp.get("auxilioProductividad")),
        ("Auxilio Comunic", emp.get("auxilioComunic")),
    ]

    if incluir_salario and any(v and v > 0 for _, v in aux_items):
        story.append(Spacer(1, 8))
        story.append(Paragraph(
            "Más un auxilio no salarial de manera liberalidad por concepto de:", body_style
        ))
        for label, v in aux_items:
            if v and v > 0:
                story.append(Spacer(1, 6))
                story.append(Paragraph(
                    f"<b>{label}:</b> ${int(v):,}".replace(",", "."), body_style
                ))

    story.append(Spacer(1, 12))
    story.append(Paragraph(
        "Para mayor información de ser necesario: PBX 7006232 o celular 3183385709.", info_style
    ))
    story.append(Spacer(1, 8))
    story.append(Paragraph(
        f"La presente certificación se expide a solicitud del interesado el {fecha_cert} "
        f"en la ciudad de Bogotá.",
        info_style,
    ))
    story.append(Spacer(1, 8))
    story.append(Paragraph("Cordialmente,", info_style))

    # Frame: empieza debajo del header, termina encima del área de firma
    frame = Frame(85, 340, width - 85 * 2, height - 430, showBoundary=0)
    frame.addFromList(story, c)

    # ── 6. Firma ──────────────────────────────────────────────────────────────
    y_base = 300
    c.setFont("Times-Bold", 12)
    c.setFillColorRGB(0, 0, 0)
    c.drawCentredString(width / 2, y_base + 10, "PATRICIA LEAL AROCA")
    c.drawCentredString(width / 2, y_base - 5, "Certificado laboral")
    c.drawCentredString(width / 2, y_base - 17, "Gerente de gestión humana")
    c.drawCentredString(width / 2, y_base - 29, "Integra cadena de servicios")

    firma_path = os.path.join(imagenes_dir, "firmaPatricia.png")
    if os.path.exists(firma_path):
        c.drawImage(firma_path, x=width / 2 - 75, y=y_base - 5,
                    width=150, height=55, mask="auto")

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