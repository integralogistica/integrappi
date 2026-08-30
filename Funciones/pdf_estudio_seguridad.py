"""PDF del Estudio de Seguridad (reportlab platypus, A4 vertical).

Genera el informe consolidado EXCLUSIVAMENTE desde el documento persistido de
`estudios_seguridad` — nunca de objetos volátiles — lo que lo hace reproducible:
`POST /{consulta_id}/pdf/regenerar` reconstruye el mismo informe sin volver a
tocar los portales.

Estructura (referencia: reporte de TusDatos):
  1. Portada / resumen ejecutivo con semáforos por fuente y QR de verificación.
  2. Detalle de manifiestos RNDC (tabla, tope de filas).
  3. Detalle de Procuraduría (veredicto destacado).
  4. Trazabilidad / auditoría.
  5. Disposiciones legales (Ley 1238 de 2008, Ley 1581 de 2012).
  6. Marca de agua diagonal en TODAS las páginas: empresa | usuario | fecha |
     consulta_id (identifica el origen de cualquier copia/screenshot).
  7. Footer "Página X de Y" (NumberedCanvas de dos pasadas).
"""
from __future__ import annotations

import io
import logging
import os
import re
from datetime import datetime, timedelta, timezone

from reportlab import rl_config
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas as canvas_module
from reportlab.platypus import (
    BaseDocTemplate,
    Frame,
    PageTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
)
from xml.sax.saxutils import escape

# PDF reproducible byte a byte: fija /CreationDate y el /ID del trailer.
# Sin esto, el mismo doc de estudio generaría hashes distintos y la
# regeneración no podría verificarse por sha256.
rl_config.invariant = 1

logger = logging.getLogger(__name__)

# --- Configuración -------------------------------------------------------------
MAX_VIAJES_PDF = int(os.getenv("SEGURIDAD_MAX_VIAJES_PDF", "300"))
URL_PUBLICA = os.getenv("SEGURIDAD_ESTUDIOS_URL_PUBLICA", "http://localhost:8000")
LOGO_DEFAULT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "imagenes", "logo_integra.png")
_TZ_BOGOTA = timezone(timedelta(hours=-5))  # Colombia es UTC−5

ANCHO, ALTO = A4
MARGEN = 16 * mm

# Colores de estado (plan): EXITO verde, ADVERTENCIA ámbar, fallo rojo.
COLOR_EXITO = colors.HexColor("#1A7F37")
COLOR_ADVERTENCIA = colors.HexColor("#B58900")
COLOR_FALLO = colors.HexColor("#C0392B")
COLOR_NEUTRO = colors.HexColor("#57606A")
COLOR_PRIMARIO = colors.HexColor("#0F2A43")
COLOR_FONDO_TABLA = colors.HexColor("#F0F3F7")

ESTADO_GLOBAL_TEXTO = {
    "COMPLETADA": ("ESTUDIO COMPLETADO", COLOR_EXITO),
    "COMPLETADA_CON_ADVERTENCIAS": ("COMPLETADO CON ADVERTENCIAS", COLOR_ADVERTENCIA),
    "PARCIAL": ("PARCIAL — FUENTE(S) NO DISPONIBLE(S)", COLOR_FALLO),
    "ERROR": ("ERROR — SIN RESULTADOS", COLOR_FALLO),
    "EN_PROGRESO": ("EN PROGRESO", COLOR_NEUTRO),
}
ESTADO_FUENTE_TEXTO = {
    "EXITO": ("CONSULTADA", COLOR_EXITO),
    "ADVERTENCIA": ("CON ADVERTENCIA", COLOR_ADVERTENCIA),
    "NO_DISPONIBLE": ("NO DISPONIBLE", COLOR_FALLO),
    "ERROR": ("ERROR", COLOR_FALLO),
    "DESHABILITADA": ("NO HABILITADA", COLOR_NEUTRO),
}

# Columnas del portal RNDC que caben en A4 (las demás se omiten con nota),
# con su peso de ancho relativo (la tabla totaliza el ancho útil de la hoja).
COLUMNAS_VIAJE = [
    ("Nro. de Radicado", 1.0),
    ("Fecha Hora Radicación", 1.15),
    ("Nombre Empresa Transportadora", 1.7),
    ("Origen", 1.1),
    ("Destino", 1.1),
    ("Placa", 0.7),
    ("Tipo Doc.", 0.7),
    ("Estado", 0.6),
]


def _fecha_colombia(dt: datetime | None, con_hora: bool = True) -> str:
    """UTC naive (patrón del proyecto) → hora Colombia legible."""
    if not dt:
        return "—"
    local = dt.replace(tzinfo=timezone.utc).astimezone(_TZ_BOGOTA)
    return local.strftime("%d/%m/%Y %H:%M:%S hora Colombia" if con_hora else "%d/%m/%Y")


class NumberedCanvas(canvas_module.Canvas):
    """Canvas de dos pasadas para 'Página X de Y' (receta canónica de reportlab).

    `invariant=1` fija el /CreationDate interno del PDF: sin él el mismo doc
    produce bytes distintos en cada generación y la reproducibilidad
    (regenerar sin re-consultar) no sería verificable por hash.
    """

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("invariant", 1)
        super().__init__(*args, **kwargs)
        self._saved: list = []

    def showPage(self):
        self._saved.append(dict(self.__dict__))
        self._startPage()

    def save(self):
        total = len(self._saved)
        for estado in self._saved:
            self.__dict__.update(estado)
            self._dibujar_pie(total)
            super().showPage()
        super().save()

    def _dibujar_pie(self, total: int):
        self.saveState()
        self.setFont("Helvetica", 7)
        self.setFillColor(COLOR_NEUTRO)
        self.drawString(MARGEN, 8 * mm, "Documento confidencial — uso restringido al proceso de selección")
        consulta_id = getattr(self, "_consulta_id_pdf", "")
        if consulta_id:
            self.drawRightString(ANCHO - MARGEN, 8 * mm, consulta_id)
        self.drawCentredString(ANCHO / 2, 8 * mm, f"Página {self._pageNumber} de {total}")
        self.restoreState()


class CanvasEstudio(NumberedCanvas):
    """Canvas del estudio: lleva los datos de marca de agua y pie como atributos
    de CLASE para que la instancia que crea platypus vía canvasmaker los tenga."""

    _consulta_id_pdf = ""
    _wm_l1 = ""
    _wm_l2 = ""


def _marca_agua(cv: canvas_module.Canvas, doc: BaseDocTemplate):
    """Marca de agua diagonal repetida en toda la página (onPage)."""
    texto_l1 = getattr(CanvasEstudio, "_wm_l1", "")
    texto_l2 = getattr(CanvasEstudio, "_wm_l2", "")
    if not texto_l1:
        return
    cv.saveState()
    cv.setFontSize(10)
    cv.setFillColor(colors.grey, alpha=0.12)
    cv.translate(ANCHO / 2, ALTO / 2)
    cv.rotate(45)
    for dx, dy in ((-200, 160), (-200, 20), (-200, -120), (-200, -260)):
        cv.drawCentredString(dx, dy, texto_l1)
        if texto_l2:
            cv.drawCentredString(dx, dy - 14, texto_l2)
    cv.restoreState()


def _encabezado(cv: canvas_module.Canvas, doc: BaseDocTemplate):
    """Línea superior de identificación en cada página."""
    cv.saveState()
    cv.setFont("Helvetica", 7)
    cv.setFillColor(COLOR_NEUTRO)
    cv.drawString(MARGEN, ALTO - 10 * mm, "ESTUDIO DE SEGURIDAD")
    consulta_id = getattr(CanvasEstudio, "_consulta_id_pdf", "")
    if consulta_id:
        cv.drawRightString(ANCHO - MARGEN, ALTO - 10 * mm, f"Consulta {consulta_id}")
    cv.setStrokeColor(COLOR_FONDO_TABLA)
    cv.line(MARGEN, ALTO - 12 * mm, ANCHO - MARGEN, ALTO - 12 * mm)
    cv.restoreState()


def _qr_verificacion(url: str, tamano: float = 24 * mm):
    """QR con la URL pública de verificación (reportlab.graphics)."""
    try:
        from reportlab.graphics.barcode.qr import QrCodeWidget
        from reportlab.graphics.shapes import Drawing
        from reportlab.graphics import renderPDF

        qr = QrCodeWidget(url, barLevel="M")
        b = qr.getBounds()
        ancho_qr = b[2] - b[0]
        alto_qr = b[3] - b[1]
        escala = tamano / ancho_qr
        dibujo = Drawing(tamano, tamano)
        dibujo.add(qr)
        dibujo.scale(escala, escala)
        dibujo.translate(-(b[0] * escala) * 0, -(b[1] * escala) * 0)
        return dibujo
    except Exception as exc:
        logger.error("No se pudo generar el QR de verificación: %s", exc)
        return Paragraph(f"Verificación: {url}", ParagraphStyle("qr_fallback", fontName="Helvetica", fontSize=6))


# --- Construcción del PDF ------------------------------------------------------

def generar_pdf_estudio(estudio: dict, empresa: dict | None = None) -> bytes:
    """Construye el informe en bytes desde el doc del estudio (reproducible).

    `estudio` es el documento de `estudios_seguridad` (sin _id). `empresa`
    aporta logo_url/nombre si están disponibles en cache del actor.
    """
    empresa = empresa or {}
    fuentes = estudio.get("fuentes") or {}
    rndc = fuentes.get("manifiestos_rndc") or {}
    proc = fuentes.get("procuraduria") or {}
    pol = fuentes.get("policia") or {}
    runt = fuentes.get("runt") or {}
    pdf_info = estudio.get("pdf") or {}

    consulta_id = estudio.get("consulta_id", "")
    estado_global = estudio.get("estado", "EN_PROGRESO")
    creado = estudio.get("creado_en")

    buffer = io.BytesIO()
    fecha_wm = creado.replace(tzinfo=timezone.utc).astimezone(_TZ_BOGOTA).strftime("%Y-%m-%d %H:%M") if creado else ""
    CanvasEstudio._consulta_id_pdf = consulta_id
    CanvasEstudio._wm_l1 = f"{estudio.get('empresa_nombre', '')} | {estudio.get('usuario', '')} | {fecha_wm}"
    CanvasEstudio._wm_l2 = f"{consulta_id} | Generado por Integra Logística"

    doc = BaseDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=MARGEN, rightMargin=MARGEN, topMargin=16 * mm, bottomMargin=14 * mm,
        title=f"Estudio de Seguridad {consulta_id}",
        author="Integra Logística",
        subject=f"Estudio de seguridad cédula (consulta {consulta_id})",
    )
    marco = Frame(MARGEN, 14 * mm, ANCHO - 2 * MARGEN, ALTO - 30 * mm, id="cuerpo")
    plantilla = PageTemplate(id="estudio", frames=[marco], onPage=lambda c, d: (_marca_agua(c, d), _encabezado(c, d)))
    doc.addPageTemplates([plantilla])

    estilos = getSampleStyleSheet()
    estilo_titulo = ParagraphStyle("titulo", parent=estilos["Title"], fontSize=20, textColor=COLOR_PRIMARIO, spaceAfter=2)
    estilo_sub = ParagraphStyle("sub", parent=estilos["Normal"], fontSize=9, textColor=COLOR_NEUTRO)
    estilo_h2 = ParagraphStyle("h2", parent=estilos["Heading2"], textColor=COLOR_PRIMARIO, spaceBefore=10)
    estilo_normal = ParagraphStyle("normal", parent=estilos["Normal"], fontSize=9, leading=13)
    estilo_peq = ParagraphStyle("peq", parent=estilos["Normal"], fontSize=7.5, leading=10, textColor=COLOR_NEUTRO)
    # Celda de tabla con word-wrap: los strings crudos en Table NO se parten y
    # se desbordan (SHA-256, nombres de anexos, mensajes largos del portal).
    # splitLongWords parte palabras sin espacios (hashes de 64 caracteres).
    estilo_celda = ParagraphStyle(
        "celda", parent=estilos["Normal"], fontSize=8.5, leading=11,
        splitLongWords=1, splitLongChars=1, wordWrap="LTR",
    )
    estilo_celda_b = ParagraphStyle("celdaB", parent=estilo_celda, fontName="Helvetica-Bold")
    # Cabecera de "Resumen por fuente": fondo azul → letra BLANCA.
    estilo_celda_cab = ParagraphStyle("celdaCab", parent=estilo_celda_b, textColor=colors.white)

    def celda(texto: str, negrita: bool = False) -> Paragraph:
        return Paragraph(escape(str(texto or "—")), estilo_celda_b if negrita else estilo_celda)

    cuento: list = []

    # ── 1. Portada / resumen ejecutivo ──────────────────────────────────────
    cuento.append(Paragraph("ESTUDIO DE SEGURIDAD", estilo_titulo))
    cuento.append(Paragraph("Informe consolidado de consultas en fuentes públicas — Integra Logística", estilo_sub))
    cuento.append(Spacer(0, 6 * mm))

    etiqueta_estado, color_estado = ESTADO_GLOBAL_TEXTO.get(estado_global, (estado_global, COLOR_NEUTRO))
    badge = Table(
        [[Paragraph(f"<b>{etiqueta_estado}</b>", ParagraphStyle("badge", fontName="Helvetica", fontSize=11, textColor=colors.white, alignment=1))]],
        colWidths=[90 * mm],
    )
    badge.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), color_estado),
        ("BOX", (0, 0), (-1, -1), 0.5, color_estado),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]))
    cuento.append(badge)
    cuento.append(Spacer(0, 4 * mm))

    datos_persona = [
        ["Cédula consultada", estudio.get("cedula", "")],
        ["Nombre consultado", estudio.get("nombre_consultado") or "No disponible en fuentes"],
        ["Fecha y hora de consulta", _fecha_colombia(creado)],
        ["Empresa solicitante", estudio.get("empresa_nombre", "")],
        ["Usuario responsable", f"{estudio.get('usuario_nombre', '')} ({estudio.get('usuario', '')})"],
        ["Identificador de consulta", consulta_id],
    ]
    if estudio.get("placa"):
        datos_persona.insert(2, ["Placa consultada (RUNT)", estudio.get("placa")])
    tabla_persona = Table(
        [[celda(k, negrita=True), celda(v)] for k, v in datos_persona],
        colWidths=[45 * mm, 115 * mm],
    )
    tabla_persona.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("BACKGROUND", (0, 0), (0, -1), COLOR_FONDO_TABLA),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.white),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    cuento.append(tabla_persona)
    cuento.append(Spacer(0, 4 * mm))

    cuento.append(Paragraph("Resumen por fuente", estilo_h2))
    filas_resumen = [["Fuente", "Estado", "Resultado"]]
    etiqueta_rndc, _ = ESTADO_FUENTE_TEXTO.get(rndc.get("estado", "ERROR"), ("—", COLOR_NEUTRO))
    filas_resumen.append([
        "Manifiestos RNDC (365 días)",
        etiqueta_rndc,
        f"{rndc.get('total', 0)} viajes registrados" if rndc.get("estado") == "EXITO" else _resumen_error(rndc),
    ])
    etiqueta_proc, _ = ESTADO_FUENTE_TEXTO.get(proc.get("estado", "ERROR"), ("—", COLOR_NEUTRO))
    filas_resumen.append([
        "Procuraduría General de la Nación",
        etiqueta_proc,
        _texto_veredicto(proc),
    ])
    etiqueta_pol, _ = ESTADO_FUENTE_TEXTO.get(pol.get("estado", "ERROR"), ("—", COLOR_NEUTRO))
    filas_resumen.append([
        "Policía Nacional — Antecedentes Judiciales",
        etiqueta_pol,
        _texto_veredicto_policia(pol),
    ])
    etiqueta_runt, _ = ESTADO_FUENTE_TEXTO.get(runt.get("estado", "ERROR"), ("—", COLOR_NEUTRO))
    filas_resumen.append([
        f"RUNT — Vehículo {estudio.get('placa') or (runt.get('placa') or '')}".rstrip(),
        etiqueta_runt,
        _texto_veredicto_runt(runt),
    ])
    tabla_resumen = Table(
        [
            [Paragraph(escape(str(v)), estilo_celda_cab) for v in filas_resumen[0]]
        ] + [
            [celda(v) for v in fila]
            for fila in filas_resumen[1:]
        ],
        colWidths=[62 * mm, 38 * mm, 60 * mm],
    )
    tabla_resumen.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8.5),
        ("BACKGROUND", (0, 0), (-1, 0), COLOR_PRIMARIO),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, COLOR_FONDO_TABLA]),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#D5DBE3")),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    cuento.append(tabla_resumen)
    cuento.append(Spacer(0, 4 * mm))

    # QR de verificación de autenticidad (como la referencia TusDatos).
    url_verificacion = (
        f"{URL_PUBLICA}/seguridad/estudios/verificar/{consulta_id}"
        f"?codigo={estudio.get('codigo_verificacion', '')}"
    )
    try:
        tabla_qr = Table(
            [[_qr_verificacion(url_verificacion), Paragraph(
                "<b>Verificación de autenticidad</b><br/>"
                "Escanee el código QR o visite la URL para confirmar que este "
                "reporte fue generado por Integra Logística y consultar sus "
                "datos básicos (fecha, empresa solicitante y estado).<br/><br/>"
                f"{url_verificacion}",
                estilo_peq,
            )]],
            colWidths=[30 * mm, 130 * mm],
        )
        tabla_qr.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))
        cuento.append(tabla_qr)
    except Exception as exc:
        logger.error("Bloque QR no se pudo construir: %s", exc)

    # ── 2. Detalle manifiestos RNDC ──────────────────────────────────────────
    cuento.append(Paragraph("Manifiestos de carga — RNDC (Mintransporte)", estilo_h2))
    if rndc.get("estado") == "EXITO":
        cuento.append(Paragraph(
            f"Ventana consultada: {rndc.get('desde', '—')} a {rndc.get('hasta', '—')} · "
            f"Últimos <b>{rndc.get('total', 0)}</b> viajes:  · Origen de datos: {_texto_origen(rndc)}",
            estilo_normal,
        ))
        cuento.append(Spacer(0, 2 * mm))
        viajes = rndc.get("viajes") or []
        if not viajes:
            cuento.append(Paragraph(
                "El portal NO registró manifiestos de carga para la cédula en la ventana consultada.",
                ParagraphStyle("sin_viajes", parent=estilo_normal, textColor=COLOR_NEUTRO),
            ))
        else:
            cuento.append(_tabla_viajes(viajes, rndc.get("columnas") or []))
            if len(viajes) > MAX_VIAJES_PDF:
                cuento.append(Paragraph(
                    f"Se muestran los primeros {MAX_VIAJES_PDF} de {len(viajes)} viajes; "
                    "el detalle completo queda en el registro del estudio.",
                    estilo_peq,
                ))
    else:
        cuento.append(_parrafo_estado_fuente(rndc, "RNDC"))

    # ── 3. Detalle Procuraduría ──────────────────────────────────────────────
    cuento.append(Paragraph("Antecedentes disciplinarios — Procuraduría General de la Nación", estilo_h2))
    if proc.get("estado") in {"EXITO", "ADVERTENCIA"}:
        no_registra = proc.get("no_registra")
        if no_registra is True:
            texto, color = "NO REGISTRA SANCIONES NI INHABILIDADES VIGENTES", COLOR_EXITO
        elif no_registra is False:
            texto, color = "REGISTRA ANOTACIONES DISCIPLINARIAS — VER DETALLE", COLOR_FALLO
        else:
            texto, color = "VEREDICTO NO CONCLUSIVO — VER CERTIFICADO ADJUNTO", COLOR_ADVERTENCIA
        tabla_veredicto = Table(
            [[Paragraph(f"<b>{texto}</b>", ParagraphStyle("veredicto", fontName="Helvetica", fontSize=10.5, textColor=colors.white, alignment=1))]],
            colWidths=[160 * mm],
        )
        tabla_veredicto.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), color),
            ("TOPPADDING", (0, 0), (-1, -1), 7),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
        ]))
        cuento.append(tabla_veredicto)
        cuento.append(Spacer(0, 2 * mm))
        detalle_proc = [
            ["Nombre según certificado", proc.get("nombre_certificado") or "No disponible"],
            ["Mensaje del certificado", (proc.get("mensaje") or "—")[:300]],
            ["Certificado oficial (anexo)", (
                f"Adjunto a este estudio ({_nombre_anexo(estudio)}) · SHA-256: "
                f"{proc.get('pdf_sha256') or '—'}"
            )],
            ["Origen de datos", _texto_origen(proc)],
        ]
        tabla_proc = Table(
            [[celda(k, negrita=True), celda(v)] for k, v in detalle_proc],
            colWidths=[45 * mm, 115 * mm],
        )
        tabla_proc.setStyle(TableStyle([
            ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("BACKGROUND", (0, 0), (0, -1), COLOR_FONDO_TABLA),
            ("GRID", (0, 0), (-1, -1), 0.4, colors.white),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]))
        cuento.append(tabla_proc)
    else:
        cuento.append(_parrafo_estado_fuente(proc, "la Procuraduría"))

    # ── 4. Detalle Policía (antecedentes judiciales) ────────────────────────
    cuento.append(Paragraph("Antecedentes judiciales — Policía Nacional", estilo_h2))
    if pol.get("estado") in {"EXITO", "ADVERTENCIA"}:
        no_registra_pol = pol.get("no_registra")
        if no_registra_pol is True:
            texto_pol, color_pol = "NO REGISTRA ANTECEDENTES JUDICIALES", COLOR_EXITO
        elif no_registra_pol is False:
            texto_pol, color_pol = "REGISTRA REQUERIMIENTO JUDICIAL — VER DETALLE", COLOR_FALLO
        else:
            # El portal no genera PDF (Decreto 19/2012 art. 93): sin veredicto
            # legible el resultado no es concluyente, sin certificado adjunto.
            texto_pol, color_pol = "VEREDICTO NO CONCLUSIVO — VER MENSAJE DEL PORTAL", COLOR_ADVERTENCIA
        tabla_veredicto_pol = Table(
            [[Paragraph(f"<b>{texto_pol}</b>", ParagraphStyle("veredicto_pol", fontName="Helvetica", fontSize=10.5, textColor=colors.white, alignment=1))]],
            colWidths=[160 * mm],
        )
        tabla_veredicto_pol.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), color_pol),
            ("TOPPADDING", (0, 0), (-1, -1), 7),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
        ]))
        cuento.append(tabla_veredicto_pol)
        cuento.append(Spacer(0, 2 * mm))
        detalle_pol = [
            ["Leyenda oficial del portal", (pol.get("mensaje") or "—")[:300]],
            ["Nombre según el portal", pol.get("nombre_consultado") or "No disponible"],
        ]
        if estudio.get("anexo_policia"):
            detalle_pol.append(["Documento oficial (anexo)", (
                f"Adjunto a este estudio ({(estudio['anexo_policia'].get('gcs_ruta') or 'documento').split('/')[-1]}) · "
                f"SHA-256: {pol.get('pdf_sha256') or '—'}"
            )])
        detalle_pol.append(["Origen de datos", _texto_origen(pol)])
        tabla_pol = Table(
            [[celda(k, negrita=True), celda(v)] for k, v in detalle_pol],
            colWidths=[45 * mm, 115 * mm],
        )
        tabla_pol.setStyle(TableStyle([
            ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("BACKGROUND", (0, 0), (0, -1), COLOR_FONDO_TABLA),
            ("GRID", (0, 0), (-1, -1), 0.4, colors.white),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]))
        cuento.append(tabla_pol)
    else:
        cuento.append(_parrafo_estado_fuente(pol, "la Policía Nacional"))

    # ── 4b. Detalle RUNT (vehículo) ─────────────────────────────────────────
    cuento.append(Paragraph("Vehículo — RUNT (Mintransporte)", estilo_h2))
    if runt.get("estado") in {"EXITO", "ADVERTENCIA"}:
        soat = runt.get("soat") or {}
        no_registra_runt = runt.get("no_registra")
        if no_registra_runt is True:
            # Sobre la PLACA, no sobre la persona: nunca presentar como "limpio".
            texto_runt, color_runt = "PLACA SIN INFORMACIÓN EN EL RUNT", COLOR_NEUTRO
        elif no_registra_runt is False:
            texto_runt, color_runt = "LA CÉDULA NO CORRESPONDE A UN PROPIETARIO ACTIVO DEL VEHÍCULO", COLOR_ADVERTENCIA
        elif soat.get("vigente") is False:
            texto_runt, color_runt = "SOAT VENCIDO — VEHÍCULO SIN SEGURO VIGENTE", COLOR_FALLO
        elif soat and soat.get("vigente") is True:
            texto_runt, color_runt = f"SOAT VIGENTE — VENCE {soat.get('fecha_fin_vigencia', '—')}", COLOR_EXITO
        else:
            texto_runt, color_runt = "VEHÍCULO SIN PÓLIZA SOAT REGISTRADA — VERIFICAR", COLOR_ADVERTENCIA
        tabla_veredicto_runt = Table(
            [[Paragraph(f"<b>{texto_runt}</b>", ParagraphStyle("veredicto_runt", fontName="Helvetica", fontSize=10.5, textColor=colors.white, alignment=1))]],
            colWidths=[160 * mm],
        )
        tabla_veredicto_runt.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), color_runt),
            ("TOPPADDING", (0, 0), (-1, -1), 7),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
        ]))
        cuento.append(tabla_veredicto_runt)
        cuento.append(Spacer(0, 2 * mm))
        # Datos del vehículo: los campos que trajo el portal (dict libre).
        etiquetas_runt = {
            "placa": "Placa", "licencia_transito": "Licencia de tránsito",
            "estado_vehiculo": "Estado del vehículo", "tipo_servicio": "Tipo de servicio",
            "clase": "Clase", "marca": "Marca", "linea": "Línea", "modelo": "Modelo",
            "color": "Color", "numero_motor": "Nro. motor", "numero_chasis": "Nro. chasis",
            "numero_vin": "VIN", "cilindraje": "Cilindraje", "tipo_carroceria": "Carrocería",
            "combustible": "Combustible", "fecha_matricula_inicial": "Matrícula inicial",
            "autoridad_transito": "Autoridad de tránsito", "gravamenes": "Gravámenes",
            "clasico_antiguo": "Clásico/antiguo", "repotenciado": "Repotenciado",
        }
        detalle_runt = []
        for clave, etiqueta in etiquetas_runt.items():
            valor = (runt.get("datos_vehiculo") or {}).get(clave)
            if valor:
                detalle_runt.append([etiqueta, valor])
        if soat:
            detalle_runt.append(["SOAT — póliza", soat.get("numero", "—")])
            detalle_runt.append(["SOAT — aseguradora", soat.get("aseguradora", "—")])
            detalle_runt.append(["SOAT — vigencia", (
                f"{soat.get('fecha_inicio_vigencia', '—')} a {soat.get('fecha_fin_vigencia', '—')} "
                f"({soat.get('estado_portal', '—')})"
            )])
        if (runt.get("mensaje") or "").strip():
            detalle_runt.append(["Mensaje del portal", runt["mensaje"][:300]])
        detalle_runt.append(["Origen de datos", _texto_origen(runt)])
        tabla_runt = Table(
            [[celda(k, negrita=True), celda(v)] for k, v in detalle_runt],
            colWidths=[45 * mm, 115 * mm],
        )
        tabla_runt.setStyle(TableStyle([
            ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("BACKGROUND", (0, 0), (0, -1), COLOR_FONDO_TABLA),
            ("GRID", (0, 0), (-1, -1), 0.4, colors.white),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]))
        cuento.append(tabla_runt)
        # Historial de pólizas (máx 5 más recientes).
        polizas = runt.get("polizas") or []
        if polizas:
            cuento.append(Spacer(0, 2 * mm))
            estilo_celda_pol = ParagraphStyle("celda_pol", parent=estilo_celda, fontSize=7.5, leading=9.5)
            estilo_cab_pol = ParagraphStyle("cab_pol", parent=estilo_celda_pol, fontName="Helvetica-Bold", textColor=colors.white)
            filas_pol = [[
                Paragraph("Póliza", estilo_cab_pol), Paragraph("Vigencia", estilo_cab_pol),
                Paragraph("Aseguradora", estilo_cab_pol), Paragraph("Estado", estilo_cab_pol),
            ]]
            for p in polizas[:5]:
                filas_pol.append([
                    Paragraph(escape(str(p.get("numero", "—"))), estilo_celda_pol),
                    Paragraph(escape(f"{p.get('fecha_inicio_vigencia', '—')} → {p.get('fecha_fin_vigencia', '—')}"), estilo_celda_pol),
                    Paragraph(escape(str(p.get("aseguradora", "—"))), estilo_celda_pol),
                    Paragraph(escape(str(p.get("estado", "—"))), estilo_celda_pol),
                ])
            tabla_polizas = Table(filas_pol, colWidths=[38 * mm, 50 * mm, 52 * mm, 20 * mm])
            tabla_polizas.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), COLOR_PRIMARIO),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, COLOR_FONDO_TABLA]),
                ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#D5DBE3")),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]))
            cuento.append(Paragraph("Historial de pólizas SOAT (más recientes)", ParagraphStyle("h_pol", parent=estilo_normal, fontSize=8, textColor=COLOR_NEUTRO, spaceBefore=4)))
            cuento.append(tabla_polizas)
    else:
        cuento.append(_parrafo_estado_fuente(runt, "el RUNT"))

    # ── 5. Trazabilidad / auditoría ──────────────────────────────────────────
    cuento.append(Paragraph("Trazabilidad y auditoría", estilo_h2))
    auditoria = estudio.get("auditoria") or {}
    filas_traza = [
        ["Consulta", f"{consulta_id} · código de verificación {estudio.get('codigo_verificacion', '—')}"],
        ["Solicitado por", f"{estudio.get('usuario_nombre', '')} ({estudio.get('usuario', '')}) · {estudio.get('usuario_correo', '') or '—'}"],
        ["Empresa", estudio.get("empresa_nombre", "")],
        ["Origen técnico", f"IP {auditoria.get('ip', '—')} · {auditoria.get('user_agent', '—')[:80]}"],
        ["Creado / finalizado", f"{_fecha_colombia(estudio.get('creado_en'))} → {_fecha_colombia(estudio.get('finalizado_en'))} · {estudio.get('duracion_s') or '—'} s"],
        ["Reintentos por fuente", " · ".join(
            f"{nombre}: {int((f or {}).get('intentos', 0))} intento(s)"
            for nombre, f in (("RNDC", rndc), ("Procuraduría", proc), ("Policía", pol), ("RUNT", runt))
        )],
        ["Informe PDF", (
            f"Versión {pdf_info.get('version', 1)} · SHA-256 {(pdf_info.get('sha256') or '—')[:32]}… · "
            f"Generado {_fecha_colombia(pdf_info.get('generado_en'))}"
        )],
    ]
    tabla_traza = Table(
        [[celda(k, negrita=True), celda(v)] for k, v in filas_traza],
        colWidths=[40 * mm, 120 * mm],
    )
    tabla_traza.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8.5),
        ("BACKGROUND", (0, 0), (0, -1), COLOR_FONDO_TABLA),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.white),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    cuento.append(tabla_traza)

    # ── 6. Disposiciones legales ─────────────────────────────────────────────
    cuento.append(Paragraph("Disposiciones legales y alcance", estilo_h2))
    cuento.append(Paragraph(
        "<b>Antecedentes judiciales (Policía Nacional):</b> el portal de consulta en línea es un servicio de "
        "autoconsulta dispuesto por el artículo 94 del Decreto 019 de 2012 para que el titular valide su "
        "información judicial personal, y sus términos de uso prohíben el acceso por personas distintas del "
        "titular. Este dato fue incorporado al estudio en el marco de un proceso de verificación con "
        "autorización previa, expresa e inequívoca del titular de la información conforme a la Ley 1581 de "
        "2012; la obligación de contar con dicha autorización es del solicitante del estudio. "
        "<b>Vehículo (RUNT):</b> la información se obtuvo del Portal Público de Consulta Ciudadana del "
        "Registro Único Nacional de Tránsito, servicio de consulta abierta por placa con verificación de la "
        "cédula del propietario. Los datos corresponden a lo reportado por el Registro en la fecha de la "
        "consulta; la vigencia del SOAT es informativa y no constituye certificación de aseguramiento. "
        "<b>Ley 1238 de 2008:</b> habilita a entidades públicas y privadas a consultar el certificado de "
        "antecedentes disciplinarios de la Procuraduría General de la Nación de aspirantes a cargos o contratistas. "
        "<b>Ley 1581 de 2012 (Régimen General de Protección de Datos Personales):</b> los datos aquí contenidos "
        "se tratan con finalidad exclusiva de verificación en procesos de selección y vinculación de conductores/"
        "tenedores; el titular puede ejercer los derechos de acceso, corrección, actualización y supresión ante "
        "el responsable del tratamiento. "
        "Este informe es confidencial: su circulación está restringida al proceso que lo motivó. La información "
        "corresponde a lo reportado por las fuentes oficiales consultadas en la fecha indicada; la ausencia de "
        "registros no constituye certificación de conducta. El usuario identificado en la trazabilidad es el "
        "responsable del tratamiento de este documento.",
        estilo_peq,
    ))

    doc.build(cuento, canvasmaker=CanvasEstudio)
    buffer.seek(0)
    return buffer.getvalue()


# --- Helpers de tablas/estados ---------------------------------------------------

def _tabla_viajes(viajes: list[dict], columnas_portal: list[str]) -> Table:
    """Tabla de manifiestos con celdas Paragraph: el texto largo se parte
    DENTRO de su columna (wrap) en vez de dibujarse entero e invadir la
    columna siguiente — era el bug visual de los nombres de transportadora.

    Anchos ponderados por tipo de contenido (radicado corto, empresa larga).
    """
    seleccion = [(c, p) for c, p in COLUMNAS_VIAJE if c in columnas_portal]
    if not seleccion:  # portal cambió los nombres: fallback a las primeras 8
        seleccion = [(c, 1.0) for c in columnas_portal[:8]]
    columnas = [c for c, _ in seleccion]
    pesos = [p for _, p in seleccion]

    ancho_util = ANCHO - 2 * MARGEN
    total = sum(pesos)
    anchos = [ancho_util * p / total for p in pesos]

    estilo_celda = ParagraphStyle(
        "celda_viaje", fontName="Helvetica", fontSize=6.3, leading=7.6,
    )
    estilo_cabecera = ParagraphStyle(
        "cab_viaje", parent=estilo_celda, fontName="Helvetica-Bold", textColor=colors.white,
    )

    def celda(texto: str, estilo=estilo_celda) -> Paragraph:
        return Paragraph(escape(str(texto or "").strip()) or "&nbsp;", estilo)

    filas = [[celda(c, estilo_cabecera) for c in columnas]]
    for v in viajes[:MAX_VIAJES_PDF]:
        filas.append([celda(v.get(c, "")) for c in columnas])

    tabla = Table(filas, colWidths=anchos, repeatRows=1)
    tabla.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), COLOR_PRIMARIO),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, COLOR_FONDO_TABLA]),
        ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#D5DBE3")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 2.5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 2.5),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]))
    return tabla


def _texto_origen(fuente: dict) -> str:
    if fuente.get("origen") == "cache":
        return "Caché (consulta previa < 24 h)"
    return "Portal oficial (consulta en vivo)"


def _texto_veredicto(proc: dict) -> str:
    if proc.get("estado") not in {"EXITO", "ADVERTENCIA"}:
        return _resumen_error(proc)
    no_registra = proc.get("no_registra")
    if no_registra is True:
        return "Sin sanciones ni inhabilidades vigentes"
    if no_registra is False:
        return "Registra anotaciones — ver certificado"
    return "Veredicto no concluyente — ver certificado adjunto"


def _texto_veredicto_policia(pol: dict) -> str:
    """Espejo de _texto_veredicto con los textos del portal de la Policía
    (el portal no genera certificado PDF: el detalle es la leyenda oficial)."""
    if pol.get("estado") not in {"EXITO", "ADVERTENCIA"}:
        return _resumen_error(pol)
    no_registra = pol.get("no_registra")
    if no_registra is True:
        return "Sin asuntos pendientes con las autoridades judiciales"
    if no_registra is False:
        return "Requerido por autoridad judicial — ver detalle"
    return "Veredicto no concluyente — ver mensaje del portal"


def _texto_veredicto_runt(runt: dict) -> str:
    """Veredicto de la fuente runt para la fila resumen. OJO: el tri-estado es
    sobre la PLACA/vehículo, no sobre la persona — "sin información" nunca se
    presenta como "limpio"."""
    if runt.get("estado") not in {"EXITO", "ADVERTENCIA"}:
        return _resumen_error(runt)
    no_registra = runt.get("no_registra")
    if no_registra is True:
        return "Placa sin información en el RUNT"
    if no_registra is False:
        return "Cédula no corresponde al propietario activo del vehículo"
    soat = runt.get("soat") or {}
    if soat.get("vigente") is False:
        return "SOAT vencido — ver detalle"
    if soat and soat.get("vigente") is True:
        return f"SOAT vigente (vence {soat.get('fecha_fin_vigencia', '—')})"
    marca = (runt.get("datos_vehiculo") or {}).get("marca", "")
    return f"Vehículo identificado{f' ({marca})' if marca else ''} — sin póliza SOAT registrada"


def _resumen_error(fuente: dict) -> str:
    error = fuente.get("error") or {}
    mensaje = (error.get("mensaje") or "Fuente no disponible")[:80]
    return f"No consultada: {mensaje}"


def _parrafo_estado_fuente(fuente: dict, nombre: str) -> Paragraph:
    etiqueta, color = ESTADO_FUENTE_TEXTO.get(fuente.get("estado", "ERROR"), (fuente.get("estado", ""), COLOR_NEUTRO))
    error = fuente.get("error") or {}
    detalle = (error.get("mensaje") or "").strip()
    texto = f"<b>{nombre}</b>: {etiqueta}"
    if detalle:
        texto += f" — {detalle[:200]}"
    return Paragraph(
        texto,
        ParagraphStyle("estado_fuente", fontName="Helvetica", fontSize=9, leading=13, textColor=color),
    )


def _nombre_anexo(estudio: dict) -> str:
    anexo = estudio.get("anexo_procuraduria") or {}
    return (anexo.get("gcs_ruta") or "certificado oficial").split("/")[-1]
