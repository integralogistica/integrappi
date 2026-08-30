"""PDF de la cuenta de cobro mensual de una empresa (Estudios de Seguridad).

Sigue el patrón de Funciones/pdf_estudio_seguridad.py: reportlab platypus,
`rl_config.invariant = 1` (reproducible), NumberedCanvas "Página X de Y",
marca de agua diagonal en todas las páginas y tablas con celdas Paragraph
(wrap — el texto jamás invade la columna vecina).

Contenido: membrete, datos de la empresa, período y cierre, tabla de consumos
(fecha Colombia, consulta, cédula ENMASCARADA, estado del estudio, precio
snapshot), pagos aplicados, ajustes/reembolsos, totales congelados del cierre
y condiciones de pago.
"""
from __future__ import annotations

import io
import logging
import os
from datetime import datetime, timedelta, timezone
from xml.sax.saxutils import escape

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

# Reproducible byte a byte (mismo patrón/razón que el PDF del estudio).
rl_config.invariant = 1

logger = logging.getLogger(__name__)

CONDICIONES_DEFAULT = (
    "Condiciones de pago: la presente cuenta de cobro debe cancelarse dentro de los "
    "15 días calendario siguientes a su emisión, mediante transferencia a las cuentas "
    "de Integra Logística. Los valores corresponden al consumo del servicio de Estudios "
    "de Seguridad según el plan vigente en cada fecha de consulta. Esta cuenta de cobro "
    "no es una factura electrónica."
)
CONDICIONES = os.getenv("SEGURIDAD_COBRO_CONDICIONES") or CONDICIONES_DEFAULT

ANCHO, ALTO = A4
MARGEN = 16 * mm
_TZ_CO = timezone(timedelta(hours=-5))  # Colombia

COLOR_PRIMARIO = colors.HexColor("#0F2A43")
COLOR_FONDO_TABLA = colors.HexColor("#F0F3F7")
COLOR_NEUTRO = colors.HexColor("#57606A")
COLOR_POSITIVO = colors.HexColor("#1A7F37")
COLOR_NEGATIVO = colors.HexColor("#C0392B")

MESES = ["enero", "febrero", "marzo", "abril", "mayo", "junio",
         "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"]


def _pesos(cop: int) -> str:
    """3500 → '$3.500' (formato colombiano)."""
    signo = "-" if cop < 0 else ""
    return f"{signo}${abs(int(cop)):,.0f}".replace(",", ".")


def _fecha_colombia(dt: datetime | None) -> str:
    if not dt:
        return "—"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(_TZ_CO).strftime("%d/%m/%Y")


def _nombre_periodo(periodo: str) -> str:
    try:
        anio, mes = int(periodo[:4]), int(periodo[5:7])
        return f"{MESES[mes - 1].capitalize()} de {anio}"
    except Exception:
        return periodo


# --- Canvas con footer y marca de agua (misma receta del PDF de estudio) -------

class NumberedCanvas(canvas_module.Canvas):
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
        self.drawString(MARGEN, 8 * mm, "Documento confidencial — uso interno de cobranza")
        ref = getattr(type(self), "_ref_pdf", "")
        if ref:
            self.drawRightString(ANCHO - MARGEN, 8 * mm, ref)
        self.drawCentredString(ANCHO / 2, 8 * mm, f"Página {self._pageNumber} de {total}")
        self.restoreState()


class CanvasCuenta(NumberedCanvas):
    _ref_pdf = ""
    _wm_l1 = ""
    _wm_l2 = ""


def _marca_agua(cv, doc):
    l1 = getattr(CanvasCuenta, "_wm_l1", "")
    l2 = getattr(CanvasCuenta, "_wm_l2", "")
    if not l1:
        return
    cv.saveState()
    cv.setFontSize(10)
    cv.setFillColor(colors.grey, alpha=0.12)
    cv.translate(ANCHO / 2, ALTO / 2)
    cv.rotate(45)
    for dx, dy in ((-200, 160), (-200, 20), (-200, -120), (-200, -260)):
        cv.drawCentredString(dx, dy, l1)
        if l2:
            cv.drawCentredString(dx, dy - 14, l2)
    cv.restoreState()


def _encabezado(cv, doc):
    cv.saveState()
    cv.setFont("Helvetica", 7)
    cv.setFillColor(COLOR_NEUTRO)
    cv.drawString(MARGEN, ALTO - 10 * mm, "CUENTA DE COBRO — ESTUDIOS DE SEGURIDAD")
    ref = getattr(CanvasCuenta, "_ref_pdf", "")
    if ref:
        cv.drawRightString(ANCHO - MARGEN, ALTO - 10 * mm, ref)
    cv.setStrokeColor(COLOR_FONDO_TABLA)
    cv.line(MARGEN, ALTO - 12 * mm, ANCHO - MARGEN, ALTO - 12 * mm)
    cv.restoreState()


# --- Generación -----------------------------------------------------------------

def generar_pdf_cuenta(empresa: dict, cierre: dict, movimientos: list[dict]) -> bytes:
    """Construye la cuenta de cobro en bytes desde los totales CONGELADOS del
    cierre y la lista de movimientos del período (reproducible)."""
    periodo = cierre.get("periodo", "")
    totales = cierre.get("totales") or {}
    empresa_id = str(empresa.get("_id", ""))
    ref = f"cuenta {empresa_id[-6:]}-{periodo}"

    CanvasCuenta._ref_pdf = ref
    CanvasCuenta._wm_l1 = f"CUENTA DE COBRO | {empresa.get('nombre', '')} | {periodo}"
    CanvasCuenta._wm_l2 = "Integra Logística — Estudios de Seguridad"

    buffer = io.BytesIO()
    doc = BaseDocTemplate(
        buffer, pagesize=A4,
        leftMargin=MARGEN, rightMargin=MARGEN, topMargin=16 * mm, bottomMargin=14 * mm,
        title=f"Cuenta de cobro {periodo} — {empresa.get('nombre', '')}",
        author="Integra Logística",
        subject=f"Cuenta de cobro Estudios de Seguridad {periodo}",
    )
    marco = Frame(MARGEN, 14 * mm, ANCHO - 2 * MARGEN, ALTO - 30 * mm, id="cuerpo")
    doc.addPageTemplates([PageTemplate(id="cuenta", frames=[marco], onPage=lambda c, d: (_marca_agua(c, d), _encabezado(c, d)))])

    estilos = getSampleStyleSheet()
    estilo_titulo = ParagraphStyle("titulo", parent=estilos["Title"], fontSize=19, textColor=COLOR_PRIMARIO, spaceAfter=2)
    estilo_sub = ParagraphStyle("sub", parent=estilos["Normal"], fontSize=9, textColor=COLOR_NEUTRO)
    estilo_h2 = ParagraphStyle("h2", parent=estilos["Heading2"], textColor=COLOR_PRIMARIO, spaceBefore=10)
    estilo_peq = ParagraphStyle("peq", parent=estilos["Normal"], fontSize=7.5, leading=10, textColor=COLOR_NEUTRO)

    def celda(texto, negrita=False, blanco=False, derecha=False, color=None, tam=7.5) -> Paragraph:
        return Paragraph(
            escape(str(texto if texto not in (None, "") else "—")),
            ParagraphStyle(
                "c", fontName="Helvetica-Bold" if negrita else "Helvetica", fontSize=tam,
                leading=tam + 1.3, textColor=colors.white if blanco else (color or colors.black),
                alignment=2 if derecha else 0,
            ),
        )

    cuento: list = []

    # ── Encabezado del documento ────────────────────────────────────────────
    cuento.append(Paragraph("CUENTA DE COBRO", estilo_titulo))
    cuento.append(Paragraph("Servicio de Estudios de Seguridad — Integra Logística", estilo_sub))
    cuento.append(Spacer(0, 5 * mm))

    datos = [
        ["Empresa", empresa.get("nombre", "")],
        ["NIT", empresa.get("nit") or "—"],
        ["Período facturado", _nombre_periodo(periodo)],
        ["Fecha de cierre", _fecha_colombia(cierre.get("cerrado_en"))],
        ["Estado", "PAGADA" if cierre.get("estado") == "PAGADA" else "PENDIENTE DE COBRO"],
    ]
    tabla_datos = Table(
        [[Paragraph(f"<b>{k}</b>", ParagraphStyle("k", fontName="Helvetica", fontSize=9)),
          Paragraph(escape(str(v)), ParagraphStyle("v", fontName="Helvetica", fontSize=9))] for k, v in datos],
        colWidths=[45 * mm, 115 * mm],
    )
    tabla_datos.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (0, -1), COLOR_FONDO_TABLA),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.white),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    cuento.append(tabla_datos)

    # ── Detalle de consumos ──────────────────────────────────────────────────
    consumos = [m for m in movimientos if m.get("tipo") == "CONSUMO"]
    cuento.append(Paragraph(f"Estudios consultados ({len(consumos)})", estilo_h2))
    if consumos:
        filas = [[
            celda("Fecha", negrita=True, blanco=True), celda("Consulta", negrita=True, blanco=True),
            celda("Cédula", negrita=True, blanco=True), celda("Estado", negrita=True, blanco=True),
            celda("Plan", negrita=True, blanco=True), celda("Precio", negrita=True, blanco=True, derecha=True),
        ]]
        for m in consumos:
            precio = 0 if m.get("exento") else int(m.get("precio_unitario_cop") or 0)
            filas.append([
                celda(_fecha_colombia(m.get("creado_en"))),
                celda(m.get("consulta_id") or ("(exento)" if m.get("exento") else "—")),
                celda(_enmascarar(m.get("cedula"))),
                celda(m.get("estado_estudio") or "—"),
                celda(m.get("plan_nombre") or "—"),
                celda("EXENTO" if m.get("exento") else _pesos(precio), derecha=True),
            ])
        tabla_consumos = Table(filas, colWidths=[22 * mm, 34 * mm, 24 * mm, 30 * mm, 30 * mm, 20 * mm], repeatRows=1)
        tabla_consumos.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), COLOR_PRIMARIO),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, COLOR_FONDO_TABLA]),
            ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#D5DBE3")),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 2.5),
            ("RIGHTPADDING", (0, 0), (-1, -1), 2.5),
        ]))
        cuento.append(tabla_consumos)
    else:
        cuento.append(Paragraph("Sin estudios en el período.", estilo_peq))

    # ── Reembolsos y ajustes ─────────────────────────────────────────────────
    otros = [m for m in movimientos if m.get("tipo") in {"REEMBOLSO", "AJUSTE"}]
    if otros:
        cuento.append(Paragraph("Reembolsos y ajustes", estilo_h2))
        filas = [[
            celda("Fecha", negrita=True, blanco=True), celda("Tipo", negrita=True, blanco=True),
            celda("Detalle", negrita=True, blanco=True), celda("Valor", negrita=True, blanco=True, derecha=True),
        ]]
        for m in otros:
            filas.append([
                celda(_fecha_colombia(m.get("creado_en"))),
                celda(m.get("tipo")),
                celda(m.get("motivo") or m.get("consulta_id") or "—"),
                celda(_pesos(int(m.get("monto_cop") or 0)), derecha=True),
            ])
        tabla_otros = Table(filas, colWidths=[24 * mm, 26 * mm, 80 * mm, 30 * mm], repeatRows=1)
        tabla_otros.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), COLOR_PRIMARIO),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, COLOR_FONDO_TABLA]),
            ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#D5DBE3")),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ]))
        cuento.append(tabla_otros)

    # ── Pagos aplicados ──────────────────────────────────────────────────────
    pagos = [m for m in movimientos if m.get("tipo") == "PAGO"]
    if pagos:
        cuento.append(Paragraph("Pagos aplicados al período", estilo_h2))
        filas = [[
            celda("Fecha registro", negrita=True, blanco=True), celda("Fecha pago", negrita=True, blanco=True),
            celda("Método", negrita=True, blanco=True), celda("Referencia", negrita=True, blanco=True),
            celda("Valor", negrita=True, blanco=True, derecha=True),
        ]]
        for m in pagos:
            filas.append([
                celda(_fecha_colombia(m.get("creado_en"))),
                celda(_fecha_colombia(m.get("fecha_pago"))),
                celda(m.get("metodo") or "—"),
                celda(m.get("referencia") or "—"),
                celda(_pesos(-int(m.get("monto_cop") or 0)), derecha=True),
            ])
        tabla_pagos = Table(filas, colWidths=[26 * mm, 26 * mm, 30 * mm, 40 * mm, 38 * mm], repeatRows=1)
        tabla_pagos.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), COLOR_PRIMARIO),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, COLOR_FONDO_TABLA]),
            ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#D5DBE3")),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ]))
        cuento.append(tabla_pagos)

    # ── Totales (congelados del cierre) ──────────────────────────────────────
    cuento.append(Paragraph("Resumen financiero", estilo_h2))
    total_cop = int(totales.get("total_cop") or 0)
    filas_tot = [
        [celda("Estudios consumidos (unidades)"), celda(f"{totales.get('unidades', 0)}", derecha=True)],
        [celda("Subtotal consumos"), celda(_pesos(int(totales.get("subtotal_cop") or 0)), derecha=True)],
        [celda("Reembolsos"), celda(_pesos(int(totales.get("reembolsos_cop") or 0)), derecha=True)],
        [celda("Ajustes"), celda(_pesos(int(totales.get("ajustes_cop") or 0)), derecha=True)],
        [celda("Pagos aplicados"), celda(_pesos(int(totales.get("pagos_cop") or 0)), derecha=True)],
    ]
    color_total = COLOR_POSITIVO if total_cop <= 0 else COLOR_NEGATIVO
    filas_tot.append([
        celda("TOTAL A PAGAR" if total_cop > 0 else "SALDO A FAVOR", negrita=True),
        celda(_pesos(total_cop), negrita=True, derecha=True, color=color_total),
    ])
    tabla_tot = Table(filas_tot, colWidths=[110 * mm, 50 * mm])
    tabla_tot.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (0, -2), COLOR_FONDO_TABLA),
        ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#E8EDF3")),
        ("LINEABOVE", (0, -1), (-1, -1), 0.8, COLOR_PRIMARIO),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.white),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 3.5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3.5),
    ]))
    cuento.append(tabla_tot)

    # ── Condiciones ──────────────────────────────────────────────────────────
    cuento.append(Paragraph("Condiciones", estilo_h2))
    cuento.append(Paragraph(escape(CONDICIONES), estilo_peq))
    cuento.append(Spacer(0, 3 * mm))
    cuento.append(Paragraph(
        f"Cierre generado por {cierre.get('cerrado_por', '—')} · Referencia {ref} · "
        "Documento generado por Integra Logística.",
        estilo_peq,
    ))

    doc.build(cuento, canvasmaker=CanvasCuenta)
    buffer.seek(0)
    return buffer.getvalue()


def _enmascarar(cedula) -> str:
    cedula = str(cedula or "")
    if len(cedula) <= 4:
        return "*" * len(cedula) or "—"
    return f"{cedula[:2]}{'*' * (len(cedula) - 4)}{cedula[-2:]}"
