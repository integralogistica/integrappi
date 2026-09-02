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
    CondPageBreak,
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
URL_VERIFICACION_PUBLICA = os.getenv("SEGURIDAD_VERIFICACION_URL_PUBLICA", "").rstrip("/")
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
COLOR_FONDO_FALLO = colors.HexColor("#FADADD")  # rosado suave, solo celda Estado

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

# Leyenda oficial COMPLETA del portal de la Policía (2026-09-01): texto fijo
# que acompaña todo resultado — se imprime VERBATIM en la sección Policía.
# El `mensaje` que captura el bot es solo la LÍNEA del veredicto; este
# disclaimer legal no depende de la consulta.
LEYENDA_SU458_POLICIA = (
    "En cumplimiento de la Sentencia SU-458 del 21 de junio de 2012, proferida por la Honorable "
    "Corte Constitucional, la leyenda “NO TIENE ASUNTOS PENDIENTES CON LAS AUTORIDADES "
    "JUDICIALES” aplica para todas aquellas personas que no registran antecedentes y para quienes "
    "la autoridad judicial competente haya decretado la extinción de la condena o la prescripción "
    "de la pena.\n"
    "Esta consulta es válida siempre y cuando el número de identificación y nombres, correspondan "
    "con el documento de identidad registrado y solo aplica para el territorio colombiano de "
    "acuerdo a lo establecido en el ordenamiento constitucional."
)

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


def _enmascarar_cedula(cedula: str | None) -> str:
    """Cédula VISIBLE completa (decisión de negocio 2026-08-30: el cliente
    necesita verla para cruzar con sus registros; antes iba enmascarada).
    Se mantiene como función para que el punto de decisión sea explícito y
    fácil de revertir, y para no confundirla con la que SÍ va enmascarada en
    el endpoint público del QR y en los logs."""
    return str(cedula or "").strip() or "—"


def _vehiculo_del_estudio(estudio: dict) -> dict:
    """Vehículo validado por runt en este estudio (hoy 1; array en el doc).

    Tolerante con docs previos a 2026-08-30 (sin `vehiculos`): si hay fuente
    runt se asume que la consulta se hizo con la cédula de la persona evaluada
    (que era el comportamiento del sistema entonces). Con estudios SOLO simit
    (sin runt) no hay tríada validada: cédula del propietario None (simit
    consulta por placa y no conoce propietario — no se fabrica la afiliación).
    """
    vehiculo = ((estudio.get("vehiculos") or [{}]) or [{}])[0] or {}
    ced_evaluada = estudio.get("cedula", "")
    # runt corrió de verdad (no DESHABILITADA por el plan): solo él valida la
    # tríada. Docs viejos sin la clave runt no traen placa → da igual.
    hay_runt = ((estudio.get("fuentes") or {}).get("runt") or {}).get("estado") not in (None, "DESHABILITADA")
    ced_prop = vehiculo.get("cedula_propietario") or (ced_evaluada if hay_runt else None)
    es_evaluado = vehiculo.get("propietario_es_evaluado")
    if es_evaluado is None and ced_prop is not None:
        es_evaluado = ced_prop == ced_evaluada
    return {
        "placa": estudio.get("placa") or vehiculo.get("placa") or "",
        "cedula_propietario": ced_prop,
        "propietario_es_evaluado": bool(es_evaluado) if ced_prop is not None else None,
    }


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
    simit = fuentes.get("simit") or {}
    sena = fuentes.get("sena") or {}
    ofac = fuentes.get("ofac") or {}
    ofac_nit = fuentes.get("ofac_nit") or {}
    bdme = fuentes.get("bdme") or {}
    bdme_nit = fuentes.get("bdme_nit") or {}
    rama_judicial = fuentes.get("rama_judicial") or {}

    def _corrio(fuente: dict) -> bool:
        """La fuente corrió en ESTA consulta. DESHABILITADA = excluida por el
        plan elegido (no se consultó ni se cobró) y estado None = la clave no
        existía cuando se creó el doc (fuente posterior): en ambos casos su
        sección no se muestra — el informe presenta SOLO lo que el plan
        consultó (2026-09-01)."""
        estado = (fuente or {}).get("estado")
        return estado is not None and estado != "DESHABILITADA"
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

    def _antes_de_seccion(fuente: dict | None = None, exito_mm: float = 110, fallo_mm: float = 35) -> None:
        """Salto condicional ANTES del título de una sección de fuente
        (2026-09-01, pedido del usuario): si en la página actual no queda
        espacio para el bloque inicial de la sección (título + banner de
        veredicto + tabla de resumen), la sección empieza en la página
        SIGUIENTE — nunca más un título colgado al pie de página que
        continúa en la otra. Con ~267 mm útiles por página y un bloque
        mínimo de 110 mm quedan máximo ~3 fuentes por página (lo que pidió
        el usuario); las secciones largas (tabla de viajes) siguen fluyendo
        con salto interno de reportlab. Una fuente FALLIDA es solo título
        + un párrafo: pide mucho menos espacio (fallo_mm)."""
        minimo = fallo_mm if (fuente or {}).get("estado") in {"NO_DISPONIBLE", "ERROR"} else exito_mm
        cuento.append(CondPageBreak(minimo * mm))


    # ── 1. Portada / resumen ejecutivo ──────────────────────────────────────
    cuento.append(Paragraph("ESTUDIO DE SEGURIDAD", estilo_titulo))
    cuento.append(Paragraph("Informe consolidado de consultas en fuentes públicas — Integra Logística", estilo_sub))
    cuento.append(Spacer(0, 6 * mm))

    # En un estudio PARCIAL el resumen por fuente ya identifica con precisión
    # cuál portal no respondió. Evitar una alerta roja general al inicio.
    if estado_global != "PARCIAL":
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
        ["Cédula consultada" if estudio.get("cedula") else "NIT consultado", estudio.get("cedula") or estudio.get("nit", "")],
        ["Nombre consultado", estudio.get("nombre_consultado") or "No disponible en fuentes"],
        ["Fecha y hora de consulta", _fecha_colombia(creado)],
        ["Empresa solicitante", estudio.get("empresa_nombre", "")],
        ["Usuario responsable", f"{estudio.get('usuario_nombre', '')} ({estudio.get('usuario', '')})"],
        ["Identificador de consulta", consulta_id],
    ]
    vehiculo = _vehiculo_del_estudio(estudio)
    if vehiculo["placa"]:
        # La placa la trae runt o simit: etiqueta según quién la validó (y
        # CORRIÓ — una fuente DESHABILITADA no validó nada).
        etiqueta_placa = "Placa consultada (RUNT)" if _corrio(runt) else "Placa consultada (SIMIT)"
        datos_persona.insert(2, [etiqueta_placa, vehiculo["placa"]])
        # El propietario del vehículo puede ser OTRA persona: el informe debe
        # diferenciar quién se evalúa (conductor) de quién es dueño del carro.
        # Solo runt valida la propiedad — con solo simit no hay fila (el
        # estado de cuenta de comparendos es de la PLACA).
        if _corrio(runt) and vehiculo["cedula_propietario"] is not None:
            datos_persona.insert(3, ["Propietario del vehículo", (
                f"{_enmascarar_cedula(vehiculo['cedula_propietario'])} — es la persona evaluada"
                if vehiculo["propietario_es_evaluado"] else
                f"{_enmascarar_cedula(vehiculo['cedula_propietario'])} — DISTINTA de la persona evaluada"
            )])
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
    if _corrio(rndc):
        etiqueta_rndc, _ = ESTADO_FUENTE_TEXTO.get(rndc.get("estado", "ERROR"), ("—", COLOR_NEUTRO))
        filas_resumen.append([
            "Manifiestos RNDC (365 días)",
            etiqueta_rndc,
            f"{rndc.get('total', 0)} viajes registrados" if rndc.get("estado") == "EXITO" else _resumen_error(rndc),
        ])
    if _corrio(proc):
        etiqueta_proc, _ = ESTADO_FUENTE_TEXTO.get(proc.get("estado", "ERROR"), ("—", COLOR_NEUTRO))
        filas_resumen.append([
            "Procuraduría General de la Nación",
            etiqueta_proc,
            _texto_veredicto(proc),
        ])
    if _corrio(pol):
        etiqueta_pol, _ = ESTADO_FUENTE_TEXTO.get(pol.get("estado", "ERROR"), ("—", COLOR_NEUTRO))
        filas_resumen.append([
            "Policía Nacional — Antecedentes Judiciales",
            etiqueta_pol,
            _texto_veredicto_policia(pol),
        ])
    if _corrio(runt):
        etiqueta_runt, _ = ESTADO_FUENTE_TEXTO.get(runt.get("estado", "ERROR"), ("—", COLOR_NEUTRO))
        filas_resumen.append([
            f"RUNT — Vehículo {estudio.get('placa') or (runt.get('placa') or '')}".rstrip(),
            etiqueta_runt,
            _texto_veredicto_runt(runt),
        ])
    if _corrio(simit):
        etiqueta_simit, _ = ESTADO_FUENTE_TEXTO.get(simit.get("estado", "ERROR"), ("—", COLOR_NEUTRO))
        filas_resumen.append([
            f"SIMIT — Comparendos placa {estudio.get('placa') or (simit.get('placa') or '')}".rstrip(),
            etiqueta_simit,
            _texto_veredicto_simit(simit),
        ])
    if _corrio(sena):
        etiqueta_sena, _ = ESTADO_FUENTE_TEXTO.get(sena.get("estado", "ERROR"), ("—", COLOR_NEUTRO))
        filas_resumen.append([
            "SENA — Certificados de formación",
            etiqueta_sena,
            _texto_veredicto_sena(sena),
        ])
    if _corrio(ofac):
        etiqueta_ofac, _ = ESTADO_FUENTE_TEXTO.get(ofac.get("estado", "ERROR"), ("—", COLOR_NEUTRO))
        filas_resumen.append([
            "OFAC — Lista SDN (Lista Clinton)",
            etiqueta_ofac,
            _texto_veredicto_ofac(ofac),
        ])
    if _corrio(ofac_nit):
        etiqueta_ofac_nit, _ = ESTADO_FUENTE_TEXTO.get(ofac_nit.get("estado", "ERROR"), ("—", COLOR_NEUTRO))
        filas_resumen.append(["OFAC — Empresas por NIT", etiqueta_ofac_nit, _texto_veredicto_ofac(ofac_nit)])
    for fuente_bdme, etiqueta in ((bdme, "BDME — Persona por cédula"), (bdme_nit, "BDME — Empresa por NIT")):
        if _corrio(fuente_bdme):
            estado_txt, _ = ESTADO_FUENTE_TEXTO.get(fuente_bdme.get("estado", "ERROR"), ("—", COLOR_NEUTRO))
            if fuente_bdme.get("estado") in {"EXITO", "ADVERTENCIA"}:
                veredicto = "REPORTADO EN EL BDME" if fuente_bdme.get("reportado") else "NO REPORTADO EN EL BDME"
            else:
                veredicto = _resumen_error(fuente_bdme)
            filas_resumen.append([etiqueta, estado_txt, veredicto])
    if _corrio(rama_judicial):
        estado_txt, _ = ESTADO_FUENTE_TEXTO.get(rama_judicial.get("estado", "ERROR"), ("—", COLOR_NEUTRO))
        total_rama = int(rama_judicial.get("total_procesos") or 0)
        veredicto = (f"{total_rama} PROCESO(S) POR COINCIDENCIA DE NOMBRE — VALIDAR HOMONIMIA"
                     if total_rama else "SIN PROCESOS PARA EL NOMBRE CONSULTADO")
        if rama_judicial.get("estado") not in {"EXITO", "ADVERTENCIA"}:
            veredicto = _resumen_error(rama_judicial)
        filas_resumen.append(["Rama Judicial — Procesos por nombre", estado_txt, veredicto])
    estados_resumen = [
        (fuente or {}).get("estado")
        for fuente in (rndc, proc, pol, runt, simit, sena, ofac, ofac_nit, bdme, bdme_nit, rama_judicial)
        if _corrio(fuente)
    ]
    tabla_resumen = Table(
        [
            [Paragraph(escape(str(v)), estilo_celda_cab) for v in filas_resumen[0]]
        ] + [
            [celda(v) for v in fila]
            for fila in filas_resumen[1:]
        ],
        colWidths=[62 * mm, 38 * mm, 60 * mm],
    )
    estilos_tabla_resumen = [
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8.5),
        ("BACKGROUND", (0, 0), (-1, 0), COLOR_PRIMARIO),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, COLOR_FONDO_TABLA]),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#D5DBE3")),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]
    # Para fallos, sombrear SOLO el recuadro Estado (columna 1), no toda la
    # fila ni el resultado descriptivo.
    estilos_tabla_resumen.extend(
        ("BACKGROUND", (1, fila), (1, fila), COLOR_FONDO_FALLO)
        for fila, estado in enumerate(estados_resumen, start=1)
        if estado in {"NO_DISPONIBLE", "ERROR"}
    )
    tabla_resumen.setStyle(TableStyle(estilos_tabla_resumen))
    cuento.append(tabla_resumen)
    cuento.append(Spacer(0, 4 * mm))

    # QR de verificación de autenticidad (como la referencia TusDatos).
    codigo_verificacion = estudio.get("codigo_verificacion", "")
    # Los despliegues nuevos llevan el QR a una vista pública, responsive y
    # pensada para auditores. Sin la variable nueva conservamos el endpoint
    # JSON anterior para no romper instalaciones existentes.
    url_verificacion = (
        f"{URL_VERIFICACION_PUBLICA}?consulta={consulta_id}&codigo={codigo_verificacion}"
        if URL_VERIFICACION_PUBLICA else
        f"{URL_PUBLICA}/seguridad/estudios/verificar/{consulta_id}?codigo={codigo_verificacion}"
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
    if _corrio(rndc):
        _antes_de_seccion(rndc)
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
    elif _corrio(rndc):
        cuento.append(_parrafo_estado_fuente(rndc, "RNDC"))

    # ── 3. Detalle Procuraduría ──────────────────────────────────────────────
    if _corrio(proc):
        _antes_de_seccion(proc)
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
    elif _corrio(proc):
        cuento.append(_parrafo_estado_fuente(proc, "la Procuraduría"))

    # ── 4. Detalle Policía (antecedentes judiciales) ────────────────────────
    if _corrio(pol):
        _antes_de_seccion(pol)
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
        # Leyenda oficial COMPLETA del portal (verificada 2026-09-01 a pedido
        # del usuario): igual para toda consulta — texto fijo, no depende de
        # lo que el bot capture en `mensaje` (que es solo la línea del
        # veredicto).
        cuento.append(Spacer(0, 2 * mm))
        cuento.append(Paragraph("<b>Leyenda oficial — Sentencia SU-458 de 2012</b>", ParagraphStyle(
            "h_leyenda_pol", parent=estilo_normal, fontSize=8, textColor=COLOR_NEUTRO, spaceBefore=2,
        )))
        cuento.append(Paragraph(escape(LEYENDA_SU458_POLICIA).replace("\n", "<br/><br/>"), estilo_peq))
    elif _corrio(pol):
        cuento.append(_parrafo_estado_fuente(pol, "la Policía Nacional"))

    # ── 4b. Detalle RUNT (vehículo) ─────────────────────────────────────────
    if _corrio(runt):
        _antes_de_seccion(runt)
        cuento.append(Paragraph("Vehículo — RUNT (Mintransporte)", estilo_h2))
    # El badge exige propietario CONOCIDO (runt): con solo simit no hay tríada
    # y propietario_es_evaluado es None (no "distinto").
    if _corrio(runt) and vehiculo["placa"] and vehiculo["cedula_propietario"] is not None and not vehiculo["propietario_es_evaluado"]:
        # El dueño del vehículo NO es la persona evaluada: sin este aviso, el
        # lector atribuye al conductor un rechazo de propiedad del RUNT (o un
        # SOAT ajeno). La cédula del propietario va enmascarada.
        badge_prop = Table(
            [[Paragraph(
                f"<b>PROPIETARIO DEL VEHÍCULO (CÉDULA {_enmascarar_cedula(vehiculo['cedula_propietario'])}) "
                "ES DISTINTO DE LA PERSONA EVALUADA</b>",
                ParagraphStyle("badge_prop", fontName="Helvetica", fontSize=9.5, textColor=colors.white, alignment=1),
            )]],
            colWidths=[160 * mm],
        )
        badge_prop.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), COLOR_ADVERTENCIA),
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ]))
        cuento.append(badge_prop)
        cuento.append(Spacer(0, 2 * mm))
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
        if no_registra_runt is False:
            # El rechazo "no propietario activo" es sobre la CÉDULA con que se
            # consultó: explicitarla evita leerlo como antecedente del vehículo.
            detalle_runt.append(["Cédula consultada (propietario)", (
                f"{_enmascarar_cedula(vehiculo['cedula_propietario'])} — el portal validó "
                "la propiedad del vehículo contra esta cédula"
            )])
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
    elif _corrio(runt):
        cuento.append(_parrafo_estado_fuente(runt, "el RUNT"))

    # ── 4c. Detalle SIMIT (comparendos de la placa) ─────────────────────────
    if _corrio(simit):
        _antes_de_seccion(simit)
        cuento.append(Paragraph("Comparendos — SIMIT (Federación Colombiana de Municipios)", estilo_h2))
    if simit.get("estado") in {"EXITO", "ADVERTENCIA"}:
        total_a_pagar = simit.get("total_a_pagar") or 0
        total_deuda = simit.get("total_deuda") or 0
        total_comps = simit.get("total_comparendos") or 0
        if total_a_pagar > 0:
            texto_simit, color_simit = (
                f"COMPARENDOS PENDIENTES — SALDO EXIGIBLE {_cop_texto(total_a_pagar)}",
                COLOR_ADVERTENCIA,
            )
        elif total_comps > 0:
            # Deuda histórica sin saldo exigible (prescrita/condonada): el
            # detalle va abajo pero NO es deuda vigente (ZZZ999: 105 de
            # 1999-2000 con $0 a pagar).
            texto_simit, color_simit = (
                f"SIN SALDO EXIGIBLE — REGISTRA {int(total_comps)} ANTECEDENTES HISTÓRICOS ({_cop_texto(total_deuda)})",
                COLOR_NEUTRO,
            )
        else:
            texto_simit, color_simit = "SIN COMPARENDOS NI MULTAS REGISTRADAS", COLOR_EXITO
        tabla_veredicto_simit = Table(
            [[Paragraph(f"<b>{texto_simit}</b>", ParagraphStyle("veredicto_simit", fontName="Helvetica", fontSize=10.5, textColor=colors.white, alignment=1))]],
            colWidths=[160 * mm],
        )
        tabla_veredicto_simit.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), color_simit),
            ("TOPPADDING", (0, 0), (-1, -1), 7),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
        ]))
        cuento.append(tabla_veredicto_simit)
        cuento.append(Spacer(0, 2 * mm))
        # Resumen del estado de cuenta + detalle de la primera página.
        detalle_simit = [
            ["Placa consultada", simit.get("placa") or vehiculo["placa"] or "—"],
            ["Comparendos", int(simit.get("total_comparendos") or 0)],
            ["Multas", int(simit.get("total_multas") or 0)],
            ["Acuerdos de pago", int(simit.get("total_acuerdos") or 0)],
            ["Deuda total reportada", _cop_texto(total_deuda) if total_deuda else "—"],
            ["Saldo exigible", _cop_texto(total_a_pagar) if total_a_pagar else "$ 0"],
            ["Origen de datos", _texto_origen(simit)],
        ]
        if (simit.get("mensaje") or "").strip():
            detalle_simit.insert(6, ["Mensaje del portal", simit["mensaje"][:300]])
        tabla_simit_resumen = Table(
            [[celda(k, negrita=True), celda(v)] for k, v in detalle_simit],
            colWidths=[45 * mm, 115 * mm],
        )
        tabla_simit_resumen.setStyle(TableStyle([
            ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("BACKGROUND", (0, 0), (0, -1), COLOR_FONDO_TABLA),
            ("GRID", (0, 0), (-1, -1), 0.4, colors.white),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]))
        cuento.append(tabla_simit_resumen)
        comparendos = simit.get("comparendos") or []
        if comparendos:
            cuento.append(Spacer(0, 2 * mm))
            estilo_celda_sim = ParagraphStyle("celda_sim", parent=estilo_celda, fontSize=7.5, leading=9.5)
            estilo_cab_sim = ParagraphStyle("cab_sim", parent=estilo_celda_sim, fontName="Helvetica-Bold", textColor=colors.white)
            filas_sim = [[
                Paragraph("Número", estilo_cab_sim), Paragraph("Fecha", estilo_cab_sim),
                Paragraph("Infracción", estilo_cab_sim), Paragraph("Secretaría", estilo_cab_sim),
                Paragraph("Estado", estilo_cab_sim), Paragraph("Valor a pagar", estilo_cab_sim),
            ]]
            for c in comparendos[:10]:
                filas_sim.append([
                    Paragraph(escape(str(c.get("numero", "—"))), estilo_celda_sim),
                    Paragraph(escape(str(c.get("fecha_imposicion") or "—")), estilo_celda_sim),
                    Paragraph(escape(str(c.get("infraccion") or "—")), estilo_celda_sim),
                    Paragraph(escape(str(c.get("secretaria") or "—")), estilo_celda_sim),
                    Paragraph(escape(str(c.get("estado") or "—")), estilo_celda_sim),
                    Paragraph(escape(_cop_texto(c.get("valor_a_pagar")) if c.get("valor_a_pagar") is not None else "—"), estilo_celda_sim),
                ])
            tabla_comps = Table(filas_sim, colWidths=[22 * mm, 20 * mm, 58 * mm, 26 * mm, 20 * mm, 14 * mm])
            tabla_comps.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), COLOR_PRIMARIO),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, COLOR_FONDO_TABLA]),
                ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#D5DBE3")),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]))
            total_reg = int(simit.get("total_comparendos") or 0) + int(simit.get("total_multas") or 0)
            cuento.append(Paragraph(
                f"Detalle de comparendos y multas ({min(10, len(comparendos))} de {total_reg or len(comparendos)} — primera página del portal)",
                ParagraphStyle("h_sim", parent=estilo_normal, fontSize=8, textColor=COLOR_NEUTRO, spaceBefore=4),
            ))
            cuento.append(tabla_comps)
    elif _corrio(simit):
        cuento.append(_parrafo_estado_fuente(simit, "el SIMIT"))

    # ── 4d. Detalle SENA (certificados de formación) ─────────────────────────
    if _corrio(sena):
        _antes_de_seccion(sena)
        cuento.append(Paragraph("Formación SENA — Certificados (Servicio Nacional de Aprendizaje)", estilo_h2))
    if sena.get("estado") in {"EXITO", "ADVERTENCIA"}:
        total_certs = int(sena.get("total_certificados") or 0)
        if total_certs > 0:
            texto_sena, color_sena = f"REGISTRA {total_certs} CERTIFICADO(S) DE FORMACIÓN DISPONIBLE(S)", COLOR_PRIMARIO
        else:
            texto_sena, color_sena = "SIN CERTIFICADOS DE FORMACIÓN REGISTRADOS", COLOR_EXITO
        tabla_veredicto_sena = Table(
            [[Paragraph(f"<b>{texto_sena}</b>", ParagraphStyle("veredicto_sena", fontName="Helvetica", fontSize=10.5, textColor=colors.white, alignment=1))]],
            colWidths=[160 * mm],
        )
        tabla_veredicto_sena.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), color_sena),
            ("TOPPADDING", (0, 0), (-1, -1), 7),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
        ]))
        cuento.append(tabla_veredicto_sena)
        cuento.append(Spacer(0, 2 * mm))
        detalle_sena = [
            ["Certificados disponibles", total_certs],
            ["Origen de datos", _texto_origen(sena)],
        ]
        if (sena.get("mensaje") or "").strip():
            detalle_sena.insert(1, ["Mensaje del portal", sena["mensaje"][:300]])
        tabla_sena_resumen = Table(
            [[celda(k, negrita=True), celda(v)] for k, v in detalle_sena],
            colWidths=[45 * mm, 115 * mm],
        )
        tabla_sena_resumen.setStyle(TableStyle([
            ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("BACKGROUND", (0, 0), (0, -1), COLOR_FONDO_TABLA),
            ("GRID", (0, 0), (-1, -1), 0.4, colors.white),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]))
        cuento.append(tabla_sena_resumen)
        certificados = sena.get("certificados") or []
        if certificados:
            cuento.append(Spacer(0, 2 * mm))
            estilo_celda_sena = ParagraphStyle("celda_sena", parent=estilo_celda, fontSize=7.5, leading=9.5)
            estilo_cab_sena = ParagraphStyle("cab_sena", parent=estilo_celda_sena, fontName="Helvetica-Bold", textColor=colors.white)
            filas_sena = [[
                Paragraph("Programa", estilo_cab_sena), Paragraph("Título", estilo_cab_sena),
                Paragraph("Tipo", estilo_cab_sena), Paragraph("Certificación", estilo_cab_sena),
                Paragraph("Firma", estilo_cab_sena),
            ]]
            for c in certificados[:10]:
                filas_sena.append([
                    Paragraph(escape(str(c.get("programa") or "—")), estilo_celda_sena),
                    Paragraph(escape(str(c.get("titulo") or "—")), estilo_celda_sena),
                    Paragraph(escape(str(c.get("tipo") or "—")), estilo_celda_sena),
                    Paragraph(escape(str(c.get("fecha_certificacion") or "—")), estilo_celda_sena),
                    Paragraph(escape(str(c.get("fecha_firma") or "—")), estilo_celda_sena),
                ])
            tabla_certs = Table(filas_sena, colWidths=[56 * mm, 28 * mm, 30 * mm, 23 * mm, 23 * mm])
            tabla_certs.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), COLOR_PRIMARIO),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, COLOR_FONDO_TABLA]),
                ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#D5DBE3")),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]))
            cuento.append(Paragraph(
                f"Detalle de certificados ({min(10, len(certificados))} de {total_certs or len(certificados)} — primera página del portal)",
                ParagraphStyle("h_sena", parent=estilo_normal, fontSize=8, textColor=COLOR_NEUTRO, spaceBefore=4),
            ))
            cuento.append(tabla_certs)
    elif _corrio(sena):
        cuento.append(_parrafo_estado_fuente(sena, "el SENA"))

    # ── 4e. OFAC / Lista SDN ─────────────────────────────────────────────────
    if _corrio(ofac):
        _antes_de_seccion(ofac)
        cuento.append(Paragraph("Lista de sanciones OFAC — SDN (Lista Clinton)", estilo_h2))
    if ofac.get("estado") in {"EXITO", "ADVERTENCIA"}:
        aplica = bool(ofac.get("aplica"))
        texto_ofac = (
            "COINCIDENCIA EXACTA DE IDENTIFICACIÓN — REQUIERE REVISIÓN HUMANA"
            if aplica else "SIN COINCIDENCIA EXACTA DE IDENTIFICACIÓN EN LA LISTA SDN"
        )
        color_ofac = COLOR_ADVERTENCIA if aplica else COLOR_EXITO
        tabla_ofac_banner = Table([[Paragraph(f"<b>{texto_ofac}</b>", ParagraphStyle(
            "veredicto_ofac", fontName="Helvetica", fontSize=10.5,
            textColor=colors.white, alignment=1,
        ))]], colWidths=[160 * mm])
        tabla_ofac_banner.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), color_ofac),
            ("TOPPADDING", (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ]))
        cuento.append(tabla_ofac_banner)
        filas_ofac = [
            ["Método", "Coincidencia exacta del número de identificación (sin búsqueda difusa por nombre)"],
            ["Publicación OFAC", ofac.get("fecha_publicacion") or "—"],
            ["Registros de la lista", str(ofac.get("total_registros_lista") or "—")],
            ["Coincidencias", str(ofac.get("total_coincidencias") or 0)],
            ["SHA-256 del dataset", ofac.get("sha256_dataset") or "—"],
        ]
        tabla_ofac = Table([[celda(k, True), celda(v)] for k, v in filas_ofac], colWidths=[45 * mm, 115 * mm])
        tabla_ofac.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (0, -1), COLOR_FONDO_TABLA),
            ("GRID", (0, 0), (-1, -1), 0.4, colors.white),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]))
        cuento.append(tabla_ofac)
        for coincidencia in (ofac.get("coincidencias") or [])[:10]:
            cuento.append(Paragraph(
                "<b>Coincidencia:</b> "
                f"{escape(str(coincidencia.get('nombre') or '—'))} · UID {escape(str(coincidencia.get('uid') or '—'))} · "
                f"programa(s) {escape(', '.join(coincidencia.get('programas') or []) or '—')}",
                estilo_normal,
            ))
        if aplica:
            cuento.append(Paragraph(
                "Una coincidencia técnica no sustituye el análisis de identidad, homonimia, alcance del programa "
                "ni la decisión de cumplimiento. Debe ser revisada por una persona responsable.", estilo_peq,
            ))
    elif _corrio(ofac):
        cuento.append(_parrafo_estado_fuente(ofac, "OFAC"))

    # ── 4f. OFAC empresarial por NIT ───────────────────────────────────────
    if _corrio(ofac_nit):
        _antes_de_seccion(ofac_nit)
        cuento.append(Paragraph("OFAC — Empresa por NIT (Lista SDN)", estilo_h2))
        if ofac_nit.get("estado") in {"EXITO", "ADVERTENCIA"}:
            aplica_nit = bool(ofac_nit.get("aplica"))
            veredicto_nit = ("COINCIDENCIA EXACTA DE NIT — REQUIERE REVISIÓN HUMANA" if aplica_nit
                             else "SIN COINCIDENCIA EXACTA DEL NIT EN LA LISTA SDN")
            cuento.append(Paragraph(f"<b>{veredicto_nit}</b>", estilo_normal))
            cuento.append(Paragraph(
                f"NIT consultado: {escape(str(estudio.get('nit') or '—'))} · "
                f"Publicación OFAC: {escape(str(ofac_nit.get('fecha_publicacion') or '—'))} · "
                f"Coincidencias: {int(ofac_nit.get('total_coincidencias') or 0)}", estilo_normal,
            ))
            for coincidencia in (ofac_nit.get("coincidencias") or [])[:10]:
                cuento.append(Paragraph(
                    "<b>Entidad:</b> "
                    f"{escape(str(coincidencia.get('nombre') or '—'))} · UID {escape(str(coincidencia.get('uid') or '—'))} · "
                    f"programa(s) {escape(', '.join(coincidencia.get('programas') or []) or '—')}", estilo_normal,
                ))
        else:
            cuento.append(_parrafo_estado_fuente(ofac_nit, "OFAC por NIT"))

    # ── BDME personal y empresarial ────────────────────────────────────────
    for fuente_bdme, titulo_bdme in (
        (bdme, "BDME — Consulta personal por cédula"),
        (bdme_nit, "BDME — Consulta empresarial por NIT"),
    ):
        if not _corrio(fuente_bdme):
            continue
        _antes_de_seccion(fuente_bdme)
        cuento.append(Paragraph(titulo_bdme, estilo_h2))
        if fuente_bdme.get("estado") in {"EXITO", "ADVERTENCIA"}:
            veredicto = "REPORTADO EN EL BDME" if fuente_bdme.get("reportado") else "NO REPORTADO EN EL BDME"
            cuento.append(Paragraph(f"<b>{veredicto}</b>", estilo_normal))
            cuento.append(Paragraph(
                f"Motivo: {escape(str(fuente_bdme.get('motivo') or '—'))} · "
                f"Registros: {int(fuente_bdme.get('total_registros') or 0)} · "
                f"Origen: {_texto_origen(fuente_bdme)}", estilo_normal,
            ))
            if fuente_bdme.get("mensaje"):
                cuento.append(Paragraph(escape(str(fuente_bdme["mensaje"])[:300]), estilo_peq))
        else:
            cuento.append(_parrafo_estado_fuente(fuente_bdme, "el BDME"))

    # ── Rama Judicial: persona natural, todos los procesos ────────────────
    if _corrio(rama_judicial):
        _antes_de_seccion(rama_judicial)
        cuento.append(Paragraph("Rama Judicial — Consulta Nacional Unificada por nombre", estilo_h2))
        if rama_judicial.get("estado") in {"EXITO", "ADVERTENCIA"}:
            total_rama = int(rama_judicial.get("total_procesos") or 0)
            cuento.append(Paragraph(
                f"<b>{'REGISTRA ' + str(total_rama) + ' COINCIDENCIA(S)' if total_rama else 'SIN PROCESOS ENCONTRADOS'}</b>",
                estilo_normal,
            ))
            cuento.append(Paragraph(
                f"Nombre consultado: {escape(str(rama_judicial.get('nombre_completo') or '—'))} · "
                "Tipo de persona: Natural · Alcance: todos los procesos (no solo actuaciones recientes).",
                estilo_normal,
            ))
            for proceso in (rama_judicial.get("procesos") or [])[:20]:
                numero = proceso.get("llaveProceso") or proceso.get("numeroProceso") or proceso.get("idProceso") or "—"
                despacho = proceso.get("despacho") or proceso.get("nombreDespacho") or "—"
                fecha = proceso.get("fechaProceso") or proceso.get("fechaUltimaActuacion") or "—"
                cuento.append(Paragraph(
                    f"<b>Proceso:</b> {escape(str(numero))} · "
                    f"<b>Despacho:</b> {escape(str(despacho))} · <b>Fecha:</b> {escape(str(fecha))}",
                    estilo_peq,
                ))
            if total_rama:
                cuento.append(Paragraph(
                    "La coincidencia se basa exclusivamente en el nombre informado y puede corresponder a homónimos. "
                    "Debe verificarse la identidad y la calidad de la persona dentro de cada proceso.", estilo_peq,
                ))
        else:
            cuento.append(_parrafo_estado_fuente(rama_judicial, "la Rama Judicial"))

    # ── 5. Trazabilidad / auditoría ──────────────────────────────────────────
    cuento.append(CondPageBreak(60 * mm))  # la tabla de trazabilidad no arranca al pie
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
            for nombre, f in (("RNDC", rndc), ("Procuraduría", proc), ("Policía", pol), ("RUNT", runt), ("SIMIT", simit), ("SENA", sena), ("OFAC cédula", ofac), ("OFAC NIT", ofac_nit), ("BDME cédula", bdme), ("BDME NIT", bdme_nit), ("Rama Judicial", rama_judicial))
            if _corrio(f)
        ) or "—"],
        ["Informe PDF", (
            f"Versión {pdf_info.get('version', 1)} · SHA-256 {(pdf_info.get('sha256') or '—')[:32]}… · "
            f"Generado {_fecha_colombia(pdf_info.get('generado_en'))}"
        )],
    ]
    if vehiculo["placa"] and vehiculo["cedula_propietario"] is not None:
        filas_traza.append(["Vehículo / propietario", (
            f"Placa {vehiculo['placa']} · propietario cédula "
            f"{_enmascarar_cedula(vehiculo['cedula_propietario'])} "
            + ("(es la persona evaluada)" if vehiculo["propietario_es_evaluado"]
               else "(DISTINTA de la persona evaluada)")
        )])
    elif vehiculo["placa"]:
        # Solo simit: la placa se consultó por el estado de cuenta de
        # comparendos, sin validación de propiedad.
        filas_traza.append(["Vehículo", f"Placa {vehiculo['placa']} (consultada en SIMIT)"])
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
    # Un párrafo por fuente que CORRIÓ (el informe solo cubre lo que el plan
    # consultó); el marco general de protección de datos va siempre.
    cuento.append(Paragraph("Disposiciones legales y alcance", estilo_h2))
    bloques_legal = []
    if _corrio(pol):
        bloques_legal.append(
            "<b>Antecedentes judiciales (Policía Nacional):</b> el portal de consulta en línea es un servicio de "
            "autoconsulta dispuesto por el artículo 94 del Decreto 019 de 2012 para que el titular valide su "
            "información judicial personal, y sus términos de uso prohíben el acceso por personas distintas del "
            "titular. Este dato fue incorporado al estudio en el marco de un proceso de verificación con "
            "autorización previa, expresa e inequívoca del titular de la información conforme a la Ley 1581 de "
            "2012; la obligación de contar con dicha autorización es del solicitante del estudio."
        )
    if _corrio(runt):
        bloques_legal.append(
            "<b>Vehículo (RUNT):</b> la información se obtuvo del Portal Público de Consulta Ciudadana del "
            "Registro Único Nacional de Tránsito, servicio de consulta abierta por placa con verificación de la "
            "cédula del propietario. Los datos corresponden a lo reportado por el Registro en la fecha de la "
            "consulta; la vigencia del SOAT es informativa y no constituye certificación de aseguramiento."
        )
    if _corrio(simit):
        bloques_legal.append(
            "<b>Comparendos (SIMIT):</b> la información se obtuvo del estado de cuenta público del Sistema "
            "Integrado de Información sobre Comparendos administrado por la Federación Colombiana de "
            "Municipios, consulta ciudadana abierta por placa. La consulta es sobre el VEHÍCULO y no constituye "
            "antecedente personal ni atribuye responsabilidad por infracción a la persona evaluada; los datos "
            "corresponden a lo reportado por los organismos de tránsito en la fecha de consulta y los saldos "
            "son informativos."
        )
    if _corrio(proc):
        bloques_legal.append(
            "<b>Ley 1238 de 2008:</b> habilita a entidades públicas y privadas a consultar el certificado de "
            "antecedentes disciplinarios de la Procuraduría General de la Nación de aspirantes a cargos o contratistas."
        )
    if _corrio(sena):
        bloques_legal.append(
            "<b>Formación (SENA):</b> la información se obtuvo del portal público Certificado Digital del Servicio "
            "Nacional de Aprendizaje, consulta abierta por documento de identidad. El listado corresponde a los "
            "certificados de formación reportados como disponibles por el SENA en la fecha de consulta y NO "
            "constituye verificación de títulos ni credencial educacional de la persona evaluada."
        )
    if _corrio(ofac):
        bloques_legal.append(
            "<b>OFAC — Lista SDN:</b> la verificación se efectuó contra el dataset oficial de Specially "
            "Designated Nationals and Blocked Persons publicado por la Office of Foreign Assets Control del "
            "Departamento del Tesoro de los Estados Unidos. El resultado compara de manera exacta el número "
            "de identificación; una coincidencia requiere validación humana y análisis de identidad, programa "
            "y alcance, y no constituye por sí sola una decisión automática de rechazo."
        )
    bloques_legal.append(
        "<b>Ley 1581 de 2012 (Régimen General de Protección de Datos Personales):</b> los datos aquí contenidos "
        "se tratan con finalidad exclusiva de verificación en procesos de selección y vinculación de conductores/"
        "tenedores; el titular puede ejercer los derechos de acceso, corrección, actualización y supresión ante "
        "el responsable del tratamiento. "
        "Este informe es confidencial: su circulación está restringida al proceso que lo motivó. La información "
        "corresponde a lo reportado por las fuentes oficiales consultadas en la fecha indicada; la ausencia de "
        "registros no constituye certificación de conducta. El usuario identificado en la trazabilidad es el "
        "responsable del tratamiento de este documento."
    )
    cuento.append(Paragraph(" ".join(bloques_legal), estilo_peq))

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
    # No concluyente: solo remitir al anexo si el certificado EXISTE (con
    # veredicto ilegible hay PDF adjunto; sin PDF no hay nada que ver).
    if (proc.get("pdf_tamano") or 0) > 0:
        return "Veredicto no concluyente — ver certificado adjunto"
    return "Veredicto no concluyente (certificado no entregado por el portal)"


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


def _cop_texto(valor) -> str:
    """40257438.0 → '$ 40.257.438' (formato COP del portal, puntos de miles)."""
    try:
        return "$ {:,.0f}".format(float(valor or 0)).replace(",", ".")
    except (TypeError, ValueError):
        return "—"


def _texto_veredicto_simit(simit: dict) -> str:
    """Veredicto de la fuente simit para la fila resumen. La consulta es sobre
    la PLACA: nunca se presenta como antecedente personal de la persona
    evaluada (mismo espíritu que propietario ≠ evaluado en runt)."""
    if simit.get("estado") not in {"EXITO", "ADVERTENCIA"}:
        return _resumen_error(simit)
    total_a_pagar = simit.get("total_a_pagar") or 0
    if total_a_pagar > 0:
        total = int(simit.get("total_comparendos") or 0) + int(simit.get("total_multas") or 0)
        return f"Saldo exigible {_cop_texto(total_a_pagar)} ({total} registros) — ver detalle"
    if (simit.get("total_comparendos") or 0) > 0 or (simit.get("total_multas") or 0) > 0:
        return "Sin saldo exigible — registra antecedentes históricos"
    return "Sin comparendos ni multas registradas"


def _texto_veredicto_ofac(ofac: dict) -> str:
    if ofac.get("estado") not in {"EXITO", "ADVERTENCIA"}:
        return _resumen_error(ofac)
    if ofac.get("aplica"):
        return f"Coincidencia exacta de identificación ({int(ofac.get('total_coincidencias') or 1)}) — revisar"
    return "Sin coincidencia exacta de identificación en SDN"


def _texto_veredicto_sena(sena: dict) -> str:
    """Veredicto de la fuente sena para la fila resumen. Es información de
    FORMACIÓN, no un antecedente: el conteo es informativo y jamás se
    presenta como credencial verificada."""
    if sena.get("estado") not in {"EXITO", "ADVERTENCIA"}:
        return _resumen_error(sena)
    total = int(sena.get("total_certificados") or 0)
    if total > 0:
        return f"{total} certificado(s) de formación — ver detalle"
    return "Sin certificados de formación registrados"


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
