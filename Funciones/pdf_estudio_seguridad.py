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
  8. Evidencias de consulta: pantallazo del portal por fuente (patrón
     TusDatos; solo fuentes de navegador — las de API/dataset no generan
     captura). Los bytes viven en GCS privado (`evidencias` del doc).
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
    Image,
    PageBreak,
    PageTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
)
from xml.sax.saxutils import escape

from reportlab.lib.utils import ImageReader

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

# --- Marca del producto (seguriDatia) -----------------------------------------
# Logo horizontal con transparencia (1448×396): va en la cabecera de TODAS
# las páginas (_encabezado). Best-effort: si el archivo falta, el PDF sale
# sin logo pero idéntico en todo lo demás.
MARCA = "seguriDatia"
LOGO_SEGURIDATIA = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "imagenes", "Logo seguriDatia.png",
)

# Colores de estado (plan): EXITO verde, ADVERTENCIA ámbar, fallo rojo.
COLOR_EXITO = colors.HexColor("#1A7F37")
COLOR_ADVERTENCIA = colors.HexColor("#B58900")
COLOR_FALLO = colors.HexColor("#C0392B")
COLOR_NEUTRO = colors.HexColor("#57606A")
COLOR_PRIMARIO = colors.HexColor("#0F2A43")
COLOR_FONDO_TABLA = colors.HexColor("#F0F3F7")
COLOR_FONDO_FALLO = colors.HexColor("#FADADD")  # rosado suave, solo celda Estado
COLOR_FONDO_ADVERTENCIA = colors.HexColor("#F4B183")  # naranja, solo celda Estado

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


# Formatos de fecha que traen las fuentes (cada portal tiene el suyo):
#   ISO 'aaaa-mm-dd' (runt/simit/sena/rues/rama judicial), 'aaaa/mm/dd' (ventana
#   y tabla del RNDC), US 'mm/dd/aaaa' (Publish_Date del XML de OFAC),
#   'dd/mm/aaaa' (algunos portales), ISO con hora 'aaaa-mm-ddThh:mm:ss' y el
#   'aaaa/mm/dd hh:mm:ss' de la tabla de manifiestos. El informe unifica TODO a
#   dd/mm/aaaa (pedido 2026-09-04); lo que no parsea como fecha pasa intacto.
_RE_FECHA_ISO = re.compile(r"^(\d{4})[-/](\d{1,2})[-/](\d{1,2})")
_RE_FECHA_US = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})")


def _fecha_legible(valor, defecto: str = "—") -> str:
    """Cualquier fecha de fuente → 'dd/mm/aaaa'.

    Ambigüedad dd/mm vs mm/dd: los portales colombianos (runt/rues/simit/sena)
    entregan ISO yyyy-mm-dd, así que el slash se asume US mm/dd SOLO si el día
    y el mes no caben como dd/mm (OFAC es el único emisor mm/dd/aaaa). Si el
    texto no es una fecha conocida se devuelve tal cual (o el defecto si es
    vacío): el helper pinta celdas de datos de portales, no debe tragarse
    texto libre.
    """
    texto = str(valor or "").strip()
    if not texto:
        return defecto
    m = _RE_FECHA_ISO.match(texto)
    if m:
        anio, mes, dia = m.group(1), int(m.group(2)), int(m.group(3))
    else:
        m = _RE_FECHA_US.match(texto)
        if not m:
            return texto
        primero, segundo, anio = int(m.group(1)), int(m.group(2)), m.group(3)
        if primero > 12:  # 28/08/2026 no puede ser mm/dd → es dd/mm
            dia, mes = primero, segundo
        else:
            mes, dia = primero, segundo
    try:
        datetime(int(anio), mes, dia)  # valida rango real (13/13 falla)
    except ValueError:
        return texto
    # La hora (si viene) viaja con la fecha: 'Fecha Hora Radicación' del RNDC
    # trae 'aaaa/mm/dd hh:mm:ss'. El sufijo ISO 'Thh:mm:ss' (rama judicial) no
    # es legible y se descarta.
    resto = texto[m.end():].strip()
    if resto.startswith("T"):
        resto = ""
    return f"{dia:02d}/{mes:02d}/{anio}{' ' + resto if resto else ''}"


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
        self._destinos_pagina: list = []

    def bookmarkPage(self, key, *args, **kwargs):
        """Registra qué anclas (<a name>) se dibujan en la página ACTUAL.

        Con el two-pass, el bookmarkPage del armado liga el destino a la
        única página del doc que existe entonces; en save() se re-liga a la
        página real que se emite.
        """
        resultado = super().bookmarkPage(key, *args, **kwargs)
        self._destinos_pagina.append(key)
        return resultado

    def showPage(self):
        self._saved.append((dict(self.__dict__), list(self._destinos_pagina)))
        self._destinos_pagina = []
        self._startPage()

    def save(self):
        total = len(self._saved)
        for estado, destinos in self._saved:
            self.__dict__.update(estado)
            if destinos:
                # Los enlaces internos (botón ver evidencia del resumen)
                # deben caer en la página que se va a emitir AHORA.
                pageref = self._doc.thisPageRef()
                for clave in destinos:
                    destino = self._destinations.get(clave)
                    if destino is not None:
                        destino.setPage(pageref)
            self._dibujar_pie(total)
            super().showPage()
        super().save()

    def _dibujar_pie(self, total: int):
        from reportlab.lib.utils import simpleSplit

        self.saveState()
        # Bloque legal seguriDatia (pedido 2026-09-24): disclaimer de calidad
        # del dato + trazabilidad de quién generó el reporte, en TODAS las
        # páginas. 5.2 pt gris para caber entre el marco (14 mm) y el borde.
        disclaimer = getattr(self, "_pie_disclaimer", "")
        generado = getattr(self, "_pie_generado", "")
        y = 13.2 * mm
        for linea in simpleSplit(disclaimer, "Helvetica", 5.2, ANCHO - 2 * MARGEN) if disclaimer else []:
            self.setFont("Helvetica", 5.2)
            self.setFillColor(COLOR_NEUTRO)
            self.drawCentredString(ANCHO / 2, y, linea)
            y -= 2.3 * mm
        if generado:
            self.setFont("Helvetica-Bold", 5.4)
            self.setFillColor(COLOR_NEUTRO)
            for linea in simpleSplit(generado, "Helvetica-Bold", 5.4, ANCHO - 2 * MARGEN):
                self.drawCentredString(ANCHO / 2, y, linea)
                y -= 2.4 * mm
        self.setFont("Helvetica", 7)
        self.setFillColor(COLOR_NEUTRO)
        consulta_id = getattr(self, "_consulta_id_pdf", "")
        if consulta_id:
            self.drawRightString(ANCHO - MARGEN, 2.5 * mm, consulta_id)
        self.drawCentredString(ANCHO / 2, 2.5 * mm, f"Página {self._pageNumber} de {total}")
        self.restoreState()


class CanvasEstudio(NumberedCanvas):
    """Canvas del estudio: lleva los datos de marca de agua y pie como atributos
    de CLASE para que la instancia que crea platypus vía canvasmaker los tenga."""

    _consulta_id_pdf = ""
    _wm_l1 = ""
    _wm_l2 = ""
    _pie_disclaimer = ""
    _pie_generado = ""


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


_LOGO_CACHE: dict = {}


def _lector_logo():
    """ImageReader del logo seguriDatia, pre-escalado y cacheado UNA vez.

    El PNG original (1448×396, 400 KB) re-procesado por página hacía la
    generación ~2× más lenta: se reduce a ~420 px de ancho (suficiente para
    ~28 mm impresos) y se reutiliza en todas las páginas.
    """
    if "lector" not in _LOGO_CACHE:
        buffer = io.BytesIO()
        from PIL import Image as ImagenPIL

        with ImagenPIL.open(LOGO_SEGURIDATIA) as img:
            ancho_objetivo = 420
            alto_objetivo = round(img.height * ancho_objetivo / img.width)
            img = img.resize((ancho_objetivo, alto_objetivo), ImagenPIL.LANCZOS)
            img.save(buffer, "PNG")
        buffer.seek(0)
        _LOGO_CACHE["lector"] = ImageReader(buffer)
    return _LOGO_CACHE["lector"]


def _encabezado(cv: canvas_module.Canvas, doc: BaseDocTemplate):
    """Cabecera de cada página: identificación + logo seguriDatia (derecha)."""
    cv.saveState()
    cv.setFont("Helvetica", 7)
    cv.setFillColor(COLOR_NEUTRO)
    consulta_id = getattr(CanvasEstudio, "_consulta_id_pdf", "")
    titulo = "ESTUDIO DE SEGURIDAD" + (f" — Consulta {consulta_id}" if consulta_id else "")
    cv.drawString(MARGEN, ALTO - 10 * mm, titulo)
    # Logo seguriDatia arriba a la derecha (PNG con alfa; ~3,66:1). El fallo
    # del logo JAMÁS rompe el informe.
    try:
        lector_logo = _lector_logo()
        ancho_logo, alto_logo = lector_logo.getSize()
        alto_dibujo = 7.5 * mm
        ancho_dibujo = alto_dibujo * (ancho_logo / alto_logo)
        cv.drawImage(
            lector_logo,
            ANCHO - MARGEN - ancho_dibujo, ALTO - 13 * mm,
            width=ancho_dibujo, height=alto_dibujo,
            mask="auto",
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Logo seguriDatia no se pudo dibujar: %s", exc)
    cv.setStrokeColor(COLOR_FONDO_TABLA)
    cv.line(MARGEN, ALTO - 15 * mm, ANCHO - MARGEN, ALTO - 15 * mm)
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


def _hora_colombia(valor) -> str:
    """Fecha/hora UTC (datetime o ISO naive) → 'dd/mm/aaaa HH:MM' Colombia."""
    if not valor:
        return ""
    try:
        dt = valor if isinstance(valor, datetime) else datetime.fromisoformat(str(valor))
        if dt.tzinfo is None:  # fechas Mongo: naive pero UTC
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(_TZ_BOGOTA).strftime("%d/%m/%Y %H:%M")
    except Exception:
        return ""


_MESES_ABREV = {1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr", 5: "May", 6: "Jun",
                7: "Jul", 8: "Ago", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic"}


def _fecha_pie_generado(dt: datetime | None) -> str:
    """datetime (ya en Colombia) → '24, Sep 2026 a las 11:22AM' (pie seguriDatia)."""
    if not dt:
        return ""
    hora12 = dt.hour % 12 or 12
    ampm = "AM" if dt.hour < 12 else "PM"
    return f"{dt.day}, {_MESES_ABREV.get(dt.month, '')} {dt.year} a las {hora12}:{dt.minute:02d}{ampm}"


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
    cgr = fuentes.get("contraloria") or {}
    delitos = fuentes.get("delitos_sexuales") or {}
    pol = fuentes.get("policia") or {}
    runt = fuentes.get("runt") or {}
    simit = fuentes.get("simit") or {}
    sena = fuentes.get("sena") or {}
    sisconmp = fuentes.get("sisconmp") or {}
    ofac = fuentes.get("ofac") or {}
    ofac_nit = fuentes.get("ofac_nit") or {}
    onu_ue = fuentes.get("onu_ue") or {}
    bdme = fuentes.get("bdme") or {}
    bdme_nit = fuentes.get("bdme_nit") or {}
    rama_judicial = fuentes.get("rama_judicial") or {}
    rues = fuentes.get("rues") or {}
    situacion_militar = fuentes.get("situacion_militar") or {}

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
    fecha_wm = creado.replace(tzinfo=timezone.utc).astimezone(_TZ_BOGOTA).strftime("%d/%m/%Y %H:%M") if creado else ""
    CanvasEstudio._consulta_id_pdf = consulta_id
    CanvasEstudio._wm_l1 = f"{estudio.get('empresa_nombre', '')} | {estudio.get('usuario', '')} | {fecha_wm}"
    CanvasEstudio._wm_l2 = f"{consulta_id} | Generado por {MARCA}"
    # Pie legal seguriDatia en TODAS las páginas (pedido 2026-09-24): la fecha
    # sale del creado_en del doc (reproducible) y el generador del usuario
    # del estudio (variable: quien lanzó la consulta).
    CanvasEstudio._pie_disclaimer = (
        f"El informe de {MARCA} es el resultado de la obtención de datos públicos en su origen. "
        "El uso de la información y sus decisiones resultantes son responsabilidad del usuario final. "
        f"La calidad del dato es atribuible a la fuente y no a {MARCA}. Para mayor certeza puede "
        f"acudir directamente a la fuente pública que ofrece la información. {MARCA} no administra "
        "o gestiona las fuentes de consulta."
    )
    creado_co = creado.replace(tzinfo=timezone.utc).astimezone(_TZ_BOGOTA) if creado else None
    generador = estudio.get("usuario_nombre") or estudio.get("usuario") or "—"
    correo_gen = estudio.get("usuario_correo") or "—"
    CanvasEstudio._pie_generado = (
        f"Este reporte fue generado el {_fecha_pie_generado(creado_co)}. "
        f"Reporte generado por: {generador}. Correo: {correo_gen}."
    )

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
            "manifiestos_rndc",
        ])
    if _corrio(cgr):
        etiqueta_cgr, _ = ESTADO_FUENTE_TEXTO.get(cgr.get("estado", "ERROR"), ("—", COLOR_NEUTRO))
        filas_resumen.append([
            "Contraloría General de la República — Antecedentes Fiscales",
            etiqueta_cgr,
            _texto_veredicto_contraloria(cgr),
            "contraloria",
        ])
    if _corrio(delitos):
        etiqueta_del, _ = ESTADO_FUENTE_TEXTO.get(delitos.get("estado", "ERROR"), ("—", COLOR_NEUTRO))
        filas_resumen.append([
            "Policía — Inhabilidades Ley 1918 (delitos sexuales contra menores)",
            etiqueta_del,
            _texto_veredicto_delitos(delitos),
            "delitos_sexuales",
        ])
    if _corrio(pol):
        etiqueta_pol, _ = ESTADO_FUENTE_TEXTO.get(pol.get("estado", "ERROR"), ("—", COLOR_NEUTRO))
        filas_resumen.append([
            "Policía Nacional — Antecedentes Judiciales",
            etiqueta_pol,
            _texto_veredicto_policia(pol),
            "policia",
        ])
    if _corrio(runt):
        etiqueta_runt, _ = ESTADO_FUENTE_TEXTO.get(runt.get("estado", "ERROR"), ("—", COLOR_NEUTRO))
        filas_resumen.append([
            f"RUNT — Vehículo {estudio.get('placa') or (runt.get('placa') or '')}".rstrip(),
            etiqueta_runt,
            _texto_veredicto_runt(runt),
            "runt",
        ])
    if _corrio(simit):
        etiqueta_simit, _ = ESTADO_FUENTE_TEXTO.get(simit.get("estado", "ERROR"), ("—", COLOR_NEUTRO))
        filas_resumen.append([
            f"SIMIT — Comparendos placa {estudio.get('placa') or (simit.get('placa') or '')}".rstrip(),
            etiqueta_simit,
            _texto_veredicto_simit(simit),
            "simit",
        ])
    if _corrio(sena):
        etiqueta_sena, _ = ESTADO_FUENTE_TEXTO.get(sena.get("estado", "ERROR"), ("—", COLOR_NEUTRO))
        filas_resumen.append([
            "SENA — Certificados de formación",
            etiqueta_sena,
            _texto_veredicto_sena(sena),
            "sena",
        ])
    if _corrio(sisconmp):
        etiqueta_sis, _ = ESTADO_FUENTE_TEXTO.get(sisconmp.get("estado", "ERROR"), ("—", COLOR_NEUTRO))
        filas_resumen.append([
            "SISCONMP — Capacitaciones Mercancías Peligrosas",
            etiqueta_sis,
            _texto_veredicto_sisconmp(sisconmp),
            "sisconmp",
        ])
    if _corrio(ofac):
        etiqueta_ofac, _ = ESTADO_FUENTE_TEXTO.get(ofac.get("estado", "ERROR"), ("—", COLOR_NEUTRO))
        filas_resumen.append([
            "OFAC — Lista SDN (Lista Clinton)",
            etiqueta_ofac,
            _texto_veredicto_ofac(ofac),
            "ofac",
        ])
    if _corrio(ofac_nit):
        etiqueta_ofac_nit, _ = ESTADO_FUENTE_TEXTO.get(ofac_nit.get("estado", "ERROR"), ("—", COLOR_NEUTRO))
        filas_resumen.append(["OFAC — Empresas por NIT", etiqueta_ofac_nit, _texto_veredicto_ofac(ofac_nit), "ofac_nit"])
    if _corrio(onu_ue):
        etiqueta_onu, _ = ESTADO_FUENTE_TEXTO.get(onu_ue.get("estado", "ERROR"), ("—", COLOR_NEUTRO))
        filas_resumen.append([
            "ONU/UE — Listas internacionales de sanciones",
            etiqueta_onu,
            _texto_veredicto_onu_ue(onu_ue),
            "onu_ue",
        ])
    for fuente_bdme, etiqueta, clave_bdme in ((bdme, "BDME — Persona por cédula", "bdme"), (bdme_nit, "BDME — Empresa por NIT", "bdme_nit")):
        if _corrio(fuente_bdme):
            estado_txt, _ = ESTADO_FUENTE_TEXTO.get(fuente_bdme.get("estado", "ERROR"), ("—", COLOR_NEUTRO))
            if fuente_bdme.get("estado") in {"EXITO", "ADVERTENCIA"}:
                veredicto = "REPORTADO EN EL BDME" if fuente_bdme.get("reportado") else "NO REPORTADO EN EL BDME"
            else:
                veredicto = _resumen_error(fuente_bdme)
            filas_resumen.append([etiqueta, estado_txt, veredicto, clave_bdme])
    if _corrio(rama_judicial):
        estado_txt, _ = ESTADO_FUENTE_TEXTO.get(rama_judicial.get("estado", "ERROR"), ("—", COLOR_NEUTRO))
        total_rama = int(rama_judicial.get("total_procesos") or 0)
        veredicto = (f"{total_rama} PROCESO(S) POR COINCIDENCIA DE NOMBRE — VALIDAR HOMONIMIA"
                     if total_rama else "SIN PROCESOS PARA EL NOMBRE CONSULTADO")
        if rama_judicial.get("estado") not in {"EXITO", "ADVERTENCIA"}:
            veredicto = _resumen_error(rama_judicial)
        filas_resumen.append(["Rama Judicial — Procesos por nombre", estado_txt, veredicto, "rama_judicial"])
    if _corrio(rues):
        etiqueta_rues, _ = ESTADO_FUENTE_TEXTO.get(rues.get("estado", "ERROR"), ("—", COLOR_NEUTRO))
        filas_resumen.append([
            f"RUES — Registro Mercantil NIT {estudio.get('nit') or (rues.get('nit') or '')}".rstrip(),
            etiqueta_rues,
            _texto_veredicto_rues(rues),
            "rues",
        ])
    if _corrio(situacion_militar):
        etiqueta_sm, _ = ESTADO_FUENTE_TEXTO.get(situacion_militar.get("estado", "ERROR"), ("—", COLOR_NEUTRO))
        filas_resumen.append([
            "Ejército — Situación militar (libreta)",
            etiqueta_sm,
            _texto_veredicto_situacion_militar(situacion_militar),
            "situacion_militar",
        ])
    # Procuraduría SIEMPRE de última en el resumen (pedido 2026-09-15): es la
    # fuente más lenta y el usuario quiere el veredicto disciplinario
    # como cierre del informe.
    if _corrio(proc):
        etiqueta_proc, _ = ESTADO_FUENTE_TEXTO.get(proc.get("estado", "ERROR"), ("—", COLOR_NEUTRO))
        filas_resumen.append([
            "Procuraduría General de la Nación",
            etiqueta_proc,
            _texto_veredicto(proc),
            "procuraduria",
        ])
    estados_resumen = [
        (fuente or {}).get("estado")
        for fuente in (rndc, cgr, delitos, pol, runt, simit, sena, sisconmp, ofac, ofac_nit, onu_ue, bdme, bdme_nit, rama_judicial, rues, situacion_militar, proc)
        if _corrio(fuente)
    ]
    # Botón de navegación (pedido 2026-09-24): el nombre de la fuente en el
    # resumen es un LINK INTERNO a la página de su evidencia (ancla ev_<fuente>
    # en la sección final) — solo si la fuente tiene evidencia (las de
    # API/dataset no generan captura).
    evidencias_resumen = estudio.get("evidencias") or {}

    def _celda_nombre_resumen(fila: list):
        clave = fila[3] if len(fila) > 3 else None
        if clave and evidencias_resumen.get(clave):
            # Link interno SIN subrayado (pedido del usuario): solo el color
            # azul distingue que es clicable.
            return Paragraph(
                f'<a href="#ev_{clave}" color="#0F2A43">{escape(str(fila[0]))}</a>',
                estilo_celda,
            )
        return celda(fila[0])

    tabla_resumen = Table(
        [
            [Paragraph(escape(str(v)), estilo_celda_cab) for v in filas_resumen[0]]
        ] + [
            [_celda_nombre_resumen(fila), celda(fila[1]), celda(fila[2])]
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
    estilos_tabla_resumen.extend(
        ("BACKGROUND", (1, fila), (1, fila), COLOR_FONDO_ADVERTENCIA)
        for fila, estado in enumerate(estados_resumen, start=1)
        if estado == "ADVERTENCIA"
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
            f"Ventana consultada: {_fecha_legible(rndc.get('desde'))} a {_fecha_legible(rndc.get('hasta'))} · "
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

    # ── 3b. Detalle Contraloría (antecedentes fiscales) ──────────────────────
    if _corrio(cgr):
        _antes_de_seccion(cgr)
        cuento.append(Paragraph("Antecedentes fiscales — Contraloría General de la República", estilo_h2))
    if cgr.get("estado") in {"EXITO", "ADVERTENCIA"}:
        no_registra_cgr = cgr.get("no_registra")
        if no_registra_cgr is True:
            texto_cgr, color_cgr = "NO SE ENCUENTRA REPORTADO COMO RESPONSABLE FISCAL (SIBOR)", COLOR_EXITO
        elif no_registra_cgr is False:
            texto_cgr, color_cgr = "SE ENCUENTRA REPORTADO COMO RESPONSABLE FISCAL — VER DETALLE", COLOR_FALLO
        else:
            texto_cgr, color_cgr = "VEREDICTO NO CONCLUSIVO — VER MENSAJE DEL PORTAL", COLOR_ADVERTENCIA
        tabla_veredicto_cgr = Table(
            [[Paragraph(f"<b>{texto_cgr}</b>", ParagraphStyle("veredicto_cgr", fontName="Helvetica", fontSize=10.5, textColor=colors.white, alignment=1))]],
            colWidths=[160 * mm],
        )
        tabla_veredicto_cgr.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), color_cgr),
            ("TOPPADDING", (0, 0), (-1, -1), 7),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
        ]))
        cuento.append(tabla_veredicto_cgr)
        cuento.append(Spacer(0, 2 * mm))
        detalle_cgr = [
            ["Resultado de la consulta", (cgr.get("mensaje") or "—")[:300]],
            ["Código de verificación CGR", cgr.get("codigo_verificacion") or "No disponible"],
            ["Origen de datos", _texto_origen(cgr)],
        ]
        tabla_cgr = Table(
            [[celda(k, negrita=True), celda(v)] for k, v in detalle_cgr],
            colWidths=[45 * mm, 115 * mm],
        )
        tabla_cgr.setStyle(TableStyle([
            ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("BACKGROUND", (0, 0), (0, -1), COLOR_FONDO_TABLA),
            ("GRID", (0, 0), (-1, -1), 0.4, colors.white),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]))
        cuento.append(tabla_cgr)
        cuento.append(Spacer(0, 2 * mm))
        cuento.append(Paragraph(
            "El certificado oficial de la CGR se procesó en memoria para leer el veredicto; no se "
            "adjunta ni se publica. El código de verificación permite constatar la autenticidad del "
            "certificado directamente ante la Contraloría.",
            estilo_peq,
        ))
    elif _corrio(cgr):
        cuento.append(_parrafo_estado_fuente(cgr, "la Contraloría"))

    # ── 3c. Detalle Inhabilidades Ley 1918 (delitos sexuales contra menores) ─
    if _corrio(delitos):
        _antes_de_seccion(delitos)
        cuento.append(Paragraph("Inhabilidades — Delitos sexuales contra menores (Ley 1918)", estilo_h2))
    if delitos.get("estado") in {"EXITO", "ADVERTENCIA"}:
        no_registra_del = delitos.get("no_registra")
        if no_registra_del is True:
            texto_del, color_del = "NO REGISTRA INHABILIDAD (LEY 1918 DE 2018)", COLOR_EXITO
        elif no_registra_del is False:
            texto_del, color_del = "REGISTRA INHABILIDAD — REVISIÓN HUMANA OBLIGATORIA", COLOR_FALLO
        else:
            texto_del, color_del = "VEREDICTO NO CONCLUSIVO — VER MENSAJE DEL PORTAL", COLOR_ADVERTENCIA
        tabla_veredicto_del = Table(
            [[Paragraph(f"<b>{texto_del}</b>", ParagraphStyle("veredicto_del", fontName="Helvetica", fontSize=10.5, textColor=colors.white, alignment=1))]],
            colWidths=[160 * mm],
        )
        tabla_veredicto_del.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), color_del),
            ("TOPPADDING", (0, 0), (-1, -1), 7),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
        ]))
        cuento.append(tabla_veredicto_del)
        cuento.append(Spacer(0, 2 * mm))
        detalle_del = [
            ["Resultado de la consulta", (delitos.get("mensaje") or "—")[:300]],
        ]
        if delitos.get("fecha_consulta"):
            detalle_del.append(["Consulta ante la DIJIN", delitos["fecha_consulta"]])
        if delitos.get("empresa_consultante"):
            detalle_del.append(["Empresa consultante", delitos["empresa_consultante"]])
        detalle_del.append(["Origen de datos", _texto_origen(delitos)])
        tabla_del = Table(
            [[celda(k, negrita=True), celda(v)] for k, v in detalle_del],
            colWidths=[45 * mm, 115 * mm],
        )
        tabla_del.setStyle(TableStyle([
            ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("BACKGROUND", (0, 0), (0, -1), COLOR_FONDO_TABLA),
            ("GRID", (0, 0), (-1, -1), 0.4, colors.white),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]))
        cuento.append(tabla_del)
        cuento.append(Spacer(0, 2 * mm))
        cuento.append(Paragraph(
            "Consulta en línea del registro de inhabilidades de la Policía Nacional (DIJIN). El "
            "resultado corresponde al veredicto del portal en la fecha indicada, emitido para la "
            "empresa consultante identificada arriba.",
            estilo_peq,
        ))
    elif _corrio(delitos):
        cuento.append(_parrafo_estado_fuente(delitos, "las inhabilidades Ley 1918"))

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
        rtm = runt.get("rtm") or {}
        no_registra_runt = runt.get("no_registra")
        if no_registra_runt is True:
            # Sobre la PLACA, no sobre la persona: nunca presentar como "limpio".
            texto_runt, color_runt = "PLACA SIN INFORMACIÓN EN EL RUNT", COLOR_NEUTRO
        elif no_registra_runt is False:
            texto_runt, color_runt = "LA CÉDULA NO CORRESPONDE A UN PROPIETARIO ACTIVO DEL VEHÍCULO", COLOR_ADVERTENCIA
        elif soat.get("vigente") is False:
            texto_runt, color_runt = "SOAT VENCIDO — VEHÍCULO SIN SEGURO VIGENTE", COLOR_FALLO
        elif rtm and rtm.get("vigente") is False:
            # RTM vencida (2026-09-14): el vehículo no está al día en revisión
            # técnico-mecánica aunque el SOAT esté vigente.
            texto_runt, color_runt = (
                "REVISIÓN TÉCNICO-MECÁNICA (RTM) VENCIDA"
                + (f" EL {_fecha_legible(rtm.get('fecha_vigencia'))}" if rtm.get("fecha_vigencia") else "")
                + " — VEHÍCULO NO AL DÍA EN REVISIÓN"
            ), COLOR_FALLO
        elif soat and soat.get("vigente") is True:
            sufijo_rtm = ""
            if rtm and rtm.get("vigente") is True:
                sufijo_rtm = f" · RTM VIGENTE (VENCE {_fecha_legible(rtm.get('fecha_vigencia'))})"
            texto_runt, color_runt = f"SOAT VIGENTE — VENCE {_fecha_legible(soat.get('fecha_fin_vigencia'))}{sufijo_rtm}", COLOR_EXITO
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
                detalle_runt.append([etiqueta, _fecha_legible(valor) if "fecha" in clave else valor])
        if soat:
            detalle_runt.append(["SOAT — póliza", soat.get("numero", "—")])
            detalle_runt.append(["SOAT — aseguradora", soat.get("aseguradora", "—")])
            detalle_runt.append(["SOAT — vigencia", (
                f"{_fecha_legible(soat.get('fecha_inicio_vigencia'))} a {_fecha_legible(soat.get('fecha_fin_vigencia'))} "
                f"({soat.get('estado_portal', '—')})"
            )])
        if rtm:
            detalle_runt.append(["RTM — certificado", str(rtm.get("numero_certificado", "—"))])
            detalle_runt.append(["RTM — CDA", rtm.get("cda", "—")])
            detalle_runt.append(["RTM — vigencia", (
                f"expedada {_fecha_legible(rtm.get('fecha_expedicion'))} · "
                f"vence {_fecha_legible(rtm.get('fecha_vigencia'))}"
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
        # Historial de pólizas SOAT (máx 5) y revisiones RTM (máx 3).
        estilo_celda_pol = ParagraphStyle("celda_pol", parent=estilo_celda, fontSize=7.5, leading=9.5)
        estilo_cab_pol = ParagraphStyle("cab_pol", parent=estilo_celda_pol, fontName="Helvetica-Bold", textColor=colors.white)
        polizas = runt.get("polizas") or []
        if polizas:
            cuento.append(Spacer(0, 2 * mm))
            filas_pol = [[
                Paragraph("Póliza", estilo_cab_pol), Paragraph("Vigencia", estilo_cab_pol),
                Paragraph("Aseguradora", estilo_cab_pol), Paragraph("Estado", estilo_cab_pol),
            ]]
            for p in polizas[:5]:
                filas_pol.append([
                    Paragraph(escape(str(p.get("numero", "—"))), estilo_celda_pol),
                    Paragraph(escape(
                        f"{_fecha_legible(p.get('fecha_inicio_vigencia'))} → {_fecha_legible(p.get('fecha_fin_vigencia'))}"
                    ), estilo_celda_pol),
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
        revisiones = runt.get("revisiones") or []
        if revisiones:
            cuento.append(Spacer(0, 2 * mm))
            filas_rtm = [[
                Paragraph("Certificado", estilo_cab_pol), Paragraph("Vigencia", estilo_cab_pol),
                Paragraph("CDA", estilo_cab_pol),
            ]]
            for rev in revisiones[:3]:
                filas_rtm.append([
                    Paragraph(escape(str(rev.get("numero_certificado", "—"))), estilo_celda_pol),
                    Paragraph(escape(f"hasta {_fecha_legible(rev.get('fecha_vigencia'))}"), estilo_celda_pol),
                    Paragraph(escape(str(rev.get("cda", "—"))), estilo_celda_pol),
                ])
            tabla_rtm = Table(filas_rtm, colWidths=[30 * mm, 40 * mm, 90 * mm])
            tabla_rtm.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), COLOR_PRIMARIO),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, COLOR_FONDO_TABLA]),
                ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#D5DBE3")),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]))
            cuento.append(Paragraph("Historial de revisiones técnico-mecánicas (más recientes)", ParagraphStyle("h_rtm", parent=estilo_normal, fontSize=8, textColor=COLOR_NEUTRO, spaceBefore=4)))
            cuento.append(tabla_rtm)
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
                    Paragraph(escape(_fecha_legible(c.get("fecha_imposicion"))), estilo_celda_sim),
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
                    Paragraph(escape(_fecha_legible(c.get("fecha_certificacion"))), estilo_celda_sena),
                    Paragraph(escape(_fecha_legible(c.get("fecha_firma"))), estilo_celda_sena),
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

    # ── 4d-bis. Detalle SISCONMP (capacitaciones Mercancías Peligrosas) ─────
    if _corrio(sisconmp):
        _antes_de_seccion(sisconmp)
        cuento.append(Paragraph(
            "Capacitaciones en Mercancías Peligrosas — SISCONMP (Ministerio de Transporte)", estilo_h2))
    if sisconmp.get("estado") in {"EXITO", "ADVERTENCIA"}:
        caps = sisconmp.get("capacitaciones") or []
        total_caps = int(sisconmp.get("total_capacitaciones") or 0)
        hay_vigente = any(c.get("vigente") is True for c in caps)
        hay_vencida = any(c.get("vigente") is False for c in caps)
        if total_caps == 0:
            texto_sis, color_sis = "SIN CAPACITACIONES DE MERCANCÍAS PELIGROSAS REGISTRADAS", COLOR_EXITO
        elif hay_vigente:
            texto_sis, color_sis = f"REGISTRA {total_caps} CAPACITACIÓN(ES) — AL MENOS UNA VIGENTE", COLOR_EXITO
        elif hay_vencida:
            texto_sis, color_sis = f"REGISTRA {total_caps} CAPACITACIÓN(ES) — NINGUNA VIGENTE (VENCIDAS)", COLOR_ADVERTENCIA
        else:
            texto_sis, color_sis = f"REGISTRA {total_caps} CAPACITACIÓN(ES) — VIGENCIA NO REPORTADA", COLOR_PRIMARIO
        tabla_veredicto_sis = Table(
            [[Paragraph(f"<b>{texto_sis}</b>", ParagraphStyle("veredicto_sisconmp", fontName="Helvetica", fontSize=10.5, textColor=colors.white, alignment=1))]],
            colWidths=[160 * mm],
        )
        tabla_veredicto_sis.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), color_sis),
            ("TOPPADDING", (0, 0), (-1, -1), 7),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
        ]))
        cuento.append(tabla_veredicto_sis)
        cuento.append(Spacer(0, 2 * mm))
        ciudadano = " ".join(f"{sisconmp.get('nombres') or ''} {sisconmp.get('apellidos') or ''}".split())
        detalle_sis = [
            ["Capacitaciones registradas", total_caps],
            ["Ciudadano según el portal", ciudadano or "—"],
            ["Origen de datos", _texto_origen(sisconmp)],
        ]
        if (sisconmp.get("mensaje") or "").strip():
            detalle_sis.insert(1, ["Mensaje del portal", sisconmp["mensaje"][:300]])
        tabla_sis_resumen = Table(
            [[celda(k, negrita=True), celda(v)] for k, v in detalle_sis],
            colWidths=[45 * mm, 115 * mm],
        )
        tabla_sis_resumen.setStyle(TableStyle([
            ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("BACKGROUND", (0, 0), (0, -1), COLOR_FONDO_TABLA),
            ("GRID", (0, 0), (-1, -1), 0.4, colors.white),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]))
        cuento.append(tabla_sis_resumen)
        if caps:
            cuento.append(Spacer(0, 2 * mm))
            estilo_celda_sis = ParagraphStyle("celda_sisconmp", parent=estilo_celda, fontSize=7.5, leading=9.5)
            estilo_cab_sis = ParagraphStyle("cab_sisconmp", parent=estilo_celda_sis, fontName="Helvetica-Bold", textColor=colors.white)
            filas_sis = [[
                Paragraph("Capacitación", estilo_cab_sis), Paragraph("Entidad", estilo_cab_sis),
                Paragraph("Institución educativa", estilo_cab_sis), Paragraph("Expedición", estilo_cab_sis),
                Paragraph("Vencimiento", estilo_cab_sis), Paragraph("Vigente", estilo_cab_sis),
            ]]
            for c in caps[:10]:
                if c.get("vigente") is True:
                    vigencia_txt, color_vig = "SÍ", "#1A7F37"
                elif c.get("vigente") is False:
                    vigencia_txt, color_vig = "NO (VENCIDA)", "#B58900"
                else:
                    vigencia_txt, color_vig = "—", "#57606A"
                nombre_cap = f"{c.get('tipo_capacitacion') or ''}: {c.get('nombre') or '—'}".strip(": ")
                if c.get("clase"):
                    nombre_cap += f" · Clase {c['clase']}"
                if c.get("tipo_vehiculo"):
                    nombre_cap += f" · Veh. {c['tipo_vehiculo']}"
                filas_sis.append([
                    Paragraph(escape(nombre_cap), estilo_celda_sis),
                    Paragraph(escape(str(c.get("entidad_certificadora") or "—")), estilo_celda_sis),
                    Paragraph(escape(str(c.get("institucion_educativa") or "—")), estilo_celda_sis),
                    Paragraph(escape(_fecha_legible(c.get("fecha_expedicion"))), estilo_celda_sis),
                    Paragraph(escape(_fecha_legible(c.get("fecha_vencimiento"))), estilo_celda_sis),
                    Paragraph(f'<font color="{color_vig}">{vigencia_txt}</font>', estilo_celda_sis),
                ])
            tabla_caps = Table(filas_sis, colWidths=[52 * mm, 18 * mm, 38 * mm, 17 * mm, 17 * mm, 18 * mm])
            tabla_caps.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), COLOR_PRIMARIO),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, COLOR_FONDO_TABLA]),
                ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#D5DBE3")),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]))
            cuento.append(Paragraph(
                f"Detalle de capacitaciones ({min(10, len(caps))} de {total_caps or len(caps)})",
                ParagraphStyle("h_sisconmp", parent=estilo_normal, fontSize=8, textColor=COLOR_NEUTRO, spaceBefore=4),
            ))
            cuento.append(tabla_caps)
    elif _corrio(sisconmp):
        cuento.append(_parrafo_estado_fuente(sisconmp, "el SISCONMP"))

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
            ["Publicación OFAC", _fecha_legible(ofac.get("fecha_publicacion"))],
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
                f"Publicación OFAC: {escape(_fecha_legible(ofac_nit.get('fecha_publicacion')))} · "
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

    # ── 4g. Listas internacionales ONU / UE ──────────────────────────────────
    if _corrio(onu_ue):
        _antes_de_seccion(onu_ue)
        cuento.append(Paragraph("Listas internacionales de sanciones — ONU / Unión Europea", estilo_h2))
        if onu_ue.get("estado") in {"EXITO", "ADVERTENCIA"}:
            aplica_onu = bool(onu_ue.get("aplica"))
            no_disponibles = ", ".join(onu_ue.get("listas_no_disponibles") or [])
            texto_onu = (
                "COINCIDENCIA EXACTA DE IDENTIFICACIÓN — REQUIERE REVISIÓN HUMANA"
                if aplica_onu else
                "SIN COINCIDENCIA EXACTA DE IDENTIFICACIÓN EN LAS LISTAS CONSULTADAS"
            )
            color_onu = COLOR_ADVERTENCIA if aplica_onu else COLOR_EXITO
            tabla_onu_banner = Table([[Paragraph(f"<b>{texto_onu}</b>", ParagraphStyle(
                "veredicto_onu_ue", fontName="Helvetica", fontSize=10.5,
                textColor=colors.white, alignment=1,
            ))]], colWidths=[160 * mm])
            tabla_onu_banner.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, -1), color_onu),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ]))
            cuento.append(tabla_onu_banner)
            filas_onu = [
                ["Método", "Coincidencia exacta del número de identificación (sin búsqueda difusa por nombre)"],
            ]
            for etiqueta_lista, meta_lista in (onu_ue.get("listas") or {}).items():
                filas_onu.append([
                    f"Publicación {etiqueta_lista}",
                    f"{_fecha_legible(meta_lista.get('fecha_publicacion'))} · "
                    f"{int(meta_lista.get('total_registros_lista') or 0)} persona(s) · "
                    f"SHA-256 {str(meta_lista.get('sha256_dataset') or '—')[:16]}…",
                ])
            filas_onu.append(["Coincidencias", str(onu_ue.get("total_coincidencias") or 0)])
            if no_disponibles:
                filas_onu.append(["Listas no disponibles", no_disponibles])
            tabla_onu = Table([[celda(k, True), celda(v)] for k, v in filas_onu], colWidths=[45 * mm, 115 * mm])
            tabla_onu.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (0, -1), COLOR_FONDO_TABLA),
                ("GRID", (0, 0), (-1, -1), 0.4, colors.white),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]))
            cuento.append(tabla_onu)
            for coincidencia in (onu_ue.get("coincidencias") or [])[:10]:
                cuento.append(Paragraph(
                    "<b>Coincidencia:</b> "
                    f"{escape(str(coincidencia.get('nombre') or '—'))} · lista {escape(str(coincidencia.get('lista') or '—'))} · "
                    f"referencia {escape(str(coincidencia.get('referencia') or coincidencia.get('uid') or '—'))} · "
                    f"programa(s) {escape(', '.join(coincidencia.get('programas') or []) or '—')}",
                    estilo_normal,
                ))
            if aplica_onu:
                cuento.append(Paragraph(
                    "Una coincidencia técnica no sustituye el análisis de identidad, homonimia y alcance del "
                    "programa de sanciones. Debe ser revisada por una persona responsable antes de cualquier "
                    "decisión.", estilo_peq,
                ))
        else:
            cuento.append(_parrafo_estado_fuente(onu_ue, "las listas ONU/UE"))

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
                fecha = _fecha_legible(proceso.get("fechaProceso") or proceso.get("fechaUltimaActuacion"))
                cuento.append(Paragraph(
                    f"<b>Proceso:</b> {escape(str(numero))} · "
                    f"<b>Despacho:</b> {escape(str(despacho))} · <b>Fecha:</b> {escape(fecha)}",
                    estilo_peq,
                ))
                sujetos = str(proceso.get("sujetosProcesales") or "").strip()
                if sujetos:
                    partes_sujetos = []
                    for parte in sujetos.split("|"):
                        parte = parte.strip()
                        if not parte:
                            continue
                        if ":" in parte:
                            calidad, persona = parte.split(":", 1)
                            partes_sujetos.append(
                                f"<b>{escape(calidad.strip())}:</b> {escape(persona.strip())}"
                            )
                        else:
                            partes_sujetos.append(escape(parte))
                    cuento.append(Paragraph(
                        "<b>Sujetos procesales</b><br/>" + "<br/>".join(partes_sujetos),
                        estilo_peq,
                    ))
            if total_rama:
                cuento.append(Paragraph(
                    "La coincidencia se basa exclusivamente en el nombre informado y puede corresponder a homónimos. "
                    "Debe verificarse la identidad y la calidad de la persona dentro de cada proceso.", estilo_peq,
                ))
        else:
            cuento.append(_parrafo_estado_fuente(rama_judicial, "la Rama Judicial"))

    # ── 4g. RUES — Registro Mercantil por NIT ───────────────────────────────
    if _corrio(rues):
        _antes_de_seccion(rues)
        cuento.append(Paragraph("RUES — Registro Mercantil (Confecámaras)", estilo_h2))
        if rues.get("estado") in {"EXITO", "ADVERTENCIA"}:
            no_registra_rues = bool(rues.get("no_registra"))
            estado_mat = (rues.get("estado_matricula") or "").strip().upper()
            if no_registra_rues:
                texto_rues = "NIT SIN REGISTRO EN EL REGISTRO MERCANTIL"
                color_rues = COLOR_NEUTRO
            elif estado_mat == "ACTIVA":
                renovacion = rues.get("fecha_renovacion")
                texto_rues = (
                    f"MATRÍCULA ACTIVA — RENOVADA HASTA {_fecha_legible(renovacion)}"
                    if renovacion else "MATRÍCULA ACTIVA"
                )
                color_rues = COLOR_EXITO
            else:
                texto_rues = f"MATRÍCULA {estado_mat or 'SIN ESTADO'} — LA EMPRESA NO ESTÁ ACTIVA EN REGISTRO MERCANTIL"
                color_rues = COLOR_ADVERTENCIA
            tabla_rues_banner = Table([[Paragraph(f"<b>{texto_rues}</b>", ParagraphStyle(
                "veredicto_rues", fontName="Helvetica", fontSize=10.5,
                textColor=colors.white, alignment=1,
            ))]], colWidths=[160 * mm])
            tabla_rues_banner.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, -1), color_rues),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ]))
            cuento.append(tabla_rues_banner)
            if not no_registra_rues:
                ciiu = rues.get("ciiu") or {}
                actividades = " · ".join(
                    f"{escape(str((a.get('codigo') or '').strip()))} {escape(str((a.get('descripcion') or '').strip()))}"
                    for a in (ciiu.get("principal"), ciiu.get("secundaria"), ciiu.get("terciaria"))
                    if (a or {}).get("codigo") or (a or {}).get("descripcion")
                ) or "—"
                filas_rues = [
                    ["Razón social", rues.get("razon_social") or "—"],
                    ["NIT", rues.get("nit_con_dv") or (rues.get("nit") or estudio.get("nit") or "—")],
                    ["Estado de la matrícula", estado_mat or "—"],
                    ["Cámara de Comercio", f"{rues.get('camara') or '—'} · matrícula {rues.get('matricula') or '—'}"],
                    ["Fecha de matrícula", _fecha_legible(rues.get("fecha_matricula"))],
                    ["Última renovación", (
                        f"{_fecha_legible(rues.get('fecha_renovacion'))} · último año renovado {rues.get('ultimo_ano_renovado') or '—'}"
                    )],
                    ["Fecha de cancelación", _fecha_legible(rues.get("fecha_cancelacion"))],
                    ["Tipo de sociedad", (
                        f"{rues.get('tipo_sociedad') or '—'} · {rues.get('organizacion_juridica') or '—'}"
                    )],
                    ["Categoría", rues.get("categoria_matricula") or "—"],
                    ["Actividad económica (CIIU)", actividades],
                    ["Ubicación", f"{rues.get('municipio') or '—'} · {rues.get('departamento') or '—'}"],
                ]
                tabla_rues = Table(
                    [[celda(k, True), celda(v)] for k, v in filas_rues],
                    colWidths=[45 * mm, 115 * mm],
                )
                tabla_rues.setStyle(TableStyle([
                    ("BACKGROUND", (0, 0), (0, -1), COLOR_FONDO_TABLA),
                    ("GRID", (0, 0), (-1, -1), 0.4, colors.white),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ]))
                cuento.append(tabla_rues)
                for representante in (rues.get("representantes") or [])[:5]:
                    cuento.append(Paragraph(
                        "<b>Representante legal:</b> "
                        f"{escape(str(representante.get('nombre') or '—'))} · "
                        f"cédula {escape(str(representante.get('documento') or '—'))}",
                        estilo_peq,
                    ))
            if rues.get("mensaje"):
                cuento.append(Paragraph(escape(str(rues["mensaje"])[:300]), estilo_peq))
            cuento.append(Paragraph(
                "Información pública informativa reportada por la cámara de comercio correspondiente en la fecha "
                "de consulta; NO constituye el Certificado de Existencia y Representación Legal ni el certificado "
                "de matrícula (los expide la cámara de comercio ante solicitud).",
                estilo_peq,
            ))
        else:
            cuento.append(_parrafo_estado_fuente(rues, "el RUES"))

    # ── 4h. Detalle situación militar (libreta militar, Ejército) ───────────
    if _corrio(situacion_militar):
        _antes_de_seccion(situacion_militar)
        cuento.append(Paragraph(
            "Situación Militar — Libreta Militar (Ejército Nacional)", estilo_h2))
    if situacion_militar.get("estado") in {"EXITO", "ADVERTENCIA"}:
        estado_tarjeta = (situacion_militar.get("estado_tarjeta_militar") or "").strip()
        if situacion_militar.get("no_registra"):
            texto_sm, color_sm = "CIUDADANO SIN REGISTRO DE SITUACIÓN MILITAR EN EL SISTEMA", COLOR_EXITO
        elif situacion_militar.get("estado") == "ADVERTENCIA":
            texto_sm, color_sm = f"SITUACIÓN MILITAR SIN DEFINIR — {estado_tarjeta}", COLOR_ADVERTENCIA
        elif estado_tarjeta:
            texto_sm, color_sm = f"SITUACIÓN MILITAR: {estado_tarjeta}", COLOR_EXITO
        else:
            texto_sm, color_sm = "CONSULTA REALIZADA — VER DETALLE", COLOR_PRIMARIO
        tabla_veredicto_sm = Table(
            [[Paragraph(f"<b>{texto_sm}</b>", ParagraphStyle("veredicto_sm", fontName="Helvetica", fontSize=10.5, textColor=colors.white, alignment=1))]],
            colWidths=[160 * mm],
        )
        tabla_veredicto_sm.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), color_sm),
            ("TOPPADDING", (0, 0), (-1, -1), 7),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
        ]))
        cuento.append(tabla_veredicto_sm)
        cuento.append(Spacer(0, 2 * mm))
        detalle_sm = [
            ["Ciudadano según el certificado", situacion_militar.get("nombre_completo") or "—"],
            ["Estado tarjeta militar", estado_tarjeta or "—"],
            ["Origen de datos", _texto_origen(situacion_militar)],
        ]
        if situacion_militar.get("fecha_expedicion"):
            detalle_sm.insert(2, ["Expedición del certificado", _fecha_legible(situacion_militar.get("fecha_expedicion"))])
        if (situacion_militar.get("mensaje") or "").strip():
            detalle_sm.insert(1, ["Mensaje del portal", situacion_militar["mensaje"][:300]])
        tabla_sm_resumen = Table(
            [[celda(k, negrita=True), celda(v)] for k, v in detalle_sm],
            colWidths=[55 * mm, 105 * mm],
        )
        tabla_sm_resumen.setStyle(TableStyle([
            ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("BACKGROUND", (0, 0), (0, -1), COLOR_FONDO_TABLA),
            ("GRID", (0, 0), (-1, -1), 0.4, colors.white),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]))
        cuento.append(tabla_sm_resumen)
        cuento.append(Paragraph(
            "Certificación pública del Comando de Reclutamiento y Control Reservas del Ejército Nacional "
            "(gratuita, sin valor como documento de identificación militar). La definición de la situación "
            "militar corresponde a las Leyes 1861 de 2017 y 1184 de 2008 y al Decreto 977 de 2018.",
            estilo_peq,
        ))
    elif _corrio(situacion_militar):
        cuento.append(_parrafo_estado_fuente(situacion_militar, "la situación militar"))

    # ── 3. Detalle Procuraduría — SIEMPRE LA ÚLTIMA fuente del informe ───────
    # (pedido 2026-09-15: el veredicto disciplinario de la PGN queda como
    # cierre; además es la fuente más lenta del módulo).
    if _corrio(proc):
        _antes_de_seccion(proc)
        cuento.append(Paragraph("Antecedentes disciplinarios — Procuraduría General de la Nación", estilo_h2))
    if proc.get("estado") in {"EXITO", "ADVERTENCIA"}:
        no_registra = proc.get("no_registra")
        if no_registra is True:
            texto, color = "NO REGISTRA SANCIONES NI INHABILIDADES VIGENTES", COLOR_EXITO
        elif no_registra is False:
            texto, color = "REGISTRA ANOTACIONES DISCIPLINARIAS", COLOR_FALLO
        else:
            texto, color = "VEREDICTO NO CONCLUSIVO", COLOR_ADVERTENCIA
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
            ["Nombre consultado", proc.get("nombre_certificado") or proc.get("nombre_consultado") or "No disponible"],
            ["Resultado de la consulta", (proc.get("mensaje") or "—")[:300]],
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
            for nombre, f in (("RNDC", rndc), ("Contraloría", cgr), ("Inhabilidades 1918", delitos), ("Policía", pol), ("RUNT", runt), ("SIMIT", simit), ("SENA", sena), ("SISCONMP", sisconmp), ("OFAC cédula", ofac), ("OFAC NIT", ofac_nit), ("ONU/UE", onu_ue), ("BDME cédula", bdme), ("BDME NIT", bdme_nit), ("Rama Judicial", rama_judicial), ("RUES", rues), ("Situación militar", situacion_militar), ("Procuraduría", proc))
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
            "Municipios, consulta ciudadana abierta por placa. La consulta es sobre el VEHÍCULO y "
            "no&nbsp;constituye&nbsp;antecedente&nbsp;personal ni atribuye responsabilidad por infracción a la persona evaluada; los datos "
            "corresponden a lo reportado por los organismos de tránsito en la fecha de consulta y los saldos "
            "son informativos."
        )
    if _corrio(cgr):
        bloques_legal.append(
            "<b>Antecedentes fiscales (Contraloría General de la República):</b> la información se obtuvo del "
            "Certificado de Antecedentes Fiscales de persona natural que la CGR expide de forma pública y "
            "gratuita contra el Sistema de Información del Boletín de Responsables Fiscales (SIBOR), exigible "
            "en procesos de contratación (Decreto 2150 de 1995). El veredicto corresponde a lo certificado por "
            "la CGR en la fecha de la consulta; la ausencia de reporte no constituye certificación de "
            "responsabilidad fiscal futura."
        )
    if _corrio(delitos):
        bloques_legal.append(
            "<b>Inhabilidades por delitos sexuales contra menores (Ley 1918 de 2018):</b> la consulta "
            "se efectuó en el registro en línea que administra la Policía Nacional (DIJIN), creado por "
            "la Ley 1918 de 2018 y reglamentado por el Decreto 753 de 2019 EXACTAMENTE para verificación "
            "de aspirantes a cargos, oficios o profesiones por parte de entidades y empresas (la Ley 2375 "
            "de 2024 extendió su alcance). El resultado corresponde al veredicto del registro en la fecha "
            "de la consulta; una inhabilidad reportada exige revisión humana del antecedente y del alcance "
            "de la sanción antes de cualquier decisión, conforme a las Leyes 1581 de 2012 y 1712 de 2014."
        )
    if _corrio(sena):
        bloques_legal.append(
            "<b>Formación (SENA):</b> la información se obtuvo del portal público Certificado Digital del Servicio "
            "Nacional de Aprendizaje, consulta abierta por documento de identidad. El listado corresponde a los "
            "certificados de formación reportados como disponibles por el SENA en la fecha de consulta y NO "
            "constituye verificación de títulos ni credencial educacional de la persona evaluada."
        )
    if _corrio(sisconmp):
        bloques_legal.append(
            "<b>Capacitaciones en Mercancías Peligrosas (SISCONMP):</b> la información se obtuvo del portal "
            "público de consulta del Sistema de Información de Conductores que Transportan Mercancías Peligrosas "
            "del Ministerio de Transporte, consulta abierta por documento de identidad, correspondiente al "
            "registro de capacitaciones exigido por la Resolución 1223 de 2014. El listado y las vigencias "
            "corresponden a lo reportado por el Ministerio en la fecha de consulta y son de carácter informativo; "
            "la ausencia de capacitación vigente no constituye por sí sola inhabilidad para conducir, y su "
            "interpretación corresponde al proceso de verificación de cada solicitante."
        )
    if _corrio(ofac):
        bloques_legal.append(
            "<b>OFAC — Lista SDN:</b> la verificación se efectuó contra el dataset oficial de Specially "
            "Designated Nationals and Blocked Persons publicado por la Office of Foreign Assets Control del "
            "Departamento del Tesoro de los Estados Unidos. El resultado compara de manera exacta el número "
            "de identificación; una coincidencia requiere validación humana y análisis de identidad, programa "
            "y alcance, y no constituye por sí sola una decisión automática de rechazo."
        )
    if _corrio(onu_ue):
        bloques_legal.append(
            "<b>Listas internacionales de sanciones (ONU/UE):</b> la verificación se efectuó contra la Lista "
            "Consolidada del Comité de Sanciones del Consejo de Seguridad de las Naciones Unidas y el Consolidated "
            "Financial Sanctions File 1.1 publicado por la Comisión Europea — ambos datasets oficiales de "
            "distribución pública. El resultado compara de manera EXACTA el número de identificación contra "
            "documentos de identidad registrados en las listas (sin búsqueda difusa por nombre); una coincidencia "
            "requiere validación humana de identidad, nacionalidad y alcance del régimen de sanciones, y no "
            "constituye por sí sola una decisión automática de rechazo. La ausencia de coincidencia refleja las "
            "listas en su fecha de publicación."
        )
    if _corrio(rues):
        bloques_legal.append(
            "<b>Registro Mercantil (RUES):</b> la información se obtuvo del portal público de consulta "
            "ciudadana del Registro Único Empresarial y Social (Confecámaras), servicio de consulta abierta "
            "por NIT. Los datos —incluido el estado de la matrícula y la representación legal— corresponden "
            "a lo reportado por la cámara de comercio correspondiente en la fecha de consulta y son de "
            "carácter informativo; NO constituyen el Certificado de Existencia y Representación Legal ni "
            "certificación mercantil expedida por la cámara."
        )
    if _corrio(situacion_militar):
        bloques_legal.append(
            "<b>Situación militar (libreta militar):</b> la información se obtuvo del certificado público de "
            "estado de situación militar que expide en línea el Comando de Reclutamiento y Control Reservas "
            "del Ejército Nacional, consulta abierta por documento de identidad que el propio portal declara "
            "de carácter público y sin requerir autorización del titular (artículo 10 de la Ley 1581 de 2012), "
            "conforme a la Ley 1861 de 2017, el Decreto 977 de 2018 y la Ley 1184 de 2008. El estado "
            "corresponde a lo certificado por el Ejército en la fecha de consulta y es de carácter informativo; "
            "la certificación no constituye documento de identificación militar ni reemplaza la tarjeta militar."
        )
    if _corrio(proc):
        # Procuraduría siempre de última (pedido 2026-09-15), también aquí.
        bloques_legal.append(
            "<b>Ley 1238 de 2008:</b> habilita a entidades públicas y privadas a consultar el certificado de "
            "antecedentes disciplinarios de la Procuraduría General de la Nación de aspirantes a cargos o contratistas."
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

    # ── 8. Evidencias de consulta (pantallazos del portal por fuente) ──────
    # Patrón TusDatos: una página por fuente con la captura del viewport del
    # portal en el momento de la consulta. Solo fuentes de navegador; las de
    # API/dataset (OFAC, ONU/UE, RUES) no generan captura. Los bytes viven en
    # GCS privado y el doc guarda la referencia → la REGENERACIÓN los relee.
    evidencias = estudio.get("evidencias") or {}
    if evidencias:
        # Mismo orden canónico del informe (Procuraduría siempre de última).
        orden_evidencias = (
            "manifiestos_rndc", "contraloria", "delitos_sexuales", "policia",
            "runt", "simit", "sena", "sisconmp", "bdme", "bdme_nit", "rama_judicial",
            "situacion_militar", "procuraduria",
        )
        nombres_evidencia = {
            "manifiestos_rndc": "Manifiestos RNDC (365 días)",
            "contraloria": "Contraloría General — Antecedentes Fiscales",
            "delitos_sexuales": "Policía — Inhabilidades Ley 1918",
            "policia": "Policía Nacional — Antecedentes Judiciales",
            "runt": f"RUNT — Vehículo {estudio.get('placa') or ''}".rstrip(),
            "simit": f"SIMIT — Comparendos placa {estudio.get('placa') or ''}".rstrip(),
            "sena": "SENA — Certificados de formación",
            "sisconmp": "SISCONMP — Capacitaciones Mercancías Peligrosas",
            "situacion_militar": "Ejército Nacional — Certificado de situación militar",
            "bdme": "BDME — Persona por cédula",
            "bdme_nit": "BDME — Empresa por NIT",
            "rama_judicial": "Rama Judicial — Consulta Nacional de Procesos",
            "procuraduria": "Procuraduría General de la Nación",
        }
        cuento.append(PageBreak())
        cuento.append(Paragraph("Evidencias de consulta", estilo_h2))
        cuento.append(Paragraph(
            "Capturas del área visible del portal en el momento de la consulta (una por fuente), "
            "incluidas como soporte de auditoría de que la consulta se realizó contra la fuente "
            "oficial. Corresponden a lo mostrado por el portal en la fecha y hora indicadas; las "
            "fuentes consultadas por API o dataset oficial (OFAC, ONU/UE, RUES) no generan captura.",
            estilo_peq,
        ))
        for fuente_ev in [f for f in orden_evidencias if f in evidencias]:
            info_ev = evidencias[fuente_ev] or {}
            titulo_ev = nombres_evidencia.get(fuente_ev, fuente_ev)
            cuento.append(PageBreak())
            momento = _hora_colombia(info_ev.get("capturado_en")) or _hora_colombia(
                (fuentes.get(fuente_ev) or {}).get("consultado_en")
            )
            sha_corto = (info_ev.get("sha256") or "")[:16]
            # Ancla de destino del link del resumen (botón "ver evidencia").
            cuento.append(Paragraph(f'<a name="ev_{fuente_ev}"/>' + escape(titulo_ev), estilo_h2))
            cuento.append(Paragraph(
                f"Captura: {momento or '—'} · SHA-256: {sha_corto or '—'}",
                estilo_peq,
            ))
            cuento.append(Spacer(0, 3 * mm))
            datos_img = None
            try:
                from Funciones import storage_seguridad

                datos_img = storage_seguridad.descargar_blob(info_ev.get("gcs_ruta") or "")
            except Exception as exc:
                logger.error("Evidencia %s no se pudo descargar de GCS: %s", fuente_ev, exc)
            incrustada = False
            if datos_img:
                try:
                    lector = ImageReader(io.BytesIO(datos_img))
                    w_img, h_img = lector.getSize()
                    ancho_util = ANCHO - 2 * MARGEN
                    alto_util = ALTO - 30 * mm - 60 * mm  # marco menos título+metadatos
                    escala = min(ancho_util / w_img, alto_util / h_img)
                    # Image() exige str/file-like (un ImageReader lo rechaza en
                    # reportlab 4.x: "expected str, bytes or os.PathLike object")
                    cuento.append(Image(io.BytesIO(datos_img), width=w_img * escala, height=h_img * escala))
                    incrustada = True
                except Exception as exc:
                    logger.error("Evidencia %s no se pudo incrustar: %s", fuente_ev, exc)
            if not incrustada:
                cuento.append(Paragraph(
                    f"Evidencia de {escape(titulo_ev)} no disponible en este momento "
                    "(la captura no se pudo recuperar del almacenamiento privado).",
                    estilo_normal,
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
        filas.append([
            # La columna de fecha del portal trae 'aaaa/mm/dd hh:mm:ss': al
            # mostrarla se unifica la parte de la fecha a dd/mm/aaaa (la hora
            # del radicado viaja con ella, tal cual la reporta el portal).
            celda(_fecha_legible(v.get(c, "")) if "fecha" in c.lower() else v.get(c, ""))
            for c in columnas
        ])

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
        return "Registra anotaciones disciplinarias"
    return "Veredicto no concluyente"


def _texto_veredicto_delitos(delitos: dict) -> str:
    """Veredicto de la fuente delitos_sexuales para la fila resumen (Ley 1918)."""
    if delitos.get("estado") not in {"EXITO", "ADVERTENCIA"}:
        return _resumen_error(delitos)
    no_registra = delitos.get("no_registra")
    if no_registra is True:
        return "No registra inhabilidad (Ley 1918)"
    if no_registra is False:
        return "REGISTRA INHABILIDAD — revisión humana"
    return "Veredicto no concluyente — ver mensaje del portal"


def _texto_veredicto_contraloria(cgr: dict) -> str:
    """Veredicto de la fuente contraloria para la fila resumen (SIBOR)."""
    if cgr.get("estado") not in {"EXITO", "ADVERTENCIA"}:
        return _resumen_error(cgr)
    no_registra = cgr.get("no_registra")
    if no_registra is True:
        return "No reportado como responsable fiscal"
    if no_registra is False:
        return "Reportado como responsable fiscal — ver detalle"
    return "Veredicto no concluyente"


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
    rtm = runt.get("rtm") or {}
    if soat.get("vigente") is False:
        return "SOAT vencido — ver detalle"
    if rtm and rtm.get("vigente") is False:
        return f"RTM vencida ({_fecha_legible(rtm.get('fecha_vigencia'))}) — ver detalle"
    if soat and soat.get("vigente") is True:
        sufijo = f" · RTM vigente ({_fecha_legible(rtm.get('fecha_vigencia'))})" if rtm and rtm.get("vigente") is True else ""
        return f"SOAT vigente (vence {_fecha_legible(soat.get('fecha_fin_vigencia'))}){sufijo}"
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


def _texto_veredicto_sisconmp(sisconmp: dict) -> str:
    """Veredicto de la fuente sisconmp para la fila resumen: informativo del
    registro de capacitaciones MP; la VIGENCIA es el semáforo (análogo SOAT/
    RTM: vencidas sin ninguna vigente → ADVERTENCIA, decisión 2026-09-25)."""
    if sisconmp.get("estado") not in {"EXITO", "ADVERTENCIA"}:
        return _resumen_error(sisconmp)
    caps = sisconmp.get("capacitaciones") or []
    total = int(sisconmp.get("total_capacitaciones") or 0)
    if total == 0:
        return "Sin capacitaciones de Mercancías Peligrosas registradas"
    if any(c.get("vigente") is True for c in caps):
        return f"{total} capacitación(es) — al menos una vigente"
    if any(c.get("vigente") is False for c in caps):
        return f"{total} capacitación(es) — NINGUNA vigente (vencidas)"
    return f"{total} capacitación(es) — vigencia no reportada"


def _texto_veredicto_situacion_militar(sm: dict) -> str:
    """Veredicto de la fuente situacion_militar para la fila resumen:
    informativo del estado certificado; la situación SIN DEFINIR es la
    advertencia (decisión 2026-09-25: obligación militar vigente = riesgo
    operativo para conducción)."""
    if sm.get("estado") not in {"EXITO", "ADVERTENCIA"}:
        return _resumen_error(sm)
    if sm.get("no_registra"):
        return "Sin registro de situación militar con cédula"
    estado = (sm.get("estado_tarjeta_militar") or "").strip()
    if sm.get("estado") == "ADVERTENCIA":
        return f"Situación sin definir: {estado}" if estado else "Situación sin definir"
    return estado or "Ver detalle"


def _texto_veredicto_onu_ue(onu_ue: dict) -> str:
    """Veredicto de la fuente onu_ue para la fila resumen: igual que OFAC, una
    coincidencia exacta exige revisión humana (nunca rechazo automático)."""
    if onu_ue.get("estado") not in {"EXITO", "ADVERTENCIA"}:
        return _resumen_error(onu_ue)
    if onu_ue.get("aplica"):
        return f"Coincidencia exacta ({int(onu_ue.get('total_coincidencias') or 1)}) — revisar"
    no_disponibles = ", ".join(onu_ue.get("listas_no_disponibles") or [])
    veredicto = "Sin coincidencia exacta de identificación"
    if no_disponibles:
        veredicto += f" ({no_disponibles} no disponible)"
    return veredicto


def _texto_veredicto_rues(rues: dict) -> str:
    """Veredicto de la fuente rues para la fila resumen. El tri-estado es
    sobre el NIT consultado (registro mercantil), jamás un "limpio" de la
    empresa evaluada."""
    if rues.get("estado") not in {"EXITO", "ADVERTENCIA"}:
        return _resumen_error(rues)
    if rues.get("no_registra"):
        return "NIT sin registro en Registro Mercantil"
    estado_mat = (rues.get("estado_matricula") or "").strip().upper()
    if estado_mat == "ACTIVA":
        return "Matrícula mercantil ACTIVA"
    return f"Matrícula {estado_mat or 'sin estado'} — empresa no activa"


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
