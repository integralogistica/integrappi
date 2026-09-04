# Funciones/bot_procuraduria.py
"""
Bot del certificado de antecedentes disciplinarios de la Procuraduría.

https://www.procuraduria.gov.co/Pages/consulta-de-antecedentes.aspx

La Ley 1238 de 2008 dispone que entidades públicas O PRIVADAS consulten este
certificado de aspirantes a cargos/contratos (diseñado para verificación de
terceros, a diferencia del portal de la Policía que es de autoconsulta).
Captcha: operación aritmética en TEXTO PLANO (mismo patrón que bot_rndc2).

Flujo: cargar -> leer pregunta (#lblPregunta) -> resolver (aritmética con
regex; conocimiento general con Gemini vía GEMINI_API_KEY) -> tipo CC +
número -> certificado ordinario -> Generar -> el certificado llega como PDF
(se captura como descarga) o página de resultados; se extrae el veredicto.
"""
import asyncio
import logging
import os
import re
import threading
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import async_playwright

# Cargar .env del proyecto para GEMINI_API_KEY cuando se ejecute standalone.
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

logger = logging.getLogger(__name__)

PORTAL_URL = "https://www.procuraduria.gov.co/Pages/consulta-de-antecedentes.aspx"
# ?nocache=1 (hallazgo del usuario 2026-09-02): evita que el CDN/portal sirva
# una versión cacheada del iframe del formulario (a veces venía sin estado de
# sesión y el postback no respondía).
PORTAL_URL_CONSULTA = PORTAL_URL + "?nocache=1"
FORMULARIO_URL = "https://apps.procuraduria.gov.co/webcert/inicio.aspx"
IFRAME_SELECTOR = 'iframe[src*="apps.procuraduria.gov.co/webcert/inicio.aspx"]'
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.6-flash")

_LOCK = threading.Lock()
_TIMEOUT_MS = int(os.getenv("SEGURIDAD_PROCURADURIA_TIMEOUT_MS", "90000"))


class BotProcuraduriaError(Exception):
    """Error del bot de antecedentes de la Procuraduría."""


class BotProcuraduriaSinResultado(Exception):
    """El portal no entregó la página del certificado (postback lento o caído).

    Lanzar (en vez de retornar vacío) dispara el REINTENTO del orquestador y
    NO se cachea nada (fix 2026-08-30: antes un resultado vacío silencioso era
    ADVERTENCIA 'no concluyente' cacheada 24 h)."""


def _resolver_captcha_texto(texto: str) -> Optional[str]:
    m = re.search(r"(\d+)\s*([+\-*x×])\s*(\d+)", texto or "")
    if not m:
        return None
    a, op, b = int(m.group(1)), m.group(2), int(m.group(3))
    if op == "+":
        return str(a + b)
    if op == "-":
        return str(a - b)
    return str(a * b)


_TILDES = str.maketrans("áéíóúüÁÉÍÓÚÜ", "aeiouuAEIOUU")


def _resolver_captcha_documento(
    pregunta: str, cedula: str, nombres: str | None = None, apellidos: str | None = None,
) -> Optional[str]:
    """Preguntas cuya respuesta deriva de DATOS QUE EL CONSULTANTE CONOCE —
    el portal las verifica contra el formulario:

    - DEL DOCUMENTO (hallazgo 2026-09-01): "escriba los dos últimos dígitos
      del documento a consultar" etc. → la cédula que el bot ya diligenció
      (antes iba a parar a Gemini SIN la cédula → inventaba → fallo).
    - DEL NOMBRE (estrategia 2026-09-01): "¿cuál es el primer nombre de la
      persona que está consultando?" etc. → el portal cliente ahora PIDE
      nombres y apellidos cuando el plan incluye procuraduria.

    Determinista y gratuita: sin red. Solo patrones inequívocos (nunca
    adivinar los que no entren aquí; p.ej. "segundo nombre" de quien solo
    tiene uno → None → Gemini/reintento)."""
    q = (pregunta or "").translate(_TILDES).lower()
    doc = re.sub(r"\D", "", cedula or "")
    if doc and any(k in q for k in ("documento", "cedula", "identificacion")):
        if "dos ultimos digitos" in q or "ultimos dos digitos" in q:
            return doc[-2:]
        if "tres ultimos digitos" in q or "ultimos tres digitos" in q:
            return doc[-3:]
        if "primer digito" in q or "primer numero" in q:
            return doc[0]
        if "cuantos digitos" in q or "cuantos numeros" in q or "cuantas cifras" in q:
            return str(len(doc))
    # Preguntas de NOMBRE: respuesta = el token correspondiente de los
    # nombres/apellidos que diligenció el consultante (SIN tildes, ya
    # normalizados por el endpoint). Variante vista 2026-09-02: "las dos
    # primeras letras del primer nombre".
    if "nombre" in q or "apellido" in q:
        tokens_n = (nombres or "").split()
        tokens_a = (apellidos or "").split()
        if "primer nombre" in q and tokens_n:
            if "dos primeras letras" in q:
                return tokens_n[0][:2]
            if "tres primeras letras" in q:
                return tokens_n[0][:3]
            return tokens_n[0]
        if "segundo nombre" in q and len(tokens_n) > 1:
            return tokens_n[1]
        if "primer apellido" in q and tokens_a:
            if "dos primeras letras" in q:
                return tokens_a[0][:2]
            if "tres primeras letras" in q:
                return tokens_a[0][:3]
            return tokens_a[0]
        if "segundo apellido" in q and len(tokens_a) > 1:
            return tokens_a[1]
    return None


def _resolver_captcha_gemini(
    pregunta: str, cedula: str | None = None,
    nombres: str | None = None, apellidos: str | None = None,
) -> str:
    """Resuelve una pregunta de conocimiento general del captcha con Gemini.

    Igual que la lectura de documentos de En Ruta (vehiculos.py): temperature 0
    y respuesta de una sola palabra/cifra. Sin contexto de conversación.
    `cedula`/`nombres`/`apellidos` (2026-09-01) le dan el CONTEXTO del
    formulario para las variantes derivadas de él.
    maxOutputTokens 8192: en Gemini 3.x el thinking CUENTA en el presupuesto
    (con 512 devuelve vacío — bug ya visto en En Ruta).
    """
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise BotProcuraduriaError("Captcha de conocimiento general y no hay GEMINI_API_KEY configurada")
    contexto = ""
    if cedula or nombres or apellidos:
        partes = []
        if cedula:
            partes.append(f"el número de documento consultado es {cedula}")
        if nombres:
            partes.append(f"los nombres de la persona consultada son {nombres}")
        if apellidos:
            partes.append(f"los apellidos son {apellidos}")
        contexto = f"Contexto del formulario: {'; '.join(partes)}. Si la pregunta se responde con ese contexto, úsalo. "
    cuerpo = {
        "contents": [{"role": "user", "parts": [{
            "text": (
                f"{contexto}"
                f"Responde esta pregunta de un formulario público colombiano con UNA sola "
                f"palabra o cifra, sin puntuación, sin explicación, en mayúsculas si es texto, "
                f"sin artículos (el/la). Pregunta: {pregunta}"
            )
        }]}],
        "generationConfig": {"temperature": 0, "maxOutputTokens": 8192},
    }
    try:
        r = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={api_key}",
            json=cuerpo,
            timeout=30,
        )
    except requests.RequestException as exc:
        raise BotProcuraduriaError(f"Gemini no respondió para el captcha: {exc}") from exc
    if r.status_code != 200:
        raise BotProcuraduriaError(f"Gemini error {r.status_code} en captcha")
    candidatos = r.json().get("candidates", [])
    if not candidatos:
        raise BotProcuraduriaError("Gemini no devolvió respuesta para el captcha")
    texto = "".join(p.get("text", "") for p in candidatos[0].get("content", {}).get("parts", [])).strip()
    if not texto:
        raise BotProcuraduriaError("Gemini devolvió vacío para el captcha")
    # primera palabra, limpia
    # El portal puede pedir explícitamente "sin tilde". Normalizar siempre es
    # seguro para estas respuestas cortas y evita rechazos como BOGOTÁ/BOGOTA.
    return texto.split()[0].strip(".,;:").translate(_TILDES)


async def consultar_antecedentes(
    cedula: str, headed: bool = False,
    nombres: str | None = None, apellidos: str | None = None,
) -> Dict[str, Any]:
    """Consulta el certificado ORDINARIO de antecedentes de una cédula.

    El ordinario contiene las sanciones/inhabilidades VIGENTES (el que se
    exige en contratación). Retorna: cedula, texto_resultado, no_registra
    (bool), mensaje y texto_resultado. El portal puede entregar internamente
    un PDF, pero solo se usa en memoria para leer el veredicto y se descarta.

    `nombres`/`apellidos` (2026-09-01, SIN tildes y en mayúsculas — el
    endpoint los normaliza): pista para las preguntas del captcha sobre el
    nombre de la persona consultada ("¿cuál es el primer nombre…?"). El
    portal cliente los pide cuando el plan incluye procuraduria; sin ellos
    esas variantes caen a Gemini/reintento.
    """
    cedula_norm = re.sub(r"\D", "", cedula or "")
    if not 3 <= len(cedula_norm) <= 15:
        raise BotProcuraduriaError("Cédula inválida")

    async with async_playwright() as p:
        navegador = await p.chromium.launch(headless=not headed)
        try:
            contexto = await navegador.new_context(
                viewport={"width": 1366, "height": 900},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
                locale="es-CO",
                ignore_https_errors=True,
                accept_downloads=True,
            )
            pagina = await contexto.new_page()

            # El certificado puede llegar como descarga PDF: atraparla si ocurre
            pdf_bytes: Optional[bytes] = None
            tarea_descarga: Optional[asyncio.Task] = None

            async def _esperar_descarga():
                nonlocal pdf_bytes
                try:
                    async with pagina.expect_download(timeout=_TIMEOUT_MS) as info:
                        pass  # se dispara cuando el portal responde con PDF
                    descarga = await info.value
                    pdf_bytes = await (await descarga.path()).read_bytes()
                except Exception:
                    pass  # no hubo descarga: el resultado viene como página

            try:
                # Entrar por la página oficial. El formulario se sirve dentro de
                # un iframe de apps.procuraduria.gov.co; navegar directamente a
                # ese host era justamente lo que producía los timeouts reportados.
                # ?nocache=1 (2026-09-02): evita la copia cacheada del portal.
                await pagina.goto(PORTAL_URL_CONSULTA, wait_until="domcontentloaded", timeout=_TIMEOUT_MS)
                try:
                    elemento_iframe = await pagina.wait_for_selector(
                        IFRAME_SELECTOR, state="attached", timeout=20000,
                    )
                    vista = await elemento_iframe.content_frame()
                    if vista is None:
                        raise PlaywrightError("iframe sin contenido")
                    await vista.wait_for_selector("#lblPregunta", timeout=20000)
                except PlaywrightError:
                    # El portal institucional a veces omite el iframe para
                    # navegadores automatizados. Usar entonces su URL oficial
                    # exacta, sin el antiguo parámetro tpo=2.
                    await pagina.goto(
                        FORMULARIO_URL, wait_until="domcontentloaded", timeout=_TIMEOUT_MS,
                    )
                    vista = pagina
                    await vista.wait_for_selector("#lblPregunta", timeout=_TIMEOUT_MS)
            except PlaywrightError as exc:
                # ERR_CONNECTION_TIMED_OUT/RESET y timeouts de navegación son
                # indisponibilidad del portal, no un error funcional del bot.
                # El orquestador reintenta y, si persiste, lo presenta como
                # NO_DISPONIBLE sin envenenar la caché.
                raise BotProcuraduriaSinResultado(
                    f"No fue posible conectar con el portal de la Procuraduría: {exc}"
                ) from exc
            await pagina.wait_for_timeout(2000)

            # 1) Leer y resolver el captcha (span #lblPregunta). Tres vías:
            # aritmética por regex (gratuita), derivada del DOCUMENTO o del
            # NOMBRE (deterministas: la cédula ya diligenciada y los
            # nombres/apellidos que informó el consultante) o conocimiento
            # general por Gemini (con ese mismo contexto).
            texto_captcha = (await vista.inner_text("#lblPregunta")).strip()
            respuesta = _resolver_captcha_texto(texto_captcha)
            if respuesta is None:
                respuesta = _resolver_captcha_documento(texto_captcha, cedula_norm, nombres, apellidos)
                if respuesta is not None:
                    logger.info("[BOT PGN] Captcha de documento/nombre (determinista): %r -> %r", texto_captcha, respuesta)
            if respuesta is None:
                logger.info("[BOT PGN] Captcha de conocimiento: %r -> Gemini", texto_captcha)
                respuesta = _resolver_captcha_gemini(texto_captcha, cedula_norm, nombres, apellidos)
                logger.info("[BOT PGN] Respuesta Gemini: %r", respuesta)

            # 2) Llenar el formulario (CC = value 1 en el select del portal)
            await vista.select_option("#ddlTipoID", "1")
            await vista.fill("#txtNumID", cedula_norm)
            # La versión antigua (tpo=2) pedía escoger certificado
            # ordinario; la consulta oficial actual (tpo=1) ya no muestra ese
            # radio y genera directamente el certificado correspondiente.
            radio_ordinario = vista.locator("#rblTipoCert_0")
            if await radio_ordinario.count() and await radio_ordinario.is_visible():
                await radio_ordinario.check()
            await vista.fill("#txtRespuestaPregunta", respuesta)

            # 3) Enviar (postback WebForms). Armar la trampa de descarga antes.
            tarea_descarga = asyncio.create_task(_esperar_descarga())
            boton_consulta = vista.locator("#btnConsultar, #btnExportar").first
            if not await boton_consulta.count():
                raise BotProcuraduriaSinResultado(
                    "El portal no mostró el botón para generar el certificado"
                )
            await boton_consulta.click()

            # Esperar la página de resultados. El postback es LENTO e
            # intermitente (visto tardar >45 s, con spinner "Consultando por
            # favor espere..."). Desde el rediseño del portal (2026-09-02) el
            # veredicto llega INLINE en .datosConsultado y #btnDescargar ya no
            # existe: esperar ambos marcadores (el que llegue primero) en vez
            # de un networkidle+3s que se rinde antes de que termine el postback.
            # 180 s (2026-09-04): el orquestador le da a procuraduría 300 s de
            # presupuesto — antes se rendía a los 60 s y leía la página antes
            # de que terminara el postback (SinResultado prematuro), gastando
            # el intento sin aprovechar el presupuesto. Navegación + captcha +
            # 180 s de postback siguen cabiendo en los 300 s.
            try:
                await vista.wait_for_selector(
                    ".datosConsultado, #btnDescargar", timeout=180000, state="attached",
                )
            except Exception:
                pass  # puede ser error de captcha: leer la página igual
            try:
                # El botón ya cargó (o falló): networkidle corto, solo asentar
                await vista.wait_for_load_state("networkidle", timeout=20000)
            except Exception:
                pass
            await pagina.wait_for_timeout(3000)

            # Darle chance a la descarga si va en curso
            if tarea_descarga:
                try:
                    await asyncio.wait_for(asyncio.shield(tarea_descarga), timeout=15)
                except asyncio.TimeoutError:
                    pass

            texto_resultado = " ".join((await vista.inner_text("body")).split())

            # Nombre del consultado desde la sección "Datos del ciudadano"
            # (portal rediseñado 2026-09-02): el texto plano CONCATENA los
            # <span> del nombre ("JHOAMORLANDOAMAYATOVAR"), así que se leen los
            # spans del DOM y se unen con espacio. Análogo a nombre_consultado
            # del bot de Policía.
            nombre_consultado = ""
            try:
                spans = await vista.locator(".datosConsultado span").all_inner_texts()
                nombre_consultado = " ".join(" ".join(s.split()) for s in spans if s.strip())
            except Exception:
                pass

            # 4b) La página de resultados entrega el PDF con el botón de imagen
            # #btnDescargar (postback WebForms), no con un link <a>.
            if not pdf_bytes and "descargue su certificado" in texto_resultado.lower():
                boton_pdf = vista.locator("#btnDescargar")
                if await boton_pdf.count():
                    async with pagina.expect_download(timeout=_TIMEOUT_MS) as info_dl:
                        await boton_pdf.click()
                    descarga = await info_dl.value
                    ruta_temp = await descarga.path()
                    pdf_bytes = ruta_temp.read_bytes()

            # 4) Veredicto del texto visible (o del PDF en memoria si la PGN
            # solo lo comunicó allí). El certificado no se guarda ni expone.
            no_registra = None
            mensaje = ""
            fuente_texto = texto_resultado
            texto_pdf = ""
            if pdf_bytes:
                try:
                    texto_pdf = _texto_pdf(pdf_bytes)
                    fuente_texto = texto_resultado + " " + texto_pdf
                except Exception:
                    pass
            # Veredicto del PDF: la fórmula oficial es "NO REGISTRA SANCIONES NI
            # INHABILIDADES VIGENTES" — chequear el NO primero (el regex sin él
            # capturaba a mitad de la frase y daba el veredicto invertido).
            m = re.search(r"(NO\s+REGISTRA\s+SANCIONES[^.]{0,80})", fuente_texto, re.IGNORECASE)
            if m:
                no_registra = True
                mensaje = m.group(1).strip()
            else:
                m2 = re.search(r"certifica[^.]{0,400}?\b(?:REGISTRA|registra)\b\s+(?:sanciones|anotaciones|inhabilidades)[^.]{0,120}", fuente_texto, re.IGNORECASE)
                if m2:
                    no_registra = False
                    mensaje = m2.group(0)[-160:].strip()
                else:
                    # Portal rediseñado (hallazgo 2026-09-02): el veredicto llega
                    # INLINE en la misma página (sección "Datos del ciudadano"),
                    # ya no hay #btnDescargar ni PDF que descargar. Chequear el
                    # NO primero: "no presenta antecedentes" contiene "presenta
                    # antecedentes" (mismo orden del fix de la fórmula oficial).
                    m3 = re.search(r"((?:el\s+)?ciudadano\s+no\s+presenta\s+antecedentes)", fuente_texto, re.IGNORECASE)
                    if m3:
                        no_registra = True
                        mensaje = "El ciudadano no presenta antecedentes"
                    else:
                        m4 = re.search(r"ciudadano\s+presenta\s+antecedentes[^.<]{0,200}", fuente_texto, re.IGNORECASE)
                        if m4:
                            no_registra = False
                            mensaje = m4.group(0).strip()[:160]
                        elif pdf_bytes:
                            mensaje = "La Procuraduría respondió, pero no fue posible interpretar el veredicto"

            # La consulta solo es útil si produjo un veredicto. Un archivo sin
            # texto interpretable tampoco cuenta como éxito.
            if no_registra is None:
                raise BotProcuraduriaSinResultado(
                    "El portal de la Procuraduría no entregó un veredicto "
                    f"(postback sin respuesta). Texto visible: {texto_resultado[:120]!r}"
                )

            return {
                "cedula": cedula_norm,
                "no_registra": no_registra,
                "mensaje": mensaje,
                "nombre_consultado": nombre_consultado,
                "texto_resultado": texto_resultado[:1500],
                "texto_pdf": texto_pdf[:2500],
                "html": await vista.content(),
            }
        finally:
            await navegador.close()


def _texto_pdf(pdf_bytes: bytes) -> str:
    """Extrae texto del PDF del certificado (pdfplumber ya está en el proyecto)."""
    import io

    import pdfplumber

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        return "\n".join((p.extract_text() or "") for p in pdf.pages)


def consultar_antecedentes_sync(
    cedula: str, nombres: str | None = None, apellidos: str | None = None,
) -> Dict[str, Any]:
    """Versión síncrona para asyncio.to_thread, igual que bot_rndc2."""
    with _LOCK:
        return asyncio.run(consultar_antecedentes(cedula, nombres=nombres, apellidos=apellidos))


if __name__ == "__main__":
    import json
    import sys

    cedula = sys.argv[1] if len(sys.argv) > 1 else "1033688842"
    nombres = sys.argv[2] if len(sys.argv) > 2 else None
    apellidos = sys.argv[3] if len(sys.argv) > 3 else None
    r = consultar_antecedentes_sync(cedula, nombres=nombres, apellidos=apellidos)
    html = r.pop("html", "")
    try:
        salida = Path(__file__).resolve().parents[1] / "descargas_procuraduria"
        salida.mkdir(exist_ok=True)
        (salida / "resultado.html").write_text(html, encoding="utf-8")
    except Exception as exc:  # dump de debug: jamás tumbar el CLI
        print(f"(dump no escrito: {exc})")
    print(json.dumps(r, ensure_ascii=False, indent=1))
