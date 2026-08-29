# Funciones/bot_procuraduria.py
"""
Bot del certificado de antecedentes disciplinarios de la Procuraduría.

https://apps.procuraduria.gov.co/webcert/inicio.aspx?tpo=2

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
from playwright.async_api import async_playwright

# Cargar .env del proyecto para GEMINI_API_KEY cuando se ejecute standalone.
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

logger = logging.getLogger(__name__)

PORTAL_URL = "https://apps.procuraduria.gov.co/webcert/inicio.aspx?tpo=2"
SALIDA = Path(__file__).resolve().parents[1] / "descargas_procuraduria"
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.6-flash")

_LOCK = threading.Lock()
_TIMEOUT_MS = 60000


class BotProcuraduriaError(Exception):
    """Error del bot de antecedentes de la Procuraduría."""


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


def _resolver_captcha_gemini(pregunta: str) -> str:
    """Resuelve una pregunta de conocimiento general del captcha con Gemini.

    Igual que la lectura de documentos de En Ruta (vehiculos.py): temperature 0
    y respuesta de una sola palabra/cifra. Sin contexto de conversación.
    """
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise BotProcuraduriaError("Captcha de conocimiento general y no hay GEMINI_API_KEY configurada")
    cuerpo = {
        "contents": [{"role": "user", "parts": [{
            "text": (
                f"Responde esta pregunta de un formulario público colombiano con UNA sola "
                f"palabra o cifra, sin puntuación, sin explicación, en mayúsculas si es texto, "
                f"sin artículos (el/la). Pregunta: {pregunta}"
            )
        }]}],
        "generationConfig": {"temperature": 0, "maxOutputTokens": 512},
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
    return texto.split()[0].strip(".,;:")


async def consultar_antecedentes(cedula: str, headed: bool = False) -> Dict[str, Any]:
    """Consulta el certificado ORDINARIO de antecedentes de una cédula.

    El ordinario contiene las sanciones/inhabilidades VIGENTES (el que se
    exige en contratación). Retorna: cedula, texto_resultado, no_registra
    (bool | None), pdf_bytes (| None si no vino PDF) y pdf_ruta.
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

            await pagina.goto(PORTAL_URL, wait_until="domcontentloaded", timeout=_TIMEOUT_MS)
            await pagina.wait_for_timeout(2000)

            # 1) Leer y resolver el captcha (span #lblPregunta). Dos vías:
            # aritmética por regex (gratuita) o conocimiento general por Gemini.
            texto_captcha = (await pagina.inner_text("#lblPregunta")).strip()
            respuesta = _resolver_captcha_texto(texto_captcha)
            if respuesta is None:
                logger.info("[BOT PGN] Captcha de conocimiento: %r -> Gemini", texto_captcha)
                respuesta = _resolver_captcha_gemini(texto_captcha)
                logger.info("[BOT PGN] Respuesta Gemini: %r", respuesta)

            # 2) Llenar el formulario (CC = value 1 en el select del portal)
            await pagina.select_option("#ddlTipoID", "1")
            await pagina.fill("#txtNumID", cedula_norm)
            await pagina.check("#rblTipoCert_0")  # ordinario
            await pagina.fill("#txtRespuestaPregunta", respuesta)

            # 3) Enviar (postback WebForms). Armar la trampa de descarga antes.
            tarea_descarga = asyncio.create_task(_esperar_descarga())
            await pagina.click("#btnExportar")

            # Esperar: o la descarga del PDF o la página de resultados
            try:
                await pagina.wait_for_load_state("networkidle", timeout=_TIMEOUT_MS)
            except Exception:
                pass
            await pagina.wait_for_timeout(3000)

            # Darle chance a la descarga si va en curso
            if tarea_descarga:
                try:
                    await asyncio.wait_for(asyncio.shield(tarea_descarga), timeout=15)
                except asyncio.TimeoutError:
                    pass

            texto_resultado = " ".join((await pagina.inner_text("body")).split())

            # 4b) La página de resultados entrega el PDF con el botón de imagen
            # #btnDescargar (postback WebForms), no con un link <a>.
            if not pdf_bytes and "descargue su certificado" in texto_resultado.lower():
                boton_pdf = pagina.locator("#btnDescargar")
                if await boton_pdf.count():
                    async with pagina.expect_download(timeout=_TIMEOUT_MS) as info_dl:
                        await boton_pdf.click()
                    descarga = await info_dl.value
                    ruta_temp = await descarga.path()
                    pdf_bytes = ruta_temp.read_bytes()

            # 4) Si vino PDF, guardarlo
            pdf_ruta: Optional[str] = None
            if pdf_bytes:
                SALIDA.mkdir(exist_ok=True)
                pdf_ruta = str(SALIDA / f"certificado_{cedula_norm}.pdf")
                Path(pdf_ruta).write_bytes(pdf_bytes)

            # 5) Veredicto del texto visible (o del propio PDF si no hay página)
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
                elif pdf_bytes or "certificado" in fuente_texto.lower():
                    # PDF generado pero sin veredicto legible: se entrega crudo
                    mensaje = "Certificado generado; ver PDF"

            return {
                "cedula": cedula_norm,
                "no_registra": no_registra,
                "mensaje": mensaje,
                "texto_resultado": texto_resultado[:1500],
                "texto_pdf": texto_pdf[:2500],
                "pdf_bytes": pdf_bytes,
                "pdf_ruta": pdf_ruta,
                "html": await pagina.content(),
            }
        finally:
            await navegador.close()


def _texto_pdf(pdf_bytes: bytes) -> str:
    """Extrae texto del PDF del certificado (pdfplumber ya está en el proyecto)."""
    import io

    import pdfplumber

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        return "\n".join((p.extract_text() or "") for p in pdf.pages)


def consultar_antecedentes_sync(cedula: str) -> Dict[str, Any]:
    """Versión síncrona para asyncio.to_thread, igual que bot_rndc2."""
    with _LOCK:
        return asyncio.run(consultar_antecedentes(cedula))


if __name__ == "__main__":
    import json
    import sys

    cedula = sys.argv[1] if len(sys.argv) > 1 else "1033688842"
    r = consultar_antecedentes_sync(cedula)
    html = r.pop("html", "")
    r["pdf_bytes"] = f"<{len(r['pdf_bytes'])} bytes>" if r.get("pdf_bytes") else None
    (Path(__file__).resolve().parents[1] / "descargas_procuraduria" / "resultado.html").write_text(html, encoding="utf-8")
    print(json.dumps(r, ensure_ascii=False, indent=1))
