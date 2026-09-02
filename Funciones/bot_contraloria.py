# Funciones/bot_contraloria.py
"""
Bot del Certificado de Antecedentes Fiscales — Persona Natural (CGR).

https://www.contraloria.gov.co/web/guest/persona-natural

La consulta de antecedentes fiscales es pública y gratuita; el certificado es
el requisito habitual de contratación (análogo disciplinario: Procuraduría).

Portal descubierto con scripts/probar_contraloria.py (2026-09-02):
  - El formulario vive en un IFRAME WebForms de cfiscal.contraloria.gov.co
    (CertificadoPersonaNatural.aspx): #ddlTipoDocumento (value "CC" = Cédula
    de Ciudadanía), #txtNumeroDocumento, submit #btnBuscar.
  - Captcha: reCAPTCHA v2 resuelto por 2Captcha (method=userrecaptcha,
    ~US$0.003; sitekey leído del iframe de Google con fallback por rotación).
    El token se INYECTA en el textarea g-recaptcha-response del iframe (el
    servidor valida el campo POST, no el checkbox visual).
  - TRAMPA 1: el validationGroup reqCertificados exige, además del form, TRES
    radios de una ENCUESTA oculta (#jumbotronEncuesta: rbdExpectativa,
    rbdOportunidad, rbdUtilidad) — sin marcarlos WebForms bloquea el postback
    sin mensaje visible. Se marcan por JS.
  - TRAMPA 2: el submit es un postback PARCIAL async (ScriptManager): no hay
    navegación ni evento load; el certificado llega como DESCARGA PDF
    ({cedula}.pdf) — se atrapa con expect_download.
  - El veredicto vive en el TEXTO del PDF (pdfplumber en memoria); el archivo
    no se persiste ni expone.

Flujo: portal → iframe → CC + cédula → solve captcha → inyectar token +
marcar encuesta → Buscar → descargar PDF → extraer veredicto.
"""
import asyncio
import logging
import os
import re
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv
from playwright.async_api import async_playwright

# Cargar .env del proyecto para la key del captcha cuando se ejecute standalone.
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

logger = logging.getLogger(__name__)

PORTAL_URL = "https://www.contraloria.gov.co/web/guest/persona-natural"
IFRAME_HOST = "cfiscal.contraloria.gov.co"
SALIDA = Path(__file__).resolve().parents[1] / "descargas_contraloria"

SITEKEY_FALLBACK = "6LcfnjwUAAAAAIyl8ehhox7ZYqLQSVl_w1dmYIle"

# Bloqueo para serializar consultas al portal (una a la vez, como los demás bots).
_LOCK = threading.Lock()

_TIMEOUT_MS = int(os.getenv("SEGURIDAD_CONTRALORIA_TIMEOUT_MS", "90000"))
_CAPTCHA_BASE = os.getenv("SEGURIDAD_CONTRALORIA_CAPTCHA_URL", "https://2captcha.com").rstrip("/")
# API_KEY_CAPTCHA es la key ya usada por los demás bots del proyecto.
_CAPTCHA_KEY = os.getenv("SEGURIDAD_CONTRALORIA_CAPTCHA_KEY", "").strip() or os.getenv("API_KEY_CAPTCHA", "").strip()
_CAPTCHA_TIMEOUT_S = float(os.getenv("SEGURIDAD_CONTRALORIA_CAPTCHA_TIMEOUT_S", "90"))
_CAPTCHA_POOL_S = 5.0

# Veredictos del certificado (texto del PDF, fórmula oficial del SIBOR). El
# "no reportado" es el caso limpio; chequearlo PRIMERO porque las fórmulas se
# contienen entre sí (mismo orden del fix de Procuraduría).
_RE_NO_REPORTA = re.compile(r"NO\s+SE\s+ENCUENTRA\s+REPORTADO\s+COMO\s+RESPONSABLE\s+FISCAL", re.IGNORECASE)
_RE_REPORTA = re.compile(r"SE\s+ENCUENTRA\s+REPORTADO\s+COMO\s+RESPONSABLE\s+FISCAL[^.]{0,200}", re.IGNORECASE)
_RE_CODIGO_VERIFICACION = re.compile(r"C[oó]digo\s+de\s+Verificaci[oó]n\s+(\d{6,})", re.IGNORECASE)


class BotContraloriaError(Exception):
    """Error del bot de antecedentes fiscales de la Contraloría."""


class BotContraloriaSinCaptchaKey(BotContraloriaError):
    """Falta configurar la key del resolvedor (fallo de config, accionable)."""


class BotContraloriaCaptchaFallido(BotContraloriaError):
    """El resolvedor rechazó el pedido o el portal rechazó el token."""


class BotContraloriaSinResultado(BotContraloriaError):
    """El portal no entregó el certificado (postback sin descarga)."""


def _resolver_recaptcha(sitekey: str, url_pagina: str) -> str:
    """Resuelve el reCAPTCHA v2 vía servicio 2Captcha-compatible (como Policía).

    Síncrona (requests): se llama desde la corutina con asyncio.to_thread para
    no bloquear el loop mientras se hace el polling de res.php.
    """
    if not _CAPTCHA_KEY:
        raise BotContraloriaSinCaptchaKey("Falta configurar SEGURIDAD_CONTRALORIA_CAPTCHA_KEY para la fuente contraloria")
    try:
        r = requests.get(f"{_CAPTCHA_BASE}/in.php", params={
            "key": _CAPTCHA_KEY, "method": "userrecaptcha",
            "googlekey": sitekey, "pageurl": url_pagina, "json": 1,
        }, timeout=30)
        dato = r.json()
    except requests.RequestException as exc:
        raise BotContraloriaCaptchaFallido(f"El resolvedor de captcha no respondió: {exc}") from exc
    if dato.get("status") != 1:
        raise BotContraloriaCaptchaFallido(f"El resolvedor rechazó el pedido: {dato.get('request')}")
    captcha_id = str(dato.get("request"))

    logger.info("[BOT CGR] captcha pedido %s; sondeando cada %.0f s", captcha_id, _CAPTCHA_POOL_S)
    fin = time.monotonic() + _CAPTCHA_TIMEOUT_S
    while time.monotonic() < fin:
        time.sleep(_CAPTCHA_POOL_S)
        try:
            r2 = requests.get(f"{_CAPTCHA_BASE}/res.php", params={
                "key": _CAPTCHA_KEY, "action": "get", "id": captcha_id, "json": 1,
            }, timeout=30)
            dato2 = r2.json()
        except requests.RequestException as exc:
            raise BotContraloriaCaptchaFallido(f"El resolvedor de captcha no respondió: {exc}") from exc
        if dato2.get("status") == 1:
            return str(dato2["request"])
        if dato2.get("request") != "CAPCHA_NOT_READY":
            raise BotContraloriaCaptchaFallido(f"El resolvedor reportó: {dato2.get('request')}")
    raise BotContraloriaCaptchaFallido(f"El resolvedor no resolvió el captcha en {_CAPTCHA_TIMEOUT_S:.0f} s")


def _texto_pdf(pdf_bytes: bytes) -> str:
    """Extrae texto del PDF del certificado (pdfplumber ya está en el proyecto)."""
    import io

    import pdfplumber

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        return "\n".join((p.extract_text() or "") for p in pdf.pages)


async def consultar_antecedentes_fiscales(cedula: str, headed: bool = False) -> Dict[str, Any]:
    """Consulta el certificado de antecedentes fiscales de una cédula (tipo CC).

    Retorna: cedula, no_registra (bool | None), mensaje (fórmula del
    certificado ≤300), texto_resultado (página), texto_pdf, pdf_bytes (en
    memoria, no se persiste) y html. `no_registra=None` con PDF = la CGR
    respondió pero el veredicto no fue interpretable (ADVERTENCIA).
    """
    cedula_norm = re.sub(r"\D", "", cedula or "")
    if not 3 <= len(cedula_norm) <= 15:
        raise BotContraloriaError("Cédula inválida")

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

            # 1) Portal institucional (Liferay) → el form vive en el iframe de
            #    cfiscal.contraloria.gov.co. Entrar por la página oficial.
            await pagina.goto(PORTAL_URL, wait_until="domcontentloaded", timeout=_TIMEOUT_MS)
            vista = None
            for _ in range(10):
                for marco in pagina.frames:
                    if IFRAME_HOST in (marco.url or ""):
                        vista = marco
                        break
                if vista:
                    break
                await pagina.wait_for_timeout(1000)
            if vista is None:
                raise BotContraloriaSinResultado(
                    f"El portal de la Contraloría no mostró el formulario (iframe {IFRAME_HOST} ausente)"
                )
            await vista.wait_for_selector("#btnBuscar", timeout=30000)
            await pagina.wait_for_timeout(1500)

            # 2) Formulario: CC + cédula.
            await vista.select_option("#ddlTipoDocumento", "CC")
            await vista.fill("#txtNumeroDocumento", cedula_norm)

            # 3) Sitekey: leerlo del iframe de reCAPTCHA (cubre rotación).
            sitekey = ""
            for marco in pagina.frames:
                m = re.search(r"[?&]k=([0-9A-Za-z_-]{20,})", marco.url or "")
                if m and "recaptcha" in (marco.url or ""):
                    sitekey = m.group(1)
                    break
            if not sitekey:
                sitekey = SITEKEY_FALLBACK
                logger.warning("[BOT CGR] sitekey no leído del DOM; usando fallback")

            # 4) Captcha: solve en un hilo (no bloquea el loop).
            token = await asyncio.wait_for(
                asyncio.to_thread(_resolver_recaptcha, sitekey, vista.url),
                timeout=_CAPTCHA_TIMEOUT_S + 15,
            )

            # 5) Inyectar el token en el textarea oculto + marcar la ENCUESTA
            #    obligatoria del validationGroup (sin ella WebForms bloquea el
            #    postback SILENTEMENTE — hallazgo de la sonda 2026-09-02).
            await vista.evaluate(
                """(tok) => {
                    const ta = document.getElementById('g-recaptcha-response');
                    if (ta) { ta.value = tok; ta.style.display = 'block'; }
                    const marcar = (id) => {
                        const r = document.getElementById(id);
                        if (r) {
                            r.checked = true;
                            r.dispatchEvent(new Event('change', {bubbles: true}));
                        }
                    };
                    marcar('rbdExpectativa_0');
                    marcar('rbdOportunidad_0');
                    marcar('rbdUtilidad_0');
                }""", token,
            )

            # 6) Buscar: postback PARCIAL async (sin navegación) cuya respuesta
            #    es la DESCARGA del PDF. Trampa de descarga alrededor del click.
            pdf_bytes: Optional[bytes] = None
            async with pagina.expect_download(timeout=_TIMEOUT_MS) as info:
                await vista.locator("#btnBuscar").click()
            descarga = await info.value
            ruta_temp = await descarga.path()
            if ruta_temp:
                pdf_bytes = ruta_temp.read_bytes()

            # Dump de debug (jamás tumba la consulta).
            html = await vista.content()
            try:
                SALIDA.mkdir(exist_ok=True)
                if pdf_bytes:
                    (SALIDA / f"certificado_{cedula_norm}.pdf").write_bytes(pdf_bytes)
                (SALIDA / f"resultado_{cedula_norm}.html").write_text(html, encoding="utf-8")
            except Exception as exc:
                logger.warning("[BOT CGR] dump de debug no se pudo escribir: %s", exc)

            # 7) Veredicto del texto del PDF (en memoria). El texto visible de
            #    la página no cambia: el certificado SOLO llega como descarga.
            texto_resultado = " ".join((await vista.inner_text("body")).split())
            texto_pdf = ""
            if pdf_bytes:
                try:
                    texto_pdf = _texto_pdf(pdf_bytes)
                except Exception as exc:
                    logger.warning("[BOT CGR] PDF sin texto interpretable: %s", exc)

            no_registra: Optional[bool] = None
            mensaje = ""
            fuente_texto = f"{texto_resultado} {texto_pdf}"
            m = _RE_NO_REPORTA.search(fuente_texto)
            if m:
                no_registra = True
                mensaje = "No se encuentra reportado como responsable fiscal (SIBOR)"
            else:
                m2 = _RE_REPORTA.search(fuente_texto)
                if m2:
                    no_registra = False
                    mensaje = " ".join(m2.group(0).split())[:200]
                elif pdf_bytes:
                    mensaje = "La Contraloría respondió, pero no fue posible interpretar el veredicto"
            codigo_verificacion = ""
            m3 = _RE_CODIGO_VERIFICACION.search(texto_pdf)
            if m3:
                codigo_verificacion = m3.group(1)

            # Anti-envenenamiento: sin PDF y sin veredicto no hay consulta
            # válida (posible captcha rechazado o cambio del portal).
            if no_registra is None and not pdf_bytes:
                raise BotContraloriaSinResultado(
                    "El portal de la Contraloría no entregó el certificado "
                    f"(postback sin descarga). Texto visible: {texto_resultado[:120]!r}"
                )

            return {
                "cedula": cedula_norm,
                "no_registra": no_registra,
                "mensaje": mensaje[:300],
                "codigo_verificacion": codigo_verificacion,
                "texto_resultado": texto_resultado[:1500],
                "texto_pdf": texto_pdf[:2500],
                "pdf_bytes": pdf_bytes,
                "html": html,
            }
        finally:
            await navegador.close()


def consultar_antecedentes_fiscales_sync(cedula: str) -> Dict[str, Any]:
    """Versión síncrona para asyncio.to_thread, igual que los demás bots."""
    with _LOCK:
        return asyncio.run(consultar_antecedentes_fiscales(cedula))


if __name__ == "__main__":
    import json
    import sys

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if len(sys.argv) < 2:
        print("Uso: python Funciones/bot_contraloria.py CEDULA [--headed]")
        sys.exit(2)
    resultado = asyncio.run(
        consultar_antecedentes_fiscales(sys.argv[1], headed="--headed" in sys.argv)
    )
    resultado.pop("pdf_bytes", None)
    resultado.pop("html", None)
    print(json.dumps(resultado, ensure_ascii=False, indent=2, default=str))
