# Funciones/bot_policia.py
"""
Bot de antecedentes judiciales (pasado judicial) de la Policía Nacional.

https://antecedentes.policia.gov.co:7005/WebJudicial/ (JSF + PrimeFaces 7.0)

⚠️ El portal es de AUTOCONSULTA del titular (art. 94 del Decreto 019 de 2012)
y sus términos prohíben expresamente la consulta por terceros: la fuente
"policia" NO va habilitada por defecto en las empresas (opt-in por empresa,
con autorización documentada del titular bajo la Ley 1581 de 2012).

Resultado (descubierto 2026-08-30, dump descargas_policia/paso4_resultado.html):
la página de resultado es formAntecedentes.xhtml y el mensaje vive en el span
#form:mensajeCiudadano, sin PDF (el certificado fue eliminado por el art. 93
del Decreto 19 de 2012). Dos leyendas oficiales (FAQ del portal):
  - "NO TIENE ASUNTOS PENDIENTES CON LAS AUTORIDADES JUDICIALES"  → no_registra
    True (Sentencia SU-458 de 2012: sin antecedentes o condena extinguida).
  - "ACTUALMENTE NO ES REQUERIDO POR AUTORIDAD JUDICIAL" → no_registra False
    (ejecución de sentencia o información judicial sin actualizar).
  - El mensaje incluye la línea "Apellidos y Nombres: <b>NOMBRE</b>" → se
    aprovecha como nombre_consultado de la fuente.
  - La vista tiene un PrimeFaces Poll que a los 60 s devuelve a index: el
    resultado se lee apenas carga, sin esperas largas.

Captcha: reCAPTCHA v2 de Google resuelto por servicio externo estilo 2Captcha
(in.php method=userrecaptcha + poll res.php). Key en SEGURIDAD_POLICIA_CAPTCHA_
KEY (fallback API_KEY_CAPTCHA, la ya usada por otros procesos del proyecto).

Flujo: index (aceptar términos) → antecedentes.xhtml → cc + cédula → resolver
captcha → inyectar token → Consultar → leer mensajeCiudadano.
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

PORTAL_URL = "https://antecedentes.policia.gov.co:7005/WebJudicial/"
SALIDA = Path(__file__).resolve().parents[1] / "descargas_policia"

SITEKEY_FALLBACK = "6LcsIwQaAAAAAFCsaI-dkR6hgKsZwwJRsmE0tIJH"

# Bloqueo para serializar consultas al portal (una a la vez, como los demás bots).
_LOCK = threading.Lock()

_TIMEOUT_MS = 45000              # Playwright: goto/clicks/esperas puntuales
_PASO_RESULTADO_MS = 30000       # espera de la página de resultado tras Consultar
_CAPTCHA_BASE = os.getenv("SEGURIDAD_POLICIA_CAPTCHA_URL", "https://2captcha.com").rstrip("/")
# API_KEY_CAPTCHA es la key ya usada por los demás procesos del proyecto.
_CAPTCHA_KEY = os.getenv("SEGURIDAD_POLICIA_CAPTCHA_KEY", "").strip() or os.getenv("API_KEY_CAPTCHA", "").strip()
_CAPTCHA_TIMEOUT_S = float(os.getenv("SEGURIDAD_POLICIA_CAPTCHA_TIMEOUT_S", "90"))
_CAPTCHA_POOL_S = 5.0            # polling de res.php

# Leyendas oficiales del portal (preguntas.xhtml + SU-458). El regex de la
# leyenda "no registra" es el más específico: la página de resultado también
# cita la leyenda DENTRO del texto explicativo, pero siempre en el mismo orden
# (veredicto primero), por eso se busca la PRIMERA aparición del cuerpo.
_RE_NO_REGISTRA = re.compile(r"NO\s+TIENE\s+ASUNTOS\s+PENDIENTES[^<.]{0,120}", re.IGNORECASE)
_RE_REGISTRA = re.compile(r"ACTUALMENTE\s+NO\s+ES\s+REQUERIDO\s+POR\s+AUTORIDAD\s+JUDICIAL", re.IGNORECASE)
_RE_NOMBRE = re.compile(r"Apellidos y Nombres:\s*<b>([^<]+)</b>", re.IGNORECASE)


class BotPoliciaError(Exception):
    """Error del bot de antecedentes judiciales de la Policía."""


class BotPoliciaSinCaptchaKey(BotPoliciaError):
    """Falta configurar la key del resolvedor (fallo de config, accionable)."""


class BotPoliciaCaptchaFallido(BotPoliciaError):
    """El resolvedor rechazó el pedido o el portal rechazó el token."""


class BotPoliciaSinResultado(BotPoliciaError):
    """La página de resultado no contenía veredicto ni datos (anti-envenenamiento)."""


def _resolver_recaptcha(sitekey: str, url_pagina: str) -> str:
    """Resuelve el reCAPTCHA v2 vía servicio 2Captcha-compatible.

    Síncrona (requests): se llama desde la corutina con asyncio.to_thread para
    no bloquear el loop mientras se hace el polling de res.php.
    """
    if not _CAPTCHA_KEY:
        raise BotPoliciaSinCaptchaKey("Falta configurar SEGURIDAD_POLICIA_CAPTCHA_KEY para la fuente policia")
    try:
        r = requests.get(f"{_CAPTCHA_BASE}/in.php", params={
            "key": _CAPTCHA_KEY, "method": "userrecaptcha",
            "googlekey": sitekey, "pageurl": url_pagina, "json": 1,
        }, timeout=30)
        dato = r.json()
    except requests.RequestException as exc:
        raise BotPoliciaCaptchaFallido(f"El resolvedor de captcha no respondió: {exc}") from exc
    if dato.get("status") != 1:
        raise BotPoliciaCaptchaFallido(f"El resolvedor rechazó el pedido: {dato.get('request')}")
    captcha_id = str(dato.get("request"))

    logger.info("[BOT POLICIA] captcha pedido %s; sondeando cada %.0f s", captcha_id, _CAPTCHA_POOL_S)
    fin = time.monotonic() + _CAPTCHA_TIMEOUT_S
    while time.monotonic() < fin:
        time.sleep(_CAPTCHA_POOL_S)
        try:
            r2 = requests.get(f"{_CAPTCHA_BASE}/res.php", params={
                "key": _CAPTCHA_KEY, "action": "get", "id": captcha_id, "json": 1,
            }, timeout=30)
            dato2 = r2.json()
        except requests.RequestException as exc:
            raise BotPoliciaCaptchaFallido(f"El resolvedor de captcha no respondió: {exc}") from exc
        if dato2.get("status") == 1:
            return str(dato2["request"])
        if dato2.get("request") != "CAPCHA_NOT_READY":
            raise BotPoliciaCaptchaFallido(f"El resolvedor reportó: {dato2.get('request')}")
    raise BotPoliciaCaptchaFallido(f"El resolvedor no resolvió el captcha en {_CAPTCHA_TIMEOUT_S:.0f} s")


async def consultar_antecedentes_policia(cedula: str, headed: bool = False) -> Dict[str, Any]:
    """Consulta los antecedentes judiciales de una cédula (tipo CC).

    Retorna: cedula, no_registra (bool | None), mensaje (leyenda oficial ≤300),
    nombre_consultado (si el portal lo trae), texto_resultado, pdf_bytes (None:
    el portal no genera PDF), pdf_ruta (None) y html.
    """
    cedula_norm = re.sub(r"\D", "", cedula or "")
    if not 3 <= len(cedula_norm) <= 15:
        raise BotPoliciaError("Cédula inválida")

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

            # 1) Términos: marcar Acepto (el radio dispara un ajax que habilita
            #    #continuarBtn) y continuar. El callback JS redirige a
            #    antecedentes.xhtml; sin aceptar, el portal responde 302 a index.
            await pagina.goto(PORTAL_URL + "index.xhtml", wait_until="domcontentloaded", timeout=_TIMEOUT_MS)
            await pagina.wait_for_timeout(1500)
            await pagina.locator("input[name='aceptaOption'][value='true']").check()
            try:
                await pagina.wait_for_selector("#continuarBtn:not([disabled])", timeout=10000)
            except Exception as exc:
                raise BotPoliciaError("El portal no habilitó el botón Enviar tras aceptar términos") from exc
            async with pagina.expect_navigation(wait_until="domcontentloaded", timeout=_TIMEOUT_MS):
                await pagina.click("#continuarBtn")
            if "antecedentes.xhtml" not in pagina.url:
                raise BotPoliciaError(f"El portal no dejó pasar al formulario (URL: {pagina.url})")
            await pagina.wait_for_selector("#cedulaTipo", timeout=_TIMEOUT_MS)
            await pagina.wait_for_timeout(1500)

            # 2) Sitekey: leerlo del iframe de reCAPTCHA (cubre rotación).
            sitekey = ""
            for marco in pagina.frames:
                m = re.search(r"[?&]k=([0-9A-Za-z_-]{20,})", marco.url or "")
                if m:
                    sitekey = m.group(1)
                    break
            if not sitekey:
                sitekey = SITEKEY_FALLBACK
                logger.warning("[BOT POLICIA] sitekey no leído del DOM; usando fallback")

            # 3) Formulario: CC + cédula.
            await pagina.select_option("#cedulaTipo", "cc")
            await pagina.fill("#cedulaInput", cedula_norm)

            # 4) Captcha: solve en un hilo (no bloquea el loop) con cinturón
            #    extra sobre el polling interno.
            token = await asyncio.wait_for(
                asyncio.to_thread(_resolver_recaptcha, sitekey, pagina.url),
                timeout=_CAPTCHA_TIMEOUT_S + 15,
            )

            # 5) Inyectar el token en el textarea oculto (y en el del iframe).
            await pagina.evaluate(
                """(tok) => {
                    const poner = (doc) => {
                        const ta = doc.getElementById('g-recaptcha-response');
                        if (ta) { ta.value = tok; ta.style.display = 'block'; }
                    };
                    poner(document);
                    for (const f of document.querySelectorAll('iframe[title*=recaptcha], iframe[src*=recaptcha]')) {
                        try { poner(f.contentDocument); } catch (e) {}
                    }
                }""", token,
            )

            # 6) Consultar: la página de resultado es formAntecedentes.xhtml
            #    (navegación completa). El botón por rol: los ids j_idt* son
            #    autogenerados y cambian entre deploys. Sin descarga: el portal
            #    no genera PDF (Decreto 19/2012 art. 93), pero la trampa de
            #    descarga queda barata por si el portal cambia de forma.
            pdf_bytes: Optional[bytes] = None

            async def _esperar_descarga():
                nonlocal pdf_bytes
                try:
                    async with pagina.expect_download(timeout=_PASO_RESULTADO_MS) as info:
                        pass
                    descarga = await info.value
                    pdf_bytes = await (await descarga.path()).read_bytes()
                except Exception:
                    pass  # no hubo descarga: el resultado llega como página

            tarea_descarga = asyncio.create_task(_esperar_descarga())
            try:
                async with pagina.expect_navigation(wait_until="domcontentloaded", timeout=_PASO_RESULTADO_MS):
                    await pagina.get_by_role("button", name="Consultar").click()
            except Exception:
                pass  # pudo ser submit Ajax sin navegación completa
            # El PrimeFaces Poll de la vista devuelve a index a los 60 s: leer
            # el resultado apenas estabilice, sin esperas largas.
            await pagina.wait_for_timeout(2500)
            try:
                await asyncio.wait_for(asyncio.shield(tarea_descarga), timeout=5)
            except asyncio.TimeoutError:
                pass

            html = await pagina.content()
            texto_resultado = " ".join((await pagina.inner_text("body")).split())

            (SALIDA / f"resultado_{cedula_norm}.html").write_text(html, encoding="utf-8")

            # 7) Veredicto. Primero el error del formulario (#j_idt10 en
            #    antecedentes, form:messages en el resultado): un error de
            #    captcha consume reintento; otro error es un resultado
            #    legítimo del portal (sin veredicto).
            error_portal = ""
            for sel in ("#j_idt10 .ui-messages-error-detail", "#form\\:messages .ui-messages-error-detail"):
                loc = pagina.locator(sel)
                if await loc.count():
                    textos = await loc.all_inner_texts()
                    error_portal = " ".join((textos or [""]).split())
                    if error_portal:
                        break
            if "captcha" in error_portal.lower():
                raise BotPoliciaCaptchaFallido(f"El portal rechazó el captcha: {error_portal[:150]}")

            no_registra: Optional[bool] = None
            mensaje = ""
            m = _RE_NO_REGISTRA.search(texto_resultado)
            if m:
                no_registra = True
                mensaje = m.group(0).strip()
            else:
                m2 = _RE_REGISTRA.search(texto_resultado)
                if m2:
                    no_registra = False
                    mensaje = m2.group(0).strip()

            nombre_consultado = ""
            m3 = _RE_NOMBRE.search(html)
            if m3:
                nombre_consultado = " ".join(m3.group(1).split())

            # Anti-envenenamiento (análogo al "Consulta realizada" de RNDC): un
            # resultado sin leyenda, sin nombre y sin error del portal es una
            # respuesta incompleta — NUNCA se cachea como éxito.
            if no_registra is None and not nombre_consultado and not error_portal and not pdf_bytes:
                raise BotPoliciaSinResultado(
                    "La página de resultado no contenía veredicto ni datos (posible cambio del portal)",
                )

            pdf_ruta: Optional[str] = None
            if pdf_bytes:
                SALIDA.mkdir(exist_ok=True)
                pdf_ruta = str(SALIDA / f"certificado_{cedula_norm}.pdf")
                Path(pdf_ruta).write_bytes(pdf_bytes)

            return {
                "cedula": cedula_norm,
                "no_registra": no_registra,
                "mensaje": mensaje[:300],
                "nombre_consultado": nombre_consultado,
                "texto_resultado": texto_resultado[:1500],
                "pdf_bytes": pdf_bytes,
                "pdf_ruta": pdf_ruta,
                "html": html,
            }
        finally:
            await navegador.close()


def consultar_antecedentes_policia_sync(cedula: str) -> Dict[str, Any]:
    """Versión síncrona para asyncio.to_thread, igual que los demás bots."""
    with _LOCK:
        return asyncio.run(consultar_antecedentes_policia(cedula))


if __name__ == "__main__":
    import json
    import sys

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if len(sys.argv) < 2:
        print("Uso: python Funciones/bot_policia.py CEDULA [--headed]")
        sys.exit(2)
    resultado = consultar_antecedentes_policia_sync(sys.argv[1])
    resultado.pop("pdf_bytes", None)
    resultado.pop("html", None)
    print(json.dumps(resultado, ensure_ascii=False, indent=2, default=str))
