"""Sonda temporal (2026-09-02): el bot de Procuraduría dejó de entregar veredicto
(postback sin respuesta). Replica el flujo del bot paso a paso dejando dumps
(pregunta, respuesta dada, HTML y screenshot tras el click) para ver qué cambió.
"""
import asyncio
import json
import re
import sys
from pathlib import Path

from playwright.async_api import async_playwright

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from Funciones.bot_procuraduria import (  # noqa: E402
    FORMULARIO_URL,
    IFRAME_SELECTOR,
    PORTAL_URL,
    _resolver_captcha_documento,
    _resolver_captcha_gemini,
    _resolver_captcha_texto,
)

SALIDA = Path(__file__).resolve().parents[1] / "descargas_procuraduria"
CEDULA = sys.argv[1] if len(sys.argv) > 1 else "1033688842"


async def main():
    async with async_playwright() as p:
        navegador = await p.chromium.launch(headless=True)
        contexto = await navegador.new_context(
            viewport={"width": 1366, "height": 900},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
            locale="es-CO",
            ignore_https_errors=True,
        )
        pagina = await contexto.new_page()
        await pagina.goto(PORTAL_URL, wait_until="domcontentloaded", timeout=90000)
        try:
            el = await pagina.wait_for_selector(IFRAME_SELECTOR, state="attached", timeout=20000)
            vista = await el.content_frame()
            await vista.wait_for_selector("#lblPregunta", timeout=20000)
            print("[1] entro por IFRAME")
        except Exception as exc:
            print(f"[1] sin iframe ({exc}); navegando directo a {FORMULARIO_URL}")
            await pagina.goto(FORMULARIO_URL, wait_until="domcontentloaded", timeout=90000)
            vista = pagina
            await vista.wait_for_selector("#lblPregunta", timeout=90000)
        await pagina.wait_for_timeout(2000)

        pregunta = (await vista.inner_text("#lblPregunta")).strip()
        print(f"[2] pregunta: {pregunta!r}")
        respuesta = _resolver_captcha_texto(pregunta)
        via = "aritmetica"
        if respuesta is None:
            respuesta = _resolver_captcha_documento(pregunta, CEDULA)
            via = "documento/nombre"
        if respuesta is None:
            respuesta = _resolver_captcha_gemini(pregunta, CEDULA)
            via = "gemini"
        print(f"[3] respuesta ({via}): {respuesta!r}")

        await vista.select_option("#ddlTipoID", "1")
        await vista.fill("#txtNumID", CEDULA)
        radio = vista.locator("#rblTipoCert_0")
        if await radio.count() and await radio.is_visible():
            await radio.check()
            print("[4] radio ordinario marcado")
        await vista.fill("#txtRespuestaPregunta", respuesta)

        botones = await vista.locator("input[type=submit], input[type=button], button").all()
        print("[5] botones visibles:", [
            (await b.get_attribute("id"), await b.get_attribute("value")) for b in botones
        ])
        btn = vista.locator("#btnConsultar, #btnExportar").first
        if not await btn.count():
            print("[5] NO HAY #btnConsultar/#btnExportar")
        else:
            await btn.click()
            print("[6] click hecho; esperando respuesta...")
        try:
            await vista.wait_for_selector("#btnDescargar", timeout=45000, state="attached")
            print("[7] apareció #btnDescargar")
        except Exception:
            print("[7] sin #btnDescargar")
        try:
            await vista.wait_for_load_state("networkidle", timeout=20000)
        except Exception:
            pass
        await pagina.wait_for_timeout(3000)

        (SALIDA / "sonda_despues_click.html").write_text(await vista.content(), encoding="utf-8")
        await pagina.screenshot(path=str(SALIDA / "sonda_despues_click.png"), full_page=True)
        texto = " ".join((await vista.inner_text("body")).split())
        print(f"[8] texto visible ({len(texto)} chars): {texto[:400]!r}")
        # mensajes de validación webforms
        val = await vista.locator("[id*=alidator], .error, [style*='red']").all()
        for v in val[:5]:
            t = (await v.inner_text()).strip()
            if t:
                print(f"[9] validador: {t[:150]!r}")
        await navegador.close()


asyncio.run(main())
