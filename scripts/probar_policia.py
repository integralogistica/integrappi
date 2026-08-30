"""Sonda exploratoria: consulta de antecedentes judiciales en el portal de la Policía.

https://antecedentes.policia.gov.co:7005/WebJudicial/ (JSF + PrimeFaces 7.0)

DESCUBRE la página de resultado REAL (nadie la ha visto: exige resolver el
reCAPTCHA v2). El dump previo resultado_1033688842.html (2026-08-29) es solo
el formulario re-renderizado con el error "Debe seleccionar las imagenes
correspondientes del Captcha" — la página de resultado sigue desconocida.

Según la FAQ del portal (preguntas.xhtml) el resultado son 2 leyendas oficiales
y NO genera PDF (el certificado fue eliminado por el Decreto 19/2012 art. 93):
  - "NO TIENE ASUNTOS PENDIENTES CON LAS AUTORIDADES JUDICIALES"  (SU-458/2012)
  - "ACTUALMENTE NO ES REQUERIDO POR AUTORIDAD JUDICIAL"
De aquí salen los regex definitivos de Funciones/bot_policia.py.

⚠️ El portal es de AUTOCONSULTA del titular (art. 94 Decreto 019/2012) y sus
términos prohíben expresamente la consulta por terceros: usar solo con cédulas
de colaboradores que autoricen (pruebas internas), nunca en barrido.

Uso (desde integrappi/):
    python scripts/probar_policia.py 1033688842                 # completo (gasta 1 solve de 2Captcha)
    python scripts/probar_policia.py 1033688842 --solo-formulario   # sin captcha: valida selectores
    python scripts/probar_policia.py 1033688842 --token TOKEN   # token resuelto a mano (no gasta)
    python scripts/probar_policia.py 1033688842 --headless      # sin ventana
"""
from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

PORTAL = "https://antecedentes.policia.gov.co:7005/WebJudicial/"
SALIDA = Path(__file__).resolve().parents[1] / "descargas_policia"
TIMEOUT_MS = 45000
CAPTCHA_BASE = os.getenv("SEGURIDAD_POLICIA_CAPTCHA_URL", "https://2captcha.com").rstrip("/")
# API_KEY_CAPTCHA es la key ya usada por los demás bots del proyecto.
CAPTCHA_KEY = os.getenv("SEGURIDAD_POLICIA_CAPTCHA_KEY", "").strip() or os.getenv("API_KEY_CAPTCHA", "").strip()
CAPTCHA_TIMEOUT_S = float(os.getenv("SEGURIDAD_POLICIA_CAPTCHA_TIMEOUT_S", "90"))


def resolver_recaptcha(sitekey: str, url_pagina: str) -> str:
    """Solve vía 2Captcha (in.php userrecaptcha + poll res.php cada 5 s)."""
    import requests

    r = requests.get(f"{CAPTCHA_BASE}/in.php", params={
        "key": CAPTCHA_KEY, "method": "userrecaptcha",
        "googlekey": sitekey, "pageurl": url_pagina, "json": 1,
    }, timeout=30)
    dato = r.json()
    if dato.get("status") != 1:
        raise RuntimeError(f"in.php rechazó el pedido: {dato.get('request')}")
    captcha_id = dato["request"]
    print(f"[captcha] pedido {captcha_id}; sondeando cada 5 s (máx {CAPTCHA_TIMEOUT_S:.0f} s)…")

    import time
    fin = time.monotonic() + CAPTCHA_TIMEOUT_S
    while time.monotonic() < fin:
        time.sleep(5)
        r2 = requests.get(f"{CAPTCHA_BASE}/res.php", params={
            "key": CAPTCHA_KEY, "action": "get", "id": captcha_id, "json": 1,
        }, timeout=30)
        dato2 = r2.json()
        if dato2.get("status") == 1:
            token = dato2["request"]
            print(f"[captcha] resuelto ({len(token)} chars)")
            return token
        if dato2.get("request") != "CAPCHA_NOT_READY":
            raise RuntimeError(f"res.php error: {dato2.get('request')}")
    raise RuntimeError("timeout esperando el solve")


async def main(cedula: str, solo_formulario: bool, token_manual: str | None, headed: bool) -> None:
    SALIDA.mkdir(exist_ok=True)
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

            # Paso 1: términos
            await pagina.goto(PORTAL + "index.xhtml", wait_until="domcontentloaded", timeout=TIMEOUT_MS)
            await pagina.wait_for_timeout(1500)
            (SALIDA / "paso1_index.html").write_text(await pagina.content(), encoding="utf-8")
            print("[1] index cargado")

            # Paso 2: aceptar y continuar
            await pagina.locator("input[name='aceptaOption'][value='true']").check()
            await pagina.wait_for_selector("#continuarBtn:not([disabled])", timeout=10000)
            await pagina.screenshot(path=str(SALIDA / "paso2_aceptar.png"))
            async with pagina.expect_navigation(wait_until="domcontentloaded", timeout=TIMEOUT_MS):
                await pagina.click("#continuarBtn")
            if "antecedentes.xhtml" not in pagina.url:
                (SALIDA / "paso2_sin_pasar.html").write_text(await pagina.content(), encoding="utf-8")
                raise RuntimeError(f"El portal no dejó pasar a antecedentes (URL: {pagina.url})")
            await pagina.wait_for_selector("#cedulaTipo", timeout=TIMEOUT_MS)
            await pagina.wait_for_timeout(1500)
            (SALIDA / "paso3_antecedentes.html").write_text(await pagina.content(), encoding="utf-8")
            print(f"[2] formulario alcanzado: {pagina.url}")

            # Sitekey desde el iframe (k= del src de recaptcha)
            sitekey = ""
            for marco in pagina.frames:
                m = re.search(r"[?&]k=([0-9A-Za-z_-]{20,})", marco.url or "")
                if m:
                    sitekey = m.group(1)
                    break
            print(f"[3] sitekey leído del DOM: {sitekey or '(no encontrado; usar el de constante)'}")

            # Paso 4: formulario (cc + cédula)
            await pagina.select_option("#cedulaTipo", "cc")
            await pagina.fill("#cedulaInput", cedula)
            print(f"[4] formulario llenado (cc / {cedula})")

            if solo_formulario:
                print("[fin] --solo-formulario: sin submit. Dumps en descargas_policia/")
                return

            # Paso 5: captcha
            token = token_manual or await asyncio.to_thread(resolver_recaptcha, sitekey, pagina.url)
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
            print(f"[5] token inyectado ({'manual' if token_manual else '2Captcha'})")

            # Paso 6: Consultar (trampa de descarga por si el resultado es PDF)
            pdf_bytes = b""

            async def _esperar_descarga():
                nonlocal pdf_bytes
                try:
                    async with pagina.expect_download(timeout=30000) as info:
                        pass
                    descarga = await info.value
                    pdf_bytes = await (await descarga.path()).read_bytes()
                except Exception:
                    pass

            tarea = asyncio.create_task(_esperar_descarga())
            try:
                async with pagina.expect_navigation(wait_until="domcontentloaded", timeout=30000):
                    await pagina.get_by_role("button", name="Consultar").click()
            except Exception:
                pass  # submit Ajax sin navegación completa
            await pagina.wait_for_timeout(4000)
            try:
                await asyncio.wait_for(asyncio.shield(tarea), timeout=10)
            except asyncio.TimeoutError:
                pass

            (SALIDA / "paso4_resultado.html").write_text(await pagina.content(), encoding="utf-8")
            await pagina.screenshot(path=str(SALIDA / "paso4_resultado.png"), full_page=True)
            if pdf_bytes:
                (SALIDA / f"paso4_resultado_{cedula}.pdf").write_bytes(pdf_bytes)

            # Resumen: error del portal, leyendas, texto visible
            errores = await pagina.locator("#j_idt10 .ui-messages-error-detail").all_inner_texts()
            cuerpo = " ".join((await pagina.inner_text("body")).split())
            print("\n===== RESUMEN DEL DESCUBRIMIENTO =====")
            print(f"URL final: {pagina.url}")
            print(f"PDF descargado: {len(pdf_bytes)} bytes" if pdf_bytes else "PDF descargado: no")
            if errores:
                print(f"Error del portal (#j_idt10): {errores}")
            for leyenda in ("NO TIENE ASUNTOS PENDIENTES", "ACTUALMENTE NO ES REQUERIDO"):
                m = re.search(re.escape(leyenda) + r"[^.]{0,120}", cuerpo, re.IGNORECASE)
                print(f"Leyenda '{leyenda}': {'SÍ → ' + m.group(0)[:160] if m else 'no'}")
            print(f"\nTexto visible (primeros 800 chars):\n{cuerpo[:800]}")
            print("\nDumps en descargas_policia/ (paso4_resultado.html es la página de resultado).")
        finally:
            await navegador.close()


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(description="Sonda del portal de antecedentes judiciales de la Policía")
    parser.add_argument("cedula")
    parser.add_argument("--solo-formulario", action="store_true", help="Llega al formulario sin submit (sin gastar captcha)")
    parser.add_argument("--token", help="Token g-recaptcha-response resuelto a mano")
    parser.add_argument("--headless", action="store_true", help="Sin ventana (default: headed)")
    args = parser.parse_args()
    if not args.solo_formulario and not args.token and not CAPTCHA_KEY:
        print("ERROR: falta SEGURIDAD_POLICIA_CAPTCHA_KEY en .env (o use --token/--solo-formulario)")
        sys.exit(2)
    asyncio.run(main(args.cedula, args.solo_formulario, args.token, headed=not args.headless))
