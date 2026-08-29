"""Inspección del portal público RNDC2 (Historial de Viajes) para automatización.

Carga la página con Playwright, dumpea los campos del formulario y el captcha
(HTML + screenshot) para diseñar el bot. No envía nada: solo observa.
"""
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

URL = "https://rndc2.mintransporte.gov.co/logistica/ctl/HistorialViajes/mid/394"
SALIDA = Path(__file__).resolve().parents[1] / "descargas_rndc2"
SALIDA.mkdir(exist_ok=True)


def main():
    encabezados = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
        "Accept-Language": "es-CO,es;q=0.9",
    }
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        contexto = navegador.new_context(
            viewport={"width": 1366, "height": 900},
            extra_http_headers=encabezados,
            ignore_https_errors=True,
        )
        pagina = contexto.new_page()
        print(f"Cargando {URL} ...")
        pagina.goto(URL, wait_until="domcontentloaded", timeout=90000)
        pagina.wait_for_timeout(4000)

        # Dump de inputs/selects/imagenes del formulario
        campos = pagina.eval_on_selector_all(
            "input, select, textarea, img",
            """els => els.map(e => ({
                tag: e.tagName, type: e.type || '', name: e.name || '',
                id: e.id || '', placeholder: e.placeholder || '',
                src: (e.src || '').slice(0, 120), alt: e.alt || '',
            }))""",
        )
        print(f"\n=== {len(campos)} elementos de formulario/imagen ===")
        for c in campos:
            if c["tag"] in ("IMG",):
                if "captcha" in (c["src"] + c["alt"]).lower() or c["src"]:
                    print("IMG", c)
            else:
                print(c)

        # Texto visible que contenga 'captcha', 'suma', 'operación', números con + - *
        texto = pagina.inner_text("body")
        lineas = [l.strip() for l in texto.split("\n") if l.strip()]
        print(f"\n=== Texto visible ({len(lineas)} líneas, primeras 60) ===")
        for l in lineas[:60]:
            print(" |", l[:150])

        pagina.screenshot(path=str(SALIDA / "historial_viajes.png"), full_page=True)
        html = pagina.content()
        (SALIDA / "historial_viajes.html").write_text(html, encoding="utf-8")
        print(f"\nScreenshot y HTML guardados en {SALIDA}")
        navegador.close()


if __name__ == "__main__":
    main()
