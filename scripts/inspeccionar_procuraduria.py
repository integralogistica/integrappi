"""Inspección del portal de certificado de antecedentes de la Procuraduría.

Carga la página con Playwright y dumpea: texto visible, campos de formulario,
imágenes (captcha?) y links. No envía nada: solo observa.
"""
from pathlib import Path

from playwright.sync_api import sync_playwright

URL = "https://apps.procuraduria.gov.co/webcert/inicio.aspx?tpo=2"
SALIDA = Path(__file__).resolve().parents[1] / "descargas_procuraduria"
SALIDA.mkdir(exist_ok=True)


def main():
    with sync_playwright() as p:
        navegador = p.chromium.launch(headless=True)
        contexto = navegador.new_context(
            viewport={"width": 1366, "height": 900},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
            locale="es-CO",
            ignore_https_errors=True,
        )
        pagina = contexto.new_page()
        print(f"Cargando {URL} ...")
        pagina.goto(URL, wait_until="domcontentloaded", timeout=90000)
        pagina.wait_for_timeout(4000)

        texto = pagina.inner_text("body")
        lineas = [l.strip() for l in texto.split("\n") if l.strip()]
        print(f"\n=== Texto visible ({len(lineas)} líneas, primeras 45) ===")
        for l in lineas[:45]:
            print(" |", l[:160])

        print("\n=== Campos de formulario ===")
        campos = pagina.eval_on_selector_all(
            "input, select, textarea, iframe",
            """els => els.map(e => ({
                tag: e.tagName, type: e.type || '', name: e.name || '',
                id: e.id || '', src: (e.src || '').slice(0, 140),
                title: e.title || '',
            }))""",
        )
        for c in campos:
            if c["type"] == "hidden":
                continue
            print(c)

        pagina.screenshot(path=str(SALIDA / "inicio.png"), full_page=True)
        (SALIDA / "inicio.html").write_text(pagina.content(), encoding="utf-8")
        print(f"\nGuardado en {SALIDA}")
        navegador.close()


if __name__ == "__main__":
    main()
