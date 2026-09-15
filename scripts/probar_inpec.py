"""Sonda del buscador de población privada de la libertad del INPEC.

https://www.inpec.gov.co/registro-de-la-poblacion-privada-de-la-libertad

Uso:
  python scripts/probar_inpec.py --solo-formulario   # estructura, sin consultar
  python scripts/probar_inpec.py --solo-formulario --headed
Dumps paso a paso en descargas_inpec/.
"""
import argparse
import asyncio
import io
import json
import logging
import re
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("sonda-inpec")

PORTAL_URL = "https://www.inpec.gov.co/registro-de-la-poblacion-privada-de-la-libertad"
SALIDA = Path(__file__).resolve().parents[1] / "descargas_inpec"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")


async def inventario(pagina, prefijo: str) -> dict:
    datos = await pagina.evaluate(
        """() => {
            const visibles = (el) => {
                const r = el.getBoundingClientRect();
                return r.width > 0 && r.height > 0;
            };
            const inputs = [...document.querySelectorAll('input, select, textarea')]
                .filter(visibles).map(e => ({
                    tag: e.tagName, id: e.id, name: e.getAttribute('name'),
                    type: e.type, ph: e.placeholder, valor: e.value.slice(0, 40),
                }));
            const botones = [...document.querySelectorAll('button, input[type=submit], a.btn')]
                .filter(visibles).map(e => (e.innerText || e.value || '').trim().slice(0, 60));
            const captchas = [...document.querySelectorAll('.g-recaptcha, iframe[src*=recaptcha], iframe[title*=captcha], [class*=captcha]')]
                .map(e => ({tag: e.tagName, cls: e.className, sitekey: e.getAttribute('data-sitekey')}));
            const iframes = [...document.querySelectorAll('iframe')].map(e => e.src.slice(0, 120));
            return {inputs, botones, captchas, iframes};
        }"""
    )
    SALIDA.mkdir(exist_ok=True)
    html = await pagina.content()
    (SALIDA / f"{prefijo}.html").write_text(html, encoding="utf-8")
    texto = " ".join((await pagina.inner_text("body")).split())
    (SALIDA / f"{prefijo}.txt").write_text(texto, encoding="utf-8")
    return {"url": pagina.url, **datos, "texto_inicio": texto[:400]}


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--solo-formulario", action="store_true", help="solo estructura, sin consultar")
    parser.add_argument("--headed", action="store_true")
    args = parser.parse_args()

    async with async_playwright() as p:
        navegador = await p.chromium.launch(headless=not args.headed)
        pagina = await navegador.new_page(viewport={"width": 1366, "height": 900}, user_agent=UA)
        logger.info("cargando %s", PORTAL_URL)
        await pagina.goto(PORTAL_URL, wait_until="domcontentloaded", timeout=90000)
        await pagina.wait_for_timeout(6000)
        estructura = await inventario(pagina, "formulario")
        print(json.dumps(estructura, ensure_ascii=False, indent=2)[:5000])
        await navegador.close()


if __name__ == "__main__":
    asyncio.run(main())
