"""Sonda del portal de Inhabilidades por delitos sexuales contra menores
(Ley 1918 de 2018) de la Policía Nacional.

https://inhabilidades.policia.gov.co:8080/

Uso:
  python scripts/probar_delitos_sexuales.py --solo-formulario      # estructura
  python scripts/probar_delitos_sexuales.py CEDULA FECHA_EXP EMPRESA NIT [--headed]
      FECHA_EXP en DD/MM/AAAA (fecha de expedición de la cédula).
Gasta 1 solve de reCAPTCHA v2 (~US$0.003) en la consulta viva.
Dumps paso a paso en descargas_delitos/.
"""
import argparse
import asyncio
import io
import json
import logging
import os
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import requests
from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv(Path(__file__).resolve().parents[1] / ".env")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("sonda-delitos")

PORTAL_URL = "https://inhabilidades.policia.gov.co:8080/"
SALIDA = Path(__file__).resolve().parents[1] / "descargas_delitos"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")

_CAPTCHA_BASE = os.getenv("SEGURIDAD_DELITOS_CAPTCHA_URL", "https://2captcha.com").rstrip("/")
_CAPTCHA_KEY = (os.getenv("SEGURIDAD_DELITOS_CAPTCHA_KEY", "").strip()
                or os.getenv("API_KEY_CAPTCHA", "").strip())
_CAPTCHA_TIMEOUT_S = 90.0
SITEKEY_FALLBACK = "6LflZLwUAAAAAP6-I_SuqVa1YDSTqfMyk43peb_M"


def _resolver_recaptcha(sitekey: str, url_pagina: str) -> str:
    if not _CAPTCHA_KEY:
        raise SystemExit("Falta API_KEY_CAPTCHA en .env")
    r = requests.get(f"{_CAPTCHA_BASE}/in.php", params={
        "key": _CAPTCHA_KEY, "method": "userrecaptcha",
        "googlekey": sitekey, "pageurl": url_pagina, "json": 1,
    }, timeout=30)
    dato = r.json()
    if dato.get("status") != 1:
        raise SystemExit(f"in.php rechazado: {dato.get('request')}")
    captcha_id = str(dato["request"])
    logger.info("captcha pedido %s", captcha_id)
    fin = time.monotonic() + _CAPTCHA_TIMEOUT_S
    while time.monotonic() < fin:
        time.sleep(5)
        r2 = requests.get(f"{_CAPTCHA_BASE}/res.php", params={
            "key": _CAPTCHA_KEY, "action": "get", "id": captcha_id, "json": 1,
        }, timeout=30)
        d2 = r2.json()
        if d2.get("status") == 1:
            return str(d2["request"])
        if d2.get("request") != "CAPCHA_NOT_READY":
            raise SystemExit(f"res.php: {d2.get('request')}")
    raise SystemExit("captcha sin resolver en 90 s")


async def inventario(pagina, prefijo: str) -> dict:
    datos = await pagina.evaluate(
        """() => {
            const visibles = (el) => { const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
            return {
                url: location.href,
                inputs: [...document.querySelectorAll('input, select, textarea')].filter(visibles).map(e => ({
                    tag: e.tagName, id: e.id, name: e.getAttribute('name'), type: e.type, ph: e.placeholder,
                })),
                botones: [...document.querySelectorAll('button, input[type=submit], a.btn')].filter(visibles)
                    .map(e => (e.innerText || e.value || '').trim().slice(0, 60)),
                sitekey: (document.querySelector('.g-recaptcha') || {}).getAttribute
                    ? document.querySelector('.g-recaptcha').getAttribute('data-sitekey') : null,
                forms: [...document.querySelectorAll('form')].map(f => ({id: f.id, action: f.action.slice(0, 80), method: f.method})),
            };
        }"""
    )
    SALIDA.mkdir(exist_ok=True)
    (SALIDA / f"{prefijo}.html").write_text(await pagina.content(), encoding="utf-8")
    texto = " ".join((await pagina.inner_text("body")).split())
    (SALIDA / f"{prefijo}.txt").write_text(texto, encoding="utf-8")
    return {**datos, "texto_inicio": texto[:400]}


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("cedula", nargs="?")
    parser.add_argument("fecha_exp", nargs="?")
    parser.add_argument("empresa", nargs="?")
    parser.add_argument("nit", nargs="?")
    parser.add_argument("--solo-formulario", action="store_true")
    parser.add_argument("--headed", action="store_true")
    args = parser.parse_args()

    async with async_playwright() as p:
        navegador = await p.chromium.launch(headless=not args.headed)
        pagina = await navegador.new_page(viewport={"width": 1366, "height": 900}, user_agent=UA)
        logger.info("cargando %s", PORTAL_URL)
        await pagina.goto(PORTAL_URL, wait_until="domcontentloaded", timeout=60000)
        await pagina.wait_for_selector("#nuip", state="visible", timeout=30000)
        await pagina.wait_for_timeout(2000)
        print(json.dumps(await inventario(pagina, "formulario"), ensure_ascii=False, indent=2)[:4000])

        if args.solo_formulario or not (args.cedula and args.fecha_exp):
            await navegador.close()
            return

        # Consulta viva: llenar form + resolver reCAPTCHA + términos + submit.
        await pagina.select_option("#tipo", "CC")
        await pagina.fill("#nuip", args.cedula)
        await pagina.fill("#fechaExpNuip", args.fecha_exp)
        await pagina.fill("#nombreEmpresa", args.empresa or "SONDA")
        await pagina.fill("#nitEmpresa", args.nit or "900000000")
        sitekey = await pagina.evaluate(
            "() => (document.querySelector('.g-recaptcha') || {}).getAttribute ? document.querySelector('.g-recaptcha').getAttribute('data-sitekey') : ''"
        ) or SITEKEY_FALLBACK
        logger.info("sitekey: %s", sitekey)
        token = await asyncio.to_thread(_resolver_recaptcha, sitekey, pagina.url)
        # main.js (obfuscado, decodificado 2026-09-14): jquery.validate exige
        # grecaptcha.getResponse() != '' y el NIT con DV (máscara 000000000-0).
        # El botón se habilita con el checkbox de términos. El submit NATIVO
        # (form.submit()) no dispara los handlers de validate: basta stub del
        # getResponse + campo oculto #captcha con el token.
        await pagina.evaluate(
            """(tok) => {
                const ta = document.getElementById('g-recaptcha-response');
                if (ta) { ta.value = tok; ta.style.display = 'block'; }
                const oculto = document.getElementById('captcha');
                if (oculto) oculto.value = tok;
                try { window.grecaptcha.getResponse = () => tok; } catch (e) {}
                const cb = document.getElementById('cbCondiciones');
                if (cb) { cb.checked = true; cb.dispatchEvent(new Event('change', {bubbles: true})); }
                const btn = document.getElementById('btnConsultar');
                if (btn) btn.disabled = false;
            }""", token,
        )
        await pagina.evaluate("() => document.getElementById('frmCons').submit()")
        # El submit es un POST completo (action=/consulta): esperar navegación.
        await pagina.wait_for_load_state("domcontentloaded", timeout=60000)
        await pagina.wait_for_timeout(6000)
        print(json.dumps(await inventario(pagina, "resultado"), ensure_ascii=False, indent=2)[:6000])
        await navegador.close()


if __name__ == "__main__":
    asyncio.run(main())
