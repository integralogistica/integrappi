"""Sonda exploratoria: Certificado de Antecedentes Fiscales — Persona Natural (CGR).

https://www.contraloria.gov.co/web/guest/persona-natural

Portal Liferay: el formulario vive en un acordeón/portlet que el HTML estático
no muestra. Primera pasada de descubrimiento (este script): cargar la página,
abrir el acordeón "Persona Natural", inventariar forms/iframes/inputs y guardar
dumps paso a paso en descargas_contraloria/. Los selectores definitivos de
Funciones/bot_contraloria.py salen de aquí.

Uso (desde integrappi/):
    python scripts/probar_contraloria.py --solo-formulario
    python scripts/probar_contraloria.py 1033688842 --token TOKEN   # captcha a mano
    python scripts/probar_contraloria.py 1033688842                 # completo (gasta 1 solve)
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

PORTAL = "https://www.contraloria.gov.co/web/guest/persona-natural"
SALIDA = Path(__file__).resolve().parents[1] / "descargas_contraloria"
TIMEOUT_MS = 60000
CAPTCHA_BASE = os.getenv("SEGURIDAD_CONTRALORIA_CAPTCHA_URL", "https://2captcha.com").rstrip("/")
CAPTCHA_KEY = os.getenv("SEGURIDAD_CONTRALORIA_CAPTCHA_KEY", "").strip() or os.getenv("API_KEY_CAPTCHA", "").strip()
CAPTCHA_TIMEOUT_S = float(os.getenv("SEGURIDAD_CONTRALORIA_CAPTCHA_TIMEOUT_S", "90"))


def resolver_recaptcha(sitekey: str, url_pagina: str) -> str:
    """reCAPTCHA v2 vía 2Captcha (method=userrecaptcha + poll res.php), como Policía."""
    import time

    import requests

    if not CAPTCHA_KEY:
        raise RuntimeError("falta API_KEY_CAPTCHA en .env")
    r = requests.get(f"{CAPTCHA_BASE}/in.php", params={
        "key": CAPTCHA_KEY, "method": "userrecaptcha",
        "googlekey": sitekey, "pageurl": url_pagina, "json": 1,
    }, timeout=30)
    dato = r.json()
    if dato.get("status") != 1:
        raise RuntimeError(f"in.php rechazó el pedido: {dato.get('request')}")
    captcha_id = str(dato["request"])
    print(f"[captcha] pedido {captcha_id}; sondeando cada 5 s (máx {CAPTCHA_TIMEOUT_S:.0f} s)…")
    fin = time.monotonic() + CAPTCHA_TIMEOUT_S
    while time.monotonic() < fin:
        time.sleep(5)
        r2 = requests.get(f"{CAPTCHA_BASE}/res.php", params={
            "key": CAPTCHA_KEY, "action": "get", "id": captcha_id, "json": 1,
        }, timeout=30)
        dato2 = r2.json()
        if dato2.get("status") == 1:
            print(f"[captcha] resuelto ({len(dato2['request'])} chars)")
            return dato2["request"]
        if dato2.get("request") != "CAPCHA_NOT_READY":
            raise RuntimeError(f"res.php error: {dato2.get('request')}")
    raise RuntimeError("timeout esperando el solve")


JS_INVENTARIO = """() => {
    const visible = e => !!(e.offsetWidth || e.offsetHeight || e.getClientRects().length);
    return {
        inputs: [...document.querySelectorAll('input, select')].filter(visible).map(e => ({
            tag: e.tagName.toLowerCase(), type: e.type || '', id: e.id || '', name: e.name || '',
            placeholder: e.placeholder || '',
            value: (e.type === 'radio' || e.tagName === 'SELECT') ? (e.value || '') : '',
        })),
        botones: [...document.querySelectorAll('button, input[type=submit], input[type=button], a[role=button]')].filter(visible).map(e => ({
            tag: e.tagName.toLowerCase(), id: e.id || '', name: e.name || '',
            texto: (e.innerText || e.value || '').trim().slice(0, 60),
        })),
        captcha_img: [...document.images].map(e => e.src).find(s => s.startsWith('data:image')) || '',
        recaptcha: !!document.querySelector('.g-recaptcha, iframe[src*="recaptcha"]'),
        iframes: [...document.querySelectorAll('iframe')].map(f => f.src || '(sin src)'),
    };
}"""


async def _dump(pagina, vista, nombre: str) -> None:
    (SALIDA / f"{nombre}.html").write_text(await vista.content(), encoding="utf-8")
    await pagina.screenshot(path=str(SALIDA / f"{nombre}.png"), full_page=True)


async def _inventario(vista, etiqueta: str) -> dict:
    inv = await vista.evaluate(JS_INVENTARIO)
    print(f"[inv:{etiqueta}] inputs: {inv['inputs']}")
    print(f"[inv:{etiqueta}] botones: {inv['botones']}")
    if inv["captcha_img"]:
        print(f"[inv:{etiqueta}] CAPTCHA IMAGEN propia (data:image): {inv['captcha_img'][:60]}…")
    if inv["recaptcha"]:
        print(f"[inv:{etiqueta}] reCAPTCHA detectado")
    if inv["iframes"]:
        print(f"[inv:{etiqueta}] iframes: {inv['iframes']}")
    return inv


async def main(cedula: str | None, solo_formulario: bool, token_manual: str | None, headed: bool) -> None:
    SALIDA.mkdir(exist_ok=True)
    async with async_playwright() as p:
        navegador = await p.chromium.launch(headless=not headed)
        try:
            contexto = await navegador.new_context(
                viewport={"width": 1366, "height": 900},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
                locale="es-CO",
                accept_downloads=True,
            )
            pagina = await contexto.new_page()

            # Paso 1: carga del portal Liferay.
            await pagina.goto(PORTAL, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
            await pagina.wait_for_timeout(5000)
            await _dump(pagina, pagina, "paso1_portal")
            print(f"[1] portal cargado: {pagina.url}")
            await _inventario(pagina, "portal")

            # Paso 2: abrir el acordeón "Persona Natural".
            acordeon = pagina.locator(
                ".accordion-header, .card-header, .panel-heading, li, div",
            ).filter(has_text=re.compile(r"^\s*Persona Natural\s*$", re.IGNORECASE)).first
            try:
                await acordeon.click(timeout=15000)
                await pagina.wait_for_timeout(4000)
                print("[2] acordeón 'Persona Natural' clickeado")
            except Exception as exc:
                print(f"[2] sin acordeón clicable ({exc})")
            await _dump(pagina, pagina, "paso2_acordeon")
            inv = await _inventario(pagina, "acordeon")

            # Paso 3: el form vive en el iframe de cfiscal.contraloria.gov.co.
            vista = pagina
            for fr in pagina.frames:
                if fr != pagina.main_frame and "cfiscal.contraloria.gov.co" in (fr.url or ""):
                    vista = fr
                    break
            if vista is not pagina:
                await vista.wait_for_load_state("domcontentloaded")
                await pagina.wait_for_timeout(3000)
                print(f"[3] form vive en IFRAME: {vista.url}")
                await _inventario(vista, "iframe")
                (SALIDA / "paso3_iframe.html").write_text(await vista.content(), encoding="utf-8")
            else:
                print("[3] no encontré el iframe de cfiscal: revise paso2_acordeon.html")

            if solo_formulario or not cedula:
                print("[fin] --solo-formulario: estructura volcada en descargas_contraloria/")
                return

            # Paso 4: tipo de documento = Cédula de Ciudadanía + número.
            await vista.select_option("#ddlTipoDocumento", "CC")
            await vista.fill("#txtNumeroDocumento", cedula)
            print(f"[4] formulario: tipo=CC cédula={cedula}")
            await _dump(pagina, vista, "paso4_formulario")

            # Paso 5: reCAPTCHA v2 — sitekey del iframe de Google, solve 2Captcha,
            # inyección del token en el textarea g-recaptcha-response del IFRAME.
            sitekey = ""
            for marco in pagina.frames:
                m = re.search(r"[?&]k=([0-9A-Za-z_-]{20,})", marco.url or "")
                if m and "recaptcha" in (marco.url or ""):
                    sitekey = m.group(1)
                    break
            print(f"[5] sitekey: {sitekey or '(no leído!)'}")
            if not sitekey:
                raise RuntimeError("no pude leer el sitekey del reCAPTCHA")
            token = token_manual or await asyncio.to_thread(
                resolver_recaptcha, sitekey, vista.url,
            )
            await vista.evaluate(
                """(tok) => {
                    const poner = (doc) => {
                        const ta = doc.getElementById('g-recaptcha-response');
                        if (ta) { ta.value = tok; ta.style.display = 'block'; }
                    };
                    poner(document);
                }""", token,
            )
            verif = await vista.evaluate(
                "() => (document.getElementById('g-recaptcha-response') || {}).value || '(textarea NO existe)'"
            )
            print(f"[5] token inyectado: {verif[:30]}… ({len(verif)} chars)")

            # Paso 5b: la encuesta (jumbotronEncuesta) es OBLIGATORIA para el
            # validationGroup reqCertificados: 3 radios Sí/No (Expectativa,
            # Oportunidad, Utilidad). Marcarlas por JS (están ocultas) dispara
            # el estado del validator WebForms.
            await vista.evaluate(
                """() => {
                    const marcar = (id) => {
                        const r = document.getElementById(id);
                        if (r) {
                            r.checked = true;
                            r.dispatchEvent(new Event('change', {bubbles: true}));
                        }
                    };
                    marcar('rbdExpectativa_0'); marcar('rbdOportunidad_0'); marcar('rbdUtilidad_0');
                }"""
            )
            print("[5b] radios de encuesta marcados (Sí)")

            # Paso 6: submit WebForms (#btnBuscar) — postback dentro del iframe.
            try:
                async with pagina.expect_event("load", timeout=15000):
                    await vista.locator("#btnBuscar").click()
            except Exception as exc:
                print(f"[6] (aviso) sin evento load tras click: {type(exc).__name__}")
            await pagina.wait_for_timeout(12000)
            await _dump(pagina, vista, "paso6_resultado")

            cuerpo = " ".join((await vista.inner_text("body")).split())
            print("\n===== RESUMEN DEL DESCUBRIMIENTO =====")
            print(f"URL final: {pagina.url}")
            for leyenda in (
                "no reporta", "no registra", "antecedentes", "responsabilidad fiscal",
                "boletin", "certificado", "no se encuentra", "sin informacion",
                "captcha", "error",
            ):
                m = re.search(re.escape(leyenda) + r".{0,120}", cuerpo, re.IGNORECASE)
                print(f"'{leyenda}': {'SÍ → ' + m.group(0)[:160] if m else 'no'}")
            print(f"\nTexto visible (primeros 1500 chars):\n{cuerpo[:1500]}")
        finally:
            await navegador.close()


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(description="Sonda del portal CGR (antecedentes fiscales por cédula)")
    parser.add_argument("cedula", nargs="?", help="sin --solo-formulario: cédula a consultar")
    parser.add_argument("--solo-formulario", action="store_true", help="Solo estructura, sin submit")
    parser.add_argument("--token", help="Captcha resuelto a mano")
    parser.add_argument("--headless", action="store_true", help="Sin ventana (default: headed)")
    args = parser.parse_args()
    if not args.solo_formulario and not args.token and not CAPTCHA_KEY:
        print("ERROR: falta API_KEY_CAPTCHA en .env — o use --token/--solo-formulario")
        sys.exit(2)
    asyncio.run(main(args.cedula, args.solo_formulario, args.token, headed=not args.headless))
