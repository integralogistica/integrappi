"""Sonda exploratoria: capacitaciones de mercancía peligrosa SISCONMP (Mintransporte).

https://web.mintransporte.gov.co/sisconmp2/consultascapacitaciones/

Portal ASP.NET MVC (bootstrap + select2 — MISMA familia tecnológica que
rndc2/SICETAC de Mintransporte). Consulta por CÉDULA del conductor: devuelve
las capacitaciones/cursos de mercancía peligrosa (TMR — Transporte de
Mercancías Peligrosas). Incógnitas que resuelve esta sonda: campos del form,
captcha (imagen propia / reCAPTCHA / nada) y shape del resultado.

Uso (desde integrappi/):
    python scripts/probar_sisconmp.py 1010213062 --solo-formulario  # sin gastar
    python scripts/probar_sisconmp.py 1010213062                    # completo
    python scripts/probar_sisconmp.py 1010213062 --token TOKEN      # captcha a mano
    python scripts/probar_sisconmp.py 1010213062 --headless         # sin ventana
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

PORTAL = "https://web.mintransporte.gov.co/sisconmp2/consultascapacitaciones/"
SALIDA = Path(__file__).resolve().parents[1] / "descargas_sisconmp"
TIMEOUT_MS = 45000
CAPTCHA_BASE = os.getenv("SEGURIDAD_SISCONMP_CAPTCHA_URL", "https://2captcha.com").rstrip("/")
# API_KEY_CAPTCHA es la key ya usada por los demás bots del proyecto.
CAPTCHA_KEY = os.getenv("SEGURIDAD_SISCONMP_CAPTCHA_KEY", "").strip() or os.getenv("API_KEY_CAPTCHA", "").strip()
CAPTCHA_TIMEOUT_S = float(os.getenv("SEGURIDAD_SISCONMP_CAPTCHA_TIMEOUT_S", "90"))


def resolver_captcha_imagen(img_data_url: str) -> str:
    """Captcha de imagen normal vía 2Captcha (method=base64 + poll res.php)."""
    import base64
    import time

    import requests

    b64 = img_data_url.split(",", 1)[-1]
    r = requests.post(f"{CAPTCHA_BASE}/in.php", data={
        "key": CAPTCHA_KEY, "method": "base64",
        "body": b64, "json": 1,
    }, timeout=30)
    dato = r.json()
    if dato.get("status") != 1:
        raise RuntimeError(f"in.php rechazó el pedido: {dato.get('request')}")
    captcha_id = dato["request"]
    print(f"[captcha] pedido {captcha_id}; sondeando cada 5 s (máx {CAPTCHA_TIMEOUT_S:.0f} s)…")

    fin = time.monotonic() + CAPTCHA_TIMEOUT_S
    while time.monotonic() < fin:
        time.sleep(5)
        r2 = requests.get(f"{CAPTCHA_BASE}/res.php", params={
            "key": CAPTCHA_KEY, "action": "get", "id": captcha_id, "json": 1,
        }, timeout=30)
        dato2 = r2.json()
        if dato2.get("status") == 1:
            token = dato2["request"]
            print(f"[captcha] resuelto: {token!r}")
            return token
        if dato2.get("request") != "CAPCHA_NOT_READY":
            raise RuntimeError(f"res.php error: {dato2.get('request')}")
    raise RuntimeError("timeout esperando el solve")


def resolver_recaptcha(sitekey: str, url_pagina: str) -> str:
    """Solve vía 2Captcha (in.php userrecaptcha + poll res.php cada 5 s)."""
    import time

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


async def inventario(pagina) -> None:
    """Imprime inputs/botones/captchas visibles — el corazón del descubrimiento."""
    entradas = await pagina.eval_on_selector_all(
        "input:visible, select:visible",
        """els => els.map(e => ({
            tag: e.tagName.toLowerCase(),
            type: e.type || '',
            id: e.id || '',
            name: e.name || '',
            placeholder: e.placeholder || '',
            maxlength: e.maxLength && e.maxLength > 0 ? e.maxLength : null,
        }))""",
    )
    botones = await pagina.eval_on_selector_all(
        "button:visible, input[type=submit]:visible, input[type=button]:visible, a[role=button]:visible",
        "els => els.map(e => ({ tag: e.tagName.toLowerCase(), id: e.id || '', name: e.name || '', texto: (e.innerText || e.value || '').trim() }))",
    )
    print(f"[inv] inputs visibles: {entradas}")
    print(f"[inv] botones visibles: {botones}")
    # Captcha: iframe de reCAPTCHA (k=) o hCaptcha, o componente propio.
    for marco in pagina.frames:
        m = re.search(r"[?&](?:k|sitekey)=([0-9A-Za-z_-]{20,})", marco.url or "")
        if m:
            print(f"[inv] captcha en iframe {marco.url[:90]}… sitekey={m.group(1)}")
            return
    html = await pagina.content()
    for proveedor, patron in (
        ("reCAPTCHA", r"recaptcha/api\.js\?render=([0-9A-Za-z_-]{20,})"),
        ("reCAPTCHA-explicit", r"data-sitekey=['\"]([0-9A-Za-z_-]{20,})"),
        ("hCaptcha", r"hcaptcha\.com"),
    ):
        m = re.search(patron, html)
        if m:
            print(f"[inv] captcha {proveedor}: {m.group(1) if m.groups() else 'presente'}")
            return
    img = await pagina.evaluate(
        """() => {
            for (const e of document.images) {
                if ((e.src || '').startsWith('data:image')) return e.src.slice(0, 60);
            }
            return '';
        }"""
    )
    if img:
        print(f"[inv] captcha de IMAGEN propia (data:image embebida): {img}…")
    else:
        print("[inv] sin captcha reconocido en el DOM (¿slider/puzzle propio?)")


async def main(cedula: str, solo_formulario: bool, token_manual: str | None, headed: bool) -> None:
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

            # Paso 1: carga (MVC server-rendered, pero con select2/ajax — dar margen)
            await pagina.goto(PORTAL, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
            await pagina.wait_for_timeout(4000)
            (SALIDA / "paso1_boot.html").write_text(await pagina.content(), encoding="utf-8")
            await pagina.screenshot(path=str(SALIDA / "paso1_boot.png"), full_page=True)
            print(f"[1] portal cargado: {pagina.url}")
            await inventario(pagina)

            # Paso 2: localizar el input de cédula por heurística.
            entrada = pagina.locator(
                "input[placeholder*='cédula' i], input[placeholder*='cedula' i], input[placeholder*='documento' i], "
                "input[id*='cedula' i], input[id*='documento' i], input[id*='identifica' i], "
                "input[name*='cedula' i], input[name*='documento' i], input[name*='identifica' i]"
            ).first
            try:
                await entrada.wait_for(state="visible", timeout=15000)
            except Exception:
                (SALIDA / "paso2_sin_formulario.html").write_text(await pagina.content(), encoding="utf-8")
                await pagina.screenshot(path=str(SALIDA / "paso2_sin_formulario.png"), full_page=True)
                raise RuntimeError(
                    "No se encontró el input de cédula con las heurísticas. "
                    "Revise paso1_boot.html: quizá hay una pantalla previa (términos/selección/login)."
                )
            await entrada.fill(cedula)
            await pagina.wait_for_timeout(800)
            (SALIDA / "paso3_formulario.html").write_text(await pagina.content(), encoding="utf-8")
            await pagina.screenshot(path=str(SALIDA / "paso3_formulario.png"), full_page=True)
            print(f"[2] formulario llenado: cédula={cedula}")
            print(f"    cédula input: {(await entrada.evaluate('e => e.outerHTML.slice(0, 250)'))}")
            otras = await pagina.eval_on_selector_all(
                "input:visible",
                "els => els.filter(e => !/cedula|documento|identifica/i.test(e.placeholder + ' ' + e.id + ' ' + e.name)).map(e => ({id: e.id, name: e.name, placeholder: e.placeholder, type: e.type}))",
            )
            print(f"    OTROS inputs visibles (captcha/selects/etc): {otras}")

            if solo_formulario:
                import base64
                img = await pagina.evaluate(
                    """() => {
                        for (const e of document.images) {
                            if ((e.src || '').startsWith('data:image')) return e.src;
                        }
                        return '';
                    }"""
                )
                if img:
                    b64 = img.split(",", 1)[-1]
                    (SALIDA / f"captcha_{cedula}.png").write_bytes(base64.b64decode(b64))
                    print(f"[captcha-img] imagen guardada: descargas_sisconmp/captcha_{cedula}.png")
                print("[fin] --solo-formulario: sin submit. Dumps en descargas_sisconmp/")
                return

            # Paso 3: captcha. HALLAZGO 2026-09-25: el portal usa reCAPTCHA
            # **v3 invisible** (api.js?render=… + grecaptcha.execute con
            # action 'consultar_conductor') que la PROPIA página resuelve —
            # NADA que pagar a 2Captcha (la primera corrida gastó un solve
            # inútil: el token inyectado no participa del flujo). Solo si un
            # día apareciera captcha de imagen/reCAPTCHA v2 se resuelve aquí.
            html = await pagina.content()
            es_v3 = bool(re.search(r"recaptcha/api\.js\?render=", html))
            captcha_texto: str = token_manual or ""
            if es_v3 and not captcha_texto:
                print("[3] reCAPTCHA v3 invisible: lo ejecuta la propia página ($0)")
            elif not captcha_texto:
                img = await pagina.evaluate(
                    """() => {
                        for (const e of document.images) {
                            if ((e.src || '').startsWith('data:image')) return e.src;
                        }
                        return '';
                    }"""
                )
                if img:
                    import base64
                    b64 = img.split(",", 1)[-1]
                    (SALIDA / f"captcha_{cedula}.png").write_bytes(base64.b64decode(b64))
                    captcha_texto = await asyncio.to_thread(resolver_captcha_imagen, img)
                    campo = pagina.locator(
                        "input[placeholder*='captcha' i], input[id*='captcha' i], input[name*='captcha' i]"
                    ).first
                    await campo.wait_for(state="visible", timeout=8000)
                    await campo.fill(captcha_texto)
                    print("[3] captcha de imagen resuelto")
                else:
                    sitekey = ""
                    html = await pagina.content()
                    m = re.search(r"data-sitekey=['\"]([0-9A-Za-z_-]{20,})", html)
                    if not m:
                        for marco in pagina.frames:
                            mm = re.search(r"[?&](?:k|sitekey)=([0-9A-Za-z_-]{20,})", marco.url or "")
                            if mm:
                                m = mm
                                break
                    if m:
                        sitekey = m.group(1)
                        token = await asyncio.to_thread(resolver_recaptcha, sitekey, pagina.url)
                        await pagina.evaluate(
                            "t => { const e = document.getElementById('g-recaptcha-response');"
                            " if (e) { e.style.display='none'; e.value = t; } }",
                            token,
                        )
                        print(f"[3] reCAPTCHA resuelto (sitekey {sitekey[:12]}…)")
                    else:
                        (SALIDA / "paso3_sin_captcha.html").write_text(html, encoding="utf-8")
                        print("[3] SIN captcha reconocido: se intenta submit directo (hallazgo a confirmar)")

            # Paso 4: submit. El "botón" es un <span> con onclick (no un <button>);
            # el reCAPTCHA v3 lo ejecuta la PROPIA página (gratis, invisible):
            # no hay nada que resolver — solo clic y leer.
            respuestas = []
            pagina.on("response", lambda r: respuestas.append((r.status, r.url[-70:]))
                      if "ConsultarConductor" in r.url else None)
            boton = pagina.locator("#btnConsultarMD, #btnConsultarXS").first
            try:
                await boton.click(timeout=10000)
            except Exception:
                (SALIDA / "paso4_sin_boton.html").write_text(await pagina.content(), encoding="utf-8")
                raise RuntimeError("No se encontró el botón Consultar (revise paso3_formulario.html)")
            # Resultado Ajax: paneles en #divResultados, #divNoHayRegistros si
            # está vacío, o NADA si el server rechazó el token (403 → spinner).
            try:
                await pagina.wait_for_selector(
                    "#divResultados .panel, #divNoHayRegistros:visible", timeout=30000)
            except Exception:
                print(f"[4] sin resultado tras 30 s — respuestas AJAX: {respuestas}"
                      " (¿403 = token v3 rechazado en Chromium?)")
            await pagina.wait_for_timeout(1500)
            (SALIDA / "paso5_resultado.html").write_text(await pagina.content(), encoding="utf-8")
            await pagina.screenshot(path=str(SALIDA / "paso5_resultado.png"), full_page=True)
            print(f"[4] respuestas AJAX: {respuestas}")

            cuerpo = " ".join((await pagina.inner_text("body")).split())
            print("\n===== RESUMEN DEL DESCUBRIMIENTO =====")
            print(f"URL final: {pagina.url}")
            for leyenda in (
                "capacitación", "curso", "no registra", "no tiene", "no se encuentra",
                "sin información", "vigencia", "vence", "certificado", "conductor",
            ):
                m = re.search(re.escape(leyenda) + r".{0,120}", cuerpo, re.IGNORECASE)
                print(f"'{leyenda}': {'SÍ → ' + m.group(0)[:160] if m else 'no'}")
            print(f"\nTexto visible (primeros 2000 chars):\n{cuerpo[:2000]}")
            print("\nDumps en descargas_sisconmp/ (paso5_resultado.html es la página de resultado).")
        finally:
            await navegador.close()


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(description="Sonda del portal SISCONMP (capacitaciones MP por cédula)")
    parser.add_argument("cedula")
    parser.add_argument("--solo-formulario", action="store_true", help="Llega al formulario sin submit (sin gastar captcha)")
    parser.add_argument("--token", help="Captcha/token resuelto a mano")
    parser.add_argument("--headless", action="store_true", help="Sin ventana (default: headed)")
    args = parser.parse_args()
    # El portal usa reCAPTCHA v3 que la propia página ejecuta: NO se necesita
    # key de 2Captcha para la corrida completa (la key solo aplicaría si el
    # portal migrara algún día a un captcha visible).
    asyncio.run(main(args.cedula, args.solo_formulario, args.token, headed=not args.headless))
