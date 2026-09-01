"""Sonda exploratoria: estado de cuenta de comparendos SIMIT en el portal público de la FCM.

https://www.fcm.org.co/simit/#/estado-cuenta

SPA con HASH routing (misma familia que el RUNT): NUNCA esperar `networkidle`
(la app mantiene conexiones abiertas). Se carga con `domcontentloaded` y se
espera el render por selectores. Los selectores exactos se DESCUBREN con esta
sonda (dumps paso a paso en descargas_simit/) — de aquí salen los definitivos
de Funciones/bot_simit.py.

El portal pide SOLO LA PLACA (sin cédula, sin propietario) y devuelve el
estado de cuenta: comparendos y saldos pendientes. El tipo de captcha (imagen
propia data:image o reCAPTCHA) es una de las incógnitas que resuelve esta
sonda.

Uso (desde integrappi/):
    python scripts/probar_simit.py MVX48E --solo-formulario  # sin captcha: valida estructura
    python scripts/probar_simit.py MVX48E                    # completo (gasta 1 solve de 2Captcha)
    python scripts/probar_simit.py MVX48E --token TOKEN      # captcha resuelto a mano
    python scripts/probar_simit.py MVX48E --headless         # sin ventana
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

PORTAL = "https://www.fcm.org.co/simit/#/estado-cuenta"
SALIDA = Path(__file__).resolve().parents[1] / "descargas_simit"
TIMEOUT_MS = 45000
CAPTCHA_BASE = os.getenv("SEGURIDAD_SIMIT_CAPTCHA_URL", "https://2captcha.com").rstrip("/")
# API_KEY_CAPTCHA es la key ya usada por los demás bots del proyecto.
CAPTCHA_KEY = os.getenv("SEGURIDAD_SIMIT_CAPTCHA_KEY", "").strip() or os.getenv("API_KEY_CAPTCHA", "").strip()
CAPTCHA_TIMEOUT_S = float(os.getenv("SEGURIDAD_SIMIT_CAPTCHA_TIMEOUT_S", "90"))


def resolver_captcha_imagen(img_data_url: str) -> str:
    """Captcha de imagen normal vía 2Captcha (method=base64 + poll res.php).

    El portal embebe el PNG como data:image — no hay sitekey ni iframe: se
    envía el base64 directo. ~US$0.001 por solve."""
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
        "button:visible, input[type=submit]:visible, a[role=button]:visible",
        "els => els.map(e => ({ tag: e.tagName.toLowerCase(), id: e.id || '', texto: (e.innerText || e.value || '').trim() }))",
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


async def main(placa: str, solo_formulario: bool, token_manual: str | None, headed: bool) -> None:
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

            # Paso 1: boot del SPA (hash routing → domcontentloaded, JAMÁS networkidle)
            await pagina.goto(PORTAL, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
            await pagina.wait_for_timeout(4000)  # render SPA
            (SALIDA / "paso1_boot.html").write_text(await pagina.content(), encoding="utf-8")
            await pagina.screenshot(path=str(SALIDA / "paso1_boot.png"), full_page=True)
            print(f"[1] SPA cargado: {pagina.url}")
            await inventario(pagina)

            # Paso 2: localizar el input de placa por heurística.
            entrada_placa = pagina.locator(
                "input[placeholder*='placa' i], input[id*='placa' i], input[name*='placa' i], "
                "input[formcontrolname*='placa' i], input[ng-model*='placa' i], input[data-ng-model*='placa' i]"
            ).first
            try:
                await entrada_placa.wait_for(state="visible", timeout=15000)
            except Exception:
                (SALIDA / "paso2_sin_formulario.html").write_text(await pagina.content(), encoding="utf-8")
                await pagina.screenshot(path=str(SALIDA / "paso2_sin_formulario.png"), full_page=True)
                raise RuntimeError(
                    "No se encontró el input de placa con las heurísticas. "
                    "Revise paso1_boot.html: quizá hay una pantalla previa (términos/selección/login)."
                )
            await entrada_placa.fill(placa.upper())
            await pagina.wait_for_timeout(800)
            (SALIDA / "paso3_formulario.html").write_text(await pagina.content(), encoding="utf-8")
            await pagina.screenshot(path=str(SALIDA / "paso3_formulario.png"), full_page=True)
            print(f"[2] formulario llenado: placa={placa.upper()}")
            print(f"    placa input: {(await entrada_placa.evaluate('e => e.outerHTML.slice(0, 200)'))}")
            # ¿Pide algo más (cédula/captcha)? Es un hallazgo de la sonda.
            otras = await pagina.eval_on_selector_all(
                "input:visible",
                "els => els.filter(e => !/placa/i.test(e.placeholder + ' ' + e.id + ' ' + e.name)).map(e => ({id: e.id, name: e.name, placeholder: e.placeholder, type: e.type}))",
            )
            print(f"    OTROS inputs visibles (cédula/captcha/etc): {otras}")

            if solo_formulario:
                # Guardar la imagen del captcha si la hay (data:image/png;base64):
                # de aquí sale la estrategia de solve.
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
                    (SALIDA / f"captcha_{placa}.png").write_bytes(__import__("base64").b64decode(b64))
                    print(f"[captcha-img] imagen guardada: descargas_simit/captcha_{placa}.png ({len(b64)//1024} KB b64)")
                print("[fin] --solo-formulario: sin submit. Dumps en descargas_simit/")
                return

            # Paso 3: captcha. Se prueba IMAGEN propia primero (como RUNT); si hay
            # reCAPTCHA se resuelve por sitekey (como Policía). --token para manual.
            captcha_texto: str = token_manual or ""
            if not captcha_texto:
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
                    (SALIDA / f"captcha_{placa}.png").write_bytes(__import__("base64").b64decode(b64))
                    captcha_texto = await asyncio.to_thread(resolver_captcha_imagen, img)
                    campo = pagina.locator(
                        "input[placeholder*='captcha' i], input[id*='captcha' i], input[name*='captcha' i], "
                        "input[formcontrolname*='captcha' i], input[ng-model*='captcha' i]"
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

            # Paso 4: submit. El botón real es #btnNumDocPlaca (solo icono, sin
            # texto accesible — descubierto con el inventario del paso 1).
            boton = pagina.locator("#btnNumDocPlaca").first
            if not await boton.count():
                boton = pagina.get_by_role("button", name=re.compile("consultar|buscar|verificar", re.IGNORECASE)).first
            try:
                await boton.click(timeout=10000)
            except Exception:
                (SALIDA / "paso4_sin_boton.html").write_text(await pagina.content(), encoding="utf-8")
                raise RuntimeError("No se encontró el botón Consultar/Buscar (revise paso3_formulario.html)")
            # El resultado se renderiza client-side: esperar contenido nuevo.
            await pagina.wait_for_timeout(8000)
            (SALIDA / "paso5_resultado.html").write_text(await pagina.content(), encoding="utf-8")
            await pagina.screenshot(path=str(SALIDA / "paso5_resultado.png"), full_page=True)

            cuerpo = " ".join((await pagina.inner_text("body")).split())
            print("\n===== RESUMEN DEL DESCUBRIMIENTO =====")
            print(f"URL final: {pagina.url}")
            for leyenda in (
                "comparendo", "saldo", "deuda", "no registra", "no tiene",
                "no se encuentra", "sin información", "pagado", "estado de cuenta",
            ):
                m = re.search(re.escape(leyenda) + r".{0,120}", cuerpo, re.IGNORECASE)
                print(f"'{leyenda}': {'SÍ → ' + m.group(0)[:160] if m else 'no'}")
            print(f"\nTexto visible (primeros 1500 chars):\n{cuerpo[:1500]}")
            print("\nDumps en descargas_simit/ (paso5_resultado.html es la página de resultado).")
        finally:
            await navegador.close()


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(description="Sonda del portal SIMIT de la FCM (estado de cuenta por placa)")
    parser.add_argument("placa")
    parser.add_argument("--solo-formulario", action="store_true", help="Llega al formulario sin submit (sin gastar captcha)")
    parser.add_argument("--token", help="Captcha/token resuelto a mano")
    parser.add_argument("--headless", action="store_true", help="Sin ventana (default: headed)")
    args = parser.parse_args()
    if not args.solo_formulario and not args.token and not CAPTCHA_KEY:
        print("ERROR: falta SEGURIDAD_SIMIT_CAPTCHA_KEY (o API_KEY_CAPTCHA) en .env — o use --token/--solo-formulario")
        sys.exit(2)
    asyncio.run(main(args.placa.upper(), args.solo_formulario, args.token, headed=not args.headless))
