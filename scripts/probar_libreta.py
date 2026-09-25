"""Sonda exploratoria: estado de situación militar (libreta militar, Ejército).

https://definicion.libretamilitar.mil.co/consulta-estado-situacion-militar

Portal PÚBLICO por diseño (el propio sitio declara: "La información disponible
en esta consulta es de carácter público y no requiere autorización del
titular", art. 10 Ley 1581/2012 — marco legal: Ley 1861/2017, Decreto 977/
2018, Ley 1184/2008). SPA React (el HTML estático viene vacío — carga por JS:
jamás `networkidle`, `domcontentloaded` + espera por selectores).

Incógnitas que resuelve esta sonda: campos del form, captcha, endpoint de la
consulta (¿API directo como SISCONMP?) y shape del resultado (clase de
reservista, estado, distrito, nómina…).

Uso (desde integrappi/):
    python scripts/probar_libreta.py 1010213062 --solo-formulario  # sin submit
    python scripts/probar_libreta.py 1010213062                    # completo
    python scripts/probar_libreta.py 1010213062 --headless         # sin ventana
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

PORTAL = "https://definicion.libretamilitar.mil.co/consulta-estado-situacion-militar"
SALIDA = Path(__file__).resolve().parents[1] / "descargas_libreta"
TIMEOUT_MS = 45000
CAPTCHA_BASE = os.getenv("SEGURIDAD_LIBRETA_CAPTCHA_URL", "https://2captcha.com").rstrip("/")
CAPTCHA_KEY = os.getenv("SEGURIDAD_LIBRETA_CAPTCHA_KEY", "").strip() or os.getenv("API_KEY_CAPTCHA", "").strip()
CAPTCHA_TIMEOUT_S = float(os.getenv("SEGURIDAD_LIBRETA_CAPTCHA_TIMEOUT_S", "90"))


def resolver_captcha_imagen(img_data_url: str) -> str:
    """Captcha de imagen normal vía 2Captcha (method=base64 + poll res.php)."""
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
            print(f"[captcha] resuelto: {dato2['request']!r}")
            return dato2["request"]
        if dato2.get("request") != "CAPCHA_NOT_READY":
            raise RuntimeError(f"res.php error: {dato2.get('request')}")
    raise RuntimeError("timeout esperando el solve")


async def inventario(pagina) -> None:
    """Imprime inputs/botones/captchas visibles — el corazón del descubrimiento."""
    entradas = await pagina.eval_on_selector_all(
        "input:visible, select:visible",
        """els => els.map(e => ({
            tag: e.tagName.toLowerCase(), type: e.type || '', id: e.id || '',
            name: e.name || '', placeholder: e.placeholder || '',
        }))""",
    )
    botones = await pagina.eval_on_selector_all(
        "button:visible, input[type=submit]:visible, input[type=button]:visible, a[role=button]:visible",
        "els => els.map(e => ({ tag: e.tagName.toLowerCase(), id: e.id || '', texto: (e.innerText || e.value || '').trim() }))",
    )
    print(f"[inv] inputs visibles: {entradas}")
    print(f"[inv] botones visibles: {botones}")
    for marco in pagina.frames:
        m = re.search(r"[?&](?:k|sitekey)=([0-9A-Za-z_-]{20,})", marco.url or "")
        if m:
            print(f"[inv] captcha en iframe {marco.url[:90]}… sitekey={m.group(1)}")
            return
    html = await pagina.content()
    m = re.search(r"data-sitekey=['\"]([0-9A-Za-z_-]{20,})", html)
    if m:
        print(f"[inv] captcha reCAPTCHA explícito: {m.group(1)}")
        return
    img = await pagina.evaluate(
        """() => { for (const e of document.images) {
            const s = e.src || '';
            if (s.startsWith('data:image/png') || s.startsWith('data:image/jpeg') || s.startsWith('data:image/gif')) return s.slice(0, 60);
        } return ''; }"""
    )
    if img:
        print(f"[inv] captcha de IMAGEN propia (data:image embebida): {img}…")
    else:
        print("[inv] sin captcha reconocido en el DOM")


async def main(cedula: str, solo_formulario: bool, headed: bool) -> None:
    SALIDA.mkdir(exist_ok=True)
    async with async_playwright() as p:
        navegador = await p.chromium.launch(headless=not headed)
        try:
            contexto = await navegador.new_context(
                viewport={"width": 1366, "height": 900},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
                locale="es-CO",
            )
            pagina = await contexto.new_page()
            # Toda la red del submit (el hallazgo clave del SISCONMP: leer la
            # RESPUESTA AJAX/JSON, no el DOM).
            peticiones: list[tuple] = []

            async def on_response(r):
                url = r.url
                if any(k in url.lower() for k in ("libreta", "situacion", "situación", "consulta", "api")) \
                        and not any(k in url for k in (".js", ".css", ".png", ".svg", ".woff", ".ico")):
                    cuerpo = ""
                    ct = r.headers.get("content-type", "")
                    if "json" in ct or "text" in ct:
                        try:
                            cuerpo = (await r.text())[:800]
                        except Exception:
                            pass
                    peticiones.append((r.status, r.request.method, url[:150], cuerpo[:600]))

            pagina.on("response", on_response)

            # Paso 1: boot del SPA.
            await pagina.goto(PORTAL, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
            await pagina.wait_for_timeout(5000)
            (SALIDA / "paso1_boot.html").write_text(await pagina.content(), encoding="utf-8")
            await pagina.screenshot(path=str(SALIDA / "paso1_boot.png"), full_page=True)
            print(f"[1] SPA cargado: {pagina.url}")
            await inventario(pagina)

            # Paso 2: localizar el input de cédula por heurística.
            entrada = pagina.locator(
                "input[placeholder*='cédula' i], input[placeholder*='cedula' i], input[placeholder*='documento' i], "
                "input[placeholder*='identifica' i], input[placeholder*='número' i], input[placeholder*='numero' i], "
                "input[id*='cedula' i], input[id*='doc' i], input[id*='identifica' i], "
                "input[name*='cedula' i], input[name*='doc' i], input[name*='identifica' i], input[name*='numero' i]"
            ).first
            try:
                await entrada.wait_for(state="visible", timeout=15000)
            except Exception:
                (SALIDA / "paso2_sin_formulario.html").write_text(await pagina.content(), encoding="utf-8")
                await pagina.screenshot(path=str(SALIDA / "paso2_sin_formulario.png"), full_page=True)
                raise RuntimeError(
                    "No se encontró el input de cédula con las heurísticas. Revise paso1_boot.html."
                )
            # El select de tipo doc es OBLIGATORIO para habilitar el botón
            # (queda disabled sin él). Acepta cc/ti; el módulo consulta por CC.
            try:
                await pagina.locator("#tipoDocumento").select_option("cc")
            except Exception as exc:
                print(f"[2] ojo: no se pudo elegir CC en el select: {exc}")
            await entrada.fill(cedula)
            await pagina.wait_for_timeout(800)
            (SALIDA / "paso3_formulario.html").write_text(await pagina.content(), encoding="utf-8")
            await pagina.screenshot(path=str(SALIDA / "paso3_formulario.png"), full_page=True)
            print(f"[2] formulario llenado: cédula={cedula}")
            print(f"    input: {(await entrada.evaluate('e => e.outerHTML.slice(0, 250)'))}")
            otras = await pagina.eval_on_selector_all(
                "input:visible, select:visible",
                "els => els.filter(e => !/cedula|documento|identifica|numero|número/i.test(e.placeholder + ' ' + e.id + ' ' + e.name)).map(e => ({tag: e.tagName.toLowerCase(), id: e.id, name: e.name, placeholder: e.placeholder, type: e.type}))",
            )
            print(f"    OTROS inputs visibles: {otras}")

            if solo_formulario:
                print("[fin] --solo-formulario: sin submit. Dumps en descargas_libreta/")
                return

            # Paso 3: captcha de imagen si lo hay (best-effort; el v3 nativo no
            # necesita nada — lección SISCONMP).
            # Solo PNG/JPEG/GIF embebidos son captcha; los SVG data: son
            # ICONOS decorativos (trampa 2026-09-25: el logo govco es svg).
            img = await pagina.evaluate(
                """() => { for (const e of document.images) {
                    const s = e.src || '';
                    if (s.startsWith('data:image/png') || s.startsWith('data:image/jpeg') || s.startsWith('data:image/gif')) return s;
                } return ''; }"""
            )
            if img:
                import base64
                (SALIDA / f"captcha_{cedula}.png").write_bytes(base64.b64decode(img.split(",", 1)[-1]))
                if CAPTCHA_KEY:
                    token = await asyncio.to_thread(resolver_captcha_imagen, img)
                    campo = pagina.locator(
                        "input[placeholder*='captcha' i], input[id*='captcha' i], input[name*='captcha' i]"
                    ).first
                    await campo.wait_for(state="visible", timeout=8000)
                    await campo.fill(token)
                    print("[3] captcha de imagen resuelto")
                else:
                    print("[3] hay captcha de imagen pero falta API_KEY_CAPTCHA — submit directo")

            # Paso 4: submit — cualquier botón con texto razonable.
            boton = pagina.get_by_role("button", name=re.compile("consultar|buscar|verificar|enviar", re.IGNORECASE)).first
            try:
                await boton.click(timeout=10000)
            except Exception:
                (SALIDA / "paso4_sin_boton.html").write_text(await pagina.content(), encoding="utf-8")
                raise RuntimeError("No se encontró el botón Consultar (revise paso3_formulario.html)")
            print("[4] submit enviado; esperando 20 s…")
            await pagina.wait_for_timeout(20000)
            (SALIDA / "paso5_resultado.html").write_text(await pagina.content(), encoding="utf-8")
            await pagina.screenshot(path=str(SALIDA / "paso5_resultado.png"), full_page=True)

            print(f"\n===== RESPUESTAS DE RED (las relevantes) =====")
            for peticion in peticiones:
                print(" ", peticion)
            cuerpo = " ".join((await pagina.inner_text("body")).split())
            print(f"\nTexto visible (primeros 2000 chars):\n{cuerpo[:2000]}")
            print("\nDumps en descargas_libreta/")
        finally:
            await navegador.close()


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(description="Sonda del portal de situación militar (libreta militar)")
    parser.add_argument("cedula")
    parser.add_argument("--solo-formulario", action="store_true", help="Llega al formulario sin submit")
    parser.add_argument("--headless", action="store_true", help="Sin ventana (default: headed)")
    args = parser.parse_args()
    asyncio.run(main(args.cedula, args.solo_formulario, headed=not args.headless))
