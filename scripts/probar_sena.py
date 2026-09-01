"""Sonda exploratoria: certificados de formación del SENA (Certificado Digital).

https://certificados.sena.edu.co/CertificadoDigital/com.sena.consultacer

Portal JSF server-rendered (misma familia que el de la Policía: interactuar
por clicks, JAMÁS POSTs manuales). Formulario: radio "Consultar por:
Documento" + select Tipo de Documento + Número de Documento + captcha de
IMAGEN propia (GIF embebido como data:image — mecanismo RUNT, 2Captcha
method=base64) + botón "Refrescar Texto". El resultado es un listado de
certificados descargables; el vacío renderiza un label AJAX "No hay
resultados". Los selectores exactos se DESCUBREN con esta sonda (dumps paso
a paso en descargas_sena/) — de aquí salen los definitivos de
Funciones/bot_sena.py.

Uso (desde integrappi/):
    python scripts/probar_sena.py 1033688842 --solo-formulario  # sin captcha: valida estructura
    python scripts/probar_sena.py 1033688842                    # completo (gasta 1 solve de 2Captcha)
    python scripts/probar_sena.py 1033688842 --token TOKEN      # captcha resuelto a mano
    python scripts/probar_sena.py 1033688842 --headless         # sin ventana
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

PORTAL = "https://certificados.sena.edu.co/CertificadoDigital/com.sena.consultacer"
SALIDA = Path(__file__).resolve().parents[1] / "descargas_sena"
TIMEOUT_MS = 45000
CAPTCHA_BASE = os.getenv("SEGURIDAD_SENA_CAPTCHA_URL", "https://2captcha.com").rstrip("/")
# API_KEY_CAPTCHA es la key ya usada por los demás bots del proyecto.
CAPTCHA_KEY = os.getenv("SEGURIDAD_SENA_CAPTCHA_KEY", "").strip() or os.getenv("API_KEY_CAPTCHA", "").strip()
CAPTCHA_TIMEOUT_S = float(os.getenv("SEGURIDAD_SENA_CAPTCHA_TIMEOUT_S", "90"))


def resolver_captcha_imagen(img_data_url: str) -> str:
    """Captcha de imagen normal vía 2Captcha (method=base64 + poll res.php).

    El portal embebe el GIF como data:image — no hay sitekey ni iframe: se
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


async def inventario(pagina) -> None:
    """Imprime inputs/selects/botones/radios visibles — el corazón del descubrimiento."""
    entradas = await pagina.eval_on_selector_all(
        "input:visible, select:visible",
        """els => els.map(e => ({
            tag: e.tagName.toLowerCase(),
            type: e.type || '',
            id: e.id || '',
            name: e.name || '',
            placeholder: e.placeholder || '',
            value: (e.type === 'radio' || e.tagName === 'SELECT') ? (e.value || '') : '',
            maxlength: e.maxLength && e.maxLength > 0 ? e.maxLength : null,
        }))""",
    )
    botones = await pagina.eval_on_selector_all(
        "button:visible, input[type=submit]:visible, input[type=button]:visible, a[role=button]:visible",
        "els => els.map(e => ({ tag: e.tagName.toLowerCase(), id: e.id || '', name: e.name || '', texto: (e.innerText || e.value || '').trim() }))",
    )
    print(f"[inv] inputs/selects visibles: {entradas}")
    print(f"[inv] botones visibles: {botones}")
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
        print("[inv] sin captcha de imagen en el DOM (¿reCAPTCHA/iframe?)")


async def _guardar_captcha(pagina, cedula: str) -> str:
    """Extrae la imagen del captcha (única data:image del DOM) y la guarda."""
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
        ext = "gif" if "image/gif" in img[:40] else "png"
        (SALIDA / f"captcha_{cedula}.{ext}").write_bytes(base64.b64decode(b64))
        print(f"[captcha-img] imagen guardada: descargas_sena/captcha_{cedula}.{ext} ({len(b64) // 1024} KB b64)")
    return img


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

            # Paso 1: carga (JSF server-rendered; domcontentloaded + render).
            await pagina.goto(PORTAL, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
            await pagina.wait_for_timeout(4000)
            (SALIDA / "paso1_boot.html").write_text(await pagina.content(), encoding="utf-8")
            await pagina.screenshot(path=str(SALIDA / "paso1_boot.png"), full_page=True)
            print(f"[1] portal cargado: {pagina.url}")
            await inventario(pagina)

            # Paso 2: modo "Documento" (radio) + tipo CC + número.
            radio_doc = pagina.locator(
                "input[type=radio][value*='documento' i], input[type=radio][id*='documento' i], "
                "input[type=radio][name*='documento' i]"
            ).first
            if await radio_doc.count():
                await radio_doc.check()
                await pagina.wait_for_timeout(1200)  # postback JSF al cambiar el radio
                print("[2] radio 'Documento' marcado (postback)")
            else:
                print("[2] sin radio visible: el form ya está en modo documento")

            select_tipo = pagina.locator("select:visible").first
            if await select_tipo.count():
                opciones = await select_tipo.locator("option").all_inner_texts()
                print(f"    select tipo documento: {opciones[:12]}…")
                try:
                    await select_tipo.select_option(label=re.compile(r"CEDULA DE CIUDADANIA", re.IGNORECASE))
                    await pagina.wait_for_timeout(800)
                except Exception as exc:
                    print(f"    [warn] no pude elegir CC: {exc}")
            entrada_doc = pagina.locator(
                "input[placeholder*='documento' i], input[id*='documento' i], input[name*='documento' i], "
                "input[placeholder*='numero' i], input[id*='numero' i], input[name*='numero' i]"
            ).first
            try:
                await entrada_doc.wait_for(state="visible", timeout=15000)
            except Exception:
                (SALIDA / "paso2_sin_formulario.html").write_text(await pagina.content(), encoding="utf-8")
                await pagina.screenshot(path=str(SALIDA / "paso2_sin_formulario.png"), full_page=True)
                raise RuntimeError(
                    "No se encontró el input del número de documento con las heurísticas. "
                    "Revise paso1_boot.html (ids JSF reales)."
                )
            await entrada_doc.fill(cedula)
            await pagina.wait_for_timeout(800)
            (SALIDA / "paso3_formulario.html").write_text(await pagina.content(), encoding="utf-8")
            await pagina.screenshot(path=str(SALIDA / "paso3_formulario.png"), full_page=True)
            print(f"[2] formulario llenado: cedula={cedula}")
            print(f"    doc input: {(await entrada_doc.evaluate('e => e.outerHTML.slice(0, 200)'))}")
            otras = await pagina.eval_on_selector_all(
                "input:visible",
                "els => els.filter(e => !/documento|numero/i.test(e.placeholder + ' ' + e.id + ' ' + e.name)).map(e => ({id: e.id, name: e.name, placeholder: e.placeholder, type: e.type}))",
            )
            print(f"    OTROS inputs visibles (captcha/etc): {otras}")

            if solo_formulario:
                await _guardar_captcha(pagina, cedula)
                print("[fin] --solo-formulario: sin submit. Dumps en descargas_sena/")
                return

            # Paso 3: captcha de imagen (data:image) → 2Captcha base64; --token manual.
            captcha_texto: str = token_manual or ""
            if not captcha_texto:
                img = await _guardar_captcha(pagina, cedula)
                if not img:
                    (SALIDA / "paso3_sin_captcha.html").write_text(await pagina.content(), encoding="utf-8")
                    raise RuntimeError("No se encontró la imagen del captcha (revise paso3_formulario.html)")
                captcha_texto = await asyncio.to_thread(resolver_captcha_imagen, img)
            campo = pagina.locator(
                "input[placeholder*='texto' i], input[id*='captcha' i], input[name*='captcha' i], "
                "input[id*='texto' i], input[name*='texto' i]"
            ).first
            await campo.wait_for(state="visible", timeout=8000)
            await campo.fill(captcha_texto)
            print("[3] captcha diligenciado")

            # Paso 4: submit "Consultar" (JSF commandButton — click, jamás POST manual).
            boton = pagina.locator(
                "input[type=submit], input[type=button], button"
            ).filter(has_text=re.compile("consultar", re.IGNORECASE)).first
            if not await boton.count():
                boton = pagina.get_by_role("button", name=re.compile("consultar|buscar", re.IGNORECASE)).first
            try:
                await boton.click(timeout=10000)
            except Exception:
                (SALIDA / "paso4_sin_boton.html").write_text(await pagina.content(), encoding="utf-8")
                raise RuntimeError("No se encontró el botón Consultar (revise paso3_formulario.html)")
            # Postback JSF: esperar el render del resultado o el "No hay resultados".
            await pagina.wait_for_timeout(10000)
            (SALIDA / "paso5_resultado.html").write_text(await pagina.content(), encoding="utf-8")
            await pagina.screenshot(path=str(SALIDA / "paso5_resultado.png"), full_page=True)

            cuerpo = " ".join((await pagina.inner_text("body")).split())
            print("\n===== RESUMEN DEL DESCUBRIMIENTO =====")
            print(f"URL final: {pagina.url}")
            for leyenda in (
                "no hay resultados", "certificado", "programa", "nivel",
                "centro de formacion", "competencia", "no registra",
                "no se encuentra", "sin informacion", "adobe",
            ):
                m = re.search(re.escape(leyenda) + r".{0,120}", cuerpo, re.IGNORECASE)
                print(f"'{leyenda}': {'SÍ → ' + m.group(0)[:160] if m else 'no'}")
            print(f"\nTexto visible (primeros 1500 chars):\n{cuerpo[:1500]}")
            print("\nDumps en descargas_sena/ (paso5_resultado.html es la página de resultado).")
        finally:
            await navegador.close()


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(description="Sonda del portal SENA (certificados por cédula)")
    parser.add_argument("cedula")
    parser.add_argument("--solo-formulario", action="store_true", help="Llega al formulario sin submit (sin gastar captcha)")
    parser.add_argument("--token", help="Captcha resuelto a mano")
    parser.add_argument("--headless", action="store_true", help="Sin ventana (default: headed)")
    args = parser.parse_args()
    if not args.solo_formulario and not args.token and not CAPTCHA_KEY:
        print("ERROR: falta SEGURIDAD_SENA_CAPTCHA_KEY (o API_KEY_CAPTCHA) en .env — o use --token/--solo-formulario")
        sys.exit(2)
    asyncio.run(main(args.cedula, args.solo_formulario, args.token, headed=not args.headless))
