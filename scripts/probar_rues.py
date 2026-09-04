"""Sonda del RUES (Registro Único Empresarial, Confecámaras) por NIT.

A diferencia de las demás fuentes, el RUES NO necesita navegador: la SPA de
www.rues.org.co consulta un API Elasticsearch (elasticprd.rues.org.co) con
payloads cifrados (CryptoJS AES, passphrase embebida — ver Funciones/bot_rues.py).
Esta sonda replica el cifrado y consulta el API directamente ($0, sin captcha).

Modos:
    python scripts/probar_rues.py 901923029                # consulta viva + resumen
    python scripts/probar_rues.py 901923029 --estructura   # además imprime el contrato descifrado
    python scripts/probar_rues.py 901923029 --descubrir-endpoints
        # abre el portal con Playwright (UA real: el CDN bloquea HeadlessChrome),
        # recorre las pestañas del detalle y descifra TODOS los payloads que
        # pasan por elasticprd — para descubrir endpoints nuevos (dumps en
        # descargas_rues/).

Hallazgos calibrados con esta sonda (2026-09-03):
    - POST /query con {"term": NIT, "offset": 0, "type": 2, "filter": {...}}.
    - POST /api/Expediente/DetalleRM con {"id": <codigo_camara+matricula>}.
    - POST /api/ConsultFacultadesXCamYMatricula con {"codigo_camara", "matricula"}
      → HTML plano del representante legal ("1010213062 - ZARATE PEÑA ...").
    - Trampa: un NIT inexistente devuelve total=10000 (lista TODO): solo vale
      el hit con coincidencia EXACTA de numero_identificacion (padding de ceros).
    - Trampa: el filtro default Status=["ACTIVA"] OCULTA canceladas → enviar
      ["ACTIVA", "CANCELADA"] y leer el estado real del hit.
    - El buscador acepta términos de hasta 10 caracteres (11+ → HTTP 400).
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

SALIDA = Path(__file__).resolve().parents[1] / "descargas_rues"


def _consulta_viva(nit: str, estructura: bool) -> None:
    from Funciones.bot_rues import _cifrar, _buscar_hits, _sesion

    sesion = _sesion()
    hits = _buscar_hits(sesion, nit)
    print(f"NIT {nit}: {len(hits)} hit(s) con coincidencia exacta")
    if estructura and hits:
        print("\n=== Contrato del hit (_source del /query) ===")
        print(json.dumps(hits[0], ensure_ascii=False, indent=2)[:2000])
    from Funciones.bot_rues import consultar_rues_sync

    resultado = consultar_rues_sync(nit)
    print("\n=== Resultado del bot ===")
    print(json.dumps(resultado, ensure_ascii=False, indent=2, default=str))
    if estructura:
        print("\n=== Payloads cifrados que envía el bot (contrato) ===")
        print("POST /query:", json.dumps({
            "term": nit, "offset": 0, "type": 2,
            "filter": {"Category": ["JURIDICA", "NATURAL", "COMERCIO", "SUCURSAL", "AGENCIA"],
                        "Status": ["ACTIVA", "CANCELADA"], "advanced": False, "tipoRegistro": "RM"},
        }))
        print("POST /api/Expediente/DetalleRM:", json.dumps({"id": str(hits[0].get("id")) if hits else "…"}))
        print("POST /api/ConsultFacultadesXCamYMatricula:", json.dumps(
            {"codigo_camara": resultado.get("codigo_camara"), "matricula": resultado.get("matricula")}
        ))


async def _descubrir_endpoints(nit: str) -> None:
    """Abre el detalle en el portal real y descifra los payloads del API."""
    from playwright.async_api import async_playwright

    def _descifrar(ciphertext: str):
        try:
            from Funciones.bot_rues import RUES_SECRET_KEY
            data = base64.b64decode(ciphertext)
            if data[:8] != b"Salted__":
                return None
            salt = data[8:16]
            derivado, previo = b"", b""
            while len(derivado) < 48:
                previo = hashlib.md5(previo + RUES_SECRET_KEY.encode() + salt).digest()
                derivado += previo
            from Crypto.Cipher import AES

            plano = AES.new(derivado[:32], AES.MODE_CBC, derivado[32:48]).decrypt(data[16:])
            return plano[:-plano[-1]].decode("utf-8", "replace")
        except Exception:
            return None

    vistos: set[str] = set()
    capturados: list[str] = []

    async def on_response(respuesta):
        if "elasticprd" not in respuesta.url or "/log/" in respuesta.url:
            return
        try:
            cuerpo = json.loads(respuesta.request.post_data or "{}").get("dataBody")
        except Exception:
            return
        plano = _descifrar(cuerpo) if cuerpo else None
        if respuesta.url not in vistos:
            vistos.add(respuesta.url)
            capturados.append(f"### {respuesta.url}\nREQ: {plano}")
            print(f"### {respuesta.url}\nREQ: {plano}")

    async with async_playwright() as p:
        navegador = await p.chromium.launch(
            headless=True, args=["--disable-blink-features=AutomationControlled"]
        )
        ctx = await navegador.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
            locale="es-CO",
        )
        pagina = await ctx.new_page()
        pagina.on("response", on_response)
        await pagina.goto("https://www.rues.org.co/", wait_until="load", timeout=90000)
        await pagina.wait_for_timeout(10000)
        await pagina.keyboard.press("Escape")  # cerrar el aviso swal inicial
        await pagina.fill("#search", nit)
        await pagina.keyboard.press("Enter")
        await pagina.wait_for_timeout(9000)
        await pagina.keyboard.press("Escape")
        try:
            await pagina.click("text=Ver información", timeout=8000)
        except Exception as exc:
            print(f"(sin botón Ver información: {exc})")
        await pagina.wait_for_timeout(5000)
        for pestana in ("Actividad económica", "Representante legal", "Propietario / Establecimiento"):
            try:
                await pagina.click(f"text={pestana}", timeout=5000)
                await pagina.wait_for_timeout(4000)
            except Exception:
                pass
        SALIDA.mkdir(exist_ok=True)
        destino = SALIDA / f"endpoints_{nit}.txt"
        destino.write_text("\n\n".join(capturados), encoding="utf-8")
        print(f"\nDumps en {destino}")
        await navegador.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Sonda del API del RUES por NIT")
    parser.add_argument("nit", help="NIT sin dígito de verificación (ej. 901923029)")
    parser.add_argument("--estructura", action="store_true", help="Imprimir el contrato descifrado")
    parser.add_argument("--descubrir-endpoints", action="store_true",
                        help="Abrir el portal con Playwright y descifrar todos los payloads del API")
    args = parser.parse_args()
    if args.descubrir_endpoints:
        asyncio.run(_descubrir_endpoints(args.nit))
    else:
        _consulta_viva(args.nit, args.estructura)


if __name__ == "__main__":
    main()
