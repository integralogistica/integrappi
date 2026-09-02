"""Consulta Nacional Unificada de Procesos por nombre (persona natural)."""
from __future__ import annotations

import asyncio
import os
import re
import threading
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from playwright.async_api import async_playwright

PORTAL_URL = "https://consultaprocesos.ramajudicial.gov.co/Procesos/NombreRazonSocial"
_TIMEOUT_MS = int(os.getenv("SEGURIDAD_RAMA_TIMEOUT_MS", "60000"))
_MAX_PAGINAS = int(os.getenv("SEGURIDAD_RAMA_MAX_PAGINAS", "250"))
_LOCK = threading.Lock()


class BotRamaJudicialError(Exception): pass
class BotRamaJudicialSinResultado(BotRamaJudicialError): pass


def _nombre(valor: str) -> str:
    texto = " ".join((valor or "").strip().split()).upper()
    if len(texto) < 2 or not any(c.isalpha() for c in texto):
        raise BotRamaJudicialError("Nombres o apellidos inválidos")
    return texto


def _pagina_url(url: str, pagina: int) -> str:
    partes = urlsplit(url)
    params = dict(parse_qsl(partes.query, keep_blank_values=True))
    params["pagina"] = str(pagina)
    return urlunsplit((partes.scheme, partes.netloc, partes.path, urlencode(params), partes.fragment))


async def consultar_procesos(nombres: str, apellidos: str, headed: bool = False) -> dict[str, Any]:
    nombres, apellidos = _nombre(nombres), _nombre(apellidos)
    nombre_completo = f"{nombres} {apellidos}"
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            # Chrome del sistema en Windows; Chromium incluido por la imagen
            # oficial de Playwright en Render/Linux.
            channel="chrome" if os.name == "nt" else None, headless=not headed,
            args=["--disable-blink-features=AutomationControlled"],
        )
        try:
            page = await browser.new_page(
                locale="es-CO",
                user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                            "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"),
            )
            await page.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
            await page.goto(PORTAL_URL, wait_until="domcontentloaded", timeout=_TIMEOUT_MS)
            await page.wait_for_selector("#input-78", timeout=_TIMEOUT_MS)
            await page.wait_for_timeout(3000)
            # Todos los procesos (no solo actuaciones recientes), persona Natural.
            await page.locator("#input-67").check(force=True)
            await page.locator("#input-72").click()
            # Vuetify monta las opciones de forma asíncrona. En Render el
            # `count()` inmediato podía ejecutarse antes de que aparecieran
            # Natural/Jurídica, aunque el selector estuviera correcto.
            natural = page.locator(
                "[role=option]:visible, .v-list-item:visible"
            ).filter(has_text=re.compile(r"^\s*Natural\s*$", re.I)).first
            try:
                await natural.wait_for(state="visible", timeout=15000)
                await natural.click()
            except Exception as exc:
                raise BotRamaJudicialSinResultado(
                    "El selector de tipo de persona no terminó de cargar la opción Natural"
                ) from exc
            await page.locator("#input-78").fill(nombre_completo)
            try:
                async with page.expect_response(
                    lambda r: "/Procesos/Consulta/NombreRazonSocial" in r.url,
                    timeout=_TIMEOUT_MS,
                ) as respuesta_info:
                    await page.locator("button:visible").filter(has_text="CONSULTAR").first.click()
                respuesta = await respuesta_info.value
            except Exception as exc:
                raise BotRamaJudicialSinResultado("La Rama Judicial no respondió la consulta") from exc
            if respuesta.status != 200:
                raise BotRamaJudicialSinResultado(f"La Rama Judicial respondió HTTP {respuesta.status}")
            try:
                dato = await respuesta.json()
            except Exception as exc:
                raise BotRamaJudicialSinResultado("La Rama Judicial entregó una respuesta ilegible") from exc
            if not isinstance(dato, dict) or "procesos" not in dato or "paginacion" not in dato:
                raise BotRamaJudicialSinResultado("La respuesta no contiene procesos ni paginación")

            procesos = list(dato.get("procesos") or [])
            pag = dato.get("paginacion") or {}
            paginas = int(pag.get("cantidadPaginas") or 0)
            total = int(pag.get("cantidadRegistros") or len(procesos))
            if paginas > _MAX_PAGINAS:
                raise BotRamaJudicialSinResultado(
                    f"La consulta produjo {paginas} páginas; refine el nombre para evitar homónimos"
                )
            for numero in range(2, paginas + 1):
                siguiente = _pagina_url(respuesta.url, numero)
                bloque = await page.evaluate("""async url => {
                    const r = await fetch(url, {credentials: 'include'});
                    return {status: r.status, body: await r.json()};
                }""", siguiente)
                if bloque.get("status") != 200 or not isinstance(bloque.get("body"), dict):
                    raise BotRamaJudicialSinResultado(f"Falló la página {numero} de {paginas}")
                procesos.extend((bloque["body"].get("procesos") or []))
            if len(procesos) < total:
                raise BotRamaJudicialSinResultado(
                    f"Respuesta incompleta: llegaron {len(procesos)} de {total} procesos"
                )
            return {
                "nombre_completo": nombre_completo,
                "tipo_persona": "Natural",
                "todos_los_procesos": True,
                "no_registra": total == 0,
                "total_procesos": total,
                "procesos": procesos,
                "mensaje": ("No se encontraron procesos para el nombre consultado" if total == 0
                            else f"Se encontraron {total} procesos; validar homonimia e identidad"),
            }
        finally:
            await browser.close()


def consultar_procesos_sync(nombres: str, apellidos: str, headed: bool = False) -> dict[str, Any]:
    with _LOCK:
        return asyncio.run(consultar_procesos(nombres, apellidos, headed=headed))
