"""Bot autenticado para el Boletín de Deudores Morosos del Estado (BDME)."""
from __future__ import annotations

import asyncio
import os
import re
import threading
import time
import unicodedata
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv(Path(__file__).resolve().parents[1] / ".env")
PORTAL_URL = "https://eris.contaduria.gov.co/BDME/"
SITE_KEY = "6LcjpPwUAAAAAITxXi_1WDpOpzXfV0OztgN_Q2es"
_CAPTCHA_BASE = os.getenv("SEGURIDAD_BDME_CAPTCHA_URL", "https://2captcha.com").rstrip("/")
_CAPTCHA_KEY = os.getenv("SEGURIDAD_BDME_CAPTCHA_KEY", "").strip() or os.getenv("API_KEY_CAPTCHA", "").strip()
_CAPTCHA_TIMEOUT_S = float(os.getenv("SEGURIDAD_BDME_CAPTCHA_TIMEOUT_S", "120"))
_TIMEOUT_MS = int(os.getenv("SEGURIDAD_BDME_TIMEOUT_MS", "60000"))
_LOCK = threading.Lock()


class BotBdmeError(Exception): pass
class BotBdmeConfiguracionError(BotBdmeError): pass
class BotBdmeAutenticacionError(BotBdmeError): pass
class BotBdmeCaptchaFallido(BotBdmeError): pass
class BotBdmeSinResultado(BotBdmeError): pass


def _normalizar_documento(documento: str) -> str:
    valor = re.sub(r"\D", "", documento or "")
    if not 3 <= len(valor) <= 15:
        raise BotBdmeError("Número de identificación inválido")
    return valor


def _resolver_recaptcha() -> str:
    if not _CAPTCHA_KEY:
        raise BotBdmeConfiguracionError("Falta SEGURIDAD_BDME_CAPTCHA_KEY o API_KEY_CAPTCHA")
    try:
        alta = requests.post(f"{_CAPTCHA_BASE}/in.php", data={
            "key": _CAPTCHA_KEY, "method": "userrecaptcha", "googlekey": SITE_KEY,
            "pageurl": PORTAL_URL, "json": 1,
        }, timeout=30).json()
    except (requests.RequestException, ValueError) as exc:
        raise BotBdmeCaptchaFallido(f"El resolvedor no respondió: {exc}") from exc
    if alta.get("status") != 1:
        raise BotBdmeCaptchaFallido(f"El resolvedor rechazó el captcha: {alta.get('request')}")
    fin = time.monotonic() + _CAPTCHA_TIMEOUT_S
    while time.monotonic() < fin:
        time.sleep(5)
        dato = requests.get(f"{_CAPTCHA_BASE}/res.php", params={
            "key": _CAPTCHA_KEY, "action": "get", "id": alta["request"], "json": 1,
        }, timeout=30).json()
        if dato.get("status") == 1:
            return str(dato["request"])
        if dato.get("request") != "CAPCHA_NOT_READY":
            raise BotBdmeCaptchaFallido(f"El resolvedor reportó: {dato.get('request')}")
    raise BotBdmeCaptchaFallido("Tiempo agotado esperando el captcha BDME")


def _interpretar_resultado(texto: str, filas: list[list[str]]) -> dict[str, Any]:
    limpio = " ".join((texto or "").split())
    bajo = limpio.lower()
    # La página declara UTF-8 pero actualmente entrega varias vocales como �.
    # Por eso el patrón admite un carácter cualquiera entre "est" e "incluido".
    incluido_si = bool(re.search(r"\bsi\s+est.?\s+incluido\s+en\s+el\s+bdme", bajo))
    incluido_no = bool(re.search(r"\bno\s+est.?\s+incluido\s+en\s+el\s+bdme", bajo))
    acuerdo_si = bool(re.search(r"\bsi\s+ha\s+incumplido\s+acuerdos", bajo))
    acuerdo_no = bool(re.search(r"\bno\s+ha\s+incumplido\s+acuerdos", bajo))
    if incluido_si or acuerdo_si:
        no_registra = False
    elif incluido_no and acuerdo_no:
        no_registra = True
    else:
        no_registra = None
    if no_registra is None:
        raise BotBdmeSinResultado("BDME no entregó un veredicto reconocible")
    return {"no_registra": no_registra, "reportado": not no_registra,
            "mensaje": limpio[:1000], "filas": filas[:100],
            "total_registros": max(0, len(filas) - 1)}


async def consultar_bdme(documento: str, *, tipo: str = "cedula", headed: bool = False) -> dict[str, Any]:
    documento = _normalizar_documento(documento)
    if tipo not in {"cedula", "nit"}:
        raise BotBdmeError("Tipo BDME inválido")
    usuario, clave = os.getenv("BDME_USUARIO", "").strip(), os.getenv("BDME_CLAVE", "")
    if not usuario or not clave:
        raise BotBdmeConfiguracionError("Faltan BDME_USUARIO o BDME_CLAVE")
    motivo = "Autoconsulta" if tipo == "cedula" else "Relación contractual"
    async with async_playwright() as p:
        # Windows local usa Chrome instalado; la imagen oficial de Playwright
        # en Render/Linux trae Chromium, pero no /opt/google/chrome/chrome.
        browser = await p.chromium.launch(channel="chrome" if os.name == "nt" else None, headless=not headed,
                                          args=["--disable-blink-features=AutomationControlled"])
        try:
            page = await browser.new_page(locale="es-CO", user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"))
            await page.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
            await page.goto(PORTAL_URL, wait_until="domcontentloaded", timeout=_TIMEOUT_MS)
            await page.wait_for_selector("#panelMenu a", timeout=_TIMEOUT_MS)
            await page.wait_for_timeout(4000)
            await page.locator("#panelMenu a").first.click()
            await page.wait_for_selector("input.gwt-PasswordTextBox", timeout=15000)
            await page.locator("input.gwt-TextBox").first.fill(usuario)
            await page.locator("input.gwt-PasswordTextBox").fill(clave)
            await page.get_by_role("button", name="Ingresar").click()
            await page.wait_for_timeout(5000)
            if await page.locator("input.gwt-PasswordTextBox").count():
                dialogs = " ".join(await page.locator(".gwt-DialogBox").all_inner_texts())
                raise BotBdmeAutenticacionError("BDME rechazó el inicio de sesión: " + " ".join(dialogs.split())[-250:])
            await page.wait_for_timeout(3000)
            campos = page.locator("#panelPrincipal input[type=text]:visible")
            if not await campos.count():
                await page.locator("#panelMenu a").first.click(); await page.wait_for_timeout(3000)
            campos = page.locator("#panelPrincipal input[type=text]:visible")
            if not await campos.count():
                raise BotBdmeSinResultado("No apareció el campo de identificación")
            selects = page.locator("#panelPrincipal select:visible")
            if await selects.count() < 2:
                raise BotBdmeSinResultado("No aparecieron los selectores de identificación y motivo")

            # El portal carga los motivos por RPC al cambiar el tipo. Para una
            # cédula igual al usuario autenticado, después del blur del número
            # reemplaza la lista por la única opción Autoconsulta (valor 1).
            # NIT usa Relación contractual (valor 2).
            indice_tipo = 0 if tipo == "cedula" else 1
            selector_tipo = selects.nth(0)
            valor_actual = await selector_tipo.input_value()
            valor_tipo = await selector_tipo.locator("option").nth(indice_tipo).get_attribute("value")
            if valor_actual == valor_tipo:
                # En CC el valor suele venir seleccionado, pero GWT necesita
                # igualmente el evento change para preparar los motivos.
                await selector_tipo.dispatch_event("change")
            else:
                await selector_tipo.select_option(index=indice_tipo)
            await campos.first.fill(documento)
            await campos.first.press("Tab")
            # No basta esperar `options.length > 0`: la opción inicial ya
            # existe mientras el RPC todavía está cargando los motivos. Se
            # espera el TEXTO requerido y luego se usa el valor que el portal
            # haya asignado (los códigos internos pueden cambiar).
            try:
                motivo_busqueda = "autoconsulta" if tipo == "cedula" else "relacioncontractual"

                def normalizar(texto: str) -> str:
                    limpio = "".join(
                        c for c in unicodedata.normalize("NFD", texto or "")
                        if unicodedata.category(c) != "Mn"
                    ).casefold()
                    return re.sub(r"[^a-z0-9]", "", limpio)

                # El GWT de BDME carga este combo al primer clic/foco. Tal
                # como ocurre manualmente, se abre, se espera y se vuelve a
                # abrir hasta que llegue la opción por RPC.
                selector_motivo = selects.nth(1)
                opcion = None
                limite = time.monotonic() + (_TIMEOUT_MS / 1000)
                while time.monotonic() < limite:
                    await selector_motivo.click(force=True)
                    opciones = await selector_motivo.locator("option").evaluate_all(
                        "os => os.map(o => ({value: o.value, text: o.textContent || ''}))"
                    )
                    opcion = next(
                        (o for o in opciones if motivo_busqueda in normalizar(o["text"])), None
                    )
                    if opcion:
                        break
                    await page.wait_for_timeout(2000)
                if not opcion:
                    raise ValueError("motivo ausente")
                await selector_motivo.select_option(value=opcion["value"])
            except Exception as exc:
                raise BotBdmeSinResultado(
                    f"BDME no cargó el motivo requerido: {motivo}"
                ) from exc
            token = await asyncio.to_thread(_resolver_recaptcha)
            await page.evaluate("""t=>{const e=document.getElementById('g-recaptcha-response');
                if(!e)throw Error('captcha ausente');e.value=t;e.innerHTML=t}""", token)
            boton = page.locator("#panelPrincipal button:visible").filter(has_text=re.compile("consultar", re.I))
            if not await boton.count():
                raise BotBdmeSinResultado("No apareció el botón Consultar")
            await boton.first.click(); await page.wait_for_timeout(7000)
            cuerpo = await page.locator("#panelPrincipal").inner_text()
            if "captcha no válido" in cuerpo.lower() or "captcha no valido" in cuerpo.lower():
                raise BotBdmeCaptchaFallido("BDME rechazó el token reCAPTCHA")
            filas = await page.locator("#panelPrincipal table tr").evaluate_all(
                "trs=>trs.map(tr=>[...tr.querySelectorAll('th,td')].map(c=>c.innerText.trim())).filter(r=>r.some(Boolean))")
            resultado = _interpretar_resultado(cuerpo, filas)
            resultado.update({"documento": documento, "tipo": tipo, "motivo": motivo})
            return resultado
        finally:
            await browser.close()


def consultar_bdme_sync(documento: str, *, tipo: str = "cedula", headed: bool = False) -> dict[str, Any]:
    with _LOCK:
        return asyncio.run(consultar_bdme(documento, tipo=tipo, headed=headed))
