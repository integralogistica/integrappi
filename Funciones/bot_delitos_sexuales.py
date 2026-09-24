# Funciones/bot_delitos_sexuales.py
"""
Bot de la consulta de inhabilidades por delitos sexuales contra menores de
edad (Ley 1918 de 2018) de la Policía Nacional — DIJIN.

https://inhabilidades.policia.gov.co:8080/

Consulta pública de terceros POR DISEÑO: la Ley 1918 creó el registro para
que entidades y empresas lo verifiquen en procesos de selección (la Ley 2375
de 2024 la extendió a entrenadores/parques); el propio resultado nombra a la
empresa consultante → va en los defaults del módulo (NO es opt-in).

Portal descubierto y calibrado con scripts/probar_delitos_sexuales.py
(2026-09-14, caso real 1010213062 → "NO REGISTRA INHABILIDAD"):
  - Formulario server-rendered (NO JSF ni SPA): POST al action del form
    `#frmCons` (action=/consulta;jsessionid=…). Campos: `tipo` (CC/CX/PA),
    `nuip`, `fechaExpNuip` (DD/MM/AAAA — la fecha de EXPEDICIÓN de la
    cédula), `nombreEmpresa` + `nitEmpresa` (la empresa CONSULTANTE; NIT CON
    dígito de verificación, máscara 000000000-0), reCAPTCHA v2 y checkbox de
    términos.
  - Captcha: reCAPTCHA v2 resuelto por 2Captcha (method=userrecaptcha,
    ~US$0.003; sitekey en el DOM con fallback fijo).
  - TRAMPA (main.js obfuscado, decodificado): jquery.validate exige
    `grecaptcha.getResponse() != ''` y habilita el botón con el checkbox —
    pero el submit NATIVO (form.submit()) NO dispara los handlers de
    validate: basta stub del getResponse + campo oculto `#captcha` con el
    token + checkbox marcado.
  - El resultado es una página server-rendered en la MISMA URL /consulta:
    "…el ciudadano identificado con cédula de ciudadanía No. {CEDULA},
    NO REGISTRA INHABILIDAD…" + hora de la consulta + la empresa consultante
    + fundamento legal (Ley 1918, Decreto 753/2019, Leyes 1581 y 1712).

Flujo: portal → CC + cédula + fecha expedición + empresa + NIT(DV) → solve
captcha → stub + checkbox → submit nativo → leer veredicto.
"""
import asyncio
import logging
import os
import re
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv
from playwright.async_api import async_playwright

try:  # importado como paquete (orquestador) o standalone (CLI)
    from Funciones.captura_evidencia import capturar_viewport_jpeg
except ImportError:  # pragma: no cover - ejecución como script
    from captura_evidencia import capturar_viewport_jpeg

# Cargar .env del proyecto para la key del captcha cuando se ejecute standalone.
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

logger = logging.getLogger(__name__)

PORTAL_URL = "https://inhabilidades.policia.gov.co:8080/"
SALIDA = Path(__file__).resolve().parents[1] / "descargas_delitos"

SITEKEY_FALLBACK = "6LflZLwUAAAAAP6-I_SuqVa1YDSTqfMyk43peb_M"

# Bloqueo para serializar consultas al portal (una a la vez, como los demás bots).
_LOCK = threading.Lock()

_TIMEOUT_MS = int(os.getenv("SEGURIDAD_DELITOS_TIMEOUT_MS", "90000"))
_CAPTCHA_BASE = os.getenv("SEGURIDAD_DELITOS_CAPTCHA_URL", "https://2captcha.com").rstrip("/")
# API_KEY_CAPTCHA es la key ya usada por los demás bots del proyecto.
_CAPTCHA_KEY = (os.getenv("SEGURIDAD_DELITOS_CAPTCHA_KEY", "").strip()
                or os.getenv("API_KEY_CAPTCHA", "").strip())
_CAPTCHA_TIMEOUT_S = float(os.getenv("SEGURIDAD_DELITOS_CAPTCHA_TIMEOUT_S", "90"))
_CAPTCHA_POOL_S = 5.0

# Fórmulas del resultado (calibradas 2026-09-14). El "NO" se chequea PRIMERO
# porque las fórmulas se contienen entre sí (mismo orden del fix de PGN/CGR).
_RE_NO_REGISTRA = re.compile(r"NO\s+REGISTRA\s+INHABILIDAD", re.IGNORECASE)
_RE_REGISTRA = re.compile(r"REGISTRA\s+INHABILIDAD", re.IGNORECASE)
_RE_FECHA_CONSULTA = re.compile(r"siendo\s+las\s+(\d{2}:\d{2}:\d{2})\s+horas\s+del\s+(\d{2}/\d{2}/\d{4})", re.IGNORECASE)
_RE_FECHA_EXP = re.compile(r"^(\d{2})/(\d{2})/(\d{4})$")


class BotDelitosError(Exception):
    """Error del bot de inhabilidades (Ley 1918)."""


class BotDelitosSinCaptchaKey(BotDelitosError):
    """Falta configurar la key del resolvedor (fallo de config, accionable)."""


class BotDelitosCaptchaFallido(BotDelitosError):
    """El resolvedor rechazó el pedido o el portal rechazó el token."""


class BotDelitosSinResultado(BotDelitosError):
    """El portal no entregó un veredicto legible (anti-envenenamiento)."""


def _resolver_recaptcha(sitekey: str, url_pagina: str) -> str:
    """Resuelve el reCAPTCHA v2 vía servicio 2Captcha-compatible (como Policía/CGR).

    Síncrona (requests): se llama desde la corutina con asyncio.to_thread para
    no bloquear el loop mientras se hace el polling de res.php.
    """
    if not _CAPTCHA_KEY:
        raise BotDelitosSinCaptchaKey("Falta configurar SEGURIDAD_DELITOS_CAPTCHA_KEY para la fuente delitos_sexuales")
    try:
        r = requests.get(f"{_CAPTCHA_BASE}/in.php", params={
            "key": _CAPTCHA_KEY, "method": "userrecaptcha",
            "googlekey": sitekey, "pageurl": url_pagina, "json": 1,
        }, timeout=30)
        dato = r.json()
    except requests.RequestException as exc:
        raise BotDelitosCaptchaFallido(f"El resolvedor de captcha no respondió: {exc}") from exc
    if dato.get("status") != 1:
        raise BotDelitosCaptchaFallido(f"El resolvedor rechazó el pedido: {dato.get('request')}")
    captcha_id = str(dato.get("request"))

    logger.info("[BOT DELITOS] captcha pedido %s; sondeando cada %.0f s", captcha_id, _CAPTCHA_POOL_S)
    fin = time.monotonic() + _CAPTCHA_TIMEOUT_S
    while time.monotonic() < fin:
        time.sleep(_CAPTCHA_POOL_S)
        try:
            r2 = requests.get(f"{_CAPTCHA_BASE}/res.php", params={
                "key": _CAPTCHA_KEY, "action": "get", "id": captcha_id, "json": 1,
            }, timeout=30)
            dato2 = r2.json()
        except requests.RequestException as exc:
            raise BotDelitosCaptchaFallido(f"El resolvedor de captcha no respondió: {exc}") from exc
        if dato2.get("status") == 1:
            return str(dato2["request"])
        if dato2.get("request") != "CAPCHA_NOT_READY":
            raise BotDelitosCaptchaFallido(f"El resolvedor reportó: {dato2.get('request')}")
    raise BotDelitosCaptchaFallido(f"El resolvedor no resolvió el captcha en {_CAPTCHA_TIMEOUT_S:.0f} s")


def _nit_con_dv(nit: str) -> str:
    """Asegura el formato 000000000-0 que exige el formulario (máscara)."""
    crudo = (nit or "").strip()
    if re.fullmatch(r"\d{6,11}-\d", crudo):
        return crudo
    base = re.sub(r"-\s*\d\s*$", "", crudo)
    base = re.sub(r"\D", "", base)
    if not 6 <= len(base) <= 11:
        raise BotDelitosError("NIT de la empresa consultante inválido")
    # Dígito de verificación (módulo 11, algoritmo oficial de la DIAN): pesos
    # 3,7,13,17,… aplicados de DERECHA a IZQUIERDA (el último dígito pesa 3).
    pesos = [3, 7, 13, 17, 19, 23, 29, 37, 41, 43, 47, 53, 59, 67, 71]
    total = sum(int(d) * pesos[i] for i, d in enumerate(reversed(base)))
    resto = total % 11
    dv = "1" if resto == 1 else ("0" if resto == 0 else str(11 - resto))
    return f"{base}-{dv}"


def _fecha_ddmmaaaa(valor: str | None) -> str:
    """Normaliza la fecha de expedición a DD/MM/AAAA (acepta ISO aaaa-mm-dd)."""
    crudo = (valor or "").strip()
    m_iso = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", crudo)
    if m_iso:
        a, mes, d = m_iso.groups()
        crudo = f"{d}/{mes}/{a}"
    m = _RE_FECHA_EXP.match(crudo)
    if not m:
        raise BotDelitosError("Fecha de expedición inválida (use DD/MM/AAAA)")
    d, mes, a = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        datetime(a, mes, d)
    except ValueError as exc:
        raise BotDelitosError("Fecha de expedición inválida") from exc
    return f"{m.group(1)}/{m.group(2)}/{m.group(3)}"


async def consultar_inhabilidades(
    cedula: str, fecha_expedicion: str,
    empresa_nombre: str = "INTEGRA LOGISTICA", empresa_nit: str = "901923029-2",
    headed: bool = False,
) -> Dict[str, Any]:
    """Consulta de inhabilidades (Ley 1918) por cédula + fecha de expedición.

    `empresa_nombre`/`empresa_nit` son la empresa CONSULTANTE que el portal
    exige y estampa en el resultado (la del estudio; fallback Integra).

    Retorna: cedula, no_registra (bool | None), mensaje (fórmula ≤300),
    fecha_consulta (la que estampa el portal), empresa_consultante,
    texto_resultado y html.
    """
    cedula_norm = re.sub(r"\D", "", cedula or "")
    if not 3 <= len(cedula_norm) <= 15:
        raise BotDelitosError("Cédula inválida")
    fecha_norm = _fecha_ddmmaaaa(fecha_expedicion)
    nit_norm = _nit_con_dv(empresa_nit)
    empresa_norm = (empresa_nombre or "INTEGRA LOGISTICA").strip()[:50] or "INTEGRA LOGISTICA"

    async with async_playwright() as p:
        navegador = await p.chromium.launch(headless=not headed)
        try:
            contexto = await navegador.new_context(
                viewport={"width": 1366, "height": 900},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
                locale="es-CO",
                ignore_https_errors=True,
            )
            pagina = await contexto.new_page()

            # 1) Formulario server-rendered (ids estables).
            await pagina.goto(PORTAL_URL, wait_until="domcontentloaded", timeout=_TIMEOUT_MS)
            await pagina.wait_for_selector("#nuip", state="visible", timeout=30000)
            await pagina.wait_for_timeout(1500)

            # 2) Llenar: CC + cédula + fecha de expedición + empresa consultante.
            await pagina.select_option("#tipo", "CC")
            await pagina.fill("#nuip", cedula_norm)
            await pagina.fill("#fechaExpNuip", fecha_norm)
            await pagina.fill("#nombreEmpresa", empresa_norm)
            await pagina.fill("#nitEmpresa", nit_norm)
            await pagina.wait_for_timeout(300)

            # 3) Sitekey del DOM (cubre rotación) con fallback fijo.
            sitekey = await pagina.evaluate(
                "() => { const e = document.querySelector('.g-recaptcha');"
                " return e && e.getAttribute ? (e.getAttribute('data-sitekey') || '') : ''; }"
            ) or SITEKEY_FALLBACK

            # 4) Captcha: solve en un hilo (no bloquea el loop).
            token = await asyncio.wait_for(
                asyncio.to_thread(_resolver_recaptcha, sitekey, pagina.url),
                timeout=_CAPTCHA_TIMEOUT_S + 15,
            )

            # 5) Stub del getResponse (jquery.validate lo exige) + campo oculto
            #    #captcha con el token + términos marcados. El submit NATIVO no
            #    dispara los handlers de validate (hallazgo 2026-09-14).
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

            # 6) Submit nativo → POST completo a /consulta;jsessionid=…
            await pagina.evaluate("() => document.getElementById('frmCons').submit()")
            await pagina.wait_for_load_state("domcontentloaded", timeout=_TIMEOUT_MS)
            await pagina.wait_for_timeout(5000)

            # Dump de debug (jamás tumba la consulta).
            try:
                SALIDA.mkdir(exist_ok=True)
                (SALIDA / f"resultado_{cedula_norm}.html").write_text(await pagina.content(), encoding="utf-8")
            except Exception as exc:
                logger.warning("[BOT DELITOS] dump de debug no se pudo escribir: %s", exc)

            texto_resultado = " ".join((await pagina.inner_text("body")).split())
            captura = await capturar_viewport_jpeg(pagina)

            # 7) Veredicto: el "NO" primero (las fórmulas se contienen). La
            #    página debe traer la cédula consultada (garantía de que el
            #    resultado corresponde a ESTA persona).
            no_registra: Optional[bool] = None
            mensaje = ""
            if cedula_norm not in texto_resultado:
                raise BotDelitosSinResultado(
                    "El portal de inhabilidades no devolvió resultado para la cédula consultada "
                    f"(posible captcha rechazado o cambio del portal). Texto: {texto_resultado[:150]!r}"
                )
            if _RE_NO_REGISTRA.search(texto_resultado):
                no_registra = True
                mensaje = "No registra inhabilidad por delitos sexuales contra menores (Ley 1918 de 2018)"
            else:
                m = _RE_REGISTRA.search(texto_resultado)
                if m:
                    no_registra = False
                    contexto_veredicto = texto_resultado[max(0, m.start() - 160):m.end() + 120]
                    mensaje = " ".join(contexto_veredicto.split())[:300]
                else:
                    # Anti-envenenamiento: la página respondió sin fórmula
                    # legible → NUNCA cachear (el orquestador la reintentará).
                    raise BotDelitosSinResultado(
                        "El portal de inhabilidades no entregó un veredicto legible "
                        f"(posible fecha de expedición incorrecta o cambio del portal). Texto: {texto_resultado[:150]!r}"
                    )

            fecha_consulta = ""
            m_fecha = _RE_FECHA_CONSULTA.search(texto_resultado)
            if m_fecha:
                fecha_consulta = f"{m_fecha.group(2)} {m_fecha.group(1)}"

            return {
                "cedula": cedula_norm,
                "no_registra": no_registra,
                "mensaje": mensaje[:300],
                "fecha_consulta": fecha_consulta,
                "fecha_expedicion": fecha_norm,
                "empresa_consultante": empresa_norm,
                "texto_resultado": texto_resultado[:1500],
                "captura_jpg": captura,
                "html": await pagina.content(),
            }
        finally:
            await navegador.close()


def consultar_inhabilidades_sync(
    cedula: str, fecha_expedicion: str, empresa_nombre: str = "INTEGRA LOGISTICA",
    empresa_nit: str = "901923029-2",
) -> Dict[str, Any]:
    """Versión síncrona para asyncio.to_thread, igual que los demás bots."""
    with _LOCK:
        return asyncio.run(
            consultar_inhabilidades(cedula, fecha_expedicion, empresa_nombre, empresa_nit)
        )


if __name__ == "__main__":
    import json
    import sys

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = [a for a in sys.argv[1:] if a != "--headed"]
    if len(args) < 3:
        print("Uso: python Funciones/bot_delitos_sexuales.py CEDULA FECHA_EXP_DD/MM/AAAA EMPRESA NIT [--headed]")
        sys.exit(2)
    resultado = asyncio.run(
        consultar_inhabilidades(
            args[0], args[1],
            args[2] if len(args) > 2 else "INTEGRA LOGISTICA",
            args[3] if len(args) > 3 else "901923029-2",
            headed="--headed" in sys.argv,
        )
    )
    resultado.pop("html", None)
    print(json.dumps(resultado, ensure_ascii=False, indent=2, default=str))
