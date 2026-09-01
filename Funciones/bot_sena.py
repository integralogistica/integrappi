# Funciones/bot_sena.py
"""
Bot de consulta de certificados de formación del SENA (Certificado Digital).

https://certificados.sena.edu.co/CertificadoDigital/com.sena.consultacer

Portal PÚBLICO del SENA para consulta de certificados de formación / competencia
laboral: no restringe la consulta a terceros → la fuente "sena" SÍ va en los
defaults de empresa (como runt/simit, no es opt-in como policia). La consulta
es por cédula de la persona evaluada (tipo CC, default del portal).

Descubrimiento (2026-09-01, sonda scripts/probar_sena.py, dumps
descargas_sena/): portal GeneXUS server-rendered (NO SPA, NO JSF: ids estables
y limpios). Formulario (todos con default correcto, solo se llenan dos):
  - select vTIPO_CONSULTA   (default 1 = Documento — no se toca)
  - select vTIPO_DOCUMENTO  (default CC — no se toca)
  - input  vNUMERO_DOCUMENTO (cédula, maxlength 20)
  - img    vCAPTCHAIMAGE     (captcha de IMAGEN propio: GIF embebido data:image)
  - input  vCAPTCHATEXT      (respuesta del captcha)
  - botón  CONSULTAR
El resultado renderiza EN LA MISMA PÁGINA tras el postback:
  - filas tr#GridceContainerRow_NNNN con spans span_vVAR1 (registro),
    span_vTITULO_OBTENIDO ("TECNÓLOGO EN"), span_vTIPO (Acta/Título),
    span_vPROGRAMA, span_vFECHA_CERTIFICACION, span_vFECHA_CARGA_PDF (firma)
    y link com.sena.guardar?var1=<registro> (PDF del certificado — NO se descarga).
  - vacío legítimo: #I_NORESULTSFOUNDTEXTBLOCK_GRIDCE visible.
  - captcha equivocado: ErrorViewer GeneXUS "El texto digitado no corresponde
    con la imagen…" + el portal REFRESCA la imagen y limpia el campo.

⚠️ TRAMPA (calibrada con la sonda): "No hay resultados" también es el estado
INICIAL de la página (visible antes de consultar) y lo que muestra un captcha
equivocado. Evidencia de que el postback SE PROCESÓ = la imagen del captcha
CAMBIÓ (el portal la regenera en cada respuesta). Sin filas + sin error +
imagen igual = respuesta no procesada → BotSenaSinResultado (anti-envenenamiento).
"""
import asyncio
import logging
import os
import re
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv
from playwright.async_api import async_playwright

# Cargar .env del proyecto para la key del captcha cuando se ejecute standalone.
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

logger = logging.getLogger(__name__)

PORTAL_URL = "https://certificados.sena.edu.co/CertificadoDigital/com.sena.consultacer"
SALIDA = Path(__file__).resolve().parents[1] / "descargas_sena"

# Bloqueo para serializar consultas al portal (una a la vez, como los demás bots).
_LOCK = threading.Lock()

_TIMEOUT_MS = 45000              # Playwright: goto/clicks/esperas puntuales
_RENDER_MS = 3000                # render inicial (server-rendered: rápido)
_PASO_RESULTADO_MS = 30000       # espera del postback tras Consultar
_CAPTCHA_BASE = os.getenv("SEGURIDAD_SENA_CAPTCHA_URL", "https://2captcha.com").rstrip("/")
# API_KEY_CAPTCHA es la key ya usada por los demás procesos del proyecto.
_CAPTCHA_KEY = os.getenv("SEGURIDAD_SENA_CAPTCHA_KEY", "").strip() or os.getenv("API_KEY_CAPTCHA", "").strip()
_CAPTCHA_TIMEOUT_S = float(os.getenv("SEGURIDAD_SENA_CAPTCHA_TIMEOUT_S", "90"))
_CAPTCHA_POOL_S = 5.0            # polling de res.php

# Mensajes oficiales del portal (calibrados con la sonda 2026-09-01).
_RE_CAPTCHA_ERROR = re.compile(r"no\s+corresponde\s+con\s+la\s+imagen", re.IGNORECASE)

_MESES_ES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}


class BotSenaError(Exception):
    """Error del bot de consulta de certificados del SENA."""


class BotSenaSinCaptchaKey(BotSenaError):
    """Falta configurar la key del resolvedor (fallo de config, accionable)."""


class BotSenaCaptchaFallido(BotSenaError):
    """El resolvedor rechazó el pedido o el portal rechazó el texto."""


class BotSenaSinResultado(BotSenaError):
    """La respuesta no contenía certificados ni vacío confirmado (anti-envenenamiento)."""


def _resolver_captcha_imagen(img_data_url: str) -> str:
    """Resuelve el captcha de imagen propio del portal vía 2Captcha-compatible.

    El portal embebe el GIF como data:image (declared jpeg, magic GIF — da
    igual: se envía el base64 directo con method=base64, ~US$0.001). Síncrona
    (requests): se llama desde la corutina con asyncio.to_thread.
    """
    if not _CAPTCHA_KEY:
        raise BotSenaSinCaptchaKey("Falta configurar SEGURIDAD_SENA_CAPTCHA_KEY para la fuente sena")
    b64 = img_data_url.split(",", 1)[-1]
    try:
        r = requests.post(f"{_CAPTCHA_BASE}/in.php", data={
            "key": _CAPTCHA_KEY, "method": "base64", "body": b64, "json": 1,
        }, timeout=30)
        dato = r.json()
    except requests.RequestException as exc:
        raise BotSenaCaptchaFallido(f"El resolvedor de captcha no respondió: {exc}") from exc
    if dato.get("status") != 1:
        raise BotSenaCaptchaFallido(f"El resolvedor rechazó el pedido: {dato.get('request')}")

    captcha_id = str(dato.get("request"))
    logger.info("[BOT SENA] captcha pedido %s; sondeando cada %.0f s", captcha_id, _CAPTCHA_POOL_S)
    fin = time.monotonic() + _CAPTCHA_TIMEOUT_S
    while time.monotonic() < fin:
        time.sleep(_CAPTCHA_POOL_S)
        try:
            r2 = requests.get(f"{_CAPTCHA_BASE}/res.php", params={
                "key": _CAPTCHA_KEY, "action": "get", "id": captcha_id, "json": 1,
            }, timeout=30)
            dato2 = r2.json()
        except requests.RequestException as exc:
            raise BotSenaCaptchaFallido(f"El resolvedor de captcha no respondió: {exc}") from exc
        if dato2.get("status") == 1:
            return str(dato2["request"])
        if dato2.get("request") != "CAPCHA_NOT_READY":
            raise BotSenaCaptchaFallido(f"El resolvedor reportó: {dato2.get('request')}")
    raise BotSenaCaptchaFallido(f"El resolvedor no resolvió el captcha en {_CAPTCHA_TIMEOUT_S:.0f} s")


def _fecha_iso_es(valor: str) -> Optional[str]:
    """'09 Febrero, 2013' → '2013-02-09' (fechas del portal); None si no parsea."""
    m = re.search(r"(\d{1,2})\s+de\s+([A-Za-zÁÉÍÓÚáéíóúñÑ]+),?\s+(\d{4})", valor or "")
    if not m:
        m = re.search(r"(\d{1,2})\s+([A-Za-zÁÉÍÓÚáéíóúñÑ]+),?\s+(\d{4})", valor or "")
    if not m:
        return None
    dia, mes_txt, anio = int(m.group(1)), m.group(2).lower().strip(), int(m.group(3))
    mes = _MESES_ES.get(mes_txt)
    if not mes:
        return None
    try:
        return datetime(anio, mes, dia).date().isoformat()
    except ValueError:
        return None


# Lectura del grid de resultados por span-id (GeneXUS: estables y únicos por
# fila). Devuelve la lista de certificados en el ORDEN del portal.
_JS_LEER_FILAS = """
() => Array.from(document.querySelectorAll('tr[id^="GridceContainerRow_"]'))
    .filter(tr => tr.offsetParent !== null)
    .map(tr => {
        const por = (prefijo) => {
            const e = tr.querySelector(`span[id^="span_${prefijo}_"]`);
            return e ? e.textContent.trim() : '';
        };
        return {
            registro: por('vVAR1'),
            titulo: por('vTITULO_OBTENIDO'),
            tipo: por('vTIPO'),
            programa: por('vPROGRAMA'),
            fecha_certificacion: por('vFECHA_CERTIFICACION'),
            fecha_firma: por('vFECHA_CARGA_PDF'),
        };
    })
"""


async def _img_captcha(pagina) -> str:
    """data URL de la imagen del captcha (vacía si el portal no la muestra)."""
    return await pagina.evaluate(
        """() => {
            const e = document.getElementById('vCAPTCHAIMAGE');
            if (e && (e.src || '').startsWith('data:image')) return e.src;
            for (const im of document.images) {
                if ((im.src || '').startsWith('data:image')) return im.src;
            }
            return '';
        }"""
    )


async def _texto_error_viewer(pagina) -> str:
    """Texto del ErrorViewer de GeneXUS (vacío si no hay mensaje visible)."""
    loc = pagina.locator(".ErrorViewer, .gx-warning-message").locator("visible=true")
    if not await loc.count():
        return ""
    try:
        return " ".join(" ".join(await loc.all_inner_texts()).split())
    except Exception:
        return ""


async def consultar_sena(cedula: str, headed: bool = False) -> Dict[str, Any]:
    """Consulta los certificados de formación del SENA por cédula (tipo CC).

    Retorna: cedula, no_registra (bool: True = sin certificados disponibles;
    False = registra), total_certificados, certificados (lista ≤50 con registro,
    titulo, tipo Acta/Título, programa, fecha_certificacion, fecha_firma ISO),
    mensaje, texto_resultado, pdf_bytes (None), pdf_ruta (None) y html.
    """
    cedula_norm = re.sub(r"\D", "", cedula or "")
    if not 3 <= len(cedula_norm) <= 20:
        raise BotSenaError("Cédula inválida")

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

            # 1) Portal GeneXUS server-rendered: carga rápida, form directo.
            await pagina.goto(PORTAL_URL, wait_until="domcontentloaded", timeout=_TIMEOUT_MS)
            await pagina.wait_for_selector("#vNUMERO_DOCUMENTO", timeout=_TIMEOUT_MS)
            await pagina.wait_for_timeout(_RENDER_MS)

            # 2) Formulario: los selects ya vienen Documento + CC (default del
            #    portal, verificado con la sonda): solo cédula y captcha.
            await pagina.fill("#vNUMERO_DOCUMENTO", cedula_norm)
            await pagina.wait_for_timeout(500)

            # 3) Captcha de imagen (GIF embebido) → 2Captcha base64.
            img = await _img_captcha(pagina)
            if not img:
                raise BotSenaError("El portal no mostró la imagen del captcha (posible cambio del portal)")
            texto_captcha = await asyncio.wait_for(
                asyncio.to_thread(_resolver_captcha_imagen, img),
                timeout=_CAPTCHA_TIMEOUT_S + 15,
            )
            await pagina.fill("#vCAPTCHATEXT", texto_captcha)
            try:
                SALIDA.mkdir(exist_ok=True)
                (SALIDA / f"captcha_{cedula_norm}.gif").write_bytes(
                    __import__("base64").b64decode(img.split(",", 1)[-1])
                )
            except Exception:
                pass

            # 4) Consultar (postback de GeneXUS: click, jamás POST manual).
            try:
                await pagina.click("#CONSULTAR", timeout=10000)
            except Exception as exc:
                raise BotSenaError(f"El portal no aceptó la consulta: {exc}") from exc

            # 5) Esperar el postback: filas del grid, error de captcha o vacío
            #    CONFIRMADO (la imagen del captcha cambia al procesarse).
            fin = time.monotonic() + 30
            certificados: List[Dict[str, Any]] = []
            error_viewer = ""
            while time.monotonic() < fin:
                await pagina.wait_for_timeout(2000)
                certificados = await pagina.evaluate(_JS_LEER_FILAS)
                error_viewer = await _texto_error_viewer(pagina)
                img_despues = await _img_captcha(pagina)
                procesado = (img_despues and img_despues != img) or bool(certificados) or bool(error_viewer)
                if procesado:
                    break

            if not certificados and not error_viewer:
                img_despues = await _img_captcha(pagina)
                if not (img_despues and img_despues != img):
                    # Ni filas ni error ni señal de postback: respuesta no
                    # procesada — NUNCA tratarla como "sin certificados".
                    raise BotSenaSinResultado(
                        "La página no mostró respuesta a la consulta (posible cambio del portal)"
                    )

            # 6) Captcha rechazado por el portal → reintento del orquestador
            #    (el portal ya refrescó la imagen para el próximo intento).
            if _RE_CAPTCHA_ERROR.search(error_viewer):
                raise BotSenaCaptchaFallido(f"El portal rechazó el captcha: {error_viewer[:150]}")

            # 7) Normalizar fechas del portal ("09 Febrero, 2013" → ISO).
            for c in certificados:
                c["fecha_certificacion"] = _fecha_iso_es(c.get("fecha_certificacion", "")) or c.get("fecha_certificacion")
                c["fecha_firma"] = _fecha_iso_es(c.get("fecha_firma", "")) or c.get("fecha_firma")

            SALIDA.mkdir(exist_ok=True)
            try:
                (SALIDA / "resultado_ultimo.html").write_text(await pagina.content(), encoding="utf-8")
            except Exception:
                pass  # un dump jamás tumba la consulta
            texto_resultado = " ".join((await pagina.inner_text("body")).split())

            no_registra = not certificados
            mensaje = "" if certificados else "La cédula no registra certificados disponibles en el SENA"

            return {
                "cedula": cedula_norm,
                "no_registra": no_registra,
                "mensaje": mensaje[:300],
                "total_certificados": len(certificados),
                "certificados": certificados[:50],
                "texto_resultado": texto_resultado[:1500],
                "pdf_bytes": None,   # el PDF de cada certificado se descarga por
                "pdf_ruta": None,    # link propio — fuera de alcance (solo listado)
                "html": await pagina.content(),
            }
        finally:
            await navegador.close()


def consultar_sena_sync(cedula: str) -> Dict[str, Any]:
    """Versión síncrona para asyncio.to_thread, igual que los demás bots."""
    with _LOCK:
        return asyncio.run(consultar_sena(cedula))


if __name__ == "__main__":
    import json
    import sys

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = [a for a in sys.argv[1:] if a != "--headed"]
    if len(args) < 1:
        print("Uso: python Funciones/bot_sena.py CEDULA [--headed]")
        sys.exit(2)
    resultado = consultar_sena_sync(args[0])
    resultado.pop("pdf_bytes", None)
    resultado.pop("html", None)
    print(json.dumps(resultado, ensure_ascii=False, indent=2, default=str))
