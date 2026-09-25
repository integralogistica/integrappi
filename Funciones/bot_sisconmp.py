# Funciones/bot_sisconmp.py
"""
Bot de consulta de capacitaciones de Mercancías Peligrosas SISCONMP (Mintransporte).

https://web.mintransporte.gov.co/sisconmp2/consultascapacitaciones/

Portal PÚBLICO de consulta ciudadana (requisito exigido por la Resolución
1223 de 2014): la fuente "sisconmp" SÍ va en los defaults de empresa (no es
opt-in). Consulta por CÉDULA (tipo doc CC, default del portal).

Descubrimiento (2026-09-25, dumps descargas_sisconmp/): MVC server-rendered
(bootstrap + select2). Formulario:
  - select#cboTDI   (tipo doc; el JS del portal auto-selecciona CC — NO
                     tocarlo, la lección de ADRES/#tipoDoc)
  - input#txtNDI    (Número de Documento de Identidad)
  - span#btnConsultarMD / #btnConsultarXS  (submit — es un <span> con
                     onclick="ConsultarConductor()", NO un <button>)
El click ejecuta **reCAPTCHA v3 invisible** (`grecaptcha.execute(sitekey,
{action: 'consultar_conductor'})`) que la PROPIA página resuelve: sin
checkbox, sin desafío, sin 2Captcha — costo $0. Verificado headless 2026-09-25.

La consulta es un AJAX POST a /SISCONMP2/ConsultasCapacitaciones/
ConsultarConductor {TDI, NDI, recaptchaToken} que devuelve un ARRAY JSON:
  []  → "No se encontrarón registros" (determinante, cacheable)
  [{Apellidos, Nombres, NombreCapacitacion, TipoCapacitacion
    ('CURSO BASICO' | titulación NCL), EntidadCertificadora (MEN/SENA/…),
    InstitucionEducativa, FechaExpedicion, FechaVencimiento, FechaRegistro
    (ASP.NET /Date(ms)/), Clase, ValorNumericoClase, DescripcionClase,
    TipoVehiculo}]

⚠️ TRAMPA (por eso este bot lee la RESPUESTA AJAX, jamás el DOM): el handler
`error:` del portal muestra el MISMO "No se encontrarón registros" cuando el
server falla (403/500) que cuando la respuesta es vacía legítima — fiarse
del div envenenaría la caché con falsos "no registra".

Sin PDF del portal; el informe se genera con reportlab como en simit/sena.
"""
import asyncio
import logging
import os
import re
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from playwright.async_api import async_playwright

try:  # importado como paquete (orquestador) o standalone (CLI)
    from Funciones.captura_evidencia import capturar_viewport_jpeg
except ImportError:  # pragma: no cover - ejecución como script
    from captura_evidencia import capturar_viewport_jpeg

logger = logging.getLogger(__name__)

PORTAL_URL = os.getenv(
    "SEGURIDAD_SISCONMP_URL",
    "https://web.mintransporte.gov.co/sisconmp2/consultascapacitaciones/")
SALIDA = Path(__file__).resolve().parents[1] / "descargas_sisconmp"

# Tope de capacitaciones persistidas por sección (el total vive en
# total_capacitaciones) — análogo SEGURIDAD_MAX_CERTIFICADOS_DOC del SENA.
MAX_CAPACITACIONES_DOC = int(os.getenv("SEGURIDAD_MAX_CAPACITACIONES_DOC", "20"))

# Bloqueo para serializar consultas al portal (una a la vez, como los demás bots).
_LOCK = threading.Lock()

_TIMEOUT_MS = 45000              # Playwright: goto/esperas puntuales
_RENDER_MS = 4000                # render inicial + carga select2/CargarTDI
_PASO_RESULTADO_S = 30           # presupuesto de espera del AJAX tras submit

_COLOMBIA = timezone(timedelta(hours=-5))
_RE_FECHA_MS = re.compile(r"/Date\((\-?\d+)")   # ASP.NET /Date(1742312400000)/
_RE_FECHA_ISO = re.compile(r"(\d{4})-(\d{2})-(\d{2})")

_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")


class BotSisconmpError(Exception):
    """Error del bot de consulta de capacitaciones SISCONMP."""


class BotSisconmpSinResultado(BotSisconmpError):
    """La respuesta del portal no era el JSON de capacitaciones (anti-envenenamiento)."""


def _fecha_iso(valor: Any) -> Optional[str]:
    """/Date(1742312400000)/ o '2026-03-12…' → '2026-03-12' (hora Colombia).

    ASP.NET serializa instantes UTC en ms; el portal los muestra con
    formatearFecha(new Date(ms)) en hora LOCAL del navegador (Colombia):
    convertimos a UTC−5 para que la fecha coincida con la que ve un humano.
    """
    if valor in (None, "", 0):
        return None
    texto = str(valor)
    m = _RE_FECHA_MS.search(texto)
    if m:
        try:
            dt = datetime.fromtimestamp(int(m.group(1)) / 1000, tz=_COLOMBIA)
            return dt.date().isoformat()
        except (ValueError, OSError, OverflowError):
            return None
    m = _RE_FECHA_ISO.search(texto)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None


def _hoy_colombia() -> datetime:
    return datetime.now(_COLOMBIA)


def _capacitacion_vigente(fecha_vencimiento: Optional[str]) -> Optional[bool]:
    """None si la fecha no es legible; True/False comparando contra HOY Colombia.

    Se RECALCULA en cada hit de caché (análogo SOAT/RTM): una capacitación
    vigente ayer puede estar vencida hoy y la caché no congela el veredicto.
    """
    if not fecha_vencimiento:
        return None
    try:
        vence = datetime.fromisoformat(fecha_vencimiento).replace(tzinfo=_COLOMBIA)
    except ValueError:
        return None
    return vence.date() >= _hoy_colombia().date()


def _parsear_capacitaciones(data: List[Any]) -> Dict[str, Any]:
    """Array JSON del portal → shape cacheable del doc (capacitaciones ≤ tope)."""
    capacitaciones: List[Dict[str, Any]] = []
    apellidos = nombres = ""
    for fila in data:
        if not isinstance(fila, dict):
            continue
        if not apellidos:
            apellidos = (fila.get("Apellidos") or "").strip()
        if not nombres:
            nombres = (fila.get("Nombres") or "").strip()
        nombre_cap = (fila.get("NombreCapacitacion") or "").strip()
        if not nombre_cap:
            # El propio portal descarta estas filas al renderizar (sin panel)
            continue
        vence = _fecha_iso(fila.get("FechaVencimiento"))
        capacitaciones.append({
            "tipo_capacitacion": (fila.get("TipoCapacitacion") or "").strip()[:80],
            "nombre": nombre_cap[:200],
            "entidad_certificadora": (fila.get("EntidadCertificadora") or "").strip()[:40],
            "institucion_educativa": (fila.get("InstitucionEducativa") or "").strip()[:200],
            "fecha_expedicion": _fecha_iso(fila.get("FechaExpedicion")),
            "fecha_vencimiento": vence,
            "fecha_registro": _fecha_iso(fila.get("FechaRegistro")),
            "clase": (fila.get("Clase") or "").strip()[:60],
            "descripcion_clase": (fila.get("DescripcionClase") or "").strip()[:200],
            "tipo_vehiculo": (fila.get("TipoVehiculo") or "").strip()[:60],
        })
    return {
        "apellidos": apellidos,
        "nombres": nombres,
        "total_capacitaciones": len(capacitaciones),
        "capacitaciones": capacitaciones[:MAX_CAPACITACIONES_DOC],
    }


async def consultar_capacitaciones_sisconmp(cedula: str, headed: bool = False) -> Dict[str, Any]:
    """Consulta las capacitaciones de Mercancías Peligrosas de una cédula (CC).

    Retorna: cedula, no_registra (True = array vacío legítimo), mensaje,
    apellidos/nombres del ciudadano según el portal, total_capacitaciones,
    capacitaciones[] (curso/titulación, entidad, institución, fechas ISO,
    clase MP, tipo de vehículo) con `vigente` calculado contra HOY Colombia,
    texto_resultado, pdf_bytes (None), pdf_ruta (None), captura_jpg y html.
    """
    cedula_norm = re.sub(r"\D", "", cedula or "")
    if not (6 <= len(cedula_norm) <= 10):
        raise BotSisconmpError("Cédula inválida")

    async with async_playwright() as p:
        navegador = await p.chromium.launch(headless=not headed)
        try:
            contexto = await navegador.new_context(
                viewport={"width": 1366, "height": 900},
                user_agent=_UA,
                locale="es-CO",
                accept_downloads=True,
            )
            pagina = await contexto.new_page()

            # 1) Carga (MVC server-rendered + select2/CargarTDI por Ajax).
            await pagina.goto(PORTAL_URL, wait_until="domcontentloaded", timeout=_TIMEOUT_MS)
            await pagina.wait_for_selector("#txtNDI", timeout=_TIMEOUT_MS)
            await pagina.wait_for_timeout(_RENDER_MS)

            # 2) Formulario: SOLO la cédula. #cboTDI NO se toca (el JS del
            #    portal auto-selecciona CC; tocar un select con postback
            #    re-renderiza y borra el campo — lección ADRES 2026-09-24).
            await pagina.fill("#txtNDI", cedula_norm)
            await pagina.wait_for_timeout(500)

            # 3) Submit + captura de la RESPUESTA AJAX (la única fuente de
            #    verdad: el DOM miente — el handler error: muestra el mismo
            #    "no registra" del vacío legítimo). El reCAPTCHA v3 invisible
            #    lo ejecuta la propia página: nada que resolver ($0).
            async with pagina.expect_response(
                lambda r: "ConsultarConductor" in r.url, timeout=_PASO_RESULTADO_S * 1000
            ) as info_resp:
                try:
                    await pagina.locator("#btnConsultarMD, #btnConsultarXS").first.click(timeout=10000)
                except Exception as exc:
                    raise BotSisconmpError(f"El portal no aceptó la consulta: {exc}") from exc
            respuesta = await info_resp.value

            if respuesta.status != 200:
                # 403 = el server rechazó el token v3 (score); 5xx = caído.
                # NUNCA leer el DOM en este punto (diría "no registra").
                raise BotSisconmpError(
                    f"El portal respondió {respuesta.status} a la consulta (token v3 rechazado o caída)"
                )
            try:
                data = await respuesta.json()
            except Exception as exc:
                raise BotSisconmpSinResultado(
                    "La respuesta del portal no era el JSON de capacitaciones"
                ) from exc
            if not isinstance(data, list):
                raise BotSisconmpSinResultado(
                    f"Shape inesperado del portal: {type(data).__name__}"
                )

            # 4) Esperar el render de los paneles (evidencia) y volcar debug.
            try:
                await pagina.wait_for_selector(
                    "#divResultados .panel, #divNoHayRegistros:visible", timeout=10000)
            except Exception:
                pass  # el JSON ya es la verdad; el render es solo evidencia
            await pagina.wait_for_timeout(800)

            SALIDA.mkdir(exist_ok=True)
            try:
                (SALIDA / "resultado_ultimo.html").write_text(await pagina.content(), encoding="utf-8")
            except Exception:
                pass  # un dump jamás tumba la consulta

            no_registra = len(data) == 0
            leido = _parsear_capacitaciones(data)
            # `vigente` se recalcula (aquí y en cada hit de caché del
            # orquestador): la caché jamás congela el semáforo de vigencia.
            for cap in leido["capacitaciones"]:
                cap["vigente"] = _capacitacion_vigente(cap["fecha_vencimiento"])

            texto_resultado = " ".join((await pagina.inner_text("body")).split())
            captura = await capturar_viewport_jpeg(pagina)

            return {
                "cedula": cedula_norm,
                "no_registra": no_registra,
                "mensaje": ("No se encontrarón registros sobre el ciudadano."
                            if no_registra else ""),
                **leido,
                "texto_resultado": texto_resultado[:1500],
                "pdf_bytes": None,
                "pdf_ruta": None,
                "captura_jpg": captura,
                "html": await pagina.content(),
            }
        finally:
            await navegador.close()


def consultar_capacitaciones_sisconmp_sync(cedula: str) -> Dict[str, Any]:
    """Versión síncrona para asyncio.to_thread, igual que los demás bots."""
    with _LOCK:
        return asyncio.run(consultar_capacitaciones_sisconmp(cedula))


if __name__ == "__main__":
    import json
    import sys

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = [a for a in sys.argv[1:] if a != "--headed"]
    if len(args) < 1:
        print("Uso: python Funciones/bot_sisconmp.py CEDULA [--headed]")
        sys.exit(2)
    resultado = consultar_capacitaciones_sisconmp_sync(args[0])
    resultado.pop("pdf_bytes", None)
    resultado.pop("html", None)
    print(json.dumps(resultado, ensure_ascii=False, indent=2, default=str))
