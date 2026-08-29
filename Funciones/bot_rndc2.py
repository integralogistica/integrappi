# Funciones/bot_rndc2.py
"""
Bot de scraping del portal público RNDC2 (Ministerio de Transporte).

Consulta el "Historial de Viajes de una Placa o un Conductor":
https://rndc2.mintransporte.gov.co/logistica/ctl/HistorialViajes/mid/394

Flujo: cargar página -> leer captcha aritmético (texto plano en la página,
ej. "30 + 48") -> resolverlo -> llenar placa/cédula/fechas -> btConsultar ->
parsear la tabla de resultados.

El portal es un DotNetNuke WebForms: los IDs de servidor son estables
(dnn_ctr394_HistorialManifiestos_*), por lo que los selectores son fiables.

Contexto: el web service SOAP rechaza la consulta vehicular (proceso 12) para
las credenciales actuales (RNDC07 sistemático; ver scripts/probar_rndc_placa.py),
por lo que no existe alternativa autorizada y se usa el canal público, igual
que bot_siscore.py reemplazó el WS caído de planillas.
"""
import asyncio
import logging
import re
import threading
from typing import Any, Dict, List, Optional

from playwright.async_api import async_playwright

logger = logging.getLogger(__name__)

PORTAL_URL = "https://rndc2.mintransporte.gov.co/logistica/ctl/HistorialViajes/mid/394"

# Prefijo de los IDs de servidor del módulo (estables en DotNetNuke)
_ID = "dnn_ctr394_HistorialManifiestos"
SEL_CAPTCHA = f"#{_ID}_Cat"
SEL_RESULTADO = f"#{_ID}_Resultado"
SEL_PLACA = f"#{_ID}_PLACA"
SEL_CEDULA = f"#{_ID}_CEDULA"
SEL_TIPO_ID = f"#{_ID}_TIPOIDENTIFICACIONLISTA"
SEL_FECHA_INI = f"#{_ID}_FechaInicial"
SEL_FECHA_FIN = f"#{_ID}_FechaFinal"
SEL_BOTON = f"#{_ID}_btConsultar"

# Bloqueo para serializar consultas al portal público (una a la vez).
_LOCK = threading.Lock()

_TIMEOUT_MS = 45000
_MAX_INTENTOS_CAPTCHA = 3


class BotRNDC2Error(Exception):
    """Error del bot del portal RNDC2."""


def _resolver_captcha_texto(texto: str) -> Optional[int]:
    """Extrae y resuelve la operación aritmética del texto del captcha."""
    m = re.search(r"(\d+)\s*([+\-*x×])\s*(\d+)", texto or "")
    if not m:
        return None
    a, op, b = int(m.group(1)), m.group(2), int(m.group(3))
    if op == "+":
        return a + b
    if op == "-":
        return a - b
    return a * b


async def consultar_historial_viajes(
    placa: Optional[str] = None,
    cedula: Optional[str] = None,
    tipo_identificacion: str = "C",
    fecha_inicio: str = "",
    fecha_fin: str = "",
    headed: bool = False,
    proxy: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Consulta el historial de viajes por placa y/o cédula.

    FechaInicio/fecha_fin en formato AAAA/MM/DD (el que usa el portal).
    Retorna dict con: captcha_usado, filtros, columnas, viajes (lista de dicts),
    html (str) para depuración.
    """
    if not placa and not cedula:
        raise BotRNDC2Error("Se requiere placa o cédula para consultar")
    placa_norm = (placa or "").strip().upper()
    cedula_norm = re.sub(r"\D", "", cedula or "")

    async with async_playwright() as p:
        navegador = await p.chromium.launch(headless=not headed, proxy=proxy)
        try:
            contexto = await navegador.new_context(
                viewport={"width": 1366, "height": 900},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
                locale="es-CO",
                ignore_https_errors=True,
            )
            pagina = await contexto.new_page()
            await pagina.goto(PORTAL_URL, wait_until="domcontentloaded", timeout=_TIMEOUT_MS)

            for intento in range(1, _MAX_INTENTOS_CAPTCHA + 1):
                # 1) Leer y resolver el captcha
                await pagina.wait_for_selector(SEL_CAPTCHA, timeout=_TIMEOUT_MS)
                texto_captcha = (await pagina.inner_text(SEL_CAPTCHA)).strip()
                resultado = _resolver_captcha_texto(texto_captcha)
                if resultado is None:
                    raise BotRNDC2Error(f"No se pudo interpretar el captcha: {texto_captcha!r}")

                # 2) Llenar el formulario
                await pagina.fill(SEL_RESULTADO, str(resultado))
                if placa_norm:
                    await pagina.fill(SEL_PLACA, placa_norm)
                if cedula_norm:
                    await pagina.select_option(SEL_TIPO_ID, tipo_identificacion)
                    await pagina.fill(SEL_CEDULA, cedula_norm)
                if fecha_inicio:
                    await pagina.fill(SEL_FECHA_INI, fecha_inicio)
                if fecha_fin:
                    await pagina.fill(SEL_FECHA_FIN, fecha_fin)

                # 3) Consultar (postback Ajax dentro de un UpdatePanel, no navega)
                await pagina.click(SEL_BOTON)
                # Esperar la respuesta: el span de fecha de consulta aparece/actualiza
                try:
                    await pagina.wait_for_function(
                        """() => {
                            const s = document.getElementById('dnn_ctr394_HistorialManifiestos_lbMsgError');
                            return s && /Consulta\\s+realizada/.test(s.textContent || '');
                        }""",
                        timeout=_TIMEOUT_MS,
                    )
                except Exception:
                    # si no aparece el mensaje, esperar a que termine la petición Ajax
                    await pagina.wait_for_load_state("networkidle", timeout=_TIMEOUT_MS)

                # 4) ¿Pidió captcha de nuevo o hay resultados/tabla de error?
                # El span del captcha persiste en el DOM aunque haya resultados;
                # el criterio es si el CAMPO de respuesta sigue visible y vacío.
                campo_visible = await pagina.locator(SEL_RESULTADO).count()
                valor_actual = (await pagina.input_value(SEL_RESULTADO)) if campo_visible else ""
                tiene_tabla = await pagina.locator(f"#{_ID} table").count()
                if campo_visible and not valor_actual and not tiene_tabla:
                    # el servidor limpió el campo y no hay tabla -> captcha nuevo
                    logger.info("[BOT RNDC2] Captcha renovado (intento %d), reintentando", intento)
                    continue
                break
            else:
                raise BotRNDC2Error("El portal renovó el captcha más veces de lo esperado")

            # 5) Parsear resultados
            return await _extraer_resultados(pagina, {
                "placa": placa_norm,
                "cedula": cedula_norm,
                "tipo_identificacion": tipo_identificacion,
                "fecha_inicio": fecha_inicio,
                "fecha_fin": fecha_fin,
            }, texto_captcha)
        finally:
            await navegador.close()


async def _extraer_resultados(pagina, filtros: Dict[str, Any], captcha_usado: str) -> Dict[str, Any]:
    """Extrae los resultados del TreeView tvDatos (o su ausencia) del portal."""
    cuerpo = await pagina.inner_text("body")
    columnas: List[str] = []
    viajes: List[Dict[str, Any]] = []

    # El TreeView tvDatos renderiza el encabezado y una fila por viaje como
    # texto separado por tabs dentro de cada línea.
    tv = pagina.locator(f"#{_ID}_tvDatos")
    if await tv.count():
        texto_tv = await tv.inner_text()
        lineas = [l.rstrip("\t") for l in texto_tv.split("\n") if l.strip()]
        if lineas:
            columnas = [c.strip() for c in lineas[0].split("\t") if c.strip()]
            for linea in lineas[1:]:
                plano = " ".join(linea.split())
                if "consulta realizada" in plano.lower():
                    continue  # pie del portal, no un viaje
                celdas = [c.strip() for c in linea.split("\t")]
                if celdas and any(celdas):
                    viajes.append(dict(zip(columnas, celdas)))
    return {
        "captcha_usado": captcha_usado,
        "filtros": filtros,
        "columnas": columnas,
        "viajes": viajes,
        "mensaje_portal": _mensaje_relevante(cuerpo),
        "html": await pagina.content(),
    }


def _mensaje_relevante(cuerpo: str) -> str:
    """Extrae mensajes de error/aviso del portal ('no se encontraron', etc.)."""
    for linea in (cuerpo or "").split("\n"):
        t = linea.strip()
        if not t or t.upper() == "INGRESAR":
            continue
        if re.search(r"no se encontr|no hay|sin resultados|no registra|error|debe |ingres|consulta realizada", t, re.IGNORECASE):
            if len(t) < 250:
                return t
    return ""


def consultar_historial_viajes_sync(
    placa: Optional[str] = None,
    cedula: Optional[str] = None,
    tipo_identificacion: str = "C",
    fecha_inicio: str = "",
    fecha_fin: str = "",
) -> Dict[str, Any]:
    """Versión síncrona (crea su propio event loop), para endpoints FastAPI
    ejecutados vía asyncio.to_thread / fire-and-forget, igual que bot_siscore."""
    with _LOCK:
        return asyncio.run(
            consultar_historial_viajes(
                placa=placa,
                cedula=cedula,
                tipo_identificacion=tipo_identificacion,
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin,
            )
        )
