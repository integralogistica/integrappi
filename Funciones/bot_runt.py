# Funciones/bot_runt.py
"""
Bot de consulta ciudadana de vehículo del portal público del RUNT.

https://portalpublico.runt.gov.co/#/consulta-vehiculo/consulta/consulta-ciudadana

Portal PÚBLICO de consulta ciudadana (Mintransporte lo describe como consulta
abierta por placa + cédula del propietario): la fuente "runt" SÍ va en los
defaults de empresa (no es opt-in como policia). Requiere que la cédula sea de
un PROPIETARIO ACTIVO de la placa — si no, el portal responde "Los datos
registrados no corresponden con los propietarios activos para el vehículo
consultado" (verificado con EYX243/15887928 el 2026-08-30).

Descubrimiento (2026-08-30, dumps descargas_runt/): SPA Angular 11 + Angular
Material, HASH routing (jamás `networkidle`). Formulario:
  - input[formcontrolname=placa]            (Nro. placa)
  - mat-select[formcontrolname=procedencia] (Tipo de Documento; default
    NACIONAL = cédula ciudadanía — NO se toca)
  - input[formcontrolname=documento]        (Nro. documento del propietario)
  - input[formcontrolname=captcha]          (captcha de IMAGEN propio)
  - botón "Consultar Información"
La página de resultado es #/consulta-vehiculo/consulta/info-vehiculo con la
identificación del vehículo arriba (PLACA/LICENCIA/ESTADO/SERVICIO/CLASE) y un
ACORDEÓN de mat-expansion-panel con carga perezosa:
  - "Información general del vehículo" (abierto por defecto: marca, línea,
    modelo, color, motor, chasis, VIN, cilindraje, matrícula inicial, autoridad)
  - "Póliza SOAT" (tabla: Número de póliza, Fecha expedición, Fecha inicio de
    vigencia, Fecha fin de vigencia, Entidad expide SOAT, Código tarifa,
    Estado VIGENTE/NO VIGENTE con check_circle/cancel) — pólizas históricas
    también visibles: la PRIMERA fila es la más reciente.
  - RTM, Pólizas RC, etc.
El captcha es una imagen PNG embebida data:image/png;base64 (única del DOM) de
~5 caracteres: se resuelve por servicio 2Captcha-compatible con method=base64
(~US$0.001). Sin PDF consolidado: las descargas son por póliza/certificado
(icono download en la columna acciones); el bot NO descarga la póliza (el
informe de Integra se genera con reportlab como en policia).

El mensaje de "no propietario activo" llega en un modal SweetAlert2 (swal2-*).
"""
import asyncio
import logging
import os
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv
from playwright.async_api import async_playwright

# Cargar .env del proyecto para la key del captcha cuando se ejecute standalone.
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

logger = logging.getLogger(__name__)

PORTAL_URL = "https://portalpublico.runt.gov.co/#/consulta-vehiculo/consulta/consulta-ciudadana"
URL_RESULTADO = "#/consulta-vehiculo/consulta/info-vehiculo"
SALIDA = Path(__file__).resolve().parents[1] / "descargas_runt"

# Bloqueo para serializar consultas al portal (una a la vez, como los demás bots).
_LOCK = threading.Lock()

_TIMEOUT_MS = 45000              # Playwright: goto/clicks/esperas puntuales
_RENDER_MS = 4000                # render inicial del SPA Angular
_PASO_RESULTADO_MS = 30000       # espera de la página de resultado tras Consultar
_CAPTCHA_BASE = os.getenv("SEGURIDAD_RUNT_CAPTCHA_URL", "https://2captcha.com").rstrip("/")
# API_KEY_CAPTCHA es la key ya usada por los demás procesos del proyecto.
_CAPTCHA_KEY = os.getenv("SEGURIDAD_RUNT_CAPTCHA_KEY", "").strip() or os.getenv("API_KEY_CAPTCHA", "").strip()
_CAPTCHA_TIMEOUT_S = float(os.getenv("SEGURIDAD_RUNT_CAPTCHA_TIMEOUT_S", "90"))
_CAPTCHA_POOL_S = 5.0            # polling de res.php

# Mensajes oficiales del portal (calibrados con la sonda 2026-08-30).
_RE_NO_PROPIETARIO = re.compile(
    r"no\s+corresponden\s+con\s+los\s+propietarios\s+activos", re.IGNORECASE
)
_RE_NO_ENCONTRADO = re.compile(
    r"(?:no\s+se\s+encontr[oó]\s+(?:informaci[oó]n|ning[uú]n\s+registro)|"
    r"placa\s+no\s+(?:registrada|existe)|no\s+existe\s+informaci[oó]n)", re.IGNORECASE
)

# Iconos de Angular Material que sangran el texto plano (se limpian de los valores).
_ICONOS_MATERIAL = re.compile(
    r"\b(?:calendar_month|directions_car|car_crash|check_circle|check|cancel|eco|"
    r"list_alt|credit_card|download|folder_open|local_police|badge|blur_on|sell|lock|block)\b",
    re.IGNORECASE,
)

# Campos de la "Información general del vehículo" en el ORDEN del portal: el
# valor de cada uno va de su label al label del siguiente (el texto plano los
# pega: "MARCA: HONDA LÍNEA: CB 160F DLX MODELO: 2018 ...").
_CAMPOS_VEHICULO = [
    ("PLACA DEL VEHÍCULO", "placa"),
    ("NRO. DE LICENCIA DE TRÁNSITO", "licencia_transito"),
    ("ESTADO DEL VEHÍCULO", "estado_vehiculo"),
    ("TIPO DE SERVICIO", "tipo_servicio"),
    ("CLASE DE VEHÍCULO", "clase"),
    ("MARCA", "marca"),
    ("LÍNEA", "linea"),
    ("MODELO", "modelo"),
    ("COLOR", "color"),
    ("NÚMERO DE MOTOR", "numero_motor"),
    ("NÚMERO DE CHASIS", "numero_chasis"),
    ("NÚMERO DE VIN", "numero_vin"),
    ("CILINDRAJE", "cilindraje"),
    ("TIPO DE CARROCERÍA", "tipo_carroceria"),
    ("TIPO COMBUSTIBLE", "combustible"),
    ("FECHA DE MATRICULA INICIAL", "fecha_matricula_inicial"),
    ("AUTORIDAD DE TRÁNSITO", "autoridad_transito"),
    ("GRAVÁMENES A LA PROPIEDAD", "gravamenes"),
    ("CLÁSICO O ANTIGUO", "clasico_antiguo"),
    ("REPOTENCIADO", "repotenciado"),
]
# Fila de póliza SOAT en el texto plano (tabla expandida):
# "3453028900 04/10/2025 23/10/2025 22/10/2026 AXA COLPATRIA SEGUROS SA 112 check_circle VIGENTE"
_RE_POLIZA = re.compile(
    r"(\d{8,})\s+(\d{2}/\d{2}/\d{4})\s+(\d{2}/\d{2}/\d{4})\s+(\d{2}/\d{2}/\d{4})\s+"
    r"(.{3,60}?)\s+(\d{2,4})\s+(?:check_circle\s+|cancel\s+)?(VIGENTE|NO\s+VIGENTE|VENCIDO)",
)


class BotRuntError(Exception):
    """Error del bot de consulta de vehículo del RUNT."""


class BotRuntSinCaptchaKey(BotRuntError):
    """Falta configurar la key del resolvedor (fallo de config, accionable)."""


class BotRuntCaptchaFallido(BotRuntError):
    """El resolvedor rechazó el pedido o el portal rechazó el texto."""


class BotRuntSinResultado(BotRuntError):
    """La página de resultado no contenía datos del vehículo (anti-envenenamiento)."""


def _resolver_captcha_imagen(img_data_url: str) -> str:
    """Resuelve el captcha de imagen propio del portal vía 2Captcha-compatible.

    El portal embebe el PNG como data:image/png;base64 — sin sitekey ni iframe:
    se envía el base64 directo con method=base64 (~US$0.001). Síncrona
    (requests): se llama desde la corutina con asyncio.to_thread.
    """
    if not _CAPTCHA_KEY:
        raise BotRuntSinCaptchaKey("Falta configurar SEGURIDAD_RUNT_CAPTCHA_KEY para la fuente runt")
    b64 = img_data_url.split(",", 1)[-1]
    try:
        r = requests.post(f"{_CAPTCHA_BASE}/in.php", data={
            "key": _CAPTCHA_KEY, "method": "base64", "body": b64, "json": 1,
        }, timeout=30)
        dato = r.json()
    except requests.RequestException as exc:
        raise BotRuntCaptchaFallido(f"El resolvedor de captcha no respondió: {exc}") from exc
    if dato.get("status") != 1:
        raise BotRuntCaptchaFallido(f"El resolvedor rechazó el pedido: {dato.get('request')}")

    captcha_id = str(dato.get("request"))
    logger.info("[BOT RUNT] captcha pedido %s; sondeando cada %.0f s", captcha_id, _CAPTCHA_POOL_S)
    fin = time.monotonic() + _CAPTCHA_TIMEOUT_S
    while time.monotonic() < fin:
        time.sleep(_CAPTCHA_POOL_S)
        try:
            r2 = requests.get(f"{_CAPTCHA_BASE}/res.php", params={
                "key": _CAPTCHA_KEY, "action": "get", "id": captcha_id, "json": 1,
            }, timeout=30)
            dato2 = r2.json()
        except requests.RequestException as exc:
            raise BotRuntCaptchaFallido(f"El resolvedor de captcha no respondió: {exc}") from exc
        if dato2.get("status") == 1:
            return str(dato2["request"])
        if dato2.get("request") != "CAPCHA_NOT_READY":
            raise BotRuntCaptchaFallido(f"El resolvedor reportó: {dato2.get('request')}")
    raise BotRuntCaptchaFallido(f"El resolvedor no resolvió el captcha en {_CAPTCHA_TIMEOUT_S:.0f} s")


def _fecha_iso(valor: str) -> Optional[str]:
    """'22/10/2026' → '2026-10-22' (fechas del portal DD/MM/AAAA); None si no parsea."""
    m = re.search(r"(\d{2})/(\d{2})/(\d{4})", valor or "")
    if not m:
        return None
    d, mes, a = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        return datetime(a, mes, d).date().isoformat()
    except ValueError:
        return None


def _soat_vigente(fecha_fin_iso: Optional[str], hoy: Optional[datetime] = None) -> Optional[bool]:
    """True si la fecha fin de vigencia ≥ hoy (hora Colombia, UTC−5)."""
    if not fecha_fin_iso:
        return None
    hoy = hoy or (datetime.now(timezone.utc) - timedelta(hours=5))
    try:
        fin = datetime.strptime(fecha_fin_iso, "%Y-%m-%d")
    except ValueError:
        return None
    return fin.date() >= hoy.date()


def _limpiar_valor(valor: str) -> str:
    """Limpia un valor crudo del texto plano: espacios, ':' y iconos material."""
    valor = _ICONOS_MATERIAL.sub(" ", valor or "")
    return " ".join(valor.replace(":", " ").split()).strip()


# Terminadores extra para campos cuyo valor sigue pegado a texto ajeno: el
# valor de "CLASE" termina en el título de sección "Información general...", el
# de "COLOR" en el label "NÚMERO DE SERIE" (sin valor propio), etc.
_TERMINADORES_EXTRA = {
    "clase": r"Informaci[oó]n\s+general|Datos\s+T[eé]cnicos",
    "color": r"N[UÚ]MERO\s+DE\s+SERIE",
    "gravamenes": r"CL[AÁ]SICO\s+O\s+ANTIGUO",
    "repotenciado": r"REGRABACI[UÓ]N",
    "combustible": r"FECHA\s+DE\s+MATRICULA",
    "estado_vehiculo": r"TIPO\s+DE\s+SERVICIO",
}


async def _leer_resultado(pagina) -> Dict[str, Any]:
    """Lee la página info-vehiculo: datos del vehículo + panel SOAT expandido.

    Devuelve {datos_vehiculo, soat, polizas, no_registra, mensaje}. Los paneles
    SOAT/RTM están colapsados con carga perezosa: se expanden por título antes
    de leer (descubierto con la sonda 2026-08-30).
    """
    # Expandir SOAT (la tabla de pólizas se carga al abrir el panel).
    try:
        await pagina.locator("mat-expansion-panel-header", has_text="Póliza SOAT").first.click(timeout=8000)
        await pagina.wait_for_timeout(2500)  # carga Ajax de la tabla
    except Exception as exc:
        logger.warning("[BOT RUNT] panel SOAT no se pudo expandir: %s", exc)
    texto_plano = _ICONOS_MATERIAL.sub(" ", " ".join((await pagina.inner_text("body")).split()))
    SALIDA.mkdir(exist_ok=True)
    (SALIDA / "resultado_ultimo.html").write_text(await pagina.content(), encoding="utf-8")

    # 1) Datos del vehículo: campos en el ORDEN del portal — el valor de cada
    #    uno va de su label al label del siguiente (el texto plano los pega).
    datos_vehiculo: Dict[str, str] = {}
    for i, (label, clave) in enumerate(_CAMPOS_VEHICULO):
        if i + 1 < len(_CAMPOS_VEHICULO):
            patron = re.escape(label) + r"\s*(?:\(DD/MM/AAAA\))?\s*:?\s*(.*?)\s*" + re.escape(_CAMPOS_VEHICULO[i + 1][0])
        else:
            patron = re.escape(label) + r"\s*(?:\(DD/MM/AAAA\))?\s*:?\s*([^:.]{2,70})"
        m = re.search(patron, texto_plano)
        if not m:
            continue
        valor = m.group(1)
        # Cortar en terminadores extra cuando el label siguiente NO está
        # inmediatamente después (texto de sección intercalado).
        if clave in _TERMINADORES_EXTRA:
            m2 = re.search(rf"(.*?)\s*(?:{_TERMINADORES_EXTRA[clave]})", valor, re.IGNORECASE)
            if m2:
                valor = m2.group(1)
        datos_vehiculo[clave] = _limpiar_valor(valor)

    # 2) Mensajes del portal (modal SweetAlert2 o texto plano).
    no_registra: Optional[bool] = None
    mensaje = ""
    if _RE_NO_PROPIETARIO.search(texto_plano):
        # La cédula no es de un propietario activo de la placa: resultado
        # legítimo y determinante del portal (no un vacío sospechoso).
        no_registra = False
        mensaje = "La cédula no corresponde a un propietario activo del vehículo"
    elif _RE_NO_ENCONTRADO.search(texto_plano):
        no_registra = True
        mensaje = "La placa no registra información en el RUNT"

    # 3) Pólizas SOAT (tabla expandida): regex dirigido por la estructura fija
    #    de la fila. La PRIMERA es la póliza más reciente.
    polizas: List[Dict[str, Any]] = []
    for mm in _RE_POLIZA.finditer(texto_plano):
        polizas.append({
            "numero": mm.group(1),
            "fecha_expedicion": _fecha_iso(mm.group(2)),
            "fecha_inicio_vigencia": _fecha_iso(mm.group(3)),
            "fecha_fin_vigencia": _fecha_iso(mm.group(4)),
            "aseguradora": mm.group(5).strip(),
            "codigo_tarifa": mm.group(6),
            "estado": mm.group(7).strip(),
        })

    # 4) SOAT para el semáforo: póliza más reciente; vigente si fecha_fin ≥ hoy.
    soat: Optional[Dict[str, Any]] = None
    if polizas:
        actual = polizas[0]
        soat = {
            "numero": actual["numero"],
            "aseguradora": actual["aseguradora"],
            "fecha_inicio_vigencia": actual["fecha_inicio_vigencia"],
            "fecha_fin_vigencia": actual["fecha_fin_vigencia"],
            "estado_portal": actual["estado"],
            "vigente": _soat_vigente(actual["fecha_fin_vigencia"]),
        }

    return {
        "datos_vehiculo": datos_vehiculo,
        "soat": soat,
        "polizas": polizas[:10],
        "no_registra": no_registra,
        "mensaje": mensaje[:300],
    }


async def consultar_vehiculo_runt(placa: str, cedula: str, headed: bool = False) -> Dict[str, Any]:
    """Consulta un vehículo del RUNT por placa + cédula del propietario.

    Requiere que la cédula sea de un propietario ACTIVO de la placa (validación
    del propio portal). Retorna: placa, cedula, no_registra (bool | None),
    datos_vehiculo (dict), soat (dict | None: póliza más reciente con
    semáforo), polizas (historial ≤10), nombre_propietario ("" — el portal no
    lo expone en la vista ciudadana), mensaje, texto_resultado, pdf_bytes
    (None), pdf_ruta (None) y html.
    """
    placa_norm = re.sub(r"[^A-Za-z0-9]", "", placa or "").upper()
    if not re.fullmatch(r"[A-Z]{3}[0-9]{2}[0-9A-Z]|[A-Z]{2}[0-9]{4}", placa_norm):
        raise BotRuntError("Placa inválida")
    cedula_norm = re.sub(r"\D", "", cedula or "")
    if not 3 <= len(cedula_norm) <= 15:
        raise BotRuntError("Cédula inválida")

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

            # 1) SPA Angular hash-routing: domcontentloaded + espera de render.
            await pagina.goto(PORTAL_URL, wait_until="domcontentloaded", timeout=_TIMEOUT_MS)
            await pagina.wait_for_selector("input[formcontrolname=placa]", timeout=_TIMEOUT_MS)
            await pagina.wait_for_timeout(_RENDER_MS)

            # 2) Formulario. Procedencia default NACIONAL (CC): no se toca.
            await pagina.fill("input[formcontrolname=placa]", placa_norm)
            await pagina.fill("input[formcontrolname=documento]", cedula_norm)
            await pagina.wait_for_timeout(500)

            # 3) Captcha de imagen (única data:image del DOM) → 2Captcha base64.
            img = await pagina.evaluate(
                """() => {
                    for (const e of document.images) {
                        if ((e.src || '').startsWith('data:image')) return e.src;
                    }
                    return '';
                }"""
            )
            if not img:
                raise BotRuntError("El portal no mostró la imagen del captcha (posible cambio del portal)")
            texto_captcha = await asyncio.wait_for(
                asyncio.to_thread(_resolver_captcha_imagen, img),
                timeout=_CAPTCHA_TIMEOUT_S + 15,
            )
            await pagina.fill("input[formcontrolname=captcha]", texto_captcha)
            try:
                (SALIDA / f"captcha_{placa_norm}.png").write_bytes(
                    __import__("base64").b64decode(img.split(",", 1)[-1])
                )
            except Exception:
                pass

            # 4) Consultar. SPA: el resultado renderiza client-side en la misma
            #    pestaña (hash cambia a info-vehiculo); sin navegación completa.
            async def _esperar_descarga():
                try:
                    async with pagina.expect_download(timeout=_PASO_RESULTADO_MS) as info:
                        pass
                    await info.value
                except Exception:
                    pass  # no hay PDF consolidado; las descargas son por póliza

            tarea_descarga = asyncio.create_task(_esperar_descarga())
            try:
                await pagina.get_by_role("button", name="Consultar Información").click(timeout=10000)
            except Exception as exc:
                raise BotRuntError(f"El portal no aceptó la consulta: {exc}") from exc

            # 5) Esperar el resultado: o la página info-vehiculo (hash) o un
            #    modal de error (SweetAlert2). Ambas dentro del presupuesto.
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                await pagina.wait_for_timeout(1500)
                if URL_RESULTADO in pagina.url:
                    break
                if "swal2-title" in await pagina.content():
                    break
            await pagina.wait_for_timeout(1500)
            try:
                await asyncio.wait_for(asyncio.shield(tarea_descarga), timeout=3)
            except asyncio.TimeoutError:
                pass

            # 6) Error de captcha del portal → reintento (consume solve).
            if URL_RESULTADO not in pagina.url:
                err = ""
                loc = pagina.locator(".swal2-title, .swal2-html-container")
                if await loc.count():
                    err = " ".join(" ".join(await loc.all_inner_texts()).split())
                if "captcha" in (err or "").lower():
                    raise BotRuntCaptchaFallido(f"El portal rechazó el captcha: {err[:150]}")
                if err:
                    # Modal con mensaje de negocio (no propietario activo, placa
                    # no encontrada…): resultado determinante, se procesa igual.
                    logger.info("[BOT RUNT] modal del portal: %s", err[:200])

            resultado = await _leer_resultado(pagina)
            texto_resultado = _ICONOS_MATERIAL.sub(" ", " ".join((await pagina.inner_text("body")).split()))

            # 7) Anti-envenenamiento: sin datos del vehículo, sin pólizas y sin
            #    mensaje determinante = respuesta incompleta; NUNCA cachear.
            if (
                not resultado["datos_vehiculo"]
                and not resultado["polizas"]
                and resultado["no_registra"] is None
            ):
                raise BotRuntSinResultado(
                    "La página de resultado no contenía datos del vehículo (posible cambio del portal)"
                )

            return {
                "placa": placa_norm,
                "cedula": cedula_norm,
                **resultado,
                "nombre_propietario": "",  # el portal ciudadano no lo expone
                "texto_resultado": texto_resultado[:1500],
                "pdf_bytes": None,
                "pdf_ruta": None,
                "html": await pagina.content(),
            }
        finally:
            await navegador.close()


def consultar_vehiculo_runt_sync(placa: str, cedula: str) -> Dict[str, Any]:
    """Versión síncrona para asyncio.to_thread, igual que los demás bots."""
    with _LOCK:
        return asyncio.run(consultar_vehiculo_runt(placa, cedula))


if __name__ == "__main__":
    import json
    import sys

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = [a for a in sys.argv[1:] if a != "--headed"]
    if len(args) < 2:
        print("Uso: python Funciones/bot_runt.py PLACA CEDULA [--headed]")
        sys.exit(2)
    resultado = consultar_vehiculo_runt_sync(args[0], args[1])
    resultado.pop("pdf_bytes", None)
    resultado.pop("html", None)
    print(json.dumps(resultado, ensure_ascii=False, indent=2, default=str))
