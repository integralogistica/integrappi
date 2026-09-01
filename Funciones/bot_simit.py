# Funciones/bot_simit.py
"""
Bot de consulta del estado de cuenta de comparendos SIMIT (portal público FCM).

https://www.fcm.org.co/simit/#/estado-cuenta

Portal PÚBLICO de consulta ciudadana (acepta número de identificación O placa
en un solo campo): la fuente "simit" SÍ va en los defaults de empresa (no es
opt-in como policia). La consulta se hace SOLO por placa (alcance definido
2026-09-01): el estado de cuenta es del VEHÍCULO, no de la persona evaluada.

Descubrimiento (2026-09-01, dumps descargas_simit/): SPA con HASH routing
(jamás `networkidle`). **SIN CAPTCHA** — submit directo (fuente gratuita y
rápida, ~10 s). Formulario:
  - input#txtBusqueda               ("Número de identificación o placa del vehículo")
  - button#btnNumDocPlaca           (submit, solo icono lupa, aria-label
                                     "Realizar consulta")
  - div#messageErrorTxtBusqueda     (errores de validación del campo)
Resultado en la MISMA vista (#/estado-cuenta), tres formas:
  1. LIMPIO: <h3> "No tienes comparendos ni multas registradas en Simit"
     (+ <p> "Revisa con tu número de identificación y/o placa…").
  2. CON DEUDA: div#resumenEstadoCuenta (Comparendos/Multas/Acuerdos
     de pago/Total) + tabla "Comparendos y Multas"
     (table.table-multas-responsive, filas tr.page-row, celdas td[data-label]):
     Tipo (número en <u> + "Comparendo"/"Multa" + "Fecha imposición:"),
     Notificación, Placa, Secretaría, Infracción (popover con data-content
     completo + label abreviado "71…"), Estado ("Pendiente" + nota curso),
     Valor, Valor a pagar. Paginación "Mostrar 5/10/15" (solo primera página:
     los TOTALES del resumen son globales). Footer: "Total (105): $…" y
     span#valorTotal "Total a pagar (0): $…" — el SEMÁFORO usa valorTotal
     (verificado ZZZ999: 105 pendientes históricos 1999-2000 con $0 a pagar).
  3. MODAL #modal-multiples-personas ("varios resultados") — visible solo si
     aplica; los modales OCULTOS contienen texto plantilla con la placa
     consultada: NUNCA leer el HTML crudo, solo contenido visible.

Sin PDF consolidado del estado de cuenta (hay "Guardar estado" por correo);
el informe de Integra se genera con reportlab como en policia/runt.
"""
import asyncio
import logging
import re
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from playwright.async_api import async_playwright

logger = logging.getLogger(__name__)

PORTAL_URL = "https://www.fcm.org.co/simit/#/estado-cuenta"
SALIDA = Path(__file__).resolve().parents[1] / "descargas_simit"

# Bloqueo para serializar consultas al portal (una a la vez, como los demás bots).
_LOCK = threading.Lock()

_TIMEOUT_MS = 45000              # Playwright: goto/esperas puntuales
_RENDER_MS = 4000                # render inicial del SPA
_PASO_RESULTADO_S = 30           # presupuesto de espera del resultado tras submit

_RE_PLACA = re.compile(r"[A-Z]{3}[0-9]{2}[0-9A-Z]|[A-Z]{2}[0-9]{4}")
_RE_LIMPIO = re.compile(r"no\s+tienes\s+comparendos\s+ni\s+multas", re.IGNORECASE)
_RE_COP = re.compile(r"\$?\s*([\d.,]+)")
_RE_FECHA = re.compile(r"(\d{2})/(\d{2})/(\d{4})")


class BotSimitError(Exception):
    """Error del bot de consulta de comparendos SIMIT."""


class BotSimitSinResultado(BotSimitError):
    """La página de resultado no contenía el estado de cuenta (anti-envenenamiento)."""


def _cop(valor: str) -> Optional[float]:
    """'$ 40.257.438' → 40257438.0 (COP con puntos de miles); None si no parsea."""
    m = _RE_COP.search(valor or "")
    if not m:
        return None
    numero = m.group(1).replace(".", "").replace(",", ".")
    try:
        return float(numero)
    except ValueError:
        return None


def _entero(valor: str) -> Optional[int]:
    m = re.search(r"\d+", valor or "")
    return int(m.group(0)) if m else None


def _fecha_iso(valor: str) -> Optional[str]:
    """'19/04/2000' → '2000-04-19'; None si no parsea."""
    m = _RE_FECHA.search(valor or "")
    if not m:
        return None
    d, mes, a = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        return datetime(a, mes, d).date().isoformat()
    except ValueError:
        return None


async def _leer_resultado(pagina) -> Dict[str, Any]:
    """Lee el estado de cuenta renderizado: resumen, totales y filas visibles.

    Devuelve {total_comparendos, total_multas, total_acuerdos, total_deuda,
    total_a_pagar, comparendos[], no_registra, mensaje}. Solo contenido
    VISIBLE (los modales ocultos traen texto plantilla con la placa).
    """
    comparendos: List[Dict[str, Any]] = []

    # Filas de la tabla (solo primera página — los totales del resumen son
    # globales). El data-content del popover trae la infracción COMPLETA.
    filas = await pagina.eval_on_selector_all(
        "table.table-multas-responsive tbody tr.page-row",
        r"""els => els.map(tr => {
            const celda = (lbl) => {
                const td = Array.from(tr.querySelectorAll('td')).find(t => t.getAttribute('data-label') === lbl);
                return td || null;
            };
            const txt = (lbl) => {
                const td = celda(lbl);
                if (!td) return '';
                const clone = td.cloneNode(true);
                clone.querySelectorAll('div, p').forEach(e => e.remove());
                return ((clone.innerText || '').trim().split(/\s+/)).join(' ');
            };
            const tipo = celda('Tipo');
            const infr = celda('Infracción');
            const popover = infr ? (infr.querySelector('[data-content]') || {}) : {};
            const estado = celda('Estado');
            const estadoNota = estado ? (estado.querySelector('p') || {}).innerText || '' : '';
            return {
                numero: tipo ? ((tipo.querySelector('u') || {}).innerText || '').trim() : '',
                tipo: tipo ? ((tipo.querySelector('p.font-weight-bold') || {}).innerText || '').trim() : '',
                fecha: tipo ? ((tipo.querySelector('span.fs-13') || {}).innerText || '') : '',
                notificacion: txt('Notificación'),
                placa: txt('Placa'),
                secretaria: txt('Secretaría'),
                infraccion: (popover.getAttribute && popover.getAttribute('data-content')) || txt('Infracción'),
                estado: txt('Estado'),
                estado_nota: ((estadoNota || '').trim().split(/\s+/)).join(' '),
                valor: txt('Valor'),
                valor_a_pagar: txt('Valor a pagar'),
            };
        })""",
    )
    for fila in filas:
        comparendos.append({
            "numero": (fila.get("numero") or "").strip(),
            "tipo": (fila.get("tipo") or "").strip(),
            "fecha_imposicion": _fecha_iso(fila.get("fecha") or ""),
            "notificacion": (fila.get("notificacion") or "").strip(),
            "placa": (fila.get("placa") or "").strip(),
            "secretaria": (fila.get("secretaria") or "").strip(),
            "infraccion": " ".join((fila.get("infraccion") or "").split())[:200],
            "estado": (fila.get("estado") or "").strip(),
            "estado_nota": (fila.get("estado_nota") or "").strip(),
            "valor": _cop(fila.get("valor") or ""),
            "valor_a_pagar": _cop(fila.get("valor_a_pagar") or ""),
        })

    # Resumen (conteos y deuda total) + total a pagar (semáforo).
    resumen = ""
    loc_resumen = pagina.locator("#resumenEstadoCuenta")
    if await loc_resumen.count():
        resumen = " ".join((await loc_resumen.first.inner_text()).split())
    valor_total_txt = ""
    loc_total = pagina.locator("#valorTotal")
    if await loc_total.count():
        valor_total_txt = " ".join((await loc_total.first.inner_text()).split())

    mensaje = ""
    no_registra: Optional[bool] = None
    for h3 in await pagina.locator("h3").all():
        try:
            if not await h3.is_visible():
                continue
            texto = " ".join((await h3.inner_text()).split())
        except Exception:
            continue
        if _RE_LIMPIO.search(texto):
            mensaje = texto
            break

    return {
        "total_comparendos": _entero(re.search(r"Comparendos:\s*(\d+)", resumen).group(1)) if re.search(r"Comparendos:\s*(\d+)", resumen) else (len(comparendos) or None),
        "total_multas": _entero(re.search(r"Multas:\s*(\d+)", resumen).group(1)) if re.search(r"Multas:\s*(\d+)", resumen) else None,
        "total_acuerdos": _entero(re.search(r"Acuerdos de pago:\s*(\d+)", resumen).group(1)) if re.search(r"Acuerdos de pago:\s*(\d+)", resumen) else None,
        "total_deuda": _cop(re.search(r"Total:\s*\$?[\s\d.,]+", resumen).group(0)) if re.search(r"Total:\s*\$?[\s\d.,]+", resumen) else None,
        "total_a_pagar": _cop(valor_total_txt),
        "comparendos": comparendos,
        "no_registra": no_registra,
        "mensaje": mensaje[:300],
    }


async def consultar_comparendos_simit(placa: str, headed: bool = False) -> Dict[str, Any]:
    """Consulta el estado de cuenta SIMIT de una placa (sin cédula, sin captcha).

    Retorna: placa, no_registra (None — el portal no distingue placa inexistente
    de placa sin comparendos), mensaje, total_comparendos/multas/acuerdos,
    total_deuda (deuda total reportada), total_a_pagar (lo efectivamente
    exigible — base del semáforo), comparendos (filas visibles de la primera
    página), texto_resultado, pdf_bytes (None), pdf_ruta (None) y html.
    """
    placa_norm = re.sub(r"[^A-Za-z0-9]", "", placa or "").upper()
    if not _RE_PLACA.fullmatch(placa_norm):
        raise BotSimitError("Placa inválida")

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

            # 1) SPA hash-routing: domcontentloaded + espera del formulario.
            await pagina.goto(PORTAL_URL, wait_until="domcontentloaded", timeout=_TIMEOUT_MS)
            await pagina.wait_for_selector("#txtBusqueda", timeout=_TIMEOUT_MS)
            await pagina.wait_for_timeout(_RENDER_MS)

            # 2) Formulario (un solo campo: cédula o placa) + submit directo.
            #    SIN captcha (hallazgo de la sonda 2026-09-01).
            await pagina.fill("#txtBusqueda", placa_norm)
            await pagina.wait_for_timeout(500)
            try:
                await pagina.locator("#btnNumDocPlaca").click(timeout=10000)
            except Exception as exc:
                raise BotSimitError(f"El portal no aceptó la consulta: {exc}") from exc

            # 3) Esperar el resultado en la MISMA vista: resumen con comparendos,
            #    mensaje de limpio, o error de validación del campo.
            deadline = time.monotonic() + _PASO_RESULTADO_S
            resultado = None
            while time.monotonic() < deadline:
                await pagina.wait_for_timeout(1500)
                if await pagina.locator("#resumenEstadoCuenta").count():
                    resultado = "resumen"
                    break
                err_loc = pagina.locator("#messageErrorTxtBusqueda")
                if await err_loc.count():
                    err_txt = " ".join((await err_loc.first.inner_text()).split())
                    if err_txt:
                        resultado = f"error:{err_txt}"
                        break
                for h3 in await pagina.locator("h3").all():
                    try:
                        if not await h3.is_visible():
                            continue
                        if _RE_LIMPIO.search(await h3.inner_text()):
                            resultado = "limpio"
                            break
                    except Exception:
                        continue
                if resultado:
                    break
            await pagina.wait_for_timeout(1000)

            SALIDA.mkdir(exist_ok=True)
            try:
                (SALIDA / "resultado_ultimo.html").write_text(await pagina.content(), encoding="utf-8")
            except Exception:
                pass  # un dump jamás tumba la consulta

            if resultado and resultado.startswith("error:"):
                raise BotSimitError(f"El portal rechazó la placa: {resultado[6:150]}")

            # 4) Modal visible de múltiples resultados (varias personas para el
            #    número): no aplica a consultas por placa — no improvisar.
            modal = pagina.locator("#modal-multiples-personas")
            if await modal.count() and await modal.first.is_visible():
                raise BotSimitSinResultado(
                    "El portal reportó varios resultados para la búsqueda (no soportado)"
                )

            # 5) Anti-envenenamiento: sin resumen, sin mensaje de limpio y sin
            #    error = respuesta incompleta; NUNCA cachear.
            if resultado is None:
                raise BotSimitSinResultado(
                    "La página de resultado no contenía el estado de cuenta (posible cambio del portal)"
                )

            leido = await _leer_resultado(pagina)
            texto_resultado = " ".join((await pagina.inner_text("body")).split())
            if resultado == "limpio":
                leido.update({
                    "total_comparendos": 0,
                    "total_multas": 0,
                    "total_acuerdos": 0,
                    "total_deuda": 0.0,
                    "total_a_pagar": 0.0,
                    "comparendos": [],
                })

            return {
                "placa": placa_norm,
                **leido,
                "texto_resultado": texto_resultado[:1500],
                "pdf_bytes": None,
                "pdf_ruta": None,
                "html": await pagina.content(),
            }
        finally:
            await navegador.close()


def consultar_comparendos_simit_sync(placa: str) -> Dict[str, Any]:
    """Versión síncrona para asyncio.to_thread, igual que los demás bots."""
    with _LOCK:
        return asyncio.run(consultar_comparendos_simit(placa))


if __name__ == "__main__":
    import json
    import sys

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = [a for a in sys.argv[1:] if a != "--headed"]
    if len(args) < 1:
        print("Uso: python Funciones/bot_simit.py PLACA [--headed]")
        sys.exit(2)
    resultado = consultar_comparendos_simit_sync(args[0])
    resultado.pop("pdf_bytes", None)
    resultado.pop("html", None)
    print(json.dumps(resultado, ensure_ascii=False, indent=2, default=str))
