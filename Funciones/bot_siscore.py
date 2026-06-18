# Funciones/bot_siscore.py
"""
Bot de scraping del portal Siscore con Playwright.

Reemplaza la fuente de datos del WS de planillas (/siscore/consultar-planillas)
scrapeando el portal https://integra.appsiscore.com/app/index.php:

  Login -> GESTION DE INFORMES -> basica -> Informes Mensajeros
        -> Planilla de despacho (abre ventana nueva)
        -> por cada planilla: pegar, buscar, clic en
           "DESCARGAR ASIGNADOS ACTUALMENTE" -> leer el .xlsx.

Headless por defecto (no visible). SISCORE_BOT_HEADED=true para depurar.

IMPORTANTE: los selectores exactos del portal no se conocen de antemano, por lo
que se usa una estrategia ROBUSTA basada en texto visible + screenshots/HTML en
cada fallo. Es probable que haya que afinar selectores tras un smoke test headed.
"""
import asyncio
import atexit
import logging
import os
import re
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from . import siscore_excel_mapper as mapper

logger = logging.getLogger(__name__)

PORTAL_URL = "https://integra.appsiscore.com/app/index.php"

# Textos del menú (case-insensitive)
TXT_GESTION_INFORMES = "GESTION DE INFORMES"
TXT_BASICA = "basica"
TXT_INFORMES_MENSajeros = "informes mensajeros"
TXT_PLANILLA_DESPACHO = "planilla de despacho"
TXT_DESCARGAR_ASIGNADOS = "descargar asignados actualmente"


# ---------------------------------------------------------------------------
# Configuración desde entorno
# ---------------------------------------------------------------------------

def _cfg_str(clave: str, default: str = "") -> str:
    v = os.getenv(clave)
    return v.strip() if v and v.strip() else default


def _cfg_bool(clave: str, default: bool = False) -> bool:
    v = os.getenv(clave)
    if not v:
        return default
    return v.strip().lower() in ("1", "true", "yes", "si", "on")


def _cfg_int(clave: str, default: int) -> int:
    try:
        return int(os.getenv(clave, str(default)))
    except (TypeError, ValueError):
        return default


def _download_dir() -> Path:
    p = Path(_cfg_str("SISCORE_BOT_DOWNLOAD_DIR", "./descargas_siscore"))
    p.mkdir(parents=True, exist_ok=True)
    return p


def _construir_proxy() -> Optional[Dict[str, str]]:
    """
    Construye el dict proxy para Playwright desde SISCORE_BOT_PROXY_URL o, si no
    existe, desde VULCANO_PROXY_URL. Soporta URLs con usuario:clave embebidos.
    """
    url = _cfg_str("SISCORE_BOT_PROXY_URL") or _cfg_str("VULCANO_PROXY_URL")
    if not url:
        return None
    parsed = urlparse(url)
    proxy: Dict[str, str] = {"server": f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"}
    if parsed.username:
        proxy["username"] = parsed.username
    if parsed.password:
        proxy["password"] = parsed.password
    logger.info(f"[BOT] Proxy configurado: {parsed.hostname}:{parsed.port}")
    return proxy


# Bloqueo (threading) para serializar sesiones del portal y evitar logins
# concurrentes. Es de threading (no asyncio) porque el bot se ejecuta vía
# asyncio.run en un hilo del threadpool (cada llamada crea su propio loop).
_LOCK = threading.Lock()


# ---------------------------------------------------------------------------
# Helpers de interacción robustos (sin selectores fijos)
# ---------------------------------------------------------------------------

async def _click_por_texto(page, texto: str, timeout: int = 15000, exact: bool = False) -> bool:
    """
    Intenta hacer clic en un elemento por texto visible probando varias estrategias:
    link, button, y luego cualquier texto. Devuelve True si pudo hacer clic.
    """
    patron = re.compile(re.escape(texto), re.IGNORECASE)
    estrategias = [
        lambda: page.get_by_role("link", name=patron).first,
        lambda: page.get_by_role("button", name=patron).first,
        lambda: page.get_by_role("menuitem", name=patron).first,
        lambda: page.get_by_text(patron, exact=exact).first,
    ]
    ultimo_error = None
    for i, strat in enumerate(estrategias):
        try:
            loc = strat()
            await loc.wait_for(state="visible", timeout=timeout)
            await loc.click(timeout=timeout)
            logger.info(f"[BOT] Clic exitoso en '{texto}' (estrategia {i})")
            return True
        except Exception as e:
            ultimo_error = e
            continue
    logger.warning(f"[BOT] No se pudo hacer clic en '{texto}': {ultimo_error}")
    return False


async def _debug_dump(page, nombre: str, download_dir: Path):
    """Guarda screenshot + HTML para depurar un fallo."""
    try:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        png = download_dir / f"debug_{nombre}_{ts}.png"
        html = download_dir / f"debug_{nombre}_{ts}.html"
        await page.screenshot(path=str(png), full_page=True)
        content = await page.content()
        html.write_text(content, encoding="utf-8")
        logger.warning(f"[BOT] Dump de depuración guardado: {png}")
    except Exception as e:
        logger.error(f"[BOT] No se pudo generar dump de depuración: {e}")


async def _login(page, usuario: str, password: str, download_dir: Path):
    """Login al portal con estrategia robusta de localización de campos."""
    logger.info(f"[BOT] Navegando a {PORTAL_URL}")
    await page.goto(PORTAL_URL, wait_until="domcontentloaded")

    # Campo usuario
    user_input = None
    for sel in [
        "input[name*='user' i]", "input[name*='usu' i]",
        "input[type='email']", "input[type='text']",
    ]:
        try:
            loc = page.locator(sel).first
            await loc.wait_for(state="visible", timeout=4000)
            user_input = loc
            break
        except Exception:
            continue
    if not user_input:
        await _debug_dump(page, "login_no_user", download_dir)
        raise RuntimeError("No se encontró el campo de usuario en el login")

    await user_input.fill(usuario)

    # Campo password
    pass_input = page.locator("input[type='password']").first
    await pass_input.wait_for(state="visible", timeout=10000)
    await pass_input.fill(password)

    # Submit por texto, fallback a input[type=submit]/button
    clic_ok = await _click_por_texto(page, "ingresar", timeout=6000)
    if not clic_ok:
        clic_ok = await _click_por_texto(page, "entrar", timeout=4000)
    if not clic_ok:
        try:
            await page.locator("input[type='submit'], button[type='submit']").first.click(timeout=6000)
            clic_ok = True
        except Exception:
            clic_ok = False
    if not clic_ok:
        # Último recurso: Enter sobre el campo password
        await pass_input.press("Enter")

    # Verificar login. El campo password se oculta TANTO al entrar al dashboard
    # COMO al saltar la página de error "Usuario o Clave Incorrecta" (esta última
    # no tiene campo password). Por eso hay que distinguir los dos casos.
    try:
        await page.locator("input[type='password']").first.wait_for(state="hidden", timeout=30000)
    except Exception:
        # Fallback: buscar el enlace de cerrar sesión
        try:
            await page.get_by_text(re.compile(r"cerrar\s+sesi", re.I)).first.wait_for(
                state="visible", timeout=5000
            )
            logger.info("[BOT] Login exitoso (visible 'Cerrar Sesion')")
        except Exception:
            await _debug_dump(page, "login_fallido", download_dir)
            raise RuntimeError("Login fallido: no se salió de la página de login tras autenticarse")

    # ¿Salió la página de credenciales incorrectas? (falso positivo de "password oculto")
    if await page.get_by_text(re.compile(r"usuario o clave incorrecta|incorrecta", re.I)).count() > 0:
        await _debug_dump(page, "login_credenciales_rechazadas", download_dir)
        raise RuntimeError(
            "Login fallido: Siscore RECHAZÓ las credenciales (usuario/clave incorrectos "
            "o cuenta bloqueada por demasiados intentos automáticos). Verifica entrando "
            "manualmente a https://integra.appsiscore.com"
        )

    logger.info("[BOT] Login exitoso (campo password oculto)")

    # Dar tiempo al sidebar a renderizar (la conexión va por proxy y puede ser lenta)
    try:
        await page.wait_for_load_state("networkidle", timeout=30000)
    except Exception:
        pass
    await page.wait_for_timeout(800)


async def _navegar_a_planilla_despacho(page, context, download_dir: Path):
    """
    Navega el menú y abre 'Planilla de despacho'. Devuelve la página (popup) donde
    se opera la búsqueda. Si el portal abre una ventana nueva, devuelve esa página.
    """
    # En sesión reutilizada el sidebar (acordeones Bootstrap) queda con submenús
    # abiertos; como son data-toggle="collapse", volver a hacer clic los COLAPSARÍA
    # (por eso 'basica' quedaba hidden). Recargar deja el menú en su estado inicial
    # (colapsado) y la secuencia de clics funciona igual que en página recién cargada.
    try:
        await page.reload(wait_until="domcontentloaded")
    except Exception as e:
        logger.warning(f"[BOT] No se pudo recargar la página antes de navegar el menú: {e}")

    # Expandir cada nivel del menú
    for texto in [TXT_GESTION_INFORMES, TXT_BASICA, TXT_INFORMES_MENSajeros]:
        ok = await _click_por_texto(page, texto, timeout=12000)
        if not ok:
            await _debug_dump(page, f"menu_{texto.replace(' ', '_')}", download_dir)
            raise RuntimeError(f"No se pudo navegar al menú '{texto}'")
        # Esperar la animación de colapso de Bootstrap (~350ms)
        await page.wait_for_timeout(700)

    # 'Planilla de despacho' abre una ventana nueva (popup)
    popup = None
    try:
        async with context.expect_page(timeout=15000) as popup_info:
            ok = await _click_por_texto(page, TXT_PLANILLA_DESPACHO, timeout=12000)
            if not ok:
                raise RuntimeError("No se pudo hacer clic en 'Planilla de despacho'")
        popup = await popup_info.value
        await popup.wait_for_load_state("networkidle")
        logger.info("[BOT] Popup de 'Planilla de despacho' capturado")
    except Exception:
        # Fallback: quizá navegó en la misma página (no abrió popup)
        logger.warning("[BOT] No se capturó popup; se asume navegación en la misma página")
        await _debug_dump(page, "sin_popup", download_dir)
        popup = page

    return popup


async def _procesar_planilla(popup, numero: str, download_dir: Path,
                             lookup_divipolas: Dict[str, Dict[str, str]]) -> List[Dict[str, Any]]:
    """Busca una planilla, descarga el Excel y devuelve los registros mapeados."""
    # Localizar input de búsqueda (campo de texto visible en el popup)
    search_input = None
    for sel in [
        "input[name*='planilla' i]", "input[name*='busca' i]",
        "input[type='search']", "input[type='text']",
    ]:
        try:
            loc = popup.locator(sel).first
            await loc.wait_for(state="visible", timeout=5000)
            search_input = loc
            break
        except Exception:
            continue
    if not search_input:
        await _debug_dump(popup, f"sin_input_{numero}", download_dir)
        raise RuntimeError(f"Planilla {numero}: no se encontró el campo de búsqueda")

    await search_input.fill("")
    await search_input.fill(str(numero))

    # Clic en 'Buscar' o Enter
    if not await _click_por_texto(popup, "buscar", timeout=5000):
        await search_input.press("Enter")
    try:
        await popup.wait_for_load_state("networkidle")
    except Exception:
        pass

    # Clic en 'DESCARGAR ASIGNADOS ACTUALMENTE' capturando la descarga
    link = None
    for strat in [
        lambda: popup.get_by_role("link", name=re.compile(re.escape(TXT_DESCARGAR_ASIGNADOS), re.I)).first,
        lambda: popup.get_by_text(re.compile(re.escape(TXT_DESCARGAR_ASIGNADOS), re.I)).first,
    ]:
        try:
            loc = strat()
            await loc.wait_for(state="visible", timeout=15000)
            link = loc
            break
        except Exception:
            continue
    if not link:
        await _debug_dump(popup, f"sin_link_descarga_{numero}", download_dir)
        raise RuntimeError(f"Planilla {numero}: no se encontró el link 'DESCARGAR ASIGNADOS ACTUALMENTE'")

    # El portal entrega el archivo como HTML-disfrazado-de-.xls; el lector detecta
    # el formato por contenido, así que la extensión es solo orientativa.
    destino = download_dir / f"{numero}.xls"
    async with popup.expect_download(timeout=_cfg_int("SISCORE_BOT_TIMEOUT_MS", 60000)) as dl_info:
        await link.click()
    download = await dl_info.value
    await download.save_as(str(destino))
    logger.info(f"[BOT] Planilla {numero}: Excel descargado en {destino}")

    registros = mapper.leer_excel_a_registros(str(destino), str(numero), lookup_divipolas)

    # Limpieza del archivo temporal
    try:
        destino.unlink()
    except Exception:
        pass

    return registros


# ---------------------------------------------------------------------------
# Punto de entrada principal
# ---------------------------------------------------------------------------

async def consultar_planillas_via_bot(
    planillas: List[str],
    lookup_divipolas: Optional[Dict[str, Dict[str, str]]] = None,
    coleccion_divipolas: Any = None,
) -> Dict[str, Any]:
    """
    Consulta una lista de planillas vía el bot del portal Siscore.

    Args:
        planillas: lista de números de planilla.
        lookup_divipolas: dict {poblacion_norm: {ruta, departamento}}. Si es None
            y se pasa coleccion_divipolas, se construye aquí.
        coleccion_divipolas: colección pymongo para construir el lookup si hace falta.

    Returns:
        dict con: registros, total_registros, planillas_buscadas,
                  fecha_inicio, fecha_fin, errores.
    """
    from playwright.async_api import async_playwright

    usuario = _cfg_str("SISCORE_BOT_USER")
    password = _cfg_str("SISCORE_BOT_PASS")
    if not usuario or not password:
        raise RuntimeError("Faltan credenciales: define SISCORE_BOT_USER y SISCORE_BOT_PASS")

    if lookup_divipolas is None:
        lookup_divipolas = mapper.construir_lookup_divipolas(coleccion_divipolas) if coleccion_divipolas else {}

    headed = _cfg_bool("SISCORE_BOT_HEADED", False)
    timeout_ms = _cfg_int("SISCORE_BOT_TIMEOUT_MS", 60000)
    download_dir = _download_dir()
    proxy = _construir_proxy()

    hoy = datetime.now().strftime("%Y-%m-%d")
    todos_registros: List[Dict[str, Any]] = []
    errores: List[Dict[str, str]] = []

    with _LOCK:
        async with async_playwright() as p:
            launch_kwargs: Dict[str, Any] = {
                "headless": not headed,
                "args": ["--disable-blink-features=AutomationControlled", "--no-sandbox"],
            }
            if proxy:
                launch_kwargs["proxy"] = proxy
            browser = await p.chromium.launch(**launch_kwargs)
            try:
                context = await browser.new_context(
                    accept_downloads=True,
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    ),
                    viewport={"width": 1366, "height": 768},
                    locale="es-CO",
                )
                context.set_default_timeout(timeout_ms)
                page = await context.new_page()

                await _login(page, usuario, password, download_dir)
                popup = await _navegar_a_planilla_despacho(page, context, download_dir)

                registros_lote, errores_lote = await _procesar_lote(popup, planillas, download_dir, lookup_divipolas)
                todos_registros.extend(registros_lote)
                errores.extend(errores_lote)
            finally:
                await browser.close()

    logger.info(
        f"[BOT] Fin: {len(todos_registros)} registros, "
        f"{len(errores)} errores para {len(planillas)} planillas"
    )
    return {
        "registros": todos_registros,
        "total_registros": len(todos_registros),
        "planillas_buscadas": planillas,
        "fecha_inicio": hoy,
        "fecha_fin": hoy,
        "errores": errores,
    }


def consultar_planillas_via_bot_sync(
    planillas: List[str],
    lookup_divipolas: Optional[Dict[str, Dict[str, str]]] = None,
    coleccion_divipolas: Any = None,
) -> Dict[str, Any]:
    """
    Wrapper SÍNCRONO para llamar desde endpoints FastAPI.

    Ejecuta el bot con asyncio.run() en un ProactorEventLoop NUEVO (hilo actual),
    independiente del event loop de uvicorn. Así Playwright puede lanzar Chromium
    (subproceso) sin importar si el loop de uvicorn es SelectorEventLoop (Windows).

    IMPORTANTE: llamarlo desde un endpoint `def` (NO async) para que FastAPI lo
    ejecute en el threadpool y no congele el event loop principal.
    """
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    return asyncio.run(consultar_planillas_via_bot(
        planillas,
        lookup_divipolas=lookup_divipolas,
        coleccion_divipolas=coleccion_divipolas,
    ))


# ---------------------------------------------------------------------------
# Reuso de sesión: mantiene un navegador logueado VIVO entre requests para no
# loguearse en cada consulta (evita bloqueos del TMS por exceso de logins).
# Corre sobre un ProactorEventLoop persistente en un hilo en segundo plano.
# ---------------------------------------------------------------------------

async def _procesar_lote(popup, planillas, download_dir, lookup_divipolas):
    """Procesa una lista de planillas sobre un popup ya abierto. Devuelve (registros, errores)."""
    registros: List[Dict[str, Any]] = []
    errores: List[Dict[str, str]] = []
    for numero in planillas:
        numero = str(numero).strip()
        if not numero:
            continue
        try:
            regs = await _procesar_planilla(popup, numero, download_dir, lookup_divipolas)
            registros.extend(regs)
            if not regs:
                errores.append({"planilla": numero, "error": "Sin registros en el Excel descargado"})
        except Exception as e:
            logger.error(f"[BOT] Error procesando planilla {numero}: {e}")
            errores.append({"planilla": numero, "error": str(e)})
            continue
    return registros, errores


def _format_result(planillas, registros, errores):
    hoy = datetime.now().strftime("%Y-%m-%d")
    return {
        "registros": registros,
        "total_registros": len(registros),
        "planillas_buscadas": list(planillas),
        "fecha_inicio": hoy,
        "fecha_fin": hoy,
        "errores": errores,
    }


class BotSessionManager:
    """
    Mantiene una sesión de navegador logueada en Siscore, viva entre requests,
    sobre un ProactorEventLoop dedicado (hilo en segundo plano). Reusa la sesión
    mientras sea válida (dentro del TTL y no caída al login); si no, re-loguea.
    Así se evita loguear en cada consulta (causa de bloqueos del TMS).
    """

    def __init__(self):
        self._loop = None
        self._thread = None
        self._lock = None  # asyncio.Lock sobre el loop persistente
        self._playwright = None
        self._browser = None
        self._context = None
        self._page = None  # página principal logueada
        self._last_login = 0.0
        self._started = False

    def _ttl(self) -> int:
        return _cfg_int("SISCORE_BOT_SESSION_TTL", 900)  # 15 min por defecto

    def _ensure_started(self):
        if self._started:
            return
        ready = threading.Event()

        def _runner():
            if sys.platform == "win32":
                asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            self._lock = asyncio.Lock()
            ready.set()
            self._loop.run_forever()

        self._thread = threading.Thread(target=_runner, daemon=True)
        self._thread.start()
        ready.wait(timeout=15)
        self._started = True
        logger.info("[BOT-SESSION] Hilo/loop persistente iniciado")

    def _run(self, coro, timeout=300):
        self._ensure_started()
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return fut.result(timeout=timeout)

    async def _sesion_valida(self) -> bool:
        if not self._page or not self._browser or not self._last_login:
            return False
        try:
            if self._page.is_closed():
                return False
        except Exception:
            return False
        if time.monotonic() - self._last_login > self._ttl():
            logger.info("[BOT-SESSION] Sesión expirada por TTL; se re-logueará")
            return False
        try:
            # Si el campo password está presente, la sesión cayó al login
            if await self._page.locator("input[type='password']").count() > 0:
                logger.info("[BOT-SESSION] Sesión caída al login; se re-logueará")
                return False
            return True
        except Exception:
            return False

    async def _cerrar_recursos(self):
        for attr in ("_context", "_browser"):
            obj = getattr(self, attr, None)
            if obj is not None:
                try:
                    await obj.close()
                except Exception:
                    pass
                setattr(self, attr, None)
        self._page = None

    async def _crear_sesion(self):
        from playwright.async_api import async_playwright
        download_dir = _download_dir()
        usuario = _cfg_str("SISCORE_BOT_USER")
        password = _cfg_str("SISCORE_BOT_PASS")
        proxy = _construir_proxy()
        headed = _cfg_bool("SISCORE_BOT_HEADED", False)
        if self._playwright is None:
            self._playwright = await async_playwright().__aenter__()
        launch_kwargs: Dict[str, Any] = {
            "headless": not headed,
            "args": ["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        }
        if proxy:
            launch_kwargs["proxy"] = proxy
        self._browser = await self._playwright.chromium.launch(**launch_kwargs)
        self._context = await self._browser.new_context(
            accept_downloads=True,
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1366, "height": 768},
            locale="es-CO",
        )
        self._context.set_default_timeout(_cfg_int("SISCORE_BOT_TIMEOUT_MS", 60000))
        self._page = await self._context.new_page()
        await _login(self._page, usuario, password, download_dir)
        self._last_login = time.monotonic()
        logger.info("[BOT-SESSION] Sesión creada y logueada")

    async def _ensure_session(self):
        if await self._sesion_valida():
            logger.info("[BOT-SESSION] Reutilizando sesión activa")
            return
        await self._cerrar_recursos()
        await self._crear_sesion()

    async def _consultar_async(self, planillas, lookup_divipolas):
        async with self._lock:  # serializa el uso de la página compartida
            download_dir = _download_dir()
            try:
                await self._ensure_session()
                popup = await _navegar_a_planilla_despacho(self._page, self._context, download_dir)
                try:
                    registros, errores = await _procesar_lote(popup, planillas, download_dir, lookup_divipolas)
                finally:
                    try:
                        await popup.close()
                    except Exception:
                        pass
                return _format_result(planillas, registros, errores)
            except Exception:
                # Si falló (sesión muerta, etc.), invalidar para re-loguear la próxima vez
                self._last_login = 0.0
                raise

    def consultar(self, planillas, lookup_divipolas=None, coleccion_divipolas=None):
        """
        Entry síncrono (llamar desde endpoint `def`). Reusa la sesión si es válida.
        """
        if lookup_divipolas is None:
            lookup_divipolas = (
                mapper.construir_lookup_divipolas(coleccion_divipolas)
                if coleccion_divipolas else {}
            )
        timeout = _cfg_int("SISCORE_BOT_REQUEST_TIMEOUT", 300)
        return self._run(self._consultar_async(planillas, lookup_divipolas), timeout=timeout)

    async def _cerrar_todo(self):
        await self._cerrar_recursos()
        if self._playwright is not None:
            try:
                await self._playwright.__aexit__(None, None, None)
            except Exception:
                pass
            self._playwright = None

    def stop(self):
        if not self._loop:
            return
        loop = self._loop
        self._loop = None  # idempotente: evita reentrada (p.ej. atexit tras un stop manual)
        try:
            fut = asyncio.run_coroutine_threadsafe(self._cerrar_todo(), loop)
            fut.result(timeout=15)
        except Exception:
            pass
        try:
            loop.call_soon_threadsafe(loop.stop)
        except Exception:
            pass


# Singleton global (el loop/browser se crean de forma perezosa en el primer uso)
session_manager = BotSessionManager()
atexit.register(session_manager.stop)


# ---------------------------------------------------------------------------
# Smoke test headed (ejecutar manualmente para depurar selectores)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    from dotenv import load_dotenv
    load_dotenv()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if len(sys.argv) < 2:
        print("Uso: python -m Funciones.bot_siscore <planilla1,planilla2,...>")
        print("Ej.:  python -m Funciones.bot_siscore 123456,789012")
        sys.exit(1)

    planillas = [p.strip() for p in sys.argv[1].split(",") if p.strip()]

    # Construir lookup de divipolas si hay MONGO_URI
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    try:
        from pymongo import MongoClient
        client = MongoClient(mongo_uri)
        col = client["integra"]["divipolas"]
        lookup = mapper.construir_lookup_divipolas(col)
    except Exception as e:
        logger.warning(f"[SMOKE] No se pudo cargar divipolas ({e}); lookup vacío")
        lookup = {}

    # Forzar modo visible para poder observar el flujo
    os.environ["SISCORE_BOT_HEADED"] = "true"

    resultado = asyncio.run(consultar_planillas_via_bot(planillas, lookup_divipolas=lookup))
    import json
    print(json.dumps(resultado, ensure_ascii=False, indent=2, default=str))
