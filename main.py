from dotenv import load_dotenv
load_dotenv()

import asyncio
import logging
import sys
from contextlib import asynccontextmanager

# Windows: Playwright necesita ProactorEventLoop para lanzar Chromium (subproceso).
# Bajo uvicorn/asyncio puede caer en SelectorEventLoop, que NO soporta subprocess
# y lanza NotImplementedError al crear el navegador. En Linux (Render) es no-op.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from rutas.aut2 import ruta_usuario
from rutas.pagoSaldos import ruta_manifiestos
from rutas.novedades import ruta_novedades
from rutas.vehiculos import ruta_vehiculos
from rutas.empleados import ruta_empleado
from rutas.revision import ruta_revision
from rutas.baseusuarios import ruta_baseusuarios
from rutas.clientes import ruta_clientes
from rutas.clientes_siscore import ruta_clientes_siscore
from rutas.clientes_general import ruta_clientes_general
from rutas.ciudades_general import ruta_ciudades_general
from rutas.fletes import ruta_fletes
from rutas.tarifas_rutas_fmc import ruta_tarifas_rutas_fmc
from rutas.pedidos import ruta_pedidos
from rutas.whatsapp_integra import ruta_whatsapp_integra
from rutas.whatsapp_report_integra import ruta_whatsapp_report
from rutas.debug import ruta_debug_network
from rutas.debug_siscore import ruta_debug_siscore
from rutas.pacientes_medical_care import router as ruta_pacientes_medical_care
from rutas.pedidos_v3 import router as ruta_pedidos_v3
from rutas.sync_v3 import router as ruta_sync_v3, _obtener_config_desde_db, actualizar_ultimo_resultado
from rutas.cronograma_mc import router as ruta_cronograma_mc
from rutas.siscore_consultas import router as ruta_siscore_consultas
from rutas.divipolas import ruta_divipolas
from rutas.banco import router as ruta_banco
from rutas.indicadores_costo_operacion import router as ruta_indicadores_costo_operacion
from rutas.indicadores_cliente import router as ruta_indicadores_cliente
from rutas.disponibilidad import ruta_disponibilidad
from rutas.conductores import ruta_conductores
from rutas.otros_costos import router as ruta_otros_costos
from rutas.cuentas_placa import router as ruta_cuentas_placa
from rutas.sicetac import reanudar_jobs_excel, router as ruta_sicetac
from rutas.seguridad import router as ruta_seguridad
from rutas.seguridad_estudios import admin_router as ruta_seguridad_admin, router as ruta_seguridad_estudios
from rutas.seguridad_cobro import router as ruta_seguridad_cobro
from Funciones.sync_api_v3 import ejecutar_sync_v3, archivar_mes_v3

logger = logging.getLogger(__name__)


async def _loop_sync_v3():
    """
    Tarea de fondo: ejecuta sync_v3 en los horarios configurados (HH:MM).
    Revisa cada 30 segundos. Además, el último día de cada mes a las 00:00
    ejecuta el corte mensual (archivar_mes_v3).
    """
    from datetime import datetime
    import calendar
    import pytz

    print("[sync_v3] Tarea de fondo iniciada")  # Print para asegurar que se vea en Render
    logger.info("[sync_v3] Tarea de fondo iniciada")

    ultimo_ejecutado: str | None = None    # evita doble ejecución del sync en el mismo minuto
    ultimo_archivado: str | None = None    # evita doble archivo en el mismo mes ('YYYY-MM')
    _tz = pytz.timezone('America/Bogota')

    while True:
        try:
            await asyncio.sleep(30)  # revisa cada 30 segundos

            hoy   = datetime.now(_tz)
            ahora = hoy.strftime("%H:%M")

            # Log cada tick para debugging (temporal)
            print(f"[sync_v3] Tick - Hora Bogotá: {ahora}")  # Print para debugging

            # ── Corte mensual: último día del mes a las 00:00 ────────────────────
            ultimo_dia_mes = calendar.monthrange(hoy.year, hoy.month)[1]
            clave_mes      = hoy.strftime('%Y-%m')
            if hoy.day == ultimo_dia_mes and ahora == "00:00" and clave_mes != ultimo_archivado:
                ultimo_archivado = clave_mes
                logger.info(f"[archivo_mensual] Ejecutando corte de fin de mes {clave_mes}")
                try:
                    await asyncio.to_thread(archivar_mes_v3)
                except Exception as e:
                    logger.error(f"[archivo_mensual] Error: {e}")

            # ── Sync programado ──────────────────────────────────────────────────
            sync_config = _obtener_config_desde_db()
            if not sync_config.get("activo", True):
                print(f"[sync_v3] Sync inactivo, tick a las {ahora}")
                continue

            horarios = sync_config.get("horarios", [])
            print(f"[sync_v3] Horarios configurados: {horarios}, hora actual: {ahora}")

            if ahora in horarios and ahora != ultimo_ejecutado:
                ultimo_ejecutado = ahora
                print(f"[sync_v3] ¡EJECUTANDO sync programado a las {ahora}!")
                logger.info(f"[sync_v3] Ejecutando sync programado a las {ahora}")
                try:
                    resultado = await ejecutar_sync_v3()
                    actualizar_ultimo_resultado(resultado)
                except Exception as e:
                    logger.error(f"[sync_v3] Error en sync: {e}")
            elif ahora not in horarios:
                ultimo_ejecutado = None  # reset para que el próximo horario pueda ejecutar

        except asyncio.CancelledError:
            print("[sync_v3] Tarea cancelada (apagando servidor)")
            raise
        except Exception as e:
            print(f"[sync_v3] Error en loop principal: {e}")
            logger.error(f"[sync_v3] Error en loop principal: {e}")
            # Continuar ejecutando a pesar del error
            await asyncio.sleep(60)  # Esperar 1 min antes de reintentar


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[LIFESPAN] Iniciando aplicación...")
    logger.info("[LIFESPAN] Iniciando aplicación...")

    # Windows + uvicorn --reload: uvicorn pisa la política Proactor del import
    # superior con WindowsSelectorEventLoopPolicy (la necesita para su watcher).
    # El loop YA corrido no cambia, pero los asyncio.run de los HILOS de los bots
    # (endpoints v1: /seguridad/procuraduria etc.) heredarían Selector →
    # Playwright NotImplementedError. Re-forzar Proactor AQUÍ (esto corre tras
    # el setup de uvicorn) hace que todo loop nuevo cree subprocesos bien.
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    # Recupera cargas Excel SICE-TAC pendientes antes de atender solicitudes.
    await asyncio.to_thread(reanudar_jobs_excel)

    # Crear la tarea de fondo
    task = asyncio.create_task(_loop_sync_v3())
    print(f"[LIFESPAN] Tarea de sync creada: {task}")
    logger.info(f"[LIFESPAN] Tarea de sync creada: {task}")

    try:
        yield
    finally:
        print("[LIFESPAN] Apagando servidor, cancelando tarea de sync...")
        logger.info("[LIFESPAN] Apagando servidor, cancelando tarea de sync...")
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            print("[LIFESPAN] Tarea cancelada exitosamente")
            pass


app = FastAPI(lifespan=lifespan)
app.title = "integra"
app.version = "1"

# Configuración de CORS
# Se incluyen dominios de producción y localhost para desarrollo
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "https://integralogistica.com",
        "https://www.integralogistica.com",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
)

app.include_router(ruta_usuario)
app.include_router(ruta_manifiestos)
app.include_router(ruta_novedades)
app.include_router(ruta_vehiculos)
app.include_router(ruta_empleado)
app.include_router(ruta_baseusuarios)
app.include_router(ruta_clientes)
app.include_router(ruta_clientes_siscore)
app.include_router(ruta_clientes_general)
app.include_router(ruta_ciudades_general)
app.include_router(ruta_fletes)
app.include_router(ruta_tarifas_rutas_fmc)
app.include_router(ruta_pedidos)
app.include_router(ruta_revision)
app.include_router(ruta_whatsapp_integra)
app.include_router(ruta_whatsapp_report)
app.include_router(ruta_debug_network)
app.include_router(ruta_debug_siscore)
app.include_router(ruta_pacientes_medical_care)
app.include_router(ruta_pedidos_v3)
app.include_router(ruta_sync_v3)
app.include_router(ruta_cronograma_mc)
app.include_router(ruta_siscore_consultas)
app.include_router(ruta_divipolas)
app.include_router(ruta_banco)
app.include_router(ruta_indicadores_costo_operacion)
app.include_router(ruta_indicadores_cliente)
app.include_router(ruta_disponibilidad)
app.include_router(ruta_conductores)
app.include_router(ruta_otros_costos)
app.include_router(ruta_cuentas_placa)
app.include_router(ruta_sicetac)
app.include_router(ruta_seguridad)
app.include_router(ruta_seguridad_estudios)
app.include_router(ruta_seguridad_admin)
app.include_router(ruta_seguridad_cobro)

@app.get("/", tags=['Home'])
async def root():
    return {"message": "Hello integra"}

if __name__ == "__main__":
    import os
    import uvicorn
    # Render inyecta PORT; localmente se mantiene 8000 por defecto
    port = int(os.getenv("PORT", "8000"))
    # Playwright se ejecuta en varios hilos, cada uno con su propio event loop.
    # uvloop no admite que esos loops creen subprocesos Chromium al mismo
    # tiempo ("Racing with another loop to spawn a process"). El loop asyncio
    # estándar de Python usa ThreadedChildWatcher y sí soporta este patrón.
    uvicorn.run(app, host="0.0.0.0", port=port, loop="asyncio")
