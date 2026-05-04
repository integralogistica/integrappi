from dotenv import load_dotenv
load_dotenv()

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from rutas.aut2 import ruta_usuario
from rutas.pagoSaldos import ruta_manifiestos
from rutas.novedades import ruta_novedades
from rutas.vehiculos import ruta_vehiculos
from rutas.empleados import ruta_empleado
from rutas.revision import ruta_revision
from rutas.puente_biometrico import ruta_biometria
from rutas.baseusuarios import ruta_baseusuarios
from rutas.clientes import ruta_clientes
from rutas.clientes_siscore import ruta_clientes_siscore
from rutas.clientes_general import ruta_clientes_general
from rutas.ciudades_general import ruta_ciudades_general
from rutas.fletes import ruta_fletes
from rutas.pedidos import ruta_pedidos
from rutas.consultar_biometrico import ruta_verificacion
from rutas.whatsapp_integra import ruta_whatsapp_integra
from rutas.whatsapp_report_integra import ruta_whatsapp_report
from rutas.debug import ruta_debug_network
from rutas.debug_siscore import ruta_debug_siscore
from rutas.pacientes_medical_care import router as ruta_pacientes_medical_care
from rutas.pedidos_v3 import router as ruta_pedidos_v3
from rutas.sync_v3 import router as ruta_sync_v3, config as sync_config, actualizar_ultimo_resultado
from rutas.cronograma_mc import router as ruta_cronograma_mc
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
    logger.info("[sync_v3] Tarea de fondo iniciada")
    ultimo_ejecutado: str | None = None    # evita doble ejecución del sync en el mismo minuto
    ultimo_archivado: str | None = None    # evita doble archivo en el mismo mes ('YYYY-MM')
    _tz = pytz.timezone('America/Bogota')

    while True:
        await asyncio.sleep(30)  # revisa cada 30 segundos

        hoy   = datetime.now(_tz)
        ahora = hoy.strftime("%H:%M")

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
        if not sync_config.get("activo", True):
            continue

        horarios = sync_config.get("horarios", [])
        if ahora in horarios and ahora != ultimo_ejecutado:
            ultimo_ejecutado = ahora
            logger.info(f"[sync_v3] Ejecutando sync programado a las {ahora}")
            try:
                resultado = await ejecutar_sync_v3()
                actualizar_ultimo_resultado(resultado)
            except Exception as e:
                logger.error(f"[sync_v3] Error en sync: {e}")
        elif ahora not in horarios:
            ultimo_ejecutado = None  # reset para que el próximo horario pueda ejecutar


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(_loop_sync_v3())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
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
app.include_router(ruta_biometria)
app.include_router(ruta_baseusuarios)
app.include_router(ruta_clientes)
app.include_router(ruta_clientes_siscore)
app.include_router(ruta_clientes_general)
app.include_router(ruta_ciudades_general)
app.include_router(ruta_fletes)
app.include_router(ruta_pedidos)
app.include_router(ruta_verificacion)
app.include_router(ruta_revision)
app.include_router(ruta_whatsapp_integra)
app.include_router(ruta_whatsapp_report)
app.include_router(ruta_debug_network)
app.include_router(ruta_debug_siscore)
app.include_router(ruta_pacientes_medical_care)
app.include_router(ruta_pedidos_v3)
app.include_router(ruta_sync_v3)
app.include_router(ruta_cronograma_mc)

@app.get("/", tags=['Home'])
async def root():
    return {"message": "Hello integra"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)