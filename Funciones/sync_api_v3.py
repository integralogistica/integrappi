"""
Sincronización periódica V3 — consume directamente del API de Siscore.
Endpoint: https://integra-wms.appsiscore.com/app/ws/informe_v3.php
"""
import os
import time
import logging
from datetime import datetime

from bd.bd_cliente import bd_cliente
from rutas.pedidos_v3 import (
    _calcular_rango_fechas,
    _consultar_api_siscore_v3,
    _convertir_fecha_siscore_a_dd_mm_yyyy,
    _mapear_campos_siscore,
)
from rutas.pacientes_medical_care import ejecutar_cruce_automatico
from Funciones.whatsapp_utils_integra import enviar_template_sync

logger = logging.getLogger(__name__)

# Número de teléfono destino para notificaciones (con código de país, sin +)
# Ej: 573001234567
_NOTIFY_NUMBER = os.getenv('WHATSAPP_NOTIFY_NUMBER', '')


def _notificar_sync_v3(resultado: dict):
    """Envía WhatsApp con la plantilla confirmar_actualizacion tras cada sync."""
    if not _NOTIFY_NUMBER:
        return
    ts = resultado.get('timestamp', '')
    # Formato corto del timestamp: "09 abr 2026 09:42"
    try:
        from datetime import datetime as _dt
        ts_fmt = _dt.strptime(ts, '%Y-%m-%d %H:%M:%S').strftime('%d %b %Y %H:%M')
    except Exception:
        ts_fmt = ts

    if resultado.get('ok') and resultado.get('exitosos', 0) > 0:
        cruce = resultado.get('cruce') or {}
        linea_cruce = ''
        if cruce.get('ok'):
            linea_cruce = f"\nCruce: {cruce['total_pacientes']} pac., {cruce['total_sin_paciente']} sin match"
        cuerpo = (
            f"OK {resultado['exitosos']}/{resultado['total']} pedidos · {resultado['segundos']}s"
            + linea_cruce
            + f"\n{ts_fmt}"
        )
    else:
        errores = resultado.get('errores', [])
        detalle = errores[0] if errores else 'Error desconocido'
        cuerpo = f"ERROR sync V3\n{detalle}\n{ts_fmt}"

    try:
        res = enviar_template_sync(
            to=_NOTIFY_NUMBER,
            template_name='confirmar_actualizacion',
            language_code='es_CO',
            body_params=[cuerpo],
        )
        if res:
            logger.info(f"[sync_v3] Notificación WS enviada a {_NOTIFY_NUMBER}")
        else:
            logger.warning(f"[sync_v3] Notificación WS no enviada (tokens no configurados o error en API)")
    except Exception as e:
        logger.error(f"[sync_v3] Error enviando notificación WS: {e}")

_BD = bd_cliente['integra']
_COLECCION = _BD['v3']
_HISTORICO = _BD['v3_historico']
_CACHE_CRUCE = _BD['cache_cruce_mc']


async def ejecutar_sync_v3() -> dict:
    """
    Consume del API de Siscore, normaliza los datos y hace upsert en MongoDB.
    Retorna un dict con el resultado: exitosos, errores, total, timestamp.
    Al terminar (éxito o error) envía notificación WhatsApp si WHATSAPP_NOTIFY_NUMBER está configurado.
    """
    resultado = await _ejecutar_sync_v3_interno()
    _notificar_sync_v3(resultado)
    return resultado


async def _ejecutar_sync_v3_interno() -> dict:
    """
    Ejecuta el sync de V3 consumiendo directamente del API de Siscore.
    Calcula el rango de fechas automáticamente (1er día de hace 2 meses → hoy).
    """
    inicio = time.time()
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    errores = []
    exitosos = 0
    filtrados = 0
    total = 0

    try:
        # Paso 1: Calcular rango de fechas
        fecha_inicial, fecha_final = _calcular_rango_fechas()
        logger.info(f"[sync_v3] Rango de fechas: {fecha_inicial} a {fecha_final}")

        # Paso 2: Consultar API de Siscore
        logger.info("[sync_v3] Consultando API de Siscore...")
        respuesta_api = await _consultar_api_siscore_v3(
            fecha_inicial=fecha_inicial,
            fecha_final=fecha_final,
            centro_distribucion="TODOS",
            incluir_pedidos_manuales="NO"
        )

        # Paso 3: Validar respuesta
        if not respuesta_api.get('ok'):
            error_msg = respuesta_api.get('error', 'Error desconocido')
            logger.error(f"[sync_v3] Error del API: {error_msg}")
            return {
                'exitosos': 0,
                'errores': [error_msg],
                'total': 0,
                'timestamp': timestamp,
                'segundos': 0,
                'ok': False
            }

        datos = respuesta_api.get('data', [])
        total = len(datos)
        logger.info(f"[sync_v3] API retornó {total} registros")

        # Paso 4: Procesar y mapear registros
        operaciones = []

        for registro in datos:
            try:
                # Mapear campos de Siscore a schema MongoDB
                documento = _mapear_campos_siscore(registro)

                if documento is None:
                    filtrados += 1
                else:
                    # Agregar metadata de sync
                    documento['usuario_carga'] = 'sync_api'
                    documento['fecha_carga'] = timestamp
                    operaciones.append(documento)
                    exitosos += 1

            except Exception as e:
                errores.append(f"Error procesando registro: {str(e)}")
                logger.warning(f"[sync_v3] Error procesando registro: {e}")
                continue

        # Paso 5: Reemplazar colección en MongoDB
        if operaciones:
            try:
                _COLECCION.delete_many({})
                _COLECCION.insert_many(operaciones, ordered=False)
                logger.info(f"[sync_v3] Insertados {exitosos} registros en MongoDB")
            except Exception as e:
                errores.append(f"Error en MongoDB: {e}")
                exitosos = 0

    except Exception as e:
        error_msg = f"Error en sync V3: {str(e)}"
        logger.error(f"[sync_v3] {error_msg}")
        errores.append(error_msg)

    segundos = round(time.time() - inicio, 2)
    logger.info(f"[sync_v3] {exitosos}/{total} registros — {filtrados} filtrados — {segundos}s")

    resultado = {
        'ok': exitosos > 0,
        'exitosos': exitosos,
        'filtrados': filtrados,
        'errores': errores[:20],  # máx 20 errores en respuesta
        'total': total,
        'timestamp': timestamp,
        'segundos': segundos,
    }

    # Tras un sync exitoso, recalcular el cruce pacientes <-> V3 automáticamente
    if exitosos > 0:
        logger.info("[sync_v3] Ejecutando cruce automático post-sync...")
        resultado['cruce'] = ejecutar_cruce_automatico('sync_automatico')

    return resultado


def archivar_mes_v3() -> dict:
    """
    Guarda una copia de seguridad de la colección v3 y el último cruce en v3_historico.
    Se ejecuta automáticamente el último día de cada mes a las 00:00 (hora Bogotá).
    También se puede disparar manualmente desde POST /sync-v3/archivar.
    Usa upsert por (anio, mes) — si se ejecuta varias veces en el mismo mes, sobreescribe.
    """
    from datetime import datetime
    ahora = datetime.now()
    anio  = ahora.year
    mes   = ahora.month
    fecha_corte = ahora.strftime('%Y-%m-%d %H:%M:%S')

    try:
        registros = list(_COLECCION.find({}, {'_id': 0}))
        cruce     = _CACHE_CRUCE.find_one({'tipo': 'cruce_completo'}, {'_id': 0})

        _HISTORICO.update_one(
            {'anio': anio, 'mes': mes},
            {'$set': {
                'anio':        anio,
                'mes':         mes,
                'fecha_corte': fecha_corte,
                'total':       len(registros),
                'registros':   registros,
                'cruce':       cruce,
            }},
            upsert=True
        )
        logger.info(f"[archivo_mensual] OK — {len(registros)} registros archivados ({mes}/{anio})")
        return {'ok': True, 'anio': anio, 'mes': mes, 'total': len(registros), 'fecha_corte': fecha_corte}

    except Exception as e:
        logger.error(f"[archivo_mensual] Error: {e}")
        return {'ok': False, 'error': str(e), 'anio': anio, 'mes': mes}
