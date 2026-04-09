"""
Sincronización periódica V3 — simula consumo de API leyendo un Excel local.
Ruta del archivo: integrappi/api_pacientes.xlsx
"""
import os
import time
import logging
import pandas as pd
from datetime import datetime, timedelta

from bd.bd_cliente import bd_cliente
from Funciones.normalizacion_medical_care import (
    fx_normalizar_paciente,
    fx_normalizar_direccion,
    fx_normalizar_celular,
)
from rutas.pedidos_v3 import _parsear_fecha
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

# Ruta al Excel que simula la API
_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXCEL_PATH = os.path.join(_BASE_DIR, 'api_v3.xlsx')

# Columnas requeridas — "Fecha Solicitada" es alias de "Fecha Preferente"
_COLUMNAS_REQUERIDAS = [
    'Codigo Pedido',
    'Codigo Cliente Destino',
    'Cliente Destino',
    'Direccion Destino',
    'Divipola',
    'Telefono',
    'Fecha Pedido',
    'Fecha Preferente',   # también acepta "Fecha Solicitada"
    'Estado Pedido',
    'Piezas',
    'Peso Real',
    'Bodega Origen',
    'Ruta',
    'Municipio Destino',
    'Fecha Entrega',   # opcional — puede estar vacío o ausente
    'Planilla',        # opcional — puede estar vacío o ausente
]

# Aliases de columnas (clave: nombre alternativo en minúsculas, valor: nombre canónico)
_ALIASES = {
    'fecha solicitada': 'Fecha Preferente',
}


def ejecutar_sync_v3() -> dict:
    """
    Lee el Excel (simulando una API), normaliza los datos y hace upsert en MongoDB.
    Retorna un dict con el resultado: exitosos, errores, total, timestamp.
    Al terminar (éxito o error) envía notificación WhatsApp si WHATSAPP_NOTIFY_NUMBER está configurado.
    """
    resultado = _ejecutar_sync_v3_interno()
    _notificar_sync_v3(resultado)
    return resultado


def _ejecutar_sync_v3_interno() -> dict:
    inicio = time.time()
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    errores = []
    exitosos = 0
    total = 0

    if not os.path.exists(EXCEL_PATH):
        msg = f"Archivo no encontrado: {EXCEL_PATH}"
        logger.error(msg)
        return {'exitosos': 0, 'errores': [msg], 'total': 0, 'timestamp': timestamp,
                'segundos': 0, 'ok': False}

    try:
        df = pd.read_excel(EXCEL_PATH, engine='openpyxl')
    except Exception as e:
        msg = f"Error al leer Excel: {e}"
        logger.error(msg)
        return {'exitosos': 0, 'errores': [msg], 'total': 0, 'timestamp': timestamp,
                'segundos': 0, 'ok': False}

    # Normalizar nombres de columnas (strip + lowercase para mapeo)
    mapeo = {}
    for col in df.columns:
        col_lower = col.strip().lower()
        # ¿Es un alias?
        if col_lower in _ALIASES:
            mapeo[col] = _ALIASES[col_lower]
            continue
        # ¿Coincide con alguna columna requerida?
        for req in _COLUMNAS_REQUERIDAS:
            if col_lower == req.lower():
                mapeo[col] = req
                break

    df = df.rename(columns=mapeo)
    # Si quedaron columnas duplicadas tras el rename, conservar solo la primera aparición
    df = df.loc[:, ~df.columns.duplicated()]

    # Validar que exista al menos "Codigo Pedido"
    if 'Codigo Pedido' not in df.columns:
        msg = "El archivo no tiene la columna 'Codigo Pedido'"
        logger.error(msg)
        return {'exitosos': 0, 'errores': [msg], 'total': 0, 'timestamp': timestamp,
                'segundos': 0, 'ok': False}

    total = len(df)
    operaciones = []

    for idx, fila in df.iterrows():
        try:
            def _str(campo):
                v = fila.get(campo)
                if not pd.notna(v):
                    return ''
                if isinstance(v, float) and v.is_integer():
                    return str(int(v))
                return str(v).strip()

            codigo_pedido = _str('Codigo Pedido')
            if not codigo_pedido:
                errores.append(f"Fila {idx + 2}: 'Codigo Pedido' vacío")
                continue

            codigo_cliente   = _str('Codigo Cliente Destino')
            cliente_original = _str('Cliente Destino')
            direccion_original = _str('Direccion Destino')
            divipola         = _str('Divipola')
            telefono_original = _str('Telefono')
            fecha_pedido     = _parsear_fecha(fila.get('Fecha Pedido'))
            fecha_preferente = _parsear_fecha(fila.get('Fecha Preferente'))
            estado_pedido    = _str('Estado Pedido').upper()
            piezas           = _str('Piezas')
            peso_real        = _str('Peso Real')
            bodega_origen    = _str('Bodega Origen')
            ruta             = _str('Ruta')
            municipio_destino = _str('Municipio Destino')
            fecha_entrega     = _parsear_fecha(fila.get('Fecha Entrega'))
            planilla          = _str('Planilla')

            cliente_normalizado   = fx_normalizar_paciente(cliente_original) or cliente_original
            direccion_normalizada = fx_normalizar_direccion(direccion_original) or direccion_original
            telefono_normalizado  = fx_normalizar_celular(telefono_original) or telefono_original
            llave = f"{cliente_normalizado} {direccion_normalizada}".strip()

            documento = {
                'codigo_pedido':             codigo_pedido,
                'codigo_cliente_destino':    codigo_cliente,
                'cliente_destino':           cliente_normalizado,
                'cliente_destino_original':  cliente_original,
                'direccion_destino':         direccion_normalizada,
                'direccion_destino_original': direccion_original,
                'llave':                     llave,
                'divipola':                  divipola,
                'telefono':                  telefono_normalizado,
                'telefono_original':         telefono_original,
                'fecha_pedido':              fecha_pedido,
                'fecha_preferente':          fecha_preferente,
                'estado_pedido':             estado_pedido,
                'piezas':                    piezas,
                'peso_real':                 peso_real,
                'bodega_origen':             bodega_origen,
                'ruta':                      ruta,
                'municipio_destino':         municipio_destino,
                'fecha_entrega':             fecha_entrega,
                'planilla':                  planilla,
                'usuario_carga':             'sync_api',
                'fecha_carga':               timestamp,
            }

            operaciones.append(documento)
            exitosos += 1

        except Exception as e:
            msg = f"Fila {idx + 2}: {e}"
            errores.append(msg)
            logger.warning(f"[sync_v3] {msg}")

    # Reemplazar colección: borrar todo e insertar los nuevos
    if operaciones:
        try:
            _COLECCION.delete_many({})
            _COLECCION.insert_many(operaciones, ordered=False)
        except Exception as e:
            errores.append(f"Error en MongoDB: {e}")
            exitosos = 0

    segundos = round(time.time() - inicio, 2)
    logger.info(f"[sync_v3] {exitosos}/{total} registros — {segundos}s")

    resultado = {
        'ok': True,
        'exitosos': exitosos,
        'errores': errores[:20],   # máx 20 errores en respuesta
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
