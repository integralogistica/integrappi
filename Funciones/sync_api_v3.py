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

logger = logging.getLogger(__name__)

_BD = bd_cliente['integra']
_COLECCION = _BD['v3']

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
]

# Aliases de columnas (clave: nombre alternativo en minúsculas, valor: nombre canónico)
_ALIASES = {
    'fecha solicitada': 'Fecha Preferente',
}


def ejecutar_sync_v3() -> dict:
    """
    Lee el Excel (simulando una API), normaliza los datos y hace upsert en MongoDB.
    Retorna un dict con el resultado: exitosos, errores, total, timestamp.
    """
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
                return str(v).strip() if pd.notna(v) else ''

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
            estado_pedido    = _str('Estado Pedido')
            piezas           = _str('Piezas')
            peso_real        = _str('Peso Real')
            bodega_origen    = _str('Bodega Origen')
            ruta             = _str('Ruta')
            municipio_destino = _str('Municipio Destino')

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
                'usuario_carga':             'sync_api',
                'fecha_carga':               timestamp,
            }

            operaciones.append(documento)
            exitosos += 1

        except Exception as e:
            errores.append(f"Fila {idx + 2}: {e}")

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

    return {
        'ok': True,
        'exitosos': exitosos,
        'errores': errores[:20],   # máx 20 errores en respuesta
        'total': total,
        'timestamp': timestamp,
        'segundos': segundos,
    }
