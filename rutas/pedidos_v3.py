"""
Rutas para la carga y gestión de pedidos v3 (Medical Care)
"""
from fastapi import APIRouter, UploadFile, File, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse
import pandas as pd
import time
import json
import io
import re
import os
from datetime import datetime, timedelta, date
from typing import List, Optional
from bd.bd_cliente import bd_cliente
from Funciones.normalizacion_medical_care import (
    fx_normalizar_paciente,
    fx_normalizar_direccion,
    fx_normalizar_celular,
    fx_normalizar_base
)
from dateutil.relativedelta import relativedelta
import httpx
import logging
import asyncio

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/pedidos-v3", tags=["Pedidos V3"])


def _parsear_fecha(valor) -> str:
    """
    Convierte un valor de fecha a formato DD/MM/YYYY zero-padded.
    Soporta: serial de Excel (int/float como 46076), datetime/date, y strings en varios formatos.
    Retorna '' cuando el formato no se puede reconocer, para que el registro sea filtrado aguas arriba.
    """
    if valor is None:
        return ''
    try:
        if pd.isna(valor):
            return ''
    except Exception:
        pass

    # Serial numérico de Excel (ej: 46076)
    if isinstance(valor, (int, float)):
        try:
            fecha = datetime(1899, 12, 30) + timedelta(days=int(valor))
            return fecha.strftime('%d/%m/%Y')
        except Exception:
            return ''

    # datetime o date de Python/pandas
    if isinstance(valor, (datetime, date)):
        return valor.strftime('%d/%m/%Y')

    texto = str(valor).strip()
    if not texto:
        return ''

    # D/M/YYYY con o sin zero-padding — normalizar a DD/MM/YYYY
    partes = texto.split('/')
    if len(partes) == 3:
        try:
            dia = int(partes[0])
            mes = int(partes[1])
            anio = int(partes[2])
            if 1 <= dia <= 31 and 1 <= mes <= 12 and 1900 <= anio <= 2100:
                return f"{dia:02d}/{mes:02d}/{anio:04d}"
        except ValueError:
            pass

    # Formatos que pandas suele generar al hacer str()
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d-%m-%Y', '%Y/%m/%d'):
        try:
            return datetime.strptime(texto, fmt).strftime('%d/%m/%Y')
        except ValueError:
            continue

    return ''  # Formato no reconocido: el registro será filtrado

# Obtener base de datos y colección
bd = bd_cliente['integra']
coleccion = bd['v3']

# Crear índice en fecha_preferente para optimizar consultas por mes
try:
    coleccion.create_index([('fecha_preferente', 1)], background=True)
except Exception:
    pass  # El índice ya existe o hubo un error, continuar normalmente

# Palabras que identifican clientes institucionales (no pacientes individuales).
# Se verifican como substrings en el cliente_destino ya normalizado (mayúsculas, sin puntuación).
# HOSP cubre tanto "HOSPITAL" como "HOSP..." en general.
CLIENTES_EXCLUIDOS_PALABRAS = [
    'DAVITA', 'VANTIVE', 'CLINICA', 'FARMA', 'HOSP', '3PL', 'RTS','IPS','LABORATORIO','OINSAMED',
    'FUNDACION', 'RENAL', 'MEDICO', 'ESPECIALIDADES','SOCIEDAD', 'INSTITUTO','FRESENIUS'
]


def _es_cliente_excluido(cliente_normalizado: str) -> bool:
    """Retorna True si el cliente normalizado pertenece a una institución, no a un paciente."""
    return any(palabra in cliente_normalizado for palabra in CLIENTES_EXCLUIDOS_PALABRAS)


# ==============================================================================
# Configuración WS Siscore V3
# ==============================================================================
SISCORE_V3_ENDPOINT = "https://integra-wms.appsiscore.com/app/ws/informe_v3.php"
SISCORE_V3_TOKEN = "n0ML0cFGhJwtq4lsAeUcMzrqkn94gX4TDaPuFbbXpoA"


def _calcular_rango_fechas() -> tuple[str, str]:
    """
    Calcula el rango de fechas para consultar la API de Siscore.
    Desde el 1er día del mes que está 2 meses atrás hasta hoy.

    Ejemplos:
        - Hoy: 2026-05-04 → 2026-03-01 a 2026-05-04
        - Hoy: 2026-06-16 → 2026-04-01 a 2026-06-16

    Returns:
        Tupla (fecha_inicial, fecha_final) en formato YYYY-MM-DD
    """
    hoy = datetime.now()
    # Restar 2 meses y fijar al día 1
    fecha_inicial = (hoy - relativedelta(months=2)).replace(day=1)
    fecha_final = hoy
    return fecha_inicial.strftime('%Y-%m-%d'), fecha_final.strftime('%Y-%m-%d')


def _get_proxy_url() -> Optional[str]:
    """
    Obtiene la configuración de proxy desde variables de entorno.
    Usa VULCANO_PROXY_URL como fuente.

    Acepta estos formatos en env:
      - http://ip:3128
      - http://user:pass@ip:3128
      - ip:3128              (se normaliza a http://ip:3128)
      - user:pass@ip:3128    (se normaliza a http://user:pass@ip:3128)
    """
    proxy_url = os.getenv("VULCANO_PROXY_URL", "").strip()
    if not proxy_url:
        return None
    if "://" not in proxy_url:
        proxy_url = "http://" + proxy_url
    return proxy_url


def _convertir_fecha_siscore_a_dd_mm_yyyy(fecha_siscore: str) -> str:
    """
    Convierte fecha del formato Siscore (YYYY-MM-DD) a DD/MM/YYYY.
    Retorna cadena vacía si la fecha es inválida o vacía.

    Args:
        fecha_siscore: Fecha en formato YYYY-MM-DD

    Returns:
        Fecha en formato DD/MM/YYYY o cadena vacía
    """
    if not fecha_siscore:
        return ''
    try:
        partes = fecha_siscore.split('-')
        if len(partes) == 3:
            anio = int(partes[0])
            mes = int(partes[1])
            dia = int(partes[2])
            return f"{dia:02d}/{mes:02d}/{anio}"
    except Exception:
        pass
    return ''


def _mapear_campos_siscore(registro: dict) -> Optional[dict]:
    """
    Mapea los campos de la respuesta de Siscore V3 al schema de MongoDB.
    Retorna None si falta algún campo obligatorio o si el registro debe filtrarse.

    Args:
        registro: Diccionario con los datos de Siscore

    Returns:
        Diccionario mapeado o None si debe filtrarse
    """
    try:
        # Extraer campos de Siscore (nombres exactos de la API)
        codigo_pedido = str(registro.get('Codigo Pedido', '')).strip()
        codigo_cliente = str(registro.get('Codigo Cliente Destino', '')).strip()
        cliente_destino = str(registro.get('Cliente Destino', '')).strip()
        direccion = str(registro.get('Direccion Destino', '')).strip()
        divipola = str(registro.get('Divipola', '')).strip()
        telefono = str(registro.get('Telefono', '')).strip()
        fecha_pedido_str = registro.get('Fecha Pedido', '')
        fecha_solicitada_str = registro.get('Fecha Solicitada', '')
        fecha_entrega_str = registro.get('Fecha Entrega', '')
        estado_pedido = str(registro.get('Estado Pedido', '')).strip()
        piezas = str(registro.get('Piezas', '')).strip()
        peso_real = str(registro.get('Peso Real', '')).strip()
        bodega_origen = str(registro.get('Bodega Origen', '')).strip()
        ruta = str(registro.get('Ruta', '')).strip()
        municipio_destino = str(registro.get('Municipio Destino', '')).strip()

        # Campo obligatorio: codigo_pedido
        if not codigo_pedido:
            return None

        # Convertir fechas de Siscore (YYYY-MM-DD) a DD/MM/YYYY
        fecha_pedido = _convertir_fecha_siscore_a_dd_mm_yyyy(fecha_pedido_str)
        fecha_preferente = _convertir_fecha_siscore_a_dd_mm_yyyy(fecha_solicitada_str)
        fecha_entrega = _convertir_fecha_siscore_a_dd_mm_yyyy(fecha_entrega_str)

        # FILTRO 1: Solo registros del mes actual según fecha preferente
        hoy = datetime.now()
        mes_actual = hoy.month
        anio_actual = hoy.year

        if fecha_preferente:
            try:
                partes = fecha_preferente.split('/')
                if len(partes) == 3:
                    mes_f = int(partes[1])
                    anio_f = int(partes[2])
                    if mes_f != mes_actual or anio_f != anio_actual:
                        return None  # Filtrar: no es del mes actual
            except Exception:
                return None  # Filtrar: fecha inválida
        else:
            return None  # Filtrar: sin fecha preferente

        # FILTRO 2: Excluir clientes institucionales
        cliente_normalizado = fx_normalizar_base(cliente_destino)
        if cliente_normalizado and _es_cliente_excluido(cliente_normalizado):
            return None

        # Normalizar campos
        cliente_final = fx_normalizar_paciente(cliente_destino) or cliente_destino
        direccion_final = fx_normalizar_direccion(direccion) or direccion
        telefono_final = fx_normalizar_celular(telefono) or telefono
        llave = f"{cliente_final} {direccion_final}".strip()

        return {
            'codigo_pedido': codigo_pedido,
            'codigo_cliente_destino': codigo_cliente,
            'cliente_destino': cliente_final,
            'cliente_destino_original': cliente_destino,
            'direccion_destino': direccion_final,
            'direccion_destino_original': direccion,
            'llave': llave,
            'divipola': divipola,
            'telefono': telefono_final,
            'telefono_original': telefono,
            'fecha_pedido': fecha_pedido,
            'fecha_preferente': fecha_preferente,
            'fecha_entrega': fecha_entrega,
            'estado_pedido': estado_pedido,
            'piezas': piezas,
            'peso_real': peso_real,
            'bodega_origen': bodega_origen,
            'ruta': ruta,
            'municipio_destino': municipio_destino,
        }
    except Exception as e:
        logging.getLogger(__name__).warning(f"Error mapeando registro: {e}")
        return None


async def _consultar_api_siscore_v3(
    fecha_inicial: str,
    fecha_final: str,
    centro_distribucion: str = "TODOS",
    incluir_pedidos_manuales: str = "NO"
) -> dict:
    """
    Consulta el API de Siscore V3 de forma asíncrona.

    Args:
        fecha_inicial: Fecha inicial en formato YYYY-MM-DD
        fecha_final: Fecha final en formato YYYY-MM-DD
        centro_distribucion: Centro de distribución ("TODOS" o específico)
        incluir_pedidos_manuales: "SI" o "NO"

    Returns:
        Diccionario con la respuesta del API
    """
    payload = {
        "token": SISCORE_V3_TOKEN,
        "fecha_inicial": fecha_inicial,
        "fecha_final": fecha_final,
        "centro_distribucion": centro_distribucion,
        "incluir_pedidos_manuales": incluir_pedidos_manuales,
        "pedido_especifico": ""
    }

    timeout = httpx.Timeout(600.0, connect=120.0)  # 10 minutos total, 2 minutos para conectar (endpoint Siscore tarda 5-7 min)

    # Obtener configuración de proxy
    proxy_url = _get_proxy_url()

    logger.info(f"[API Siscore V3] Proxy: {'HABILITADO' if proxy_url else 'NO CONFIGURADO'}")
    if proxy_url:
        logger.info(f"[API Siscore V3] Proxy URL: {proxy_url.split('@')[-1]}")  # Solo muestra host:puerto

    try:
        async with httpx.AsyncClient(
            timeout=timeout,
            proxy=proxy_url,  # Usa proxy si está configurado
            trust_env=False,
        ) as client:
            response = await client.post(
                SISCORE_V3_ENDPOINT,
                json=payload,
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            return response.json()
    except httpx.HTTPStatusError as e:
        raise RuntimeError(f"Error HTTP Siscore: {e.response.status_code} - {e.response.text[:500]}")
    except httpx.ConnectTimeout as e:
        proxy_info = f" (vía proxy: {_get_proxy_url()})" if proxy_url else ""
        raise RuntimeError(f"Timeout conectando a Siscore{proxy_info}. El servidor no respondió en el tiempo esperado.")
    except httpx.ReadTimeout as e:
        proxy_info = f" (vía proxy: {_get_proxy_url()})" if proxy_url else ""
        raise RuntimeError(f"Timeout leyendo respuesta de Siscore{proxy_info}. El endpoint tardó más de 10 minutos en responder (normal: 5-7 minutos).")
    except httpx.ProxyError as e:
        raise RuntimeError(f"Error con el proxy ({_get_proxy_url()}): {str(e)}")
    except httpx.ConnectError as e:
        proxy_info = f" (vía proxy: {_get_proxy_url()})" if proxy_url else ""
        raise RuntimeError(f"Error conectando a Siscore{proxy_info}. Verifica que el proxy esté configurado y el endpoint sea accesible: {str(e)}")
    except httpx.RequestError as e:
        proxy_info = f" (vía proxy: {_get_proxy_url()})" if proxy_url else ""
        raise RuntimeError(f"Error de conexión Siscore{proxy_info}: {type(e).__name__}: {str(e)}")
    except Exception as e:
        raise RuntimeError(f"Error inesperado consultando Siscore: {type(e).__name__}: {str(e)}")


# Columnas requeridas en el Excel
COLUMNAS_REQUERIDAS = [
    'Codigo Pedido',
    'Codigo Cliente Destino',
    'Cliente Destino',
    'Direccion Destino',
    'Divipola',
    'Telefono',
    'Fecha Pedido',
    'Fecha Preferente',
    'Fecha Entrega',
    'Estado Pedido',
    'Piezas',
    'Peso Real',
    'Bodega Origen',
    'Ruta',
    'Municipio Destino'
]


@router.post("/cargar-masivo-stream")
async def cargar_pedidos_masivo_stream(
    archivo: UploadFile = File(...),
    usuario: str = Query(...)
):
    """
    Carga masiva de pedidos v3 desde un archivo Excel con progreso en tiempo real via SSE
    
    Args:
        archivo: Archivo Excel (.xlsx, .xls, .xlsm)
        usuario: Usuario que realiza la carga (parámetro de query string)
    
    Returns:
        StreamingResponse con eventos de progreso
    """
    import logging
    import asyncio
    
    logger = logging.getLogger(__name__)
    logger.info(f"=== INICIO CARGA MASIVA PEDIDOS V3 ===")
    logger.info(f"Usuario: '{usuario}'")
    logger.info(f"Archivo: {archivo.filename if archivo else 'None'}")
    
    # Validar tipo de archivo
    if not archivo.filename.endswith(('.xlsx', '.xls', '.xlsm')):
        async def error_response():
            yield f"data: {json.dumps({'error': 'El archivo debe ser un Excel (.xlsx, .xls, .xlsm)'}, ensure_ascii=False)}\n\n"
        return StreamingResponse(error_response(), media_type="text/event-stream")
    
    logger.info("Leyendo contenido del archivo...")
    
    try:
        contenido = await archivo.read()
        logger.info(f"Archivo leído: {len(contenido)} bytes")
    except Exception as read_error:
        logger.error(f"Error al leer archivo: {str(read_error)}")
        async def error_response():
            yield f"data: {json.dumps({'error': f'Error al leer archivo: {str(read_error)}'}, ensure_ascii=False)}\n\n"
        return StreamingResponse(error_response(), media_type="text/event-stream")
    
    if not contenido or len(contenido) == 0:
        logger.error("El archivo está vacío")
        async def error_response():
            yield f"data: {json.dumps({'error': 'El archivo está vacío o no se pudo leer'}, ensure_ascii=False)}\n\n"
        return StreamingResponse(error_response(), media_type="text/event-stream")
    
    # Generador de eventos SSE para el progreso
    async def generate_progress():
        tiempo_inicio = time.time()
        errores = []
        registros_exitosos = 0
        registros_filtrados = 0
        total_filas = 0
        
        try:
            # Leer archivo Excel desde el contenido en memoria
            buffer = io.BytesIO(contenido)
            
            logger.info("Intentando leer Excel...")
            
            # Determinar el tipo de archivo para pandas
            try:
                if archivo.filename.endswith('.xlsx'):
                    df = pd.read_excel(buffer, engine='openpyxl')
                elif archivo.filename.endswith('.xlsm'):
                    df = pd.read_excel(buffer, engine='openpyxl')
                else:
                    df = pd.read_excel(buffer, engine='xlrd')
                
                logger.info(f"Excel leído exitosamente: {len(df)} filas")
            except Exception as excel_error:
                logger.error(f"Error al leer Excel: {str(excel_error)}")
                yield f"data: {json.dumps({'error': f'Error al leer archivo Excel: {str(excel_error)}'}, ensure_ascii=False)}\n\n"
                return

            # Eliminar TODOS los documentos existentes antes de cargar la nueva base
            logger.info("Eliminando base anterior...")
            resultado_eliminar = coleccion.delete_many({})
            logger.info(f"Eliminados {resultado_eliminar.deleted_count} documentos de la carga anterior")

            total_filas = len(df)
            yield f"data: {json.dumps({'stage': 'reading', 'progress': 0, 'message': 'Leyendo archivo Excel...'}, ensure_ascii=False)}\n\n"
            
            # Validar columnas requeridas (case-insensitive y con trim de espacios)
            columnas_archivo = [col.strip().lower() for col in df.columns]
            columnas_requeridas_lower = [col.lower() for col in COLUMNAS_REQUERIDAS]
            
            columnas_faltantes = set(columnas_requeridas_lower) - set(columnas_archivo)
            if columnas_faltantes:
                mensaje_error = f"Faltan las siguientes columnas: {', '.join(columnas_faltantes)}"
                yield f"data: {json.dumps({'error': mensaje_error}, ensure_ascii=False)}\n\n"
                return
            
            # Crear diccionario de mapeo de columnas (case-insensitive)
            mapeo_columnas = {}
            for col_req in COLUMNAS_REQUERIDAS:
                for col_archivo in df.columns:
                    if col_req.lower() == col_archivo.strip().lower():
                        mapeo_columnas[col_archivo] = col_req
                        break
            
            # Renombrar columnas a nombres estándar (sin espacios extra)
            df = df.rename(columns=mapeo_columnas)
            
            yield f"data: {json.dumps({'stage': 'processing', 'progress': 0, 'total': total_filas, 'message': f'Procesando {total_filas} registros...'}, ensure_ascii=False)}\n\n"
            
            # Procesar cada fila
            documentos_a_insertar = []
            
            for idx, fila in df.iterrows():
                try:
                    # Extraer valores originales con manejo de nulos
                    codigo_pedido_original = str(fila.get('Codigo Pedido', '')).strip() if pd.notna(fila.get('Codigo Pedido')) else ''
                    codigo_cliente_original = str(fila.get('Codigo Cliente Destino', '')).strip() if pd.notna(fila.get('Codigo Cliente Destino')) else ''
                    cliente_destino_original = str(fila.get('Cliente Destino', '')).strip() if pd.notna(fila.get('Cliente Destino')) else ''
                    direccion_destino_original = str(fila.get('Direccion Destino', '')).strip() if pd.notna(fila.get('Direccion Destino')) else ''
                    divipola_original = str(fila.get('Divipola', '')).strip() if pd.notna(fila.get('Divipola')) else ''
                    telefono_original = str(fila.get('Telefono', '')).strip() if pd.notna(fila.get('Telefono')) else ''
                    fecha_pedido_original = _parsear_fecha(fila.get('Fecha Pedido'))
                    fecha_preferente_original = _parsear_fecha(fila.get('Fecha Preferente'))
                    fecha_entrega_original = _parsear_fecha(fila.get('Fecha Entrega'))
                    estado_pedido_original = str(fila.get('Estado Pedido', '')).strip() if pd.notna(fila.get('Estado Pedido')) else ''
                    piezas_original = str(fila.get('Piezas', '')).strip() if pd.notna(fila.get('Piezas')) else ''
                    peso_real_original = str(fila.get('Peso Real', '')).strip() if pd.notna(fila.get('Peso Real')) else ''
                    bodega_origen_original = str(fila.get('Bodega Origen', '')).strip() if pd.notna(fila.get('Bodega Origen')) else ''
                    ruta_original = str(fila.get('Ruta', '')).strip() if pd.notna(fila.get('Ruta')) else ''
                    municipio_destino_original = str(fila.get('Municipio Destino', '')).strip() if pd.notna(fila.get('Municipio Destino')) else ''

                    # FILTRO 1 (MÁS EFICIENTE): Solo cargar registros del mes actual según fecha preferente
                    # Este filtro elimina ~50%+ de los registros, así que va primero para no procesarlos innecesariamente
                    hoy = datetime.now()
                    mes_actual = hoy.month
                    anio_actual = hoy.year

                    if fecha_preferente_original:
                        try:
                            # Parsear fecha preferente (formato DD/MM/YYYY)
                            partes = fecha_preferente_original.split('/')
                            if len(partes) == 3:
                                dia_f = int(partes[0])
                                mes_f = int(partes[1])
                                anio_f = int(partes[2])

                                # Solo insertar si el mes y año coinciden con el actual
                                if mes_f != mes_actual or anio_f != anio_actual:
                                    registros_filtrados += 1
                                    continue
                        except Exception:
                            # Si no se puede parsear la fecha, excluir el registro
                            registros_filtrados += 1
                            continue
                    else:
                        # Si no tiene fecha preferente, excluir el registro
                        registros_filtrados += 1
                        continue

                    # FILTRO 2: Excluir clientes institucionales (no son pacientes individuales).
                    # Solo se procesa si pasó el filtro de fecha (ahora procesa ~50% menos registros)
                    # Se usa fx_normalizar_base (solo mayús, sin tildes/símbolos) para detectar palabras clave.
                    cliente_para_validacion = fx_normalizar_base(cliente_destino_original)
                    if cliente_para_validacion and _es_cliente_excluido(cliente_para_validacion):
                        registros_filtrados += 1
                        continue

                    # Normalizar SOLO: cliente_destino, direccion_destino y telefono
                    cliente_destino_normalizado = fx_normalizar_paciente(cliente_destino_original)
                    direccion_destino_normalizada = fx_normalizar_direccion(direccion_destino_original)
                    telefono_normalizado = fx_normalizar_celular(telefono_original)

                    # Validar campos obligatorios
                    if not codigo_pedido_original:
                        errores.append(f"Fila {idx + 2}: El campo 'Codigo Pedido' es obligatorio")
                        continue

                    cliente_final = cliente_destino_normalizado if cliente_destino_normalizado else cliente_destino_original
                    direccion_final = direccion_destino_normalizada if direccion_destino_normalizada else direccion_destino_original
                    llave = f"{cliente_final} {direccion_final}".strip()

                    documento = {
                        'codigo_pedido': codigo_pedido_original,
                        'codigo_cliente_destino': codigo_cliente_original,
                        'cliente_destino': cliente_final,
                        'cliente_destino_original': cliente_destino_original,
                        'direccion_destino': direccion_final,
                        'direccion_destino_original': direccion_destino_original,
                        'llave': llave,
                        'divipola': divipola_original,
                        'telefono': telefono_normalizado if telefono_normalizado else telefono_original,
                        'telefono_original': telefono_original,
                        'fecha_pedido': fecha_pedido_original,
                        'fecha_preferente': fecha_preferente_original,
                        'fecha_entrega': fecha_entrega_original,
                        'estado_pedido': estado_pedido_original,
                        'piezas': piezas_original,
                        'peso_real': peso_real_original,
                        'bodega_origen': bodega_origen_original,
                        'ruta': ruta_original,
                        'municipio_destino': municipio_destino_original,
                        'usuario_carga': usuario,
                        'fecha_carga': time.strftime('%Y-%m-%d %H:%M:%S')
                    }
                    
                    documentos_a_insertar.append(documento)
                    registros_exitosos += 1
                    
                    # Enviar progreso cada 10 filas o al final
                    if registros_exitosos % 10 == 0 or idx == total_filas - 1:
                        progreso = round((idx + 1) / total_filas * 100, 1)
                        yield f"data: {json.dumps({'stage': 'processing', 'progress': progreso, 'processed': registros_exitosos, 'total': total_filas, 'message': f'Procesando... {progreso}%'}, ensure_ascii=False)}\n\n"
                        await asyncio.sleep(0)  # Permitir que el event loop procese otros eventos
                
                except Exception as e:
                    errores.append(f"Fila {idx + 2}: Error al procesar - {str(e)}")
                    continue
            
            # Insertar documentos en MongoDB
            yield f"data: {json.dumps({'stage': 'saving', 'progress': 100, 'message': 'Guardando en base de datos...'}, ensure_ascii=False)}\n\n"
            
            if documentos_a_insertar:
                coleccion.insert_many(documentos_a_insertar)
            
            tiempo_fin = time.time()
            tiempo_segundos = round(tiempo_fin - tiempo_inicio, 2)
            
            # Enviar resultado final
            resultado = {
                'stage': 'complete',
                'progress': 100,
                'mensaje': f'Carga completada en {tiempo_segundos} segundos',
                'tiempo_segundos': tiempo_segundos,
                'registros_exitosos': registros_exitosos,
                'registros_filtrados': registros_filtrados,
                'registros_con_errores': len(errores),
                'errores': errores[:50] if errores else []
            }
            yield f"data: {json.dumps(resultado, ensure_ascii=False)}\n\n"
            
        except Exception as e:
            tiempo_fin = time.time()
            tiempo_segundos = round(tiempo_fin - tiempo_inicio, 2)
            error_msg = {'error': f'Error al procesar el archivo: {str(e)}', 'tiempo_segundos': tiempo_segundos}
            yield f"data: {json.dumps(error_msg, ensure_ascii=False)}\n\n"
    
    return StreamingResponse(
        generate_progress(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


@router.post("/cargar-desde-api-stream")
async def cargar_pedidos_desde_api_stream(
    usuario: str = Query(...)
):
    """
    Carga pedidos v3 directamente desde el API de Siscore con progreso en tiempo real via SSE.
    Reemplaza la carga manual de Excel.

    El rango de fechas se calcula automáticamente:
    - Desde: 1er día del mes que está 2 meses atrás
    - Hasta: Hoy

    Ejemplos:
        - Hoy: 2026-05-04 → Consulta desde 2026-03-01 hasta 2026-05-04
        - Hoy: 2026-06-16 → Consulta desde 2026-04-01 hasta 2026-06-16

    Args:
        usuario: Usuario que realiza la carga (parámetro de query string)

    Returns:
        StreamingResponse con eventos de progreso
    """
    logger = logging.getLogger(__name__)
    logger.info(f"=== INICIO CARGA DESDE API SISCORE V3 ===")
    logger.info(f"Usuario: '{usuario}'")

    # Generador de eventos SSE para el progreso
    async def generate_progress():
        tiempo_inicio = time.time()
        errores = []
        registros_procesados = 0
        registros_insertados = 0
        registros_filtrados = 0

        try:
            # Paso 1: Calcular rango de fechas
            yield f"data: {json.dumps({'stage': 'calculating', 'progress': 0, 'message': 'Calculando rango de fechas...'}, ensure_ascii=False)}\n\n"

            fecha_inicial, fecha_final = _calcular_rango_fechas()
            logger.info(f"Rango de fechas: {fecha_inicial} a {fecha_final}")

            yield f"data: {json.dumps({'stage': 'fetching', 'progress': 10, 'message': f'Consultando API de Siscore ({fecha_inicial} a {fecha_final})...'}, ensure_ascii=False)}\n\n"

            # Paso 2: Consultar API de Siscore
            try:
                respuesta_api = await _consultar_api_siscore_v3(
                    fecha_inicial=fecha_inicial,
                    fecha_final=fecha_final,
                    centro_distribucion="TODOS",
                    incluir_pedidos_manuales="NO"
                )
            except RuntimeError as api_error:
                logger.error(f"Error consultando API: {api_error}")
                yield f"data: {json.dumps({'error': f'Error al consultar API de Siscore: {str(api_error)}'}, ensure_ascii=False)}\n\n"
                return

            # Paso 3: Validar respuesta
            if not respuesta_api.get('ok'):
                error_msg = respuesta_api.get('error', 'Error desconocido')
                logger.error(f"API retornó ok=False: {error_msg}")
                yield f"data: {json.dumps({'error': f'Error del API de Siscore: {error_msg}'}, ensure_ascii=False)}\n\n"
                return

            datos = respuesta_api.get('data', [])
            total_registros = len(datos)
            logger.info(f"API retornó {total_registros} registros")

            if total_registros == 0:
                yield f"data: {json.dumps({'stage': 'complete', 'progress': 100, 'message': 'No se encontraron pedidos en el rango de fechas especificado.', 'registros_insertados': 0, 'registros_filtrados': 0}, ensure_ascii=False)}\n\n"
                return

            yield f"data: {json.dumps({'stage': 'processing', 'progress': 20, 'total': total_registros, 'message': f'Procesando {total_registros} registros...'}, ensure_ascii=False)}\n\n"

            # Paso 4: Eliminar base anterior
            logger.info("Eliminando base anterior...")
            resultado_eliminar = coleccion.delete_many({})
            logger.info(f"Eliminados {resultado_eliminar.deleted_count} documentos de la carga anterior")

            # Paso 5: Procesar y mapear registros
            documentos_a_insertar = []

            for idx, registro in enumerate(datos):
                try:
                    registros_procesados += 1

                    # Mapear campos de Siscore a schema MongoDB
                    documento = _mapear_campos_siscore(registro)

                    if documento is None:
                        registros_filtrados += 1
                    else:
                        # Agregar metadata de carga
                        documento['usuario_carga'] = usuario
                        documento['fecha_carga'] = time.strftime('%Y-%m-%d %H:%M:%S')
                        documentos_a_insertar.append(documento)
                        registros_insertados += 1

                    # Enviar progreso cada 50 registros o al final
                    if registros_procesados % 50 == 0 or idx == total_registros - 1:
                        progreso = round(20 + (registros_procesados / total_registros * 70), 1)
                        yield f"data: {json.dumps({'stage': 'processing', 'progress': progreso, 'processed': registros_procesados, 'inserted': registros_insertados, 'filtered': registros_filtrados, 'total': total_registros, 'message': f'Procesando... {progreso}%'}, ensure_ascii=False)}\n\n"
                        await asyncio.sleep(0)  # Permitir que el event loop procese otros eventos

                except Exception as e:
                    errores.append(f"Registro {idx + 1}: Error al procesar - {str(e)}")
                    logger.warning(f"Error procesando registro {idx + 1}: {e}")
                    continue

            # Paso 6: Insertar en MongoDB
            yield f"data: {json.dumps({'stage': 'saving', 'progress': 95, 'message': f'Guardando {registros_insertados} registros en base de datos...'}, ensure_ascii=False)}\n\n"

            if documentos_a_insertar:
                coleccion.insert_many(documentos_a_insertar)
                logger.info(f"Insertados {registros_insertados} registros exitosamente")

            tiempo_fin = time.time()
            tiempo_segundos = round(tiempo_fin - tiempo_inicio, 2)

            # Enviar resultado final
            resultado = {
                'stage': 'complete',
                'progress': 100,
                'mensaje': f'Carga completada en {tiempo_segundos} segundos',
                'tiempo_segundos': tiempo_segundos,
                'rango_fechas': {'inicio': fecha_inicial, 'fin': fecha_final},
                'registros_procesados': registros_procesados,
                'registros_insertados': registros_insertados,
                'registros_filtrados': registros_filtrados,
                'registros_con_errores': len(errores),
                'errores': errores[:50] if errores else []
            }
            yield f"data: {json.dumps(resultado, ensure_ascii=False)}\n\n"

        except Exception as e:
            tiempo_fin = time.time()
            tiempo_segundos = round(tiempo_fin - tiempo_inicio, 2)
            logger.error(f"Error en carga desde API: {e}")
            error_msg = {'error': f'Error al procesar la solicitud: {str(e)}', 'tiempo_segundos': tiempo_segundos}
            yield f"data: {json.dumps(error_msg, ensure_ascii=False)}\n\n"

    return StreamingResponse(
        generate_progress(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


@router.get("/")
async def obtener_pedidos(
    skip: int = 0,
    limit: int = 100,
    estado: Optional[str] = None,
    mes_actual: bool = True
):
    """
    Obtiene la lista de pedidos v3 con paginación y filtro opcional por estado

    Args:
        skip: Número de registros a saltar (paginación)
        limit: Número máximo de registros a retornar
        estado: Filtro opcional por estado de pedido
        mes_actual: Si es True, filtra por fecha preferente del mes actual (default: True)

    Returns:
        JSON con lista de pedidos
    """
    try:
        # Obtener mes y año actual
        hoy = datetime.now()
        mes_actual_num = hoy.month
        anio_actual_num = hoy.year
        mes_actual_str = f"{mes_actual_num:02d}/{anio_actual_num}"  # Formato: MM/YYYY

        # Construir filtro
        filtro = {}
        if estado:
            filtro['estado_pedido'] = estado

        # Filtrar por mes actual de fecha preferente (formato DD/MM/YYYY)
        if mes_actual:
            # Usamos regex para buscar fechas que coincidan con el mes/año actual
            # Ejemplo para abril 2026: busca DD/04/2026 (cualquier día del mes 04 del año 2026)
            filtro['fecha_preferente'] = {'$regex': f'^\\d{{2}}/{mes_actual_str}$'}

        cursor = coleccion.find(filtro).sort('fecha_carga', -1).skip(skip).limit(limit)
        pedidos = []

        for doc in cursor:
            # Verificación adicional: asegurar que la fecha preferente coincida con el mes actual
            if mes_actual and doc.get('fecha_preferente'):
                fecha_pref = doc['fecha_preferente']
                # Verificar que la fecha termina con el mes/año actual
                if not fecha_pref.endswith(f'/{mes_actual_str}'):
                    continue

            # Convertir ObjectId a string y eliminar _id
            doc['_id'] = str(doc['_id'])
            pedidos.append(doc)
        
        total = coleccion.count_documents(filtro)
        
        return {
            'pedidos': pedidos,
            'total': total,
            'skip': skip,
            'limit': limit
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f'Error al obtener pedidos: {str(e)}'
        )


@router.get("/estados")
async def obtener_estados():
    """
    Obtiene la lista de estados únicos de pedidos
    
    Returns:
        JSON con lista de estados únicos
    """
    try:
        # Usar aggregate para obtener estados únicos
        pipeline = [
            {
                '$group': {
                    '_id': '$estado_pedido',
                    'count': {'$sum': 1}
                }
            },
            {
                '$sort': {'_id': 1}
            }
        ]
        
        resultados = list(coleccion.aggregate(pipeline))
        
        # Formatear respuesta
        estados = [
            {
                'estado': r['_id'] if r['_id'] else 'Sin Estado',
                'count': r['count']
            }
            for r in resultados if r['_id'] is not None
        ]
        
        return {
            'estados': estados,
            'total': len(estados)
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f'Error al obtener estados: {str(e)}'
        )


@router.delete("/eliminar-todos")
async def eliminar_todos_pedidos(usuario: str):
    """
    Elimina todos los pedidos v3 (solo ADMIN)
    
    Args:
        usuario: Usuario que realiza la eliminación
    
    Returns:
        JSON con confirmación de eliminación
    """
    try:
        resultado = coleccion.delete_many({})
        return {
            'mensaje': f'Se eliminaron {resultado.deleted_count} registros',
            'usuario': usuario,
            'registros_eliminados': resultado.deleted_count
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f'Error al eliminar pedidos: {str(e)}'
        )