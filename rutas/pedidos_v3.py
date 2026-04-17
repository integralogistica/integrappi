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
from datetime import datetime, timedelta, date
from typing import List, Optional
from bd.bd_cliente import bd_cliente
from Funciones.normalizacion_medical_care import (
    fx_normalizar_paciente,
    fx_normalizar_direccion,
    fx_normalizar_celular
)

router = APIRouter(prefix="/pedidos-v3", tags=["Pedidos V3"])


def _parsear_fecha(valor) -> str:
    """
    Convierte un valor de fecha a formato DD/MM/YYYY.
    Soporta: serial de Excel (int/float como 46076), datetime/date, y strings en varios formatos.
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
            return str(valor)

    # datetime o date de Python/pandas
    if isinstance(valor, (datetime, date)):
        return valor.strftime('%d/%m/%Y')

    texto = str(valor).strip()
    if not texto:
        return ''

    # Ya está en DD/MM/YYYY
    if re.match(r'^\d{2}/\d{2}/\d{4}$', texto):
        return texto

    # Formatos que pandas suele generar al hacer str()
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
        try:
            return datetime.strptime(texto, fmt).strftime('%d/%m/%Y')
        except ValueError:
            continue

    return texto  # Devolver tal cual si no se reconoce el formato

# Obtener base de datos y colección
bd = bd_cliente['integra']
coleccion = bd['v3']

# Palabras que identifican clientes institucionales (no pacientes individuales).
# Se verifican como substrings en el cliente_destino ya normalizado (mayúsculas, sin puntuación).
# HOSP cubre tanto "HOSPITAL" como "HOSP..." en general.
CLIENTES_EXCLUIDOS_PALABRAS = [
    'DAVITA', 'VANTIVE', 'CLINICA', 'FARMA', 'HOSP',
    'FUNDACION', 'RENAL', 'MEDICO', 'ESPECIALIDADES','SOCIEDAD', 'INSTITUTO',
]


def _es_cliente_excluido(cliente_normalizado: str) -> bool:
    """Retorna True si el cliente normalizado pertenece a una institución, no a un paciente."""
    return any(palabra in cliente_normalizado for palabra in CLIENTES_EXCLUIDOS_PALABRAS)


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
                    estado_pedido_original = str(fila.get('Estado Pedido', '')).strip() if pd.notna(fila.get('Estado Pedido')) else ''
                    piezas_original = str(fila.get('Piezas', '')).strip() if pd.notna(fila.get('Piezas')) else ''
                    peso_real_original = str(fila.get('Peso Real', '')).strip() if pd.notna(fila.get('Peso Real')) else ''
                    bodega_origen_original = str(fila.get('Bodega Origen', '')).strip() if pd.notna(fila.get('Bodega Origen')) else ''
                    ruta_original = str(fila.get('Ruta', '')).strip() if pd.notna(fila.get('Ruta')) else ''
                    municipio_destino_original = str(fila.get('Municipio Destino', '')).strip() if pd.notna(fila.get('Municipio Destino')) else ''
                    
                    # Normalizar SOLO: cliente_destino, direccion_destino y telefono
                    cliente_destino_normalizado = fx_normalizar_paciente(cliente_destino_original)
                    direccion_destino_normalizada = fx_normalizar_direccion(direccion_destino_original)
                    telefono_normalizado = fx_normalizar_celular(telefono_original)

                    # Excluir clientes institucionales (no son pacientes individuales).
                    # Se chequea el texto ORIGINAL en mayúsculas para no depender de la normalización.
                    if _es_cliente_excluido(cliente_destino_original.upper()):
                        registros_filtrados += 1
                        continue

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


@router.get("/")
async def obtener_pedidos(
    skip: int = 0,
    limit: int = 100,
    estado: Optional[str] = None
):
    """
    Obtiene la lista de pedidos v3 con paginación y filtro opcional por estado
    
    Args:
        skip: Número de registros a saltar (paginación)
        limit: Número máximo de registros a retornar
        estado: Filtro opcional por estado de pedido
    
    Returns:
        JSON con lista de pedidos
    """
    try:
        # Construir filtro
        filtro = {}
        if estado:
            filtro['estado_pedido'] = estado
        
        cursor = coleccion.find(filtro).sort('fecha_carga', -1).skip(skip).limit(limit)
        pedidos = []
        
        for doc in cursor:
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