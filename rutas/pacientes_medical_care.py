from fastapi import APIRouter, UploadFile, File, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse
import pandas as pd
import time
import json
from typing import List
from bd.bd_cliente import bd_cliente
from Funciones.normalizacion_medical_care import (
    fx_normalizar_paciente,
    fx_normalizar_direccion,
    fx_normalizar_celular,
    fx_normalizar_municipio,
    fx_normalizar_cedula
)

router = APIRouter(prefix="/pacientes-medical-care", tags=["Medical Care"])

# Obtener base de datos y colección
bd = bd_cliente['integra']
coleccion = bd['pacientes_medical_care']

# Columnas requeridas
COLUMNAS_REQUERIDAS = [
    'sede', 'paciente', 'cedula', 'direccion', 
    'departamento', 'municipio', 'ruta', 'cedi', 'celular'
]


@router.post("/cargar-masivo-stream")
async def cargar_pacientes_masivo_stream(
    archivo: UploadFile = File(...),
    usuario: str = Query(...)
):
    """
    Carga masiva de pacientes de Medical Care con progreso en tiempo real via SSE
    
    Args:
        archivo: Archivo Excel (.xlsx, .xls, .xlsm)
        usuario: Usuario que realiza la carga (parámetro de query string)
    
    Returns:
        StreamingResponse con eventos de progreso
    """
    import logging
    import asyncio
    import io
    
    # LEER EL CONTENIDO DEL ARCHIVO ANTES DE INICIAR EL STREAMING
    # Esto evita que el archivo se cierre antes de poder leerlo
    logger = logging.getLogger(__name__)
    logger.info(f"=== INICIO CARGA MASIVA STREAM ===")
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
    
    # Ahora procesamos el contenido del archivo dentro del generador
    async def generate_progress():
        """Generador de eventos SSE para el progreso"""
        tiempo_inicio = time.time()
        errores = []
        registros_exitosos = 0
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
            
            # Validar columnas requeridas
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
            
            # Renombrar columnas a mayúsculas
            df = df.rename(columns=mapeo_columnas)
            
            yield f"data: {json.dumps({'stage': 'processing', 'progress': 0, 'total': total_filas, 'message': f'Procesando {total_filas} registros...'}, ensure_ascii=False)}\n\n"
            
            # Procesar cada fila con validación de duplicados
            documentos_a_insertar = []
            cedulas_ya_procesadas = set()
            
            for idx, fila in df.iterrows():
                try:
                    # Extraer valores originales con manejo de nulos
                    sede_original = str(fila.get('sede', '')).strip() if pd.notna(fila.get('sede')) else ''
                    paciente_original = str(fila.get('paciente', '')).strip() if pd.notna(fila.get('paciente')) else ''
                    cedula_original = str(fila.get('cedula', '')).strip() if pd.notna(fila.get('cedula')) else ''
                    direccion_original = str(fila.get('direccion', '')).strip() if pd.notna(fila.get('direccion')) else ''
                    departamento_original = str(fila.get('departamento', '')).strip() if pd.notna(fila.get('departamento')) else ''
                    municipio_original = str(fila.get('municipio', '')).strip() if pd.notna(fila.get('municipio')) else ''
                    ruta_original = str(fila.get('ruta', '')).strip() if pd.notna(fila.get('ruta')) else ''
                    cedi_original = str(fila.get('cedi', '')).strip() if pd.notna(fila.get('cedi')) else ''
                    celular_original = str(fila.get('celular', '')).strip() if pd.notna(fila.get('celular')) else ''
                    
                    # Validar campos obligatorios
                    if not paciente_original:
                        errores.append(f"Fila {idx + 2}: El campo 'paciente' es obligatorio")
                        continue
                    
                    if not cedula_original:
                        errores.append(f"Fila {idx + 2}: El campo 'cedula' es obligatorio")
                        continue
                    
                    # Normalizar SOLO: paciente, dirección, cédula y celular
                    paciente_normalizado = fx_normalizar_paciente(paciente_original)
                    cedula_normalizada = fx_normalizar_cedula(cedula_original)
                    direccion_normalizada = fx_normalizar_direccion(direccion_original)
                    celular_normalizado = fx_normalizar_celular(celular_original)
                    
                    # Validar resultados de normalización
                    if not paciente_normalizado:
                        errores.append(f"Fila {idx + 2}: Error al normalizar 'paciente'")
                        continue
                    
                    if not cedula_normalizada:
                        errores.append(f"Fila {idx + 2}: Error al normalizar 'cedula'")
                        continue
                    
                    # Validar duplicados por cédula en el archivo
                    if cedula_normalizada in cedulas_ya_procesadas:
                        errores.append(f"Fila {idx + 2}: La cédula {cedula_original} ya existe en el archivo cargado")
                        continue
                    
                    # Validar duplicados en la base de datos
                    existe_en_bd = coleccion.find_one({'cedula': cedula_normalizada})
                    if existe_en_bd:
                        errores.append(f"Fila {idx + 2}: La cédula {cedula_original} ya existe en la base de datos")
                        continue
                    
                    direccion_final = direccion_normalizada if direccion_normalizada else direccion_original
                    llave = f"{paciente_normalizado} {direccion_final}".strip()

                    # Crear documento con campos originales y normalizados
                    documento = {
                        'sede': sede_original,
                        'paciente': paciente_normalizado,
                        'paciente_original': paciente_original,
                        'cedula': cedula_normalizada,
                        'cedula_original': cedula_original,
                        'direccion': direccion_final,
                        'direccion_original': direccion_original,
                        'departamento': departamento_original,
                        'municipio': municipio_original,
                        'ruta': ruta_original,
                        'cedi': cedi_original,
                        'celular': celular_normalizado if celular_normalizado else celular_original,
                        'celular_original': celular_original,
                        'llave': llave,
                        'estado': 'ACTIVO',
                        'usuario_carga': usuario,
                        'fecha_carga': time.strftime('%Y-%m-%d %H:%M:%S')
                    }

                    documentos_a_insertar.append(documento)
                    cedulas_ya_procesadas.add(cedula_normalizada)
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


@router.post("/cargar-masivo")
async def cargar_pacientes_masivo(usuario: str, archivo: UploadFile = File(...)):
    """
    Carga masiva de pacientes de Medical Care desde un archivo Excel (versión sin streaming para compatibilidad)
    
    Args:
        usuario: Usuario que realiza la carga
        archivo: Archivo Excel (.xlsx, .xls, .xlsm)
    
    Returns:
        JSON con mensaje de éxito, tiempo de procesamiento y errores (si los hay)
    """
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    tiempo_inicio = time.time()
    errores = []
    registros_exitosos = 0
    
    logger.info(f"=== INICIO CARGA MASIVA ===")
    logger.info(f"Usuario recibido: '{usuario}'")
    logger.info(f"Archivo recibido: {archivo.filename if archivo else 'None'}")
    logger.info(f"Tipo contenido: {archivo.content_type if archivo else 'None'}")
    
    # Validar tipo de archivo
    if not archivo.filename.endswith(('.xlsx', '.xls', '.xlsm')):
        raise HTTPException(
            status_code=400,
            detail="El archivo debe ser un Excel (.xlsx, .xls, .xlsm)"
        )
    
    try:
        # Leer archivo Excel
        contenido = await archivo.read()
        
        # Determinar el tipo de archivo para pandas
        if archivo.filename.endswith('.xlsx'):
            df = pd.read_excel(contenido, engine='openpyxl')
        elif archivo.filename.endswith('.xlsm'):
            df = pd.read_excel(contenido, engine='openpyxl')
        else:
            df = pd.read_excel(contenido, engine='xlrd')
        
        # Validar columnas requeridas
        columnas_archivo = [col.strip().lower() for col in df.columns]
        columnas_requeridas_lower = [col.lower() for col in COLUMNAS_REQUERIDAS]
        
        columnas_faltantes = set(columnas_requeridas_lower) - set(columnas_archivo)
        if columnas_faltantes:
            raise HTTPException(
                status_code=400,
                detail=f"Faltan las siguientes columnas: {', '.join(columnas_faltantes)}"
            )
        
        # Crear diccionario de mapeo de columnas (case-insensitive)
        mapeo_columnas = {}
        for col_req in COLUMNAS_REQUERIDAS:
            for col_archivo in df.columns:
                if col_req.lower() == col_archivo.strip().lower():
                    mapeo_columnas[col_archivo] = col_req
                    break
        
        # Renombrar columnas a mayúsculas
        df = df.rename(columns=mapeo_columnas)
        
        # Procesar cada fila con validación de duplicados
        documentos_a_insertar = []
        cedulas_ya_procesadas = set()
        
        for idx, fila in df.iterrows():
            try:
                # Extraer valores originales con manejo de nulos
                sede_original = str(fila.get('sede', '')).strip() if pd.notna(fila.get('sede')) else ''
                paciente_original = str(fila.get('paciente', '')).strip() if pd.notna(fila.get('paciente')) else ''
                cedula_original = str(fila.get('cedula', '')).strip() if pd.notna(fila.get('cedula')) else ''
                direccion_original = str(fila.get('direccion', '')).strip() if pd.notna(fila.get('direccion')) else ''
                departamento_original = str(fila.get('departamento', '')).strip() if pd.notna(fila.get('departamento')) else ''
                municipio_original = str(fila.get('municipio', '')).strip() if pd.notna(fila.get('municipio')) else ''
                ruta_original = str(fila.get('ruta', '')).strip() if pd.notna(fila.get('ruta')) else ''
                cedi_original = str(fila.get('cedi', '')).strip() if pd.notna(fila.get('cedi')) else ''
                celular_original = str(fila.get('celular', '')).strip() if pd.notna(fila.get('celular')) else ''
                
                # Validar campos obligatorios
                if not paciente_original:
                    errores.append(f"Fila {idx + 2}: El campo 'paciente' es obligatorio")
                    continue
                
                if not cedula_original:
                    errores.append(f"Fila {idx + 2}: El campo 'cedula' es obligatorio")
                    continue
                
                # Normalizar SOLO: paciente, dirección, cédula y celular
                paciente_normalizado = fx_normalizar_paciente(paciente_original)
                cedula_normalizada = fx_normalizar_cedula(cedula_original)
                direccion_normalizada = fx_normalizar_direccion(direccion_original)
                celular_normalizado = fx_normalizar_celular(celular_original)
                
                # Validar resultados de normalización
                if not paciente_normalizado:
                    errores.append(f"Fila {idx + 2}: Error al normalizar 'paciente'")
                    continue
                
                if not cedula_normalizada:
                    errores.append(f"Fila {idx + 2}: Error al normalizar 'cedula'")
                    continue
                
                # Validar duplicados por cédula en el archivo
                if cedula_normalizada in cedulas_ya_procesadas:
                    errores.append(f"Fila {idx + 2}: La cédula {cedula_original} ya existe en el archivo cargado")
                    continue
                
                # Validar duplicados en la base de datos
                existe_en_bd = coleccion.find_one({'cedula': cedula_normalizada})
                if existe_en_bd:
                    errores.append(f"Fila {idx + 2}: La cédula {cedula_original} ya existe en la base de datos")
                    continue
                
                direccion_final = direccion_normalizada if direccion_normalizada else direccion_original
                llave = f"{paciente_normalizado} {direccion_final}".strip()

                # Crear documento con campos originales y normalizados
                documento = {
                    'sede': sede_original,
                    'paciente': paciente_normalizado,
                    'paciente_original': paciente_original,
                    'cedula': cedula_normalizada,
                    'cedula_original': cedula_original,
                    'direccion': direccion_final,
                    'direccion_original': direccion_original,
                    'departamento': departamento_original,
                    'municipio': municipio_original,
                    'ruta': ruta_original,
                    'cedi': cedi_original,
                    'celular': celular_normalizado if celular_normalizado else celular_original,
                    'celular_original': celular_original,
                    'llave': llave,
                    'estado': 'ACTIVO',
                    'usuario_carga': usuario,
                    'fecha_carga': time.strftime('%Y-%m-%d %H:%M:%S')
                }

                documentos_a_insertar.append(documento)
                cedulas_ya_procesadas.add(cedula_normalizada)
                registros_exitosos += 1

            except Exception as e:
                errores.append(f"Fila {idx + 2}: Error al procesar - {str(e)}")
                continue
        
        # Insertar documentos en MongoDB
        if documentos_a_insertar:
            coleccion.insert_many(documentos_a_insertar)
        
        tiempo_fin = time.time()
        tiempo_segundos = round(tiempo_fin - tiempo_inicio, 2)
        
        # Construir respuesta
        if errores:
            return JSONResponse(
                status_code=207,  # Multi-Status
                content={
                    'mensaje': f'Carga completada con {len(errores)} errores',
                    'tiempo_segundos': tiempo_segundos,
                    'registros_exitosos': registros_exitosos,
                    'registros_con_errores': len(errores),
                    'errores': errores[:50]  # Limitar a primeros 50 errores
                }
            )
        else:
            return {
                'mensaje': f'Se importaron correctamente {registros_exitosos} registros',
                'tiempo_segundos': tiempo_segundos,
                'registros_exitosos': registros_exitosos,
                'registros_con_errores': 0
            }
    
    except HTTPException:
        raise
    except Exception as e:
        tiempo_fin = time.time()
        tiempo_segundos = round(tiempo_fin - tiempo_inicio, 2)
        raise HTTPException(
            status_code=500,
            detail={
                'mensaje': f'Error al procesar el archivo: {str(e)}',
                'tiempo_segundos': tiempo_segundos
            }
        )


@router.get("/")
async def obtener_pacientes(
    skip: int = 0,
    limit: int = 100
):
    """
    Obtiene la lista de pacientes de Medical Care con paginación
    
    Args:
        skip: Número de registros a saltar (paginación)
        limit: Número máximo de registros a retornar
    
    Returns:
        JSON con lista de pacientes
    """
    try:
        cursor = coleccion.find().skip(skip).limit(limit)
        pacientes = []
        
        for doc in cursor:
            # Convertir ObjectId a string y eliminar _id
            doc['_id'] = str(doc['_id'])
            pacientes.append(doc)
        
        return {
            'pacientes': pacientes,
            'total': len(pacientes),
            'skip': skip,
            'limit': limit
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f'Error al obtener pacientes: {str(e)}'
        )


@router.get("/buscar")
async def buscar_paciente(cedula: str = None, paciente: str = None):
    """
    Busca pacientes por cédula o nombre de paciente
    
    Args:
        cedula: Número de cédula (opcional)
        paciente: Nombre del paciente (opcional)
    
    Returns:
        JSON con lista de pacientes coincidentes
    """
    try:
        filtro = {}
        
        if cedula:
            filtro['cedula'] = fx_normalizar_cedula(cedula)
        
        if paciente:
            filtro['paciente'] = {'$regex': paciente, '$options': 'i'}
        
        cursor = coleccion.find(filtro).limit(100)
        pacientes = []
        
        for doc in cursor:
            doc['_id'] = str(doc['_id'])
            pacientes.append(doc)
        
        return {
            'pacientes': pacientes,
            'total': len(pacientes)
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f'Error al buscar pacientes: {str(e)}'
        )


@router.get("/ocupacion-rutas")
async def ocupacion_rutas():
    """
    Agrupa pacientes por ruta y calcula similitud de llave con la colección v3
    """
    from difflib import SequenceMatcher

    # Traer todos los pacientes con sus llaves
    pacientes = list(coleccion.find(
        {},
        {'llave': 1, 'paciente_original': 1, 'ruta': 1, 'estado': 1, 'cedula_original': 1}
    ))

    # Traer todas las llaves de v3
    coleccion_v3 = bd['v3']
    llaves_v3 = [
        doc['llave'] for doc in coleccion_v3.find({'llave': {'$exists': True}}, {'llave': 1})
        if doc.get('llave')
    ]

    # Para cada paciente, encontrar la mejor similitud con v3
    resultado_pacientes = []
    for p in pacientes:
        llave_paciente = p.get('llave', '')
        if not llave_paciente:
            continue

        mejor_similitud = 0.0
        mejor_llave_v3 = ''
        for llave_v3 in llaves_v3:
            sim = SequenceMatcher(None, llave_paciente, llave_v3).ratio()
            if sim > mejor_similitud:
                mejor_similitud = sim
                mejor_llave_v3 = llave_v3

        resultado_pacientes.append({
            'paciente': p.get('paciente_original', ''),
            'cedula': p.get('cedula_original', ''),
            'ruta': p.get('ruta', '') or 'SIN RUTA',
            'llave': llave_paciente,
            'similitud': round(mejor_similitud * 100, 1),
            'llave_v3': mejor_llave_v3,
            'en_v3': mejor_similitud >= 0.8,
            'estado': p.get('estado', 'ACTIVO')
        })

    # Agrupar por ruta
    rutas: dict = {}
    for p in resultado_pacientes:
        ruta = p['ruta']
        if ruta not in rutas:
            rutas[ruta] = {'pacientes': [], 'total': 0, 'en_v3': 0}
        rutas[ruta]['pacientes'].append(p)
        rutas[ruta]['total'] += 1
        if p['en_v3']:
            rutas[ruta]['en_v3'] += 1

    # Construir respuesta ordenada por ruta
    resultado = []
    for ruta, datos in sorted(rutas.items()):
        ocupacion = round(datos['en_v3'] / datos['total'] * 100, 1) if datos['total'] > 0 else 0.0
        resultado.append({
            'ruta': ruta,
            'total_pacientes': datos['total'],
            'pacientes_en_v3': datos['en_v3'],
            'ocupacion_pct': ocupacion,
            'pacientes': sorted(datos['pacientes'], key=lambda x: x['similitud'], reverse=True)
        })

    return {'rutas': resultado}


@router.get("/v3-sin-paciente")
async def v3_sin_paciente():
    """
    Retorna registros de v3 que no tienen un paciente coincidente (similitud < 80%)
    """
    from difflib import SequenceMatcher

    # Traer todas las llaves de pacientes
    llaves_pacientes = [
        doc['llave'] for doc in coleccion.find({'llave': {'$exists': True}}, {'llave': 1})
        if doc.get('llave')
    ]

    # Traer todos los registros de v3 con sus datos relevantes
    coleccion_v3 = bd['v3']
    registros_v3 = list(coleccion_v3.find(
        {'llave': {'$exists': True}},
        {'llave': 1, 'cliente_destino_original': 1, 'direccion_destino_original': 1,
         'ruta': 1, 'estado_pedido': 1, 'telefono_original': 1, 'codigo_pedido': 1}
    ))

    sin_paciente = []
    for reg in registros_v3:
        llave_v3 = reg.get('llave', '')
        if not llave_v3:
            continue

        mejor_similitud = 0.0
        mejor_llave_paciente = ''
        for llave_p in llaves_pacientes:
            sim = SequenceMatcher(None, llave_v3, llave_p).ratio()
            if sim > mejor_similitud:
                mejor_similitud = sim
                mejor_llave_paciente = llave_p

        if mejor_similitud < 0.8:
            sin_paciente.append({
                'codigo_pedido': reg.get('codigo_pedido', ''),
                'cliente_destino': reg.get('cliente_destino_original', ''),
                'direccion_destino': reg.get('direccion_destino_original', ''),
                'ruta': reg.get('ruta', '') or 'SIN RUTA',
                'estado_pedido': reg.get('estado_pedido', ''),
                'telefono': reg.get('telefono_original', ''),
                'llave': llave_v3,
                'similitud': round(mejor_similitud * 100, 1),
                'llave_paciente_cercana': mejor_llave_paciente
            })

    # Agrupar por ruta
    rutas: dict = {}
    for reg in sin_paciente:
        ruta = reg['ruta']
        if ruta not in rutas:
            rutas[ruta] = []
        rutas[ruta].append(reg)

    resultado = [
        {
            'ruta': ruta,
            'total': len(regs),
            'registros': sorted(regs, key=lambda x: x['similitud'], reverse=True)
        }
        for ruta, regs in sorted(rutas.items())
    ]

    return {'total_sin_paciente': len(sin_paciente), 'rutas': resultado}


@router.delete("/eliminar-todos")
async def eliminar_todos_pacientes(usuario: str):
    """
    Elimina todos los pacientes de Medical Care (solo ADMIN)
    
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
            detail=f'Error al eliminar pacientes: {str(e)}'
        )


@router.post("/")
async def crear_paciente(usuario: str, paciente_data: dict):
    """
    Crea un nuevo paciente individual
    
    Args:
        usuario: Usuario que crea el paciente
        paciente_data: Datos del paciente (sede, paciente, cedula, direccion, departamento, municipio, ruta, cedi, celular)
    
    Returns:
        JSON con el paciente creado
    """
    try:
        # Extraer valores originales
        sede_original = paciente_data.get('sede', '').strip()
        paciente_original = paciente_data.get('paciente', '').strip()
        cedula_original = paciente_data.get('cedula', '').strip()
        direccion_original = paciente_data.get('direccion', '').strip()
        departamento_original = paciente_data.get('departamento', '').strip()
        municipio_original = paciente_data.get('municipio', '').strip()
        ruta_original = paciente_data.get('ruta', '').strip()
        cedi_original = paciente_data.get('cedi', '').strip()
        celular_original = paciente_data.get('celular', '').strip()
        estado = paciente_data.get('estado', 'ACTIVO').strip().upper()
        
        # Validar campos obligatorios
        if not paciente_original or not cedula_original:
            raise HTTPException(
                status_code=400,
                detail="Los campos 'paciente' y 'cedula' son obligatorios"
            )
        
        # Normalizar SOLO: paciente, dirección, cédula y celular
        paciente_normalizado = fx_normalizar_paciente(paciente_original)
        cedula_normalizada = fx_normalizar_cedula(cedula_original)
        direccion_normalizada = fx_normalizar_direccion(direccion_original)
        celular_normalizado = fx_normalizar_celular(celular_original)
        
        if not paciente_normalizado or not cedula_normalizada:
            raise HTTPException(
                status_code=400,
                detail="Error al normalizar los campos obligatorios"
            )
        
        # Validar duplicados
        existe_en_bd = coleccion.find_one({'cedula': cedula_normalizada})
        if existe_en_bd:
            raise HTTPException(
                status_code=409,
                detail=f"Ya existe un paciente con la cédula {cedula_original}"
            )
        
        direccion_final = direccion_normalizada if direccion_normalizada else direccion_original
        llave = f"{paciente_normalizado} {direccion_final}".strip()

        # Crear documento
        documento = {
            'sede': sede_original,
            'paciente': paciente_normalizado,
            'paciente_original': paciente_original,
            'cedula': cedula_normalizada,
            'cedula_original': cedula_original,
            'direccion': direccion_final,
            'direccion_original': direccion_original,
            'departamento': departamento_original,
            'municipio': municipio_original,
            'ruta': ruta_original,
            'cedi': cedi_original,
            'celular': celular_normalizado if celular_normalizado else celular_original,
            'celular_original': celular_original,
            'llave': llave,
            'estado': estado,
            'usuario_carga': usuario,
            'fecha_carga': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        resultado = coleccion.insert_one(documento)
        documento['_id'] = str(resultado.inserted_id)
        
        return {
            'mensaje': 'Paciente creado exitosamente',
            'paciente': documento
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f'Error al crear paciente: {str(e)}'
        )


@router.put("/{paciente_id}")
async def actualizar_paciente(paciente_id: str, usuario: str, paciente_data: dict):
    """
    Actualiza un paciente existente
    
    Args:
        paciente_id: ID del paciente a actualizar
        usuario: Usuario que realiza la actualización
        paciente_data: Datos actualizados del paciente
    
    Returns:
        JSON con el paciente actualizado
    """
    try:
        from bson import ObjectId
        
        # Validar ObjectId
        if not ObjectId.is_valid(paciente_id):
            raise HTTPException(
                status_code=400,
                detail="ID de paciente inválido"
            )
        
        # Verificar que el paciente existe
        paciente_existente = coleccion.find_one({'_id': ObjectId(paciente_id)})
        if not paciente_existente:
            raise HTTPException(
                status_code=404,
                detail="Paciente no encontrado"
            )
        
        # Extraer valores originales
        sede_original = paciente_data.get('sede', paciente_existente.get('sede', '')).strip()
        paciente_original = paciente_data.get('paciente', paciente_existente.get('paciente_original', '')).strip()
        cedula_original = paciente_data.get('cedula', paciente_existente.get('cedula_original', '')).strip()
        direccion_original = paciente_data.get('direccion', paciente_existente.get('direccion_original', '')).strip()
        departamento_original = paciente_data.get('departamento', paciente_existente.get('departamento_original', '')).strip()
        municipio_original = paciente_data.get('municipio', paciente_existente.get('municipio_original', '')).strip()
        ruta_original = paciente_data.get('ruta', paciente_existente.get('ruta_original', '')).strip()
        cedi_original = paciente_data.get('cedi', paciente_existente.get('cedi_original', '')).strip()
        celular_original = paciente_data.get('celular', paciente_existente.get('celular_original', '')).strip()
        estado = paciente_data.get('estado', paciente_existente.get('estado', 'ACTIVO')).strip().upper()
        
        # Normalizar SOLO: paciente, dirección, cédula y celular
        paciente_normalizado = fx_normalizar_paciente(paciente_original)
        cedula_normalizada = fx_normalizar_cedula(cedula_original)
        direccion_normalizada = fx_normalizar_direccion(direccion_original)
        celular_normalizado = fx_normalizar_celular(celular_original)
        
        # Si la cédula cambia, validar duplicados
        cedula_actual = paciente_existente.get('cedula', '')
        if cedula_normalizada != cedula_actual:
            existe_en_bd = coleccion.find_one({
                'cedula': cedula_normalizada,
                '_id': {'$ne': ObjectId(paciente_id)}
            })
            if existe_en_bd:
                raise HTTPException(
                    status_code=409,
                    detail=f"Ya existe otro paciente con la cédula {cedula_original}"
                )
        
        direccion_final = direccion_normalizada if direccion_normalizada else direccion_original
        llave = f"{paciente_normalizado} {direccion_final}".strip()

        # Crear documento actualizado
        documento_actualizado = {
            'sede': sede_original,
            'paciente': paciente_normalizado,
            'paciente_original': paciente_original,
            'cedula': cedula_normalizada,
            'cedula_original': cedula_original,
            'direccion': direccion_final,
            'direccion_original': direccion_original,
            'departamento': departamento_original,
            'municipio': municipio_original,
            'ruta': ruta_original,
            'cedi': cedi_original,
            'celular': celular_normalizado if celular_normalizado else celular_original,
            'celular_original': celular_original,
            'llave': llave,
            'estado': estado,
            'usuario_actualizacion': usuario,
            'fecha_actualizacion': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        coleccion.update_one(
            {'_id': ObjectId(paciente_id)},
            {'$set': documento_actualizado}
        )
        
        documento_actualizado['_id'] = paciente_id
        
        return {
            'mensaje': 'Paciente actualizado exitosamente',
            'paciente': documento_actualizado
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f'Error al actualizar paciente: {str(e)}'
        )


@router.delete("/{paciente_id}")
async def eliminar_paciente(paciente_id: str, usuario: str):
    """
    Elimina un paciente individual
    
    Args:
        paciente_id: ID del paciente a eliminar
        usuario: Usuario que realiza la eliminación
    
    Returns:
        JSON con confirmación de eliminación
    """
    try:
        from bson import ObjectId
        
        # Validar ObjectId
        if not ObjectId.is_valid(paciente_id):
            raise HTTPException(
                status_code=400,
                detail="ID de paciente inválido"
            )
        
        # Verificar que el paciente existe
        paciente_existente = coleccion.find_one({'_id': ObjectId(paciente_id)})
        if not paciente_existente:
            raise HTTPException(
                status_code=404,
                detail="Paciente no encontrado"
            )
        
        # Eliminar paciente
        coleccion.delete_one({'_id': ObjectId(paciente_id)})
        
        return {
            'mensaje': 'Paciente eliminado exitosamente',
            'paciente_id': paciente_id,
            'usuario': usuario
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f'Error al eliminar paciente: {str(e)}'
        )


@router.get("/{paciente_id}")
async def obtener_paciente(paciente_id: str):
    """
    Obtiene un paciente por ID
    
    Args:
        paciente_id: ID del paciente
    
    Returns:
        JSON con los datos del paciente
    """
    try:
        from bson import ObjectId
        
        # Validar ObjectId
        if not ObjectId.is_valid(paciente_id):
            raise HTTPException(
                status_code=400,
                detail="ID de paciente inválido"
            )
        
        paciente = coleccion.find_one({'_id': ObjectId(paciente_id)})
        if not paciente:
            raise HTTPException(
                status_code=404,
                detail="Paciente no encontrado"
            )
        
        paciente['_id'] = str(paciente['_id'])
        
        return {
            'paciente': paciente
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f'Error al obtener paciente: {str(e)}'
        )


# Función auxiliar para normalización base (reutilizada)
def fx_normalizar_base(txt: str) -> str:
    """Normalización básica"""
    if not txt:
        return txt
    
    # Convertir a mayúsculas y recortar
    t0 = txt.strip().upper()
    
    # Compactar espacios
    return ' '.join(t0.split())
