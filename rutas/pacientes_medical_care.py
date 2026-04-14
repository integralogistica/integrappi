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
    fx_separar_telefonos,
    fx_normalizar_municipio,
    fx_normalizar_cedula
)

router = APIRouter(prefix="/pacientes-medical-care", tags=["Medical Care"])

# Obtener base de datos y colección
bd = bd_cliente['integra']
coleccion = bd['pacientes_medical_care']
coleccion_cache = bd['cache_cruce_mc']

# Mapeo código regional → nombre CEDI
_CEDI_MAPA = {
    'CO04': 'BARRANQUILLA', 'CO05': 'CALI', 'CO06': 'BUCARAMANGA',
    'CO07': 'FUNZA', 'CO09': 'MEDELLIN',
}

def _normalizar_cel(valor: str) -> str:
    """Devuelve solo dígitos (últimos 10) de un número de celular."""
    digits = ''.join(filter(str.isdigit, valor or ''))
    return digits[-10:] if len(digits) >= 10 else digits

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

            # Cargar todas las cédulas existentes en BD de una sola vez
            cedulas_en_bd = set(
                doc['cedula'] for doc in coleccion.find({}, {'cedula': 1, '_id': 0})
            )

            # Procesar cada fila con validación de duplicados
            documentos_a_insertar = []
            cedulas_ya_procesadas = set()

            for idx, fila in enumerate(df.to_dict('records')):
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
                    if not cedula_original:
                        errores.append(f"Fila {idx + 2}: El campo 'cedula' es obligatorio")
                        continue

                    # Normalizar SOLO: paciente, dirección, cédula y celular
                    paciente_normalizado = fx_normalizar_paciente(paciente_original) if paciente_original else ''
                    cedula_normalizada = fx_normalizar_cedula(cedula_original)
                    direccion_normalizada = fx_normalizar_direccion(direccion_original)
                    telefono1, telefono2 = fx_separar_telefonos(celular_original)

                    # Validar resultados de normalización
                    if not cedula_normalizada:
                        errores.append(f"Fila {idx + 2}: Error al normalizar 'cedula'")
                        continue

                    # Validar duplicados por cédula en el archivo
                    if cedula_normalizada in cedulas_ya_procesadas:
                        errores.append(f"Fila {idx + 2}: La cédula {cedula_original} ya existe en el archivo cargado")
                        continue

                    # Validar duplicados en la base de datos (lookup O(1) contra el set precargado)
                    if cedula_normalizada in cedulas_en_bd:
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
                        'celular': telefono1,
                        'celular_original': celular_original,
                        'telefono1': telefono1,
                        'telefono2': telefono2,
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
        
        # Cargar todas las cédulas existentes en BD de una sola vez
        cedulas_en_bd = set(
            doc['cedula'] for doc in coleccion.find({}, {'cedula': 1, '_id': 0})
        )

        # Procesar cada fila con validación de duplicados
        documentos_a_insertar = []
        cedulas_ya_procesadas = set()

        for idx, fila in enumerate(df.to_dict('records')):
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
                if not cedula_original:
                    errores.append(f"Fila {idx + 2}: El campo 'cedula' es obligatorio")
                    continue

                # Normalizar SOLO: paciente, dirección, cédula y celular
                paciente_normalizado = fx_normalizar_paciente(paciente_original) if paciente_original else ''
                cedula_normalizada = fx_normalizar_cedula(cedula_original)
                direccion_normalizada = fx_normalizar_direccion(direccion_original)
                telefono1, telefono2 = fx_separar_telefonos(celular_original)

                # Validar resultados de normalización
                if not cedula_normalizada:
                    errores.append(f"Fila {idx + 2}: Error al normalizar 'cedula'")
                    continue

                # Validar duplicados por cédula en el archivo
                if cedula_normalizada in cedulas_ya_procesadas:
                    errores.append(f"Fila {idx + 2}: La cédula {cedula_original} ya existe en el archivo cargado")
                    continue

                # Validar duplicados en la base de datos (lookup O(1) contra el set precargado)
                if cedula_normalizada in cedulas_en_bd:
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
                    'celular': telefono1,
                    'celular_original': celular_original,
                    'telefono1': telefono1,
                    'telefono2': telefono2,
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
    limit: int = 100,
    cedi: str = None
):
    """
    Obtiene la lista de pacientes de Medical Care con paginación.
    Si se pasa cedi, filtra solo los pacientes de ese CEDI (case-insensitive).
    """
    try:
        filtro = {}
        if cedi:
            filtro['cedi'] = {'$regex': f'^{cedi}$', '$options': 'i'}

        cursor = coleccion.find(filtro).skip(skip).limit(limit)
        pacientes = []
        for doc in cursor:
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
async def buscar_paciente(cedula: str = None, paciente: str = None, cedi: str = None):
    """
    Busca pacientes por cédula o nombre de paciente.
    Si se pasa cedi, restringe la búsqueda a ese CEDI (case-insensitive).
    """
    try:
        filtro = {}

        if cedula:
            filtro['cedula'] = fx_normalizar_cedula(cedula)

        if paciente:
            filtro['paciente'] = {'$regex': paciente, '$options': 'i'}

        if cedi:
            filtro['cedi'] = {'$regex': f'^{cedi}$', '$options': 'i'}

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


def _fmt_fecha(v) -> str:
    """Convierte datetime/date/str a string 'YYYY-MM-DD'. Retorna '' si None o '0000-00-00'."""
    if v is None:
        return ''
    if isinstance(v, str):
        s = v[:10]
        return '' if s.startswith('0000') or s == '' else s
    try:
        return v.strftime('%Y-%m-%d')
    except Exception:
        return ''


_MESES_ES = ['ene','feb','mar','abr','may','jun','jul','ago','sep','oct','nov','dic']

def _fmt_fecha_legible(v) -> str:
    """Convierte 'YYYY-MM-DD HH:MM:SS' o datetime a '9 abr 2026'."""
    try:
        if isinstance(v, str):
            from datetime import datetime as _dt
            d = _dt.strptime(v[:10], '%Y-%m-%d')
        else:
            d = v
        return f"{d.day} {_MESES_ES[d.month - 1]} {d.year}"
    except Exception:
        return str(v)


def _calcular_cruce():
    """
    Ejecuta el cruce completo pacientes <-> V3 y retorna ambos resultados.
    Función interna reutilizada por el endpoint de recálculo.
    """
    from rapidfuzz.fuzz import ratio as fuzz_ratio

    # ── Ocupación por rutas ──────────────────────────────────────────────────
    pacientes = list(coleccion.find(
        {},
        {'llave': 1, 'paciente_original': 1, 'ruta': 1, 'estado': 1, 'cedula_original': 1}
    ))

    coleccion_v3 = bd['v3']
    llaves_v3 = [
        doc['llave'] for doc in coleccion_v3.find({'llave': {'$exists': True}}, {'llave': 1})
        if doc.get('llave')
    ]

    resultado_pacientes = []
    for p in pacientes:
        llave_paciente = p.get('llave', '')
        if not llave_paciente:
            continue
        mejor_similitud = 0.0
        mejor_llave_v3 = ''
        for llave_v3 in llaves_v3:
            sim = fuzz_ratio(llave_paciente, llave_v3) / 100.0
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
            'en_v3': mejor_similitud >= 0.75,
            'estado': p.get('estado', 'ACTIVO')
        })

    rutas_ocupacion: dict = {}
    for p in resultado_pacientes:
        ruta = p['ruta']
        if ruta not in rutas_ocupacion:
            rutas_ocupacion[ruta] = {'pacientes': [], 'total': 0, 'en_v3': 0}
        rutas_ocupacion[ruta]['pacientes'].append(p)
        rutas_ocupacion[ruta]['total'] += 1
        if p['en_v3']:
            rutas_ocupacion[ruta]['en_v3'] += 1

    ocupacion_resultado = []
    for ruta, datos in sorted(rutas_ocupacion.items()):
        ocupacion = round(datos['en_v3'] / datos['total'] * 100, 1) if datos['total'] > 0 else 0.0
        ocupacion_resultado.append({
            'ruta': ruta,
            'total_pacientes': datos['total'],
            'pacientes_en_v3': datos['en_v3'],
            'ocupacion_pct': ocupacion,
            'pacientes': sorted(datos['pacientes'], key=lambda x: x['similitud'], reverse=True)
        })

    # ── V3 sin paciente ──────────────────────────────────────────────────────
    llaves_pacientes = [p['llave'] for p in resultado_pacientes if p.get('llave')]

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
            sim = fuzz_ratio(llave_v3, llave_p) / 100.0
            if sim > mejor_similitud:
                mejor_similitud = sim
                mejor_llave_paciente = llave_p
        if mejor_similitud < 0.75:
            sin_paciente.append({
                'codigo_pedido': reg.get('codigo_pedido', ''),
                'cliente_destino': reg.get('cliente_destino_original', ''),
                'direccion_destino': reg.get('direccion_destino_original', ''),
                'ruta': reg.get('ruta', '') or 'SIN RUTA',
                'estado_pedido': reg.get('estado_pedido', ''),
                'fecha_preferente': _fmt_fecha(reg.get('fecha_preferente')),
                'telefono': reg.get('telefono_original', ''),
                'llave': llave_v3,
                'similitud': round(mejor_similitud * 100, 1),
                'llave_paciente_cercana': mejor_llave_paciente
            })

    rutas_v3: dict = {}
    for reg in sin_paciente:
        ruta = reg['ruta']
        if ruta not in rutas_v3:
            rutas_v3[ruta] = []
        rutas_v3[ruta].append(reg)

    v3_resultado = [
        {
            'ruta': ruta,
            'total': len(regs),
            'registros': sorted(regs, key=lambda x: x['similitud'], reverse=True)
        }
        for ruta, regs in sorted(rutas_v3.items())
    ]

    return ocupacion_resultado, v3_resultado, len(sin_paciente)


def ejecutar_cruce_automatico(usuario: str = 'sync_automatico') -> dict:
    """
    Ejecuta el cruce completo pacientes <-> V3 y guarda en cache_cruce_mc.
    Llamado automáticamente tras cada sync_v3 exitoso. No usa SSE.
    """
    from rapidfuzz.fuzz import ratio as fuzz_ratio
    import logging
    logger = logging.getLogger(__name__)

    try:
        pacientes = list(coleccion.find(
            {},
            {'llave': 1, 'paciente_original': 1, 'direccion_original': 1,
             'ruta': 1, 'estado': 1, 'cedula_original': 1, 'cedi': 1,
             'telefono1': 1, 'telefono2': 1}
        ))
        coleccion_v3 = bd['v3']
        registros_v3 = list(coleccion_v3.find(
            {'llave': {'$exists': True}},
            {'llave': 1, 'telefono_original': 1, 'cliente_destino_original': 1,
             'direccion_destino_original': 1, 'ruta': 1, 'estado_pedido': 1,
             'codigo_pedido': 1, 'bodega_origen': 1,
             'fecha_pedido': 1, 'fecha_preferente': 1,
             'fecha_entrega': 1, 'planilla': 1,
             'divipola': 1, 'municipio_destino': 1}
        ))
        llaves_v3 = [doc['llave'] for doc in registros_v3 if doc.get('llave')]
        docs_v3_por_llave = {doc['llave']: doc for doc in registros_v3 if doc.get('llave')}

        dict_telefonos_v3 = {}
        for doc in registros_v3:
            tel = _normalizar_cel(doc.get('telefono_original', ''))
            if len(tel) >= 7 and doc.get('llave'):
                dict_telefonos_v3[tel] = doc['llave']

        set_celulares_pacientes = set()
        for p in pacientes:
            for campo in ('telefono1', 'telefono2'):
                cel = _normalizar_cel(p.get(campo, '') or '')
                if len(cel) >= 7:
                    set_celulares_pacientes.add(cel)

        resultado_pacientes = []
        for p in pacientes:
            llave_paciente = p.get('llave', '')
            if not llave_paciente:
                continue
            cedi_raw = p.get('cedi', '') or ''
            cedi = _CEDI_MAPA.get(cedi_raw.upper(), cedi_raw.upper())
            tel1 = _normalizar_cel(p.get('telefono1', '') or '')
            tel2 = _normalizar_cel(p.get('telefono2', '') or '')
            celular_p = next((t for t in (tel1, tel2) if len(t) >= 7 and t in dict_telefonos_v3), '')
            if celular_p:
                en_v3, similitud, llave_v3_match, match_tipo = True, 100.0, dict_telefonos_v3[celular_p], 'celular'
            else:
                mejor_sim, mejor_llave = 0.0, ''
                for lv3 in llaves_v3:
                    sim = fuzz_ratio(llave_paciente, lv3) / 100.0
                    if sim > mejor_sim:
                        mejor_sim, mejor_llave = sim, lv3
                en_v3 = mejor_sim >= 0.75
                similitud = round(mejor_sim * 100, 1)
                llave_v3_match = mejor_llave
                match_tipo = 'llave'
            doc_v3 = docs_v3_por_llave.get(llave_v3_match, {}) if (en_v3 and llave_v3_match) else {}
            resultado_pacientes.append({
                'paciente': p.get('paciente_original', ''),
                'cedula': p.get('cedula_original', ''),
                'direccion_original': p.get('direccion_original', ''),
                'ruta': p.get('ruta', '') or 'SIN RUTA',
                'cedi': cedi,
                'llave': llave_paciente,
                'similitud': similitud,
                'match_tipo': match_tipo,
                'llave_v3': llave_v3_match,
                'en_v3': en_v3,
                'estado': p.get('estado', 'ACTIVO'),
                'estado_pedido': doc_v3.get('estado_pedido', ''),
                'fecha_pedido': _fmt_fecha(doc_v3.get('fecha_pedido')),
                'fecha_preferente': _fmt_fecha(doc_v3.get('fecha_preferente')),
                'fecha_entrega': _fmt_fecha(doc_v3.get('fecha_entrega')),
                'planilla': doc_v3.get('planilla', ''),
                'municipio_destino': doc_v3.get('municipio_destino', ''),
                'divipola': doc_v3.get('divipola', ''),
            })

        rutas_ocupacion: dict = {}
        for p in resultado_pacientes:
            ruta = p['ruta']
            if ruta not in rutas_ocupacion:
                rutas_ocupacion[ruta] = {'pacientes': [], 'total': 0, 'en_v3': 0, 'entregados': 0, 'cedi': p['cedi'], 'planillas': set()}
            rutas_ocupacion[ruta]['pacientes'].append(p)
            rutas_ocupacion[ruta]['total'] += 1
            if p['en_v3']:
                rutas_ocupacion[ruta]['en_v3'] += 1
            if p['en_v3'] and p.get('estado_pedido') == 'ENTREGADO':
                rutas_ocupacion[ruta]['entregados'] += 1
            if p.get('planilla'):
                rutas_ocupacion[ruta]['planillas'].add(p['planilla'])

        ocupacion_resultado = []
        for ruta, datos in sorted(rutas_ocupacion.items()):
            ocupacion = round(datos['en_v3'] / datos['total'] * 100, 1) if datos['total'] > 0 else 0.0
            entregados = datos['entregados']
            pct_entregados = round(entregados / datos['total'] * 100, 1) if datos['total'] > 0 else 0.0
            ocupacion_resultado.append({
                'ruta': ruta,
                'cedi': datos['cedi'],
                'total_pacientes': datos['total'],
                'pacientes_en_v3': datos['en_v3'],
                'ocupacion_pct': ocupacion,
                'pacientes_entregados': entregados,
                'pct_entregados': pct_entregados,
                'vehiculos': len(datos['planillas']),
                'pacientes': sorted(datos['pacientes'], key=lambda x: x['similitud'], reverse=True),
            })

        llaves_pacientes = [p['llave'] for p in resultado_pacientes if p.get('llave')]
        sin_paciente = []
        for reg in registros_v3:
            llave_v3 = reg.get('llave', '')
            if not llave_v3:
                continue
            bodega = reg.get('bodega_origen', '') or ''
            cedi_v3 = _CEDI_MAPA.get(bodega.upper(), bodega.upper())
            tel_v3 = _normalizar_cel(reg.get('telefono_original', ''))
            if tel_v3 and len(tel_v3) >= 7 and tel_v3 in set_celulares_pacientes:
                continue
            mejor_sim, mejor_llave_p = 0.0, ''
            for llave_p in llaves_pacientes:
                sim = fuzz_ratio(llave_v3, llave_p) / 100.0
                if sim > mejor_sim:
                    mejor_sim, mejor_llave_p = sim, llave_p
            if mejor_sim < 0.75:
                sin_paciente.append({
                    'codigo_pedido': reg.get('codigo_pedido', ''),
                    'cliente_destino': reg.get('cliente_destino_original', ''),
                    'direccion_destino': reg.get('direccion_destino_original', ''),
                    'ruta': reg.get('ruta', '') or 'SIN RUTA',
                    'cedi': cedi_v3,
                    'estado_pedido': reg.get('estado_pedido', ''),
                    'fecha_preferente': _fmt_fecha(reg.get('fecha_preferente')),
                    'telefono': reg.get('telefono_original', ''),
                    'llave': llave_v3,
                    'similitud': round(mejor_sim * 100, 1),
                    'llave_paciente_cercana': mejor_llave_p,
                })

        rutas_v3: dict = {}
        for reg in sin_paciente:
            ruta = reg['ruta']
            if ruta not in rutas_v3:
                rutas_v3[ruta] = {'registros': [], 'cedi': reg['cedi']}
            rutas_v3[ruta]['registros'].append(reg)

        v3_resultado = [
            {'ruta': ruta, 'cedi': datos['cedi'], 'total': len(datos['registros']),
             'registros': sorted(datos['registros'], key=lambda x: x['similitud'], reverse=True)}
            for ruta, datos in sorted(rutas_v3.items())
        ]

        fecha_calculo = time.strftime('%Y-%m-%d %H:%M:%S')
        coleccion_cache.update_one(
            {'tipo': 'cruce_completo'},
            {'$set': {
                'tipo': 'cruce_completo',
                'ocupacion_rutas': ocupacion_resultado,
                'v3_sin_paciente': v3_resultado,
                'total_sin_paciente': len(sin_paciente),
                'calculado_por': usuario,
                'fecha_calculo': fecha_calculo,
            }},
            upsert=True
        )
        logger.info(f"[cruce_automatico] OK — {len(pacientes)} pacientes, {len(sin_paciente)} sin paciente")
        import threading
        threading.Thread(
            target=enviar_excel_cruce_por_correo,
            args=(usuario, fecha_calculo),
            daemon=True
        ).start()
        return {'ok': True, 'total_pacientes': len(pacientes), 'total_sin_paciente': len(sin_paciente), 'fecha_calculo': fecha_calculo}

    except Exception as e:
        logger.error(f"[cruce_automatico] Error: {e}")
        return {'ok': False, 'error': str(e)}


@router.post("/recalcular-cruce")
async def recalcular_cruce(usuario: str, enviar_correo: bool = True):
    """
    Ejecuta el cruce pacientes <-> V3 con progreso en tiempo real via SSE.
    Guarda el resultado en cache (cache_cruce_mc) al terminar.
    """
    from rapidfuzz.fuzz import ratio as fuzz_ratio

    def generar_eventos():
        try:
            # ── Etapa 1: cargar datos ────────────────────────────────────────
            yield f"data: {json.dumps({'stage': 'loading', 'progress': 0, 'message': 'Cargando pacientes y pedidos V3...'})}\n\n"

            pacientes = list(coleccion.find(
                {},
                {'llave': 1, 'paciente_original': 1, 'direccion_original': 1,
                 'ruta': 1, 'estado': 1, 'cedula_original': 1, 'cedi': 1,
                 'telefono1': 1, 'telefono2': 1}
            ))

            coleccion_v3 = bd['v3']
            # Cargamos los registros V3 completos de una sola vez (se usan en etapas 2 y 3)
            registros_v3 = list(coleccion_v3.find(
                {'llave': {'$exists': True}},
                {'llave': 1, 'telefono_original': 1, 'cliente_destino_original': 1,
                 'direccion_destino_original': 1, 'ruta': 1, 'estado_pedido': 1,
                 'codigo_pedido': 1, 'bodega_origen': 1,
                 'fecha_pedido': 1, 'fecha_preferente': 1,
             'fecha_entrega': 1, 'planilla': 1,
             'divipola': 1, 'municipio_destino': 1}
            ))

            llaves_v3 = [doc['llave'] for doc in registros_v3 if doc.get('llave')]
            docs_v3_por_llave = {doc['llave']: doc for doc in registros_v3 if doc.get('llave')}
            total_pacientes = len(pacientes)
            total_v3 = len(registros_v3)

            # Dict teléfono → llave para cruce rápido por celular
            dict_telefonos_v3 = {}
            for doc in registros_v3:
                tel = _normalizar_cel(doc.get('telefono_original', ''))
                if len(tel) >= 7 and doc.get('llave'):
                    dict_telefonos_v3[tel] = doc['llave']

            set_celulares_pacientes = set()
            for p in pacientes:
                for campo in ('telefono1', 'telefono2'):
                    cel = _normalizar_cel(p.get(campo, '') or '')
                    if len(cel) >= 7:
                        set_celulares_pacientes.add(cel)

            yield f"data: {json.dumps({'stage': 'loading', 'progress': 8, 'message': f'{total_pacientes} pacientes y {total_v3} pedidos V3 cargados'})}\n\n"

            # ── Etapa 2: comparar pacientes contra V3 ───────────────────────
            resultado_pacientes = []
            paso_reporte = max(1, total_pacientes // 20)

            for idx, p in enumerate(pacientes):
                llave_paciente = p.get('llave', '')
                if not llave_paciente:
                    continue

                cedi_raw = p.get('cedi', '') or ''
                cedi = _CEDI_MAPA.get(cedi_raw.upper(), cedi_raw.upper())

                # Criterio 1: cruce por teléfono (más certero) — revisa telefono1 y telefono2
                tel1 = _normalizar_cel(p.get('telefono1', '') or '')
                tel2 = _normalizar_cel(p.get('telefono2', '') or '')
                celular_p = next((t for t in (tel1, tel2) if len(t) >= 7 and t in dict_telefonos_v3), '')
                if celular_p:
                    en_v3 = True
                    similitud = 100.0
                    llave_v3_match = dict_telefonos_v3[celular_p]
                    match_tipo = 'celular'
                else:
                    # Criterio 2: similitud de llave (fuzzy)
                    mejor_similitud = 0.0
                    mejor_llave_v3 = ''
                    for lv3 in llaves_v3:
                        sim = fuzz_ratio(llave_paciente, lv3) / 100.0
                        if sim > mejor_similitud:
                            mejor_similitud = sim
                            mejor_llave_v3 = lv3
                    en_v3 = mejor_similitud >= 0.75
                    similitud = round(mejor_similitud * 100, 1)
                    llave_v3_match = mejor_llave_v3
                    match_tipo = 'llave'

                doc_v3 = docs_v3_por_llave.get(llave_v3_match, {}) if (en_v3 and llave_v3_match) else {}
                resultado_pacientes.append({
                    'paciente': p.get('paciente_original', ''),
                    'cedula': p.get('cedula_original', ''),
                    'direccion_original': p.get('direccion_original', ''),
                    'ruta': p.get('ruta', '') or 'SIN RUTA',
                    'cedi': cedi,
                    'llave': llave_paciente,
                    'similitud': similitud,
                    'match_tipo': match_tipo,
                    'llave_v3': llave_v3_match,
                    'en_v3': en_v3,
                    'estado': p.get('estado', 'ACTIVO'),
                    'estado_pedido': doc_v3.get('estado_pedido', ''),
                    'fecha_pedido': _fmt_fecha(doc_v3.get('fecha_pedido')),
                    'fecha_preferente': _fmt_fecha(doc_v3.get('fecha_preferente')),
                    'fecha_entrega': _fmt_fecha(doc_v3.get('fecha_entrega')),
                    'planilla': doc_v3.get('planilla', ''),
                    'municipio_destino': doc_v3.get('municipio_destino', ''),
                    'divipola': doc_v3.get('divipola', ''),
                })

                if (idx + 1) % paso_reporte == 0 or (idx + 1) == total_pacientes:
                    pct = round(10 + ((idx + 1) / total_pacientes) * 50)
                    yield f"data: {json.dumps({'stage': 'comparing_patients', 'progress': pct, 'processed': idx + 1, 'total': total_pacientes, 'message': f'Paciente {idx + 1} de {total_pacientes}'})}\n\n"

            # Agrupar ocupación por ruta
            rutas_ocupacion: dict = {}
            for p in resultado_pacientes:
                ruta = p['ruta']
                if ruta not in rutas_ocupacion:
                    rutas_ocupacion[ruta] = {'pacientes': [], 'total': 0, 'en_v3': 0, 'entregados': 0, 'cedi': p['cedi'], 'planillas': set()}
                rutas_ocupacion[ruta]['pacientes'].append(p)
                rutas_ocupacion[ruta]['total'] += 1
                if p['en_v3']:
                    rutas_ocupacion[ruta]['en_v3'] += 1
                if p['en_v3'] and p.get('estado_pedido') == 'ENTREGADO':
                    rutas_ocupacion[ruta]['entregados'] += 1
                if p.get('planilla'):
                    rutas_ocupacion[ruta]['planillas'].add(p['planilla'])

            ocupacion_resultado = []
            for ruta, datos in sorted(rutas_ocupacion.items()):
                ocupacion = round(datos['en_v3'] / datos['total'] * 100, 1) if datos['total'] > 0 else 0.0
                entregados = datos['entregados']
                pct_entregados = round(entregados / datos['total'] * 100, 1) if datos['total'] > 0 else 0.0
                ocupacion_resultado.append({
                    'ruta': ruta,
                    'cedi': datos['cedi'],
                    'total_pacientes': datos['total'],
                    'pacientes_en_v3': datos['en_v3'],
                    'ocupacion_pct': ocupacion,
                    'pacientes_entregados': entregados,
                    'pct_entregados': pct_entregados,
                    'vehiculos': len(datos['planillas']),
                    'pacientes': sorted(datos['pacientes'], key=lambda x: x['similitud'], reverse=True)
                })

            # ── Etapa 3: V3 sin paciente ─────────────────────────────────────
            llaves_pacientes = [p['llave'] for p in resultado_pacientes if p.get('llave')]

            yield f"data: {json.dumps({'stage': 'comparing_v3', 'progress': 62, 'message': f'Verificando {total_v3} pedidos V3...'})}\n\n"

            sin_paciente = []
            paso_reporte_v3 = max(1, total_v3 // 20)

            for idx, reg in enumerate(registros_v3):
                llave_v3 = reg.get('llave', '')
                if not llave_v3:
                    continue

                bodega = reg.get('bodega_origen', '') or ''
                cedi_v3 = _CEDI_MAPA.get(bodega.upper(), bodega.upper())

                # Criterio 1: cruce por celular — si hay match no es "sin paciente"
                tel_v3 = _normalizar_cel(reg.get('telefono_original', ''))
                if tel_v3 and len(tel_v3) >= 7 and tel_v3 in set_celulares_pacientes:
                    if (idx + 1) % paso_reporte_v3 == 0 or (idx + 1) == total_v3:
                        pct = round(62 + ((idx + 1) / total_v3) * 28)
                        yield f"data: {json.dumps({'stage': 'comparing_v3', 'progress': pct, 'processed': idx + 1, 'total': total_v3, 'message': f'V3 {idx + 1} de {total_v3}'})}\n\n"
                    continue

                # Criterio 2: similitud de llave
                mejor_similitud = 0.0
                mejor_llave_paciente = ''
                for llave_p in llaves_pacientes:
                    sim = fuzz_ratio(llave_v3, llave_p) / 100.0
                    if sim > mejor_similitud:
                        mejor_similitud = sim
                        mejor_llave_paciente = llave_p

                if mejor_similitud < 0.75:
                    sin_paciente.append({
                        'codigo_pedido': reg.get('codigo_pedido', ''),
                        'cliente_destino': reg.get('cliente_destino_original', ''),
                        'direccion_destino': reg.get('direccion_destino_original', ''),
                        'ruta': reg.get('ruta', '') or 'SIN RUTA',
                        'cedi': cedi_v3,
                        'estado_pedido': reg.get('estado_pedido', ''),
                        'fecha_preferente': _fmt_fecha(reg.get('fecha_preferente')),
                        'telefono': reg.get('telefono_original', ''),
                        'llave': llave_v3,
                        'similitud': round(mejor_similitud * 100, 1),
                        'llave_paciente_cercana': mejor_llave_paciente
                    })

                if (idx + 1) % paso_reporte_v3 == 0 or (idx + 1) == total_v3:
                    pct = round(62 + ((idx + 1) / total_v3) * 28)
                    yield f"data: {json.dumps({'stage': 'comparing_v3', 'progress': pct, 'processed': idx + 1, 'total': total_v3, 'message': f'V3 {idx + 1} de {total_v3}'})}\n\n"

            rutas_v3: dict = {}
            for reg in sin_paciente:
                ruta = reg['ruta']
                if ruta not in rutas_v3:
                    rutas_v3[ruta] = {'registros': [], 'cedi': reg['cedi']}
                rutas_v3[ruta]['registros'].append(reg)

            v3_resultado = [
                {'ruta': ruta, 'cedi': datos['cedi'], 'total': len(datos['registros']),
                 'registros': sorted(datos['registros'], key=lambda x: x['similitud'], reverse=True)}
                for ruta, datos in sorted(rutas_v3.items())
            ]

            # ── Etapa 4: guardar en cache ────────────────────────────────────
            yield f"data: {json.dumps({'stage': 'saving', 'progress': 95, 'message': 'Guardando resultados...'})}\n\n"

            fecha_calculo = time.strftime('%Y-%m-%d %H:%M:%S')
            coleccion_cache.update_one(
                {'tipo': 'cruce_completo'},
                {'$set': {
                    'tipo': 'cruce_completo',
                    'ocupacion_rutas': ocupacion_resultado,
                    'v3_sin_paciente': v3_resultado,
                    'total_sin_paciente': len(sin_paciente),
                    'calculado_por': usuario,
                    'fecha_calculo': fecha_calculo,
                }},
                upsert=True
            )

            if enviar_correo:
                import threading
                threading.Thread(
                    target=enviar_excel_cruce_por_correo,
                    args=(usuario, fecha_calculo),
                    daemon=True
                ).start()

            yield f"data: {json.dumps({'stage': 'complete', 'progress': 100, 'message': 'Cruce completado', 'rutas': ocupacion_resultado, 'v3_sin_paciente': v3_resultado, 'total_sin_paciente': len(sin_paciente), 'fecha_calculo': fecha_calculo, 'calculado_por': usuario})}\n\n"

        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

    return StreamingResponse(generar_eventos(), media_type="text/event-stream")


@router.get("/ocupacion-rutas")
async def ocupacion_rutas():
    """
    Retorna el último resultado de ocupación por rutas guardado en cache.
    Para recalcular usar POST /recalcular-cruce.
    """
    cache = coleccion_cache.find_one({'tipo': 'cruce_completo'}, {'_id': 0})
    if not cache:
        return {'rutas': [], 'fecha_calculo': None, 'calculado_por': None}
    return {
        'rutas': cache.get('ocupacion_rutas', []),
        'fecha_calculo': cache.get('fecha_calculo'),
        'calculado_por': cache.get('calculado_por'),
    }


@router.get("/v3-sin-paciente")
async def v3_sin_paciente():
    """
    Retorna el último resultado de V3 sin paciente guardado en cache.
    Para recalcular usar POST /recalcular-cruce.
    """
    cache = coleccion_cache.find_one({'tipo': 'cruce_completo'}, {'_id': 0})
    if not cache:
        return {'total_sin_paciente': 0, 'rutas': [], 'fecha_calculo': None, 'calculado_por': None}
    return {
        'total_sin_paciente': cache.get('total_sin_paciente', 0),
        'rutas': cache.get('v3_sin_paciente', []),
        'fecha_calculo': cache.get('fecha_calculo'),
        'calculado_por': cache.get('calculado_por'),
    }


def _generar_excel_bytes(cache: dict, cedi: str = None) -> tuple:
    """
    Genera el Excel del cruce y retorna (bytes, nombre_archivo).
    Reutilizado por el endpoint de descarga y el envío por correo.
    """
    import io
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter

    ocupacion_rutas_data = cache.get('ocupacion_rutas', [])
    v3_sin_paciente_data = cache.get('v3_sin_paciente', [])
    fecha_calculo        = cache.get('fecha_calculo', '')
    calculado_por        = cache.get('calculado_por', '')

    if cedi:
        ocupacion_rutas_data = [r for r in ocupacion_rutas_data if (r.get('cedi') or '').upper() == cedi.upper()]
        v3_sin_paciente_data = [r for r in v3_sin_paciente_data if (r.get('cedi') or '').upper() == cedi.upper()]

    from datetime import datetime as _dt, timedelta as _td
    _hoy = _dt.now().replace(hour=0, minute=0, second=0, microsecond=0)
    _limite = _hoy + _td(days=5)

    def _fecha_dt(s):
        """Parsea 'YYYY-MM-DD' a datetime, retorna None si falla."""
        try:
            return _dt.strptime(s[:10], '%Y-%m-%d') if s else None
        except Exception:
            return None

    wb = openpyxl.Workbook()
    header_fill    = PatternFill('solid', fgColor='004D40')
    header_font    = Font(bold=True, color='FFFFFF', size=10)
    title_font     = Font(bold=True, size=12, color='004D40')
    entregado_fill = PatternFill('solid', fgColor='F1F8F1')   # verde claro — en_v3 + ENTREGADO
    urgente_fill   = PatternFill('solid', fgColor='FFF3F3')   # rojo claro  — fecha_preferente próxima/vencida
    red_font       = Font(bold=True, color='C62828', size=10)
    center         = Alignment(horizontal='center', vertical='center', wrap_text=True)
    left           = Alignment(horizontal='left',   vertical='center', wrap_text=True)
    thin_border    = Border(
        left=Side(style='thin', color='BDBDBD'), right=Side(style='thin', color='BDBDBD'),
        top=Side(style='thin', color='BDBDBD'),  bottom=Side(style='thin', color='BDBDBD')
    )

    def set_header_row(ws, row, cols):
        for c, title in enumerate(cols, 1):
            cell = ws.cell(row=row, column=c, value=title)
            cell.fill, cell.font, cell.alignment, cell.border = header_fill, header_font, center, thin_border

    def style_cell(cell, fill=None, font=None):
        cell.border = thin_border
        if fill:
            cell.fill = fill
        if font:
            cell.font = font
        cell.alignment = left

    ws1 = wb.active
    ws1.title = 'Ocupacion Rutas'
    ws1['A1'] = f'Cruce Pacientes ↔ V3  |  {_fmt_fecha_legible(fecha_calculo)}'
    ws1['A1'].font, ws1['A1'].alignment = title_font, center
    ws1.merge_cells('A1:K1')
    ws1.row_dimensions[1].height = 22
    set_header_row(ws1, 2, ['CEDI', 'Ruta', 'Paciente', 'Cédula', 'Dirección', 'Estado',
                             'En V3', 'Estado Pedido', 'F. Pedido', 'F. Preferente', 'Similitud %'])
    fila = 3
    for r in ocupacion_rutas_data:
        for p in r['pacientes']:
            en_v3          = p.get('en_v3', False)
            estado_pedido  = p.get('estado_pedido', '')
            fecha_pref_str = p.get('fecha_preferente', '')
            fecha_pref_dt  = _fecha_dt(fecha_pref_str)
            es_entregado = en_v3 and estado_pedido == 'ENTREGADO'
            es_rojo      = not es_entregado and (
                not en_v3 or
                estado_pedido == 'POR PROGRAMAR' or
                (fecha_pref_dt is not None and fecha_pref_dt <= _limite)
            )
            fill = entregado_fill if es_entregado else (urgente_fill if es_rojo else None)
            vals = [r.get('cedi',''), r['ruta'], p['paciente'], p['cedula'],
                    p.get('direccion_original',''), p.get('estado',''),
                    'SÍ' if en_v3 else 'NO', estado_pedido,
                    p.get('fecha_pedido',''), fecha_pref_str, p.get('similitud', 0)]
            fecha_pref_urgente = not es_entregado and fecha_pref_dt is not None and fecha_pref_dt <= _limite
            for c, val in enumerate(vals, 1):
                font = red_font if fecha_pref_urgente and c == 10 else None
                style_cell(ws1.cell(row=fila, column=c, value=val), fill, font)
            fila += 1
    for i, w in enumerate([14,18,28,14,32,10,6,18,12,12,12], 1):
        ws1.column_dimensions[get_column_letter(i)].width = w
    ws1.freeze_panes = 'A3'

    ws2 = wb.create_sheet('V3 Sin Paciente')
    ws2['A1'] = f'V3 Sin Paciente  |  {_fmt_fecha_legible(fecha_calculo)}'
    ws2['A1'].font, ws2['A1'].alignment = title_font, center
    ws2.merge_cells('A1:I1')
    ws2.row_dimensions[1].height = 22
    set_header_row(ws2, 2, ['CEDI', 'Ruta', 'Código Pedido', 'Cliente Destino', 'Dirección', 'Teléfono', 'Estado Pedido', 'F. Preferente', 'Similitud %'])
    fila2 = 3
    for r in v3_sin_paciente_data:
        for reg in r['registros']:
            estado_v3      = reg.get('estado_pedido', '')
            fecha_pref_str = reg.get('fecha_preferente', '')
            fecha_pref_dt  = _fecha_dt(fecha_pref_str)
            # Sin paciente = siempre rojo salvo ENTREGADO
            es_entregado_v3 = estado_v3 == 'ENTREGADO'
            fill = entregado_fill if es_entregado_v3 else urgente_fill
            fecha_pref_urgente = not es_entregado_v3 and fecha_pref_dt is not None and fecha_pref_dt <= _limite
            vals = [r.get('cedi',''), r['ruta'], reg.get('codigo_pedido',''),
                    reg.get('cliente_destino',''), reg.get('direccion_destino',''),
                    reg.get('telefono',''), estado_v3, fecha_pref_str, reg.get('similitud', 0)]
            for c, val in enumerate(vals, 1):
                font = red_font if fecha_pref_urgente and c == 8 else None
                style_cell(ws2.cell(row=fila2, column=c, value=val), fill, font)
            fila2 += 1
    for i, w in enumerate([14,18,16,28,32,13,14,12,12], 1):
        ws2.column_dimensions[get_column_letter(i)].width = w
    ws2.freeze_panes = 'A3'

    buffer = io.BytesIO()
    wb.save(buffer)
    nombre = f"cruce_mc{'_'+cedi if cedi else ''}_{fecha_calculo.replace(' ','_').replace(':','-')}.xlsx"
    return buffer.getvalue(), nombre


def enviar_excel_cruce_por_correo(calculado_por: str, fecha_calculo: str):
    """
    Envía el Excel del cruce por correo a todos los usuarios con MEDICAL_CARE.
    Se llama tras cada recálculo (automático o manual).
    """
    import os
    import logging
    import resend as _resend

    logger = logging.getLogger(__name__)
    try:
        api_key   = os.getenv('RESEND_API_KEY', '')
        mail_from = os.getenv('MAIL_FROM', 'no-reply@integralogistica.com')
        if not api_key:
            logger.warning('[cruce_email] RESEND_API_KEY no configurada')
            return
        _resend.api_key = api_key

        # Buscar usuarios con MEDICAL_CARE y correo registrado
        col_usuarios = bd['baseusuarios']
        usuarios = list(col_usuarios.find(
            {'clientes': 'MEDICAL_CARE', 'correo': {'$exists': True, '$nin': [None, '']}},
            {'correo': 1, 'nombre': 1, '_id': 0}
        ))
        destinatarios = [u['correo'] for u in usuarios if u.get('correo')]
        if not destinatarios:
            logger.warning('[cruce_email] Sin destinatarios con MEDICAL_CARE y correo registrado')
            return

        cache = coleccion_cache.find_one({'tipo': 'cruce_completo'}, {'_id': 0})
        if not cache:
            return
        excel_bytes, nombre_archivo = _generar_excel_bytes(cache)

        _resend.Emails.send({
            'from':    f'IntegrApp <{mail_from}>',
            'to':      destinatarios,
            'subject': f'Cruce Pacientes ↔ V3 — {_fmt_fecha_legible(fecha_calculo)}',
            'html': (
                f'<p>Se adjunta el reporte de cruce <strong>Pacientes ↔ V3</strong> '
                f'generado el <strong>{_fmt_fecha_legible(fecha_calculo)}</strong>'
                + (f' por <strong>{calculado_por}</strong>' if calculado_por != 'sync_automatico' else '')
                + f'.</p>'
                f'<p>El archivo contiene dos hojas: <em>Ocupacion Rutas</em> y <em>V3 Sin Paciente</em>.</p>'
                f'<p>Saludos,<br>IntegrApp</p>'
            ),
            'attachments': [{'filename': nombre_archivo, 'content': list(excel_bytes)}],
        })
        logger.info(f'[cruce_email] Excel enviado a {len(destinatarios)} usuario(s): {destinatarios}')
    except Exception as e:
        logger.error(f'[cruce_email] Error: {e}')


@router.get("/exportar-cruce-excel")
async def exportar_cruce_excel(cedi: str = None):
    """
    Genera y descarga un Excel con los resultados del último cruce.
    Dos hojas: 'Ocupacion Rutas' y 'V3 Sin Paciente'.
    Si se pasa cedi, filtra solo esa regional.
    """
    from fastapi.responses import StreamingResponse as SR
    import io

    import io
    cache = coleccion_cache.find_one({'tipo': 'cruce_completo'}, {'_id': 0})
    if not cache:
        raise HTTPException(status_code=404, detail='No hay datos calculados. Ejecute el recálculo primero.')
    excel_bytes, nombre = _generar_excel_bytes(cache, cedi)
    return SR(
        io.BytesIO(excel_bytes),
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers={'Content-Disposition': f'attachment; filename="{nombre}"'}
    )


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
        if not cedula_original:
            raise HTTPException(
                status_code=400,
                detail="El campo 'cedula' es obligatorio"
            )

        # Normalizar SOLO: paciente, dirección, cédula y celular
        paciente_normalizado = fx_normalizar_paciente(paciente_original) if paciente_original else ''
        cedula_normalizada = fx_normalizar_cedula(cedula_original)
        direccion_normalizada = fx_normalizar_direccion(direccion_original)
        telefono1, telefono2 = fx_separar_telefonos(celular_original)

        if not cedula_normalizada:
            raise HTTPException(
                status_code=400,
                detail="Error al normalizar el campo 'cedula'"
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
            'celular': telefono1,
            'celular_original': celular_original,
            'telefono1': telefono1,
            'telefono2': telefono2,
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
        paciente_normalizado = fx_normalizar_paciente(paciente_original) if paciente_original else ''
        cedula_normalizada = fx_normalizar_cedula(cedula_original)
        direccion_normalizada = fx_normalizar_direccion(direccion_original)
        telefono1, telefono2 = fx_separar_telefonos(celular_original)

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
            'celular': telefono1,
            'celular_original': celular_original,
            'telefono1': telefono1,
            'telefono2': telefono2,
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
