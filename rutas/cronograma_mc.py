"""
Rutas para la gestión del cronograma de entregas de pacientes Medical Care
"""
from fastapi import APIRouter, UploadFile, File, HTTPException, Query
from fastapi.responses import StreamingResponse
from pymongo import UpdateOne
import pandas as pd
import time
import json
import io
import re
from datetime import datetime, timedelta
from typing import Optional
from bd.bd_cliente import bd_cliente

router = APIRouter(prefix="/cronograma-mc", tags=["Cronograma Medical Care"])

bd = bd_cliente['integra']
coleccion = bd['cronograma_pacientes_mc']

try:
    coleccion.create_index([('cedula', 1), ('anio_mes', 1)], unique=True)
except Exception:
    pass


def _parsear_fecha(valor) -> str:
    """Convierte un valor de fecha a formato DD/MM/YYYY."""
    if valor is None:
        return ''
    try:
        if pd.isna(valor):
            return ''
    except Exception:
        pass

    if isinstance(valor, (int, float)):
        try:
            fecha = datetime(1899, 12, 30) + timedelta(days=int(valor))
            return fecha.strftime('%d/%m/%Y')
        except Exception:
            return str(valor)

    if hasattr(valor, 'strftime'):
        return valor.strftime('%d/%m/%Y')

    texto = str(valor).strip()
    if not texto:
        return ''

    if re.match(r'^\d{2}/\d{2}/\d{4}$', texto):
        return texto

    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
        try:
            return datetime.strptime(texto, fmt).strftime('%d/%m/%Y')
        except ValueError:
            continue

    return texto


def _fecha_a_anio_mes(fecha_str: str) -> Optional[str]:
    """Extrae YYYY-MM de una fecha DD/MM/YYYY."""
    m = re.match(r'^\d{2}/(\d{2})/(\d{4})$', (fecha_str or '').strip())
    if m:
        return f"{m.group(2)}-{m.group(1)}"
    return None


@router.post("/cargar-masivo-stream")
async def cargar_cronograma_stream(
    archivo: UploadFile = File(...),
    usuario: str = Query(...)
):
    """
    Carga masiva del cronograma de entregas desde Excel con progreso SSE.
    Columnas requeridas: cedula, fecha_entrega, fecha_cronograma
    Hace upsert por (cedula, mes) — si el paciente ya tiene cronograma ese mes, lo actualiza.
    """
    if not archivo.filename.endswith(('.xlsx', '.xls', '.xlsm')):
        async def err():
            yield f"data: {json.dumps({'error': 'El archivo debe ser Excel (.xlsx, .xls, .xlsm)'}, ensure_ascii=False)}\n\n"
        return StreamingResponse(err(), media_type="text/event-stream")

    try:
        contenido = await archivo.read()
    except Exception as e:
        async def err():
            yield f"data: {json.dumps({'error': f'Error al leer archivo: {str(e)}'}, ensure_ascii=False)}\n\n"
        return StreamingResponse(err(), media_type="text/event-stream")

    if not contenido:
        async def err():
            yield f"data: {json.dumps({'error': 'El archivo está vacío'}, ensure_ascii=False)}\n\n"
        return StreamingResponse(err(), media_type="text/event-stream")

    async def generate():
        import asyncio
        tiempo_inicio = time.time()
        errores = []
        nuevos = 0
        actualizados = 0

        try:
            buffer = io.BytesIO(contenido)
            try:
                if archivo.filename.endswith(('.xlsx', '.xlsm')):
                    df = pd.read_excel(buffer, engine='openpyxl')
                else:
                    df = pd.read_excel(buffer, engine='xlrd')
            except Exception as e:
                yield f"data: {json.dumps({'error': f'Error al leer Excel: {str(e)}'}, ensure_ascii=False)}\n\n"
                return

            total = len(df)
            yield f"data: {json.dumps({'stage': 'reading', 'progress': 0, 'message': 'Leyendo archivo Excel...'}, ensure_ascii=False)}\n\n"

            # Validar columnas requeridas (case-insensitive)
            cols_map = {c.strip().lower(): c for c in df.columns}
            requeridas = ['cedula', 'fecha_entrega', 'fecha_cronograma']
            faltantes = [r for r in requeridas if r not in cols_map]
            if faltantes:
                yield f"data: {json.dumps({'error': f'Faltan columnas: {chr(44).join(faltantes)}'}, ensure_ascii=False)}\n\n"
                return

            df = df.rename(columns={cols_map[r]: r for r in requeridas})

            yield f"data: {json.dumps({'stage': 'processing', 'progress': 0, 'total': total, 'message': f'Procesando {total} registros...'}, ensure_ascii=False)}\n\n"

            operaciones = []

            for idx, fila in df.iterrows():
                try:
                    cedula_raw = str(fila.get('cedula', '')).strip() if pd.notna(fila.get('cedula')) else ''
                    fecha_entrega = _parsear_fecha(fila.get('fecha_entrega'))
                    fecha_cronograma = _parsear_fecha(fila.get('fecha_cronograma'))

                    cedula = re.sub(r'\D', '', cedula_raw)

                    if not cedula:
                        errores.append(f"Fila {idx + 2}: cédula vacía")
                        continue
                    if not fecha_cronograma:
                        errores.append(f"Fila {idx + 2}: fecha_cronograma vacía")
                        continue

                    anio_mes = _fecha_a_anio_mes(fecha_cronograma)
                    if not anio_mes:
                        errores.append(f"Fila {idx + 2}: formato de fecha_cronograma inválido ({fecha_cronograma})")
                        continue

                    doc = {
                        'cedula': cedula,
                        'fecha_entrega': fecha_entrega,
                        'fecha_cronograma': fecha_cronograma,
                        'anio_mes': anio_mes,
                        'usuario_carga': usuario,
                        'fecha_carga': time.strftime('%Y-%m-%d %H:%M:%S'),
                    }
                    operaciones.append(
                        UpdateOne({'cedula': cedula, 'anio_mes': anio_mes}, {'$set': doc}, upsert=True)
                    )
                    nuevos += 1  # conteo provisional; bulk_write no distingue nuevos/actualizados fácilmente

                    if len(operaciones) % 10 == 0 or idx == total - 1:
                        progreso = round((idx + 1) / total * 100, 1)
                        yield f"data: {json.dumps({'stage': 'processing', 'progress': progreso, 'processed': len(operaciones), 'total': total, 'message': f'Procesando... {progreso}%'}, ensure_ascii=False)}\n\n"
                        await asyncio.sleep(0)

                except Exception as e:
                    errores.append(f"Fila {idx + 2}: {str(e)}")
                    continue

            yield f"data: {json.dumps({'stage': 'saving', 'progress': 100, 'message': 'Guardando en base de datos...'}, ensure_ascii=False)}\n\n"

            if operaciones:
                resultado_bulk = coleccion.bulk_write(operaciones, ordered=False)
                nuevos = resultado_bulk.upserted_count
                actualizados = resultado_bulk.modified_count

            tiempo_segundos = round(time.time() - tiempo_inicio, 2)
            resultado = {
                'stage': 'complete',
                'progress': 100,
                'mensaje': f'Cronograma cargado en {tiempo_segundos}s',
                'tiempo_segundos': tiempo_segundos,
                'registros_nuevos': nuevos,
                'registros_actualizados': actualizados,
                'registros_con_errores': len(errores),
                'errores': errores[:50],
            }
            yield f"data: {json.dumps(resultado, ensure_ascii=False)}\n\n"

        except Exception as e:
            yield f"data: {json.dumps({'error': f'Error inesperado: {str(e)}'}, ensure_ascii=False)}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"}
    )


@router.get("/mes-actual")
async def obtener_cronograma_mes_actual(
    anio_mes: Optional[str] = Query(default=None, description="Mes a consultar en formato YYYY-MM. Si no se envía, usa el mes actual.")
):
    """
    Retorna el cronograma del mes indicado (o el actual si no se especifica).
    Responde { anio_mes, total, registros: [{cedula, fecha_entrega, fecha_cronograma}] }
    """
    import pytz
    if anio_mes and re.match(r'^\d{4}-\d{2}$', anio_mes):
        pass  # usar el valor recibido
    else:
        tz = pytz.timezone('America/Bogota')
        anio_mes = datetime.now(tz).strftime('%Y-%m')

    cursor = coleccion.find(
        {'anio_mes': anio_mes},
        {'_id': 0, 'cedula': 1, 'fecha_entrega': 1, 'fecha_cronograma': 1}
    )
    registros = list(cursor)

    return {
        'anio_mes': anio_mes,
        'total': len(registros),
        'registros': registros,
    }


@router.delete("/mes")
async def eliminar_cronograma_mes(
    fecha_cronograma: str = Query(..., description="Fecha del mes a eliminar, formato DD/MM/YYYY"),
    usuario: str = Query(...)
):
    """
    Elimina todos los registros del cronograma de un mes dado.
    Solo ADMIN debería llamar este endpoint.
    """
    anio_mes = _fecha_a_anio_mes(fecha_cronograma)
    if not anio_mes:
        raise HTTPException(status_code=400, detail="Formato inválido. Use DD/MM/YYYY")

    resultado = coleccion.delete_many({'anio_mes': anio_mes})
    return {
        'mensaje': f'Se eliminaron {resultado.deleted_count} registros del mes {anio_mes}',
        'anio_mes': anio_mes,
        'registros_eliminados': resultado.deleted_count,
        'usuario': usuario,
    }
