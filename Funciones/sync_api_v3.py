"""
SincronizaciÃ³n periÃ³dica V3 â€” consume directamente del API de Siscore.
Endpoint: https://integra-wms.appsiscore.com/app/ws/informe_v3.php
"""
import os
import time
import logging
from datetime import datetime, timedelta
import holidays

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



def _mapear_regional_a_cedi(regional: str) -> str:
    """Mapea cÃ³digo de regional a nombre de CEDI."""
    mapa = {
        'CO04': 'BARRANQUILLA',
        'CO05': 'CALI',
        'CO06': 'BUCARAMANGA',
        'CO07': 'FUNZA',
        'CO09': 'MEDELLIN',
    }
    return mapa.get(regional, regional)


def _filtrar_pacientes_urgentes(pacientes: list, mes_actual: int, anio_actual: int) -> list:
    """
    Filtra pacientes que son urgentes:
    - Fecha preferente del mes actual
    - Menos de 6 días hábiles desde mañana
    """
    _manana = (datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)).strftime('%Y-%m-%d')
    co_holidays = holidays.CountryHoliday('CO')

    def _parsear_fecha_texto(fecha_str: str):
        if not fecha_str:
            return None
        for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y', '%Y/%m/%d'):
            try:
                return datetime.strptime(fecha_str[:10], fmt)
            except ValueError:
                continue
        return None

    def _calcular_dias_habiles(fecha_desde_str: str, fecha_hasta_str: str) -> int:
        try:
            if '/' in fecha_desde_str:
                desde = datetime.strptime(fecha_desde_str.split(' ')[0], '%d/%m/%Y')
            elif '-' in fecha_desde_str:
                desde = datetime.strptime(fecha_desde_str.split(' ')[0], '%Y-%m-%d')
            else:
                return 0
            if '/' in fecha_hasta_str:
                hasta = datetime.strptime(fecha_hasta_str.split(' ')[0], '%d/%m/%Y')
            elif '-' in fecha_hasta_str:
                hasta = datetime.strptime(fecha_hasta_str.split(' ')[0], '%Y-%m-%d')
            else:
                return 0
            dias = 0
            actual = desde
            while actual <= hasta:
                # Contar lunes-sábado (excluir solo domingos y festivos)
                if actual.weekday() < 6 and actual not in co_holidays:
                    dias += 1
                actual += timedelta(days=1)
            return dias
        except Exception:
            return 0

    pacientes_filtrados = []
    for p in pacientes:
        f_pref_teorica = p.get('f_pref_teorica', '')
        if not f_pref_teorica:
            continue
        f_pref_dt = _parsear_fecha_texto(f_pref_teorica)
        if not f_pref_dt or f_pref_dt.month != mes_actual or f_pref_dt.year != anio_actual:
            continue
        dias_habiles = _calcular_dias_habiles(_manana, f_pref_teorica)
        if dias_habiles < 6:
            pacientes_filtrados.append(p)
    return pacientes_filtrados


def obtener_estadisticas_notificaciones(cruce_cache: dict, regional: str = None, es_admin: bool = False) -> dict:
    """
    FunciÃ³n PÃšBLICA compartida para obtener estadÃ­sticas de notificaciones MC.
    Usada por:
    - Sync automÃ¡tico V3 (sync_api_v3.py)
    - RecÃ¡lculo manual (pacientes_medical_care.py)

    Garantiza que WhatsApp y Correo siempre muestren los mismos nÃºmeros.

    Args:
        cruce_cache: Cache completo del cruce desde MongoDB
        regional: CÃ³digo de regional (ej: 'CO04') o None para todas
        es_admin: Si es True, ve todas las regionales; si es False, filtra por regional

    Returns:
        dict con total_retraso_operacion, total_sin_cruce, total_pacientes, desglose_por_cedi
    """
    # Reutilizar la lÃ³gica existente pero con parÃ¡metro es_admin explÃ­cito
    _regional_param = None if es_admin else regional
    return _obtener_estadisticas_por_regional(cruce_cache, _regional_param)


def _obtener_estadisticas_por_regional(cruce_cache: dict, regional: str = None) -> dict:
    """
    Obtiene estadÃ­sticas del cruce filtradas por regional.
    USA LOS MISMOS FILTROS QUE EL EXCEL:
    1. Sin cruce (en_v3 = False)
    2. Fecha preferente del mes actual
    3. < 6 dÃ­as hÃ¡biles (urgentes)

    Esta funciÃ³n es compartida por:
    - Sync automÃ¡tico V3 (sync_api_v3.py)
    - RecÃ¡lculo manual (pacientes_medical_care.py)

    Args:
        cruce_cache: Cache completo del cruce desde MongoDB
        regional: CÃ³digo de regional (ej: 'CO04') o None para todas

    Returns:
        dict con totales y desglose por regional (para admin)
    """
    """
    Obtiene estadÃ­sticas del cruce filtradas por regional.
    USA LOS MISMOS FILTROS QUE EL EXCEL:
    1. Sin cruce (en_v3 = False)
    2. Fecha preferente del mes actual
    3. < 6 dÃ­as hÃ¡biles (urgentes)

    Args:
        cruce_cache: Cache completo del cruce desde MongoDB
        regional: CÃ³digo de regional (ej: 'CO04') o None para todas

    Returns:
        dict con totales y desglose por regional (para admin)
    """
    cedi_filtro = _mapear_regional_a_cedi(regional) if regional else None

    # Fecha actual para filtros (igual que en el Excel)
    _hoy = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    _manana = (_hoy + timedelta(days=1)).strftime('%Y-%m-%d')
    _mes_actual = _hoy.month
    _anio_actual = _hoy.year
    co_holidays = holidays.CountryHoliday('CO')

    def _parsear_fecha_texto(fecha_str: str):
        """Parsea fechas en mÃºltiples formatos."""
        if not fecha_str:
            return None
        for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y', '%Y/%m/%d'):
            try:
                return datetime.strptime(fecha_str[:10], fmt)
            except ValueError:
                continue
        return None

    def _calcular_dias_habiles(fecha_desde_str: str, fecha_hasta_str: str) -> int:
        """Calcula dÃ­as hÃ¡biles entre dos fechas."""

        try:
            # Parsear fechas
            if '/' in fecha_desde_str:
                desde = datetime.strptime(fecha_desde_str.split(' ')[0], '%d/%m/%Y')
            elif '-' in fecha_desde_str:
                desde = datetime.strptime(fecha_desde_str.split(' ')[0], '%Y-%m-%d')
            else:
                return 0

            if '/' in fecha_hasta_str:
                hasta = datetime.strptime(fecha_hasta_str.split(' ')[0], '%d/%m/%Y')
            elif '-' in fecha_hasta_str:
                hasta = datetime.strptime(fecha_hasta_str.split(' ')[0], '%Y-%m-%d')
            else:
                return 0

            # Contar dÃ­as hÃ¡biles
            dias = 0
            actual = desde
            while actual <= hasta:
                # Contar lunes-sÃ¡bado (excluir solo domingos y festivos)
                if actual.weekday() < 6 and actual not in co_holidays:
                    dias += 1
                actual += timedelta(days=1)

            return dias
        except Exception:
            return 0

    # Obtener pacientes desde ocupacion_rutas
    ocupacion_rutas = cruce_cache.get('ocupacion_rutas', [])
    pacientes_por_ruta = {}

    # Diccionario para agrupar por CEDI
    stats_por_cedi = {}

    for ruta in ocupacion_rutas:
        ruta_cedi = ruta.get('cedi', '')
        if cedi_filtro and ruta_cedi != cedi_filtro:
            continue

        # Inicializar contador para este CEDI si no existe
        if ruta_cedi not in stats_por_cedi:
            stats_por_cedi[ruta_cedi] = {
                'total_pacientes': 0,
                'retraso_operacion': 0,
                'sin_cruce': 0,
            }

        for paciente in ruta.get('pacientes', []):
            cedula = paciente.get('cedula')
            pacientes_por_ruta[cedula] = paciente

            # Contar por CEDI
            stats_por_cedi[ruta_cedi]['total_pacientes'] += 1

            # --- FILTROS PARA NOTIFICACIONES (IGUALES AL EXCEL) ---

            f_pref_teorica = paciente.get('f_pref_teorica', '')
            tiene_fecha_mes_actual = False
            es_urgente = False

            # Verificar si tiene fecha del mes actual y es urgente
            if f_pref_teorica:
                f_pref_dt = _parsear_fecha_texto(f_pref_teorica)
                if f_pref_dt and f_pref_dt.month == _mes_actual and f_pref_dt.year == _anio_actual:
                    tiene_fecha_mes_actual = True
                    # Calcular dÃ­as hÃ¡biles
                    dias_habiles = _calcular_dias_habiles(_manana, f_pref_teorica)
                    if dias_habiles < 6:
                        es_urgente = True

            # Contar retraso operaciÃ³n (solo verifica estado, sin filtro de urgencia)
            estado_cruce = (paciente.get('estado_cruce') or '').lower()
            if estado_cruce in ('retraso operación', 'retraso operacion'):
                stats_por_cedi[ruta_cedi]['retraso_operacion'] += 1

            # Contar sin cruce (requiere los 3 filtros)
            if not paciente.get('en_v3', False) and tiene_fecha_mes_actual and es_urgente:
                stats_por_cedi[ruta_cedi]['sin_cruce'] += 1

    # Si se filtra por regional, retornar solo ese CEDI
    if cedi_filtro:
        return {
            'total_retraso_operacion': stats_por_cedi.get(cedi_filtro, {}).get('retraso_operacion', 0),
            'total_sin_cruce': stats_por_cedi.get(cedi_filtro, {}).get('sin_cruce', 0),
            'total_pacientes': stats_por_cedi.get(cedi_filtro, {}).get('total_pacientes', 0),
        }

    # Si no hay filtro (admin), retornar totales y desglose
    total_retraso_operacion = sum(s['retraso_operacion'] for s in stats_por_cedi.values())
    total_sin_cruce = sum(s['sin_cruce'] for s in stats_por_cedi.values())
    total_pacientes = sum(s['total_pacientes'] for s in stats_por_cedi.values())

    return {
        'total_retraso_operacion': total_retraso_operacion,
        'total_sin_cruce': total_sin_cruce,
        'total_pacientes': total_pacientes,
        'desglose_por_cedi': stats_por_cedi,  # Incluye desglose para admin
    }


def _notificar_sync_v3(resultado: dict):
    """
    Sistema de notificaciones WhatsApp personalizado para Medical Care.

    EnvÃ­a mensajes a usuarios segÃºn sus preferencias de notificaciÃ³n:
    - 'retraso_operacion': Pacientes con retraso operaciÃ³n (requieren montaje urgente)
    - 'sin_cruce': Pacientes sin cruce (no han sido tramitados por FMC)

    TambiÃ©n guarda el historial en MongoDB para consumo por PowerBI.
    """
    from bson.objectid import ObjectId as _ObjectId

    # Solo notificar si el sync fue exitoso y hay cruce disponible
    if not resultado.get('ok') or resultado.get('exitosos', 0) == 0:
        return

    cruce = resultado.get('cruce') or {}
    if not cruce.get('ok'):
        logger.warning("[sync_v3] No hay cruce disponible para notificaciones")
        return

    # Obtener cache completo del cruce
    cruce_cache = _CACHE_CRUCE.find_one({'tipo': 'cruce_completo'})
    if not cruce_cache:
        logger.warning("[sync_v3] No hay cache de cruce disponible")
        return

    # Obtener fecha/hora actual para historial
    ahora = datetime.now()
    fecha_hora = ahora.strftime('%Y-%m-%d %H:%M:%S')

    # Obtener usuarios con MEDICAL_CARE y notificaciones_mc activas
    from bd.bd_cliente import bd_cliente as _bd_cli
    col_usuarios = _bd_cli['integra']['baseusuarios']

    # Buscar usuarios que tienen MEDICAL_CARE en clientes y notificaciones_mc configuradas
    # Maneja ambos formatos: string (antiguo) o array (nuevo)
    usuarios_notif = list(col_usuarios.find({
        'clientes': 'MEDICAL_CARE',
        '$or': [
            {'notificaciones_mc': {'$exists': True, '$ne': [], '$nin': [[None], ''], '$type': 'array'}},
            {'notificaciones_mc': {'$exists': True, '$ne': '', '$ne': None, '$type': 'string'}},
        ],
        'celular': {'$exists': True, '$ne': None, '$ne': ''}
    }))

    if not usuarios_notif:
        logger.info("[sync_v3] No hay usuarios con notificaciones MC configuradas")
        return

    logger.info(f"[sync_v3] Enviando notificaciones a {len(usuarios_notif)} usuarios")

    # Recopilar datos para PowerBI (agrupados por regional)
    datos_powerbi = []

    for usuario in usuarios_notif:
        celular = usuario.get('celular', '').strip()
        regional = usuario.get('regional', '')
        notificaciones_raw = usuario.get('notificaciones_mc', [])
        nombre_usuario = usuario.get('nombre', '')
        usuario_id = str(usuario.get('_id', ''))

        # Normalizar notificaciones a lista (maneja ambos formatos: string o array)
        if isinstance(notificaciones_raw, str):
            notificaciones = [notificaciones_raw]
        else:
            notificaciones = notificaciones_raw or []

        # Normalizar celular: eliminar espacios, guiones, parÃ©ntesis; anteponer 57 si no tiene
        celular_limpio = ''.join(c for c in celular if c.isdigit())
        if not celular_limpio.startswith('57'):
            celular_limpio = '57' + celular_limpio

        # Verificar si es ADMIN para enviar todas las regionales o solo la suya
        es_admin = (usuario.get('perfil', '').upper() == 'ADMIN')
        regional_para_stats = None if es_admin else regional

        # Obtener estadÃ­sticas (todas si es ADMIN, solo su regional si no)
        stats = _obtener_estadisticas_por_regional(cruce_cache, regional_para_stats)
        logger.info(f"[sync_v3] Stats para {nombre_usuario} (regional={regional_para_stats}, es_admin={es_admin}): sin_cruce={stats['total_sin_cruce']}, retraso={stats['total_retraso_operacion']}")

        # Determinar el texto de regional para el mensaje
        if es_admin:
            texto_regional = "TODAS LAS REGIONALES"
        else:
            texto_regional = _mapear_regional_a_cedi(regional)

        # FunciÃ³n auxiliar para formatear desglose por CEDI
        def _formatear_desglose_cedi(stats_dict, tipo):
            """Genera string con desglose por CEDI para admin."""
            if 'desglose_por_cedi' not in stats_dict:
                return ""

            # Orden de CEDIS
            orden_cedis = ['FUNZA', 'CALI', 'MEDELLIN', 'BARRANQUILLA', 'BUCARAMANGA']
            partes = []
            for cedi in orden_cedis:
                if cedi in stats_dict['desglose_por_cedi']:
                    valor = stats_dict['desglose_por_cedi'][cedi][tipo]
                    partes.append(f"{cedi}: {valor}")

            return ' | '.join(partes) if partes else ""

        # Preparar datos para PowerBI
        datos_powerbi.append({
            'fecha_hora': fecha_hora,
            'regional': regional if not es_admin else 'TODAS',
            'nombre_cedi': texto_regional,
            'usuario_id': usuario_id,
            'usuario_nombre': nombre_usuario,
            'celular': celular_limpio,
            'notificaciones': notificaciones,
            'total_retraso_operacion': stats['total_retraso_operacion'],
            'total_sin_cruce': stats['total_sin_cruce'],
            'total_pacientes': stats['total_pacientes'],
        })

        # Enviar notificaciones segÃºn tipo
        for tipo_notif in notificaciones:
            try:
                if tipo_notif == 'retraso_operacion' and stats['total_retraso_operacion'] > 0:
                    if es_admin and 'desglose_por_cedi' in stats:
                        # Mensaje con desglose para admin - una sola línea sin saltos de línea
                        desglose = _formatear_desglose_cedi(stats, 'retraso_operacion')
                        mensaje_admin = (
                            f"🚨 RETRASO OPERACIÓN - {texto_regional} | {desglose} | "
                            f"Total: {stats['total_retraso_operacion']} pedidos | Excel enviado a tu correo"
                        )
                        res = enviar_template_sync(
                            to=celular_limpio,
                            template_name='confirmar_actualizacion',
                            language_code='es_CO',
                            body_params=[mensaje_admin],
                        )
                    else:
                        # Mensaje simple para operativo - usa template oficial con 2 parámetros
                        res = enviar_template_sync(
                            to=celular_limpio,
                            template_name='retraso_operacion_fmc_',
                            language_code='es_CO',
                            body_params=[texto_regional, str(stats['total_retraso_operacion'])],
                        )
                    if res:
                        logger.info(f"[sync_v3] WS enviado a {celular_limpio} ({nombre_usuario}) - retraso_operacion")
                    else:
                        logger.warning(f"[sync_v3] WS no enviado a {celular_limpio} (tokens/error)")

                elif tipo_notif == 'sin_cruce' and stats['total_sin_cruce'] > 0:
                    if es_admin and 'desglose_por_cedi' in stats:
                        # Mensaje con desglose para admin - una sola línea sin saltos de línea
                        desglose = _formatear_desglose_cedi(stats, 'sin_cruce')
                        mensaje_admin = (
                            f"⚠️ PACIENTES SIN MONTAR - {texto_regional} | {desglose} | "
                            f"Total: {stats['total_sin_cruce']} pacientes | Excel enviado a tu correo"
                        )
                        res = enviar_template_sync(
                            to=celular_limpio,
                            template_name='confirmar_actualizacion',
                            language_code='es_CO',
                            body_params=[mensaje_admin],
                        )
                    else:
                        # Mensaje simple para operativo - usa template oficial con 2 parámetros
                        res = enviar_template_sync(
                            to=celular_limpio,
                            template_name='pacientes_sin_montar_fmc',
                            language_code='es_CO',
                            body_params=[texto_regional, str(stats['total_sin_cruce'])],
                        )
                    if res:
                        logger.info(f"[sync_v3] WS enviado a {celular_limpio} ({nombre_usuario}) - sin_cruce")
                    else:
                        logger.warning(f"[sync_v3] WS no enviado a {celular_limpio} (tokens/error)")
            except Exception as e:
                logger.error(f"[sync_v3] Error enviando notificaciÃ³n a {celular_limpio}: {e}")

    # Guardar datos agregados por regional para PowerBI
    if datos_powerbi:
        try:
            # Agrupar por regional para tener un registro por CEDI
            por_regional = {}
            for dato in datos_powerbi:
                reg = dato['regional']
                if reg not in por_regional:
                    por_regional[reg] = {
                        'fecha_hora': fecha_hora,
                        'regional': reg,
                        'nombre_cedi': dato['nombre_cedi'],
                        'total_retraso_operacion': dato['total_retraso_operacion'],
                        'total_sin_cruce': dato['total_sin_cruce'],
                        'total_pacientes': dato['total_pacientes'],
                        'usuarios_notificados': [],
                    }
                # Agregar usuario si tiene notificaciones activas con datos
                if dato['total_retraso_operacion'] > 0 or dato['total_sin_cruce'] > 0:
                    por_regional[reg]['usuarios_notificados'].append({
                        'usuario_id': dato['usuario_id'],
                        'usuario_nombre': dato['usuario_nombre'],
                        'celular': dato['celular'],
                        'notificaciones': dato['notificaciones'],
                    })

            # Insertar registros por regional
            registros_insertar = list(por_regional.values())
            if registros_insertar:
                _HISTORIAL_NOTIF.insert_many(registros_insertar)
                logger.info(f"[sync_v3] Guardados {len(registros_insertar)} registros en notificaciones_mc_historial")
        except Exception as e:
            logger.error(f"[sync_v3] Error guardando historial de notificaciones: {e}")

_BD = bd_cliente['integra']
_COLECCION = _BD['v3']
_HISTORICO = _BD['v3_historico']
_CACHE_CRUCE = _BD['cache_cruce_mc']
_HISTORIAL_NOTIF = _BD['notificaciones_mc_historial']


async def ejecutar_sync_v3() -> dict:
    """
    Consume del API de Siscore, normaliza los datos y hace upsert en MongoDB.
    Retorna un dict con el resultado: exitosos, errores, total, timestamp.
    Al terminar (Ã©xito o error) envÃ­a notificaciÃ³n WhatsApp si WHATSAPP_NOTIFY_NUMBER estÃ¡ configurado.
    """
    resultado = await _ejecutar_sync_v3_interno()
    _notificar_sync_v3(resultado)
    return resultado


async def _ejecutar_sync_v3_interno() -> dict:
    """
    Ejecuta el sync de V3 consumiendo directamente del API de Siscore.
    Calcula el rango de fechas automÃ¡ticamente (1er dÃ­a de hace 2 meses â†’ hoy).
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
        logger.info(f"[sync_v3] API retornÃ³ {total} registros")

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

        # Paso 5: Reemplazar colecciÃ³n en MongoDB
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
    logger.info(f"[sync_v3] {exitosos}/{total} registros â€” {filtrados} filtrados â€” {segundos}s")

    resultado = {
        'ok': exitosos > 0,
        'exitosos': exitosos,
        'filtrados': filtrados,
        'errores': errores[:20],  # mÃ¡x 20 errores en respuesta
        'total': total,
        'timestamp': timestamp,
        'segundos': segundos,
    }

    # Tras un sync exitoso, recalcular el cruce pacientes <-> V3 automÃ¡ticamente
    if exitosos > 0:
        logger.info("[sync_v3] Ejecutando cruce automÃ¡tico post-sync...")
        resultado['cruce'] = ejecutar_cruce_automatico('sync_automatico')

    return resultado


def archivar_mes_v3() -> dict:
    """
    Guarda una copia de seguridad de la colecciÃ³n v3 y el Ãºltimo cruce en v3_historico.
    Se ejecuta automÃ¡ticamente el Ãºltimo dÃ­a de cada mes a las 00:00 (hora BogotÃ¡).
    TambiÃ©n se puede disparar manualmente desde POST /sync-v3/archivar.
    Usa upsert por (anio, mes) â€” si se ejecuta varias veces en el mismo mes, sobreescribe.
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
        logger.info(f"[archivo_mensual] OK â€” {len(registros)} registros archivados ({mes}/{anio})")
        return {'ok': True, 'anio': anio, 'mes': mes, 'total': len(registros), 'fecha_corte': fecha_corte}

    except Exception as e:
        logger.error(f"[archivo_mensual] Error: {e}")
        return {'ok': False, 'error': str(e), 'anio': anio, 'mes': mes}
