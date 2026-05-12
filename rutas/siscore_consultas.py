from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import httpx
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
from pymongo import MongoClient

load_dotenv()

router = APIRouter(prefix="/siscore", tags=["Siscore"])
logger = logging.getLogger(__name__)

# Conexión MongoDB
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
client = MongoClient(MONGO_URI)
db = client["integra"]
coleccion_solicitudes = db["solicitud_veh_medical"]
coleccion_tramites = db["tramite_fmc"]
coleccion_tarifas = db["fletes_rutas_fmc"]
coleccion_divipolas = db["divipolas"]
coleccion_pedidos_medical = db["pedidos_medical"]

# Configuración WS Siscore V3 (misma que en pedidos_v3)
SISCORE_V3_ENDPOINT = "https://integra-wms.appsiscore.com/app/ws/informe_v3.php"
SISCORE_V3_TOKEN = "n0ML0cFGhJwtq4lsAeUcMzrqkn94gX4TDaPuFbbXpoA"


class ConsultaPlanillasRequest(BaseModel):
    planillas: List[str]
    fecha_inicio: str
    fecha_fin: str
    perfil: Optional[str] = None
    centro_distribucion: Optional[str] = None


class GuardarSolicitudRequest(BaseModel):
    usuario: str
    perfil: str
    centro_distribucion: str
    planilla: str
    piezas: int
    peso_real: float
    ruta: str
    codigos_pedido: str
    cantidad_pedidos: int
    cliente_origen: str
    municipio_destino: str
    departamento_destino: str
    regional: Optional[str] = None
    tarifa_calculada: float
    tipo_vehiculo: str
    total_solicitado: float
    tarifa_base: Optional[float] = None
    requiere_descargue: str = "NO"
    punto_adicional: bool = False
    desvio: bool = False
    tipo_veh_sicetac: Optional[str] = None


class EnviarTramiteRequest(BaseModel):
    solicitud_id: str
    usuario: str


class ConsultarTarifaRequest(BaseModel):
    centro_costo: str
    ruta: str
    peso_real: float


class GuardarBusquedaRequest(BaseModel):
    usuario: str
    perfil: str
    centro_distribucion: str
    planillas_buscadas: List[str]
    resultados_consolidados: List[dict]
    fecha_inicio: str
    fecha_fin: str


def _obtener_festivos_colombia(anio: int) -> List[str]:
    """
    Retorna lista de festivos de Colombia para un año dado (formato YYYY-MM-DD).
    Incluye festivos fijos y móviles (basados en Pascua).
    """
    from datetime import date

    festivos = []

    # Festivos fijos
    festivos_fijos = [
        (1, 1),   # 1 de enero
        (1, 6),   # 6 de enero
        (5, 1),   # 1 de mayo
        (7, 20),  # 20 de julio
        (8, 7),   # 7 de agosto
        (12, 8),  # 8 de diciembre
        (12, 25), # 25 de diciembre
    ]

    def _format_fecha(fecha: date) -> str:
        return fecha.strftime('%Y-%m-%d')

    def _mover_al_lunes(fecha: date) -> date:
        dia_sem = fecha.weekday()
        if dia_sem != 0:  # Si no es lunes
            dias_hasta_lunes = (7 - dia_sem) % 7
            if dias_hasta_lunes == 0:
                dias_hasta_lunes = 7
            return fecha + timedelta(days=dias_hasta_lunes)
        return fecha

    for mes, dia in festivos_fijos:
        festivos.append(_format_fecha(date(anio, mes, dia)))

    # Calcular Pascua
    a = anio % 19
    b = anio // 100
    c = anio % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    mes_pascua = (h + l - 7 * m + 114) // 31
    dia_pascua = ((h + l - 7 * m + 114) % 31) + 1
    pascua = date(anio, mes_pascua, dia_pascua)

    # Festivos móviles
    jueves_santo = pascua - timedelta(days=3)
    viernes_santo = pascua - timedelta(days=2)
    ascension = _mover_al_lunes(pascua + timedelta(days=39))
    corpus_christi = _mover_al_lunes(pascua + timedelta(days=60))
    sagrada_eucaristia = _mover_al_lunes(pascua + timedelta(days=68))

    festivos.extend([
        _format_fecha(jueves_santo),
        _format_fecha(viernes_santo),
        _format_fecha(ascension),
        _format_fecha(corpus_christi),
        _format_fecha(sagrada_eucaristia)
    ])

    return festivos


def _calcular_rango_3_dias_habiles() -> tuple[str, str]:
    """
    Calcula el rango de fechas: 40 días hábiles anteriores hasta hoy.
    No cuenta fines de semana ni festivos de Colombia.

    Returns:
        Tupla (fecha_inicial, fecha_final) en formato YYYY-MM-DD
    """
    hoy = datetime.now()
    festivos = _obtener_festivos_colombia(hoy.year)

    def es_festivo_o_fin_de_semana(fecha: datetime) -> bool:
        """Verifica si una fecha es festivo o fin de semana"""
        if fecha.weekday() >= 5:  # Sábado (5) o Domingo (6)
            return True
        fecha_str = fecha.strftime('%Y-%m-%d')
        return fecha_str in festivos

    # Restar 40 días hábiles
    dias_habiles_restar = 0
    fecha_actual = hoy

    while dias_habiles_restar < 40:
        fecha_actual -= timedelta(days=1)
        if not es_festivo_o_fin_de_semana(fecha_actual):
            dias_habiles_restar += 1

    fecha_final = hoy.strftime('%Y-%m-%d')
    fecha_inicial = fecha_actual.strftime('%Y-%m-%d')

    logger.info(f"Rango de fechas calculado (40 días hábiles): {fecha_inicial} a {fecha_final}")
    return fecha_inicial, fecha_final


def _get_proxy_url() -> Optional[str]:
    """Obtiene la configuración de proxy desde variables de entorno"""
    proxy_url = os.getenv('VULCANO_PROXY_URL')
    if proxy_url:
        logger.info(f"Proxy configurado: {proxy_url.split('@')[-1]}")
    return proxy_url


def _determinar_tipo_vehiculo(peso_real: float) -> str:
    """
    Determina el tipo de vehículo según el peso real.

    Rangos:
    - Hasta 1.000 kg → CARRY
    - 1.001 a 2.300 kg → NHR
    - 2.301 a 4.500 kg → TURBO
    - 4.501 a 6.100 kg → NIES
    - 6.101 a 9.000 kg → SENCILLO
    - 9.001 a 17.000 kg → PATINETA
    - Más de 17.000 kg → TRACTOMULA
    """
    if peso_real <= 1000:
        return "CARRY"
    elif peso_real <= 2300:
        return "NHR"
    elif peso_real <= 4500:
        return "TURBO"
    elif peso_real <= 6100:
        return "NIES"
    elif peso_real <= 9000:
        return "SENCILLO"
    elif peso_real <= 17000:
        return "PATINETA"
    else:
        return "TRACTOMULA"


def _obtener_tarifa_ruta(centro_costo: str, ruta: str, tipo_vehiculo: str) -> Optional[float]:
    """
    Obtiene la tarifa de fletes_rutas_fmc según ruta y tipo_vehículo.
    Cada ruta es única, no se busca por centro_costo.

    Args:
        centro_costo: Centro de costo (ej: CO05) - NO USADO, solo por compatibilidad
        ruta: Código de ruta (ej: BOG-MED)
        tipo_vehiculo: Tipo de vehículo (CARRY, NHR, TURBO, NIES, SENCILLO, PATINETA, TRACTOMULA)

    Returns:
        Valor de la tarifa o None si no se encuentra
    """
    try:
        tarifa = coleccion_tarifas.find_one({
            "ruta": ruta.upper()
        })

        if not tarifa:
            logger.warning(f"No se encontró tarifa para ruta={ruta}")
            return None

        # Mapear tipo_vehiculo al campo en BD
        tipo_map = {
            "CARRY": "carry",
            "NHR": "nhr",
            "TURBO": "turbo",
            "NIES": "nies",
            "SENCILLO": "sencillo",
            "PATINETA": "patineta",
            "TRACTOMULA": "tractomula"
        }

        campo = tipo_map.get(tipo_vehiculo.upper())
        if not campo:
            logger.error(f"Tipo de vehículo no válido: {tipo_vehiculo}")
            return None

        valor = tarifa.get(campo, 0)
        logger.info(f"Tarifa encontrada: ruta={ruta}, tipo={tipo_vehiculo}, valor={valor}")
        return float(valor)

    except Exception as e:
        logger.error(f"Error al obtener tarifa: {str(e)}")
        return None


async def _consultar_api_siscore_planillas(
    fecha_inicial: str,
    fecha_final: str,
    centro_distribucion: str = "TODOS"
) -> dict:
    """
    Consulta el API de Siscore V3 con rango de fechas (igual que pedidos_v3).

    Args:
        fecha_inicial: Fecha inicial en formato YYYY-MM-DD
        fecha_final: Fecha final en formato YYYY-MM-DD
        centro_distribucion: Centro de distribución ("TODOS" o específico)

    Returns:
        Diccionario con la respuesta del API
    """
    payload = {
        "token": SISCORE_V3_TOKEN,
        "fecha_inicial": fecha_inicial,
        "fecha_final": fecha_final,
        "centro_distribucion": centro_distribucion if centro_distribucion else "TODOS",
        "incluir_pedidos_manuales": "SI",
        "pedido_especifico": ""  # Vacío para traer todos los pedidos del rango
    }

    timeout = httpx.Timeout(300.0, connect=60.0)  # 5 minutos total
    proxy_url = _get_proxy_url()

    logger.info(f"[API Siscore] Consultando rango: {fecha_inicial} a {fecha_final}")
    logger.info(f"[API Siscore] Centro distribución: {centro_distribucion if centro_distribucion else 'TODOS'}")
    logger.info(f"[API Siscore] Proxy: {'HABILITADO' if proxy_url else 'NO CONFIGURADO'}")

    try:
        async with httpx.AsyncClient(
            timeout=timeout,
            proxy=proxy_url,
            trust_env=False,
        ) as client:
            logger.info(f"[API Siscore] Enviando payload a: {SISCORE_V3_ENDPOINT}")
            logger.info(f"[API Siscore] Payload: {payload}")

            response = await client.post(
                SISCORE_V3_ENDPOINT,
                json=payload,
                headers={"Content-Type": "application/json"}
            )

            logger.info(f"[API Siscore] Status code: {response.status_code}")
            logger.info(f"[API Siscore] Headers: {dict(response.headers)}")

            response.raise_for_status()

            result = response.json()
            logger.info(f"[API Siscore] Respuesta recibida, tipo: {type(result)}")

            # Log específico para ver la estructura de la respuesta
            if isinstance(result, dict):
                logger.info(f"[API Siscore] Claves en respuesta: {list(result.keys())}")
                if 'registros' in result:
                    logger.info(f"[API Siscore] Cantidad de registros: {len(result.get('registros', []))}")
                else:
                    logger.warning(f"[API Siscore] No hay clave 'registros' en la respuesta")
            elif isinstance(result, list):
                logger.info(f"[API Siscore] La respuesta es una lista con {len(result)} elementos")

            return result

    except httpx.HTTPStatusError as e:
        error_msg = f"Error HTTP Siscore: {e.response.status_code}"
        try:
            error_detail = e.response.text[:500]
            error_msg += f" - {error_detail}"
        except:
            pass
        raise RuntimeError(error_msg)
    except httpx.ConnectTimeout:
        raise RuntimeError("Timeout conectando a Siscore. El servidor no respondió.")
    except httpx.ReadTimeout:
        raise RuntimeError("Timeout leyendo respuesta de Siscore. El endpoint tardó demasiado.")
    except Exception as e:
        raise RuntimeError(f"Error consultando Siscore: {type(e).__name__}: {str(e)}")


@router.post("/consultar-planillas")
async def consultar_planillas(request: ConsultaPlanillasRequest):
    """
    Consulta planillas en la API de Siscore V3 filtrando por rango de fechas.
    Calcula automáticamente 3 días hábiles hacia atrás.
    Devuelve todos los registros del rango.

    Args:
        request: Objeto con planillas, fecha_inicio y fecha_fin (puede estar vacío)

    Returns:
        Diccionario con todos los registros del rango de fechas
    """
    try:
        logger.info(f"=== INICIO CONSULTA DE PLANILLAS ===")
        logger.info(f"Planillas solicitadas: {request.planillas}")
        logger.info(f"Perfil: {request.perfil}")
        logger.info(f"Centro distribución solicitado: {request.centro_distribucion}")
        logger.info(f"Rango recibido: {request.fecha_inicio} a {request.fecha_fin}")

        # Calcular rango de 30 días hábiles si no se proporciona
        if not request.fecha_inicio or not request.fecha_fin:
            fecha_inicio, fecha_fin = _calcular_rango_3_dias_habiles()
            logger.info(f"Rango calculado automáticamente: {fecha_inicio} a {fecha_fin}")
        else:
            fecha_inicio = request.fecha_inicio
            fecha_fin = request.fecha_fin

        # Determinar centro de distribución según perfil
        # Si es operativo y tiene centro_distribucion, usarlo
        # Si es perfil global o no tiene centro_distribucion, usar TODOS
        if request.centro_distribucion and request.perfil not in ['ADMIN', 'COORDINADOR', 'CONTROL', 'ANALISTA']:
            # Convertir CO07 o FUNZA al formato especial para Siscore
            if request.centro_distribucion in ['CO07', 'FUNZA']:
                centro_dist = "FUNZA - SAN DIEGO 7G"
            else:
                centro_dist = request.centro_distribucion
            logger.info(f"Filtro aplicado: Operativo con centro_distribucion={centro_dist}")
        else:
            centro_dist = "TODOS"
            logger.info(f"Filtro aplicado: Perfil global o sin centro_distribucion, usando TODOS")

        # Consultar API de Siscore (con filtro de centro de distribución si aplica)
        respuesta_api = await _consultar_api_siscore_planillas(
            fecha_inicial=fecha_inicio,
            fecha_final=fecha_fin,
            centro_distribucion=centro_dist
        )

        # LOG: Mostrar respuesta completa de Siscore
        logger.info(f"=== RESPUESTA COMPLETA DE SISCORE ===")
        logger.info(f"Tipo: {type(respuesta_api)}")
        logger.info(f"Claves: {respuesta_api.keys() if isinstance(respuesta_api, dict) else 'No es dict'}")

        # La respuesta de Siscore tiene la estructura: {ok, total, filtros, data}
        # Los registros están en 'data'
        todos_registros = respuesta_api.get('data', [])

        logger.info(f"Total registros recibidos de Siscore: {len(todos_registros)}")

        # PRIMERO: Filtrar por las planillas que el usuario solicitó
        planillas_set = set(p.strip() for p in request.planillas)
        logger.info(f"Filtrando por planillas solicitadas: {planillas_set}")

        registros_filtrados = [
            reg for reg in todos_registros
            if (reg.get('Planilla') or '').strip() in planillas_set
        ]

        logger.info(f"Registros después de filtrar por planilla: {len(registros_filtrados)}")

        # SEGUNDO: Enriquecer SOLO los registros filtrados: buscar rutas faltantes por Divipola
        for reg in registros_filtrados:
            ruta_siscore = (reg.get('Ruta') or '').strip()
            divipola = (reg.get('Divipola') or '').strip()

            # Solo buscar si la ruta está vacía
            if not ruta_siscore or ruta_siscore == '' or ruta_siscore == '-':
                if divipola:
                    logger.info(f"Planilla {reg.get('Planilla')}: Ruta vacía, buscando por Divipola '{divipola}'")

                    # Buscar en colección de divipolas
                    divipola_doc = coleccion_divipolas.find_one({"divipola": divipola})

                    if divipola_doc:
                        ruta_encontrada = divipola_doc.get('ruta', '')
                        reg['Ruta'] = ruta_encontrada
                        logger.info(f"  ✅ Ruta encontrada en divipolas: {ruta_encontrada}")
                    else:
                        logger.warning(f"  ❌ No se encontró ruta para Divipola '{divipola}'")
                        reg['Ruta'] = '-'
                else:
                    logger.warning(f"  ⚠️ Ruta vacía pero sin Divipola para buscar")
                    reg['Ruta'] = '-'

        # LOG: Mostrar registros filtrados para depurar
        if registros_filtrados:
            logger.info(f"=== REGISTROS FILTRADOS Y ENRIQUECIDOS ===")
            for i, reg in enumerate(registros_filtrados[:5]):
                logger.info(f"Registro {i}: Planilla={reg.get('Planilla', 'N/A')}, Ruta={reg.get('Ruta', 'N/A')}, Divipola={reg.get('Divipola', 'N/A')}")

        # Devolver solo los registros filtrados y enriquecidos
        return {
            "registros": registros_filtrados,
            "total_registros": len(registros_filtrados),
            "fecha_inicio": fecha_inicio,
            "fecha_fin": fecha_fin,
            "planillas_buscadas": request.planillas
        }

    except Exception as e:
        logger.error(f"Error en consulta de planillas: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error al consultar planillas: {str(e)}"
        )


@router.get("/test-connection")
async def test_connection():
    """
    Endpoint para probar la conexión con Siscore
    """
    try:
        fecha_inicial, fecha_final = _calcular_rango_3_dias_habiles()

        respuesta = await _consultar_api_siscore_planillas(
            fecha_inicial=fecha_inicial,
            fecha_final=fecha_final,
            planillas=["TEST"]  # Planilla de prueba
        )

        return {
            "status": "connected",
            "message": "Conexión exitosa con Siscore",
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"No se pudo conectar con Siscore: {str(e)}",
            "timestamp": datetime.now().isoformat()
        }


@router.get("/obtener-solicitudes-pendientes")
async def obtener_solicitudes_pendientes(usuario: str, perfil: str = "", centro_distribucion: str = ""):
    """
    Obtiene las solicitudes pendientes del usuario en solicitud_veh_medical.
    Para operativos, filtra por centro_distribucion (regional).
    Para perfiles globales (ADMIN, ANALISTA, COORDINADOR, CONTROL), muestra todas.
    """
    try:
        logger.info(f"Obteniendo solicitudes pendientes: usuario={usuario}, perfil={perfil}, centro_distribucion={centro_distribucion}")

        # Perfiles globales que ven todas las regionales
        perfiles_globales = ['ADMIN', 'ANALISTA', 'COORDINADOR', 'CONTROL']

        # Construir filtro
        filtro = {"estado": "pendiente"}

        # Si es operativo y tiene centro_distribucion, filtrar por su regional
        if perfil and perfil not in perfiles_globales and centro_distribucion:
            # Filtrar por centro_distribucion
            filtro["centro_distribucion"] = centro_distribucion
            logger.info(f"Filtro aplicado: Operativo con centro_distribucion={centro_distribucion}")
        else:
            logger.info(f"Filtro aplicado: Perfil global o sin centro_distribucion, mostrando todas las solicitudes")

        solicitudes = list(coleccion_solicitudes.find(filtro).sort("fecha_creacion", -1))

        # Convertir ObjectId a string
        for sol in solicitudes:
            sol["_id"] = str(sol["_id"])

        logger.info(f"Solicitudes encontradas: {len(solicitudes)}")

        return {
            "solicitudes": solicitudes,
            "total": len(solicitudes)
        }

    except Exception as e:
        logger.error(f"Error al obtener solicitudes pendientes: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error al obtener solicitudes: {str(e)}"
        )


@router.post("/guardar-solicitud")
async def guardar_solicitud(request: GuardarSolicitudRequest):
    """
    Guarda una solicitud de vehículo en la colección solicitud_veh_medical.
    """
    try:
        nueva_solicitud = {
            "usuario": request.usuario,
            "perfil": request.perfil,
            "centro_distribucion": request.centro_distribucion,
            "planilla": request.planilla,
            "piezas": request.piezas,
            "peso_real": request.peso_real,
            "ruta": request.ruta,
            "codigos_pedido": request.codigos_pedido,
            "cantidad_pedidos": request.cantidad_pedidos,
            "cliente_origen": request.cliente_origen,
            "municipio_destino": request.municipio_destino,
            "departamento_destino": request.departamento_destino,
            "regional": request.regional,
            "tarifa_calculada": request.tarifa_calculada,
            "tipo_vehiculo": request.tipo_vehiculo,
            "total_solicitado": request.total_solicitado,
            "tarifa_base": request.tarifa_base,
            "requiere_descargue": request.requiere_descargue,
            "punto_adicional": request.punto_adicional,
            "desvio": request.desvio,
            "tipo_veh_sicetac": request.tipo_veh_sicetac,
            "estado": "pendiente",
            "fecha_creacion": datetime.now(),
            "fecha_actualizacion": datetime.now()
        }

        result = coleccion_solicitudes.insert_one(nueva_solicitud)
        nueva_solicitud["_id"] = str(result.inserted_id)

        logger.info(f"Solicitud guardada: {nueva_solicitud['_id']} para usuario {request.usuario}")

        return {
            "mensaje": "Solicitud guardada exitosamente",
            "solicitud": nueva_solicitud
        }

    except Exception as e:
        logger.error(f"Error al guardar solicitud: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error al guardar solicitud: {str(e)}"
        )


@router.post("/enviar-tramite")
async def enviar_tramite(request: EnviarTramiteRequest):
    """
    Envía una solicitud a trámite en la colección tramite_fmc.
    """
    try:
        from bson import ObjectId

        if not ObjectId.is_valid(request.solicitud_id):
            raise HTTPException(status_code=400, detail="ID de solicitud inválido")

        # Obtener la solicitud original
        solicitud_orig = coleccion_solicitudes.find_one({"_id": ObjectId(request.solicitud_id)})

        if not solicitud_orig:
            raise HTTPException(status_code=404, detail="Solicitud no encontrada")

        # Crear el trámite
        tramite = {
            **solicitud_orig,
            "_id_orig": solicitud_orig["_id"],
            "estado": "en_revision",
            "usuario_envio": request.usuario,
            "fecha_envio": datetime.now(),
            "fecha_creacion": datetime.now()
        }

        # Eliminar el _id original para que MongoDB genere uno nuevo
        del tramite["_id"]

        result = coleccion_tramites.insert_one(tramite)
        tramite["_id"] = str(result.inserted_id)

        # Actualizar estado de la solicitud original
        coleccion_solicitudes.update_one(
            {"_id": ObjectId(request.solicitud_id)},
            {"$set": {"estado": "en_tramite", "fecha_actualizacion": datetime.now()}}
        )

        logger.info(f"Trámite enviado: {tramite['_id']} desde solicitud {request.solicitud_id}")

        return {
            "mensaje": "Solicitud enviada a trámite exitosamente",
            "tramite": tramite
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al enviar trámite: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error al enviar trámite: {str(e)}"
        )


@router.post("/consultar-tarifa")
async def consultar_tarifa(request: ConsultarTarifaRequest):
    """
    Consulta la tarifa de fletes_rutas_fmc según centro_costo, ruta y peso_real.
    Determina el tipo de vehículo según el peso y devuelve la tarifa correspondiente.

    Args:
        request: Objeto con centro_costo, ruta y peso_real

    Returns:
        Diccionario con tipo_vehiculo y tarifa_calculada
    """
    try:
        # Determinar tipo de vehículo según peso
        tipo_vehiculo = _determinar_tipo_vehiculo(request.peso_real)

        # Obtener tarifa de fletes_rutas_fmc
        tarifa_calculada = _obtener_tarifa_ruta(request.centro_costo, request.ruta, tipo_vehiculo)

        logger.info(f"Tarifa consultada: ruta={request.ruta}, peso={request.peso_real}kg, tipo={tipo_vehiculo}, tarifa={tarifa_calculada}")

        return {
            "tipo_vehiculo": tipo_vehiculo,
            "tarifa_calculada": tarifa_calculada if tarifa_calculada else 0
        }

    except Exception as e:
        logger.error(f"Error al consultar tarifa: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error al consultar tarifa: {str(e)}"
        )


@router.post("/guardar-busqueda")
async def guardar_busqueda(request: GuardarBusquedaRequest):
    """
    Guarda cada planilla como un documento independiente en pedidos_medical (como libros).
    """
    try:
        fecha_creacion = datetime.now()

        # Guardar cada resultado como un documento independiente
        for resultado in request.resultados_consolidados:
            planilla_doc = {
                "usuario_registro": request.usuario,  # Quien guardó (auditoría)
                "perfil": request.perfil,
                "centro_distribucion": request.centro_distribucion,
                "planilla": resultado.get("planilla"),
                "encontrada": resultado.get("encontrada", False),
                "piezas": resultado.get("piezas", 0),
                "peso_real": resultado.get("peso_real", 0),
                "ruta": resultado.get("ruta", "-"),
                "codigo_pedido": resultado.get("codigo_pedido", "-"),
                "cantidad_pedidos": resultado.get("cantidad_pedidos", 0),
                "cliente_origen": resultado.get("cliente_origen", "-"),
                "municipio_destino": resultado.get("municipio_destino", "-"),
                "departamento_destino": resultado.get("departamento_destino", "-"),
                "regional": resultado.get("regional"),
                "centro_costo": resultado.get("centro_costo"),
                "tarifa_calculada": resultado.get("tarifa_calculada", 0),
                "tipo_vehiculo": resultado.get("tipo_vehiculo"),
                "total_solicitado": resultado.get("total_solicitado", 0),
                "tarifa_base": resultado.get("tarifa_base"),
                "requiere_descargue": resultado.get("requiere_descargue", "NO"),
                "punto_adicional": resultado.get("punto_adicional", False),
                "desvio": resultado.get("desvio", False),
                "tipo_veh_sicetac": resultado.get("tipo_veh_sicetac"),
                "fecha_creacion": fecha_creacion
            }

            # Verificar si ya existe un documento con esta planilla
            existente = coleccion_pedidos_medical.find_one({"planilla": resultado.get("planilla")})

            if existente:
                # Actualizar el existente
                coleccion_pedidos_medical.update_one(
                    {"_id": existente["_id"]},
                    {"$set": planilla_doc}
                )
                logger.info(f"Planilla {resultado.get('planilla')}: actualizada")
            else:
                # Insertar nuevo
                coleccion_pedidos_medical.insert_one(planilla_doc)
                logger.info(f"Planilla {resultado.get('planilla')}: guardada")

        logger.info(f"Total guardado: {len(request.resultados_consolidados)} planillas en pedidos_medical")

        return {
            "mensaje": f"Se guardaron/actualizaron {len(request.resultados_consolidados)} planillas",
            "total": len(request.resultados_consolidados)
        }

    except Exception as e:
        logger.error(f"Error al guardar búsqueda: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error al guardar búsqueda: {str(e)}"
        )


@router.get("/obtener-resultados-recientes")
async def obtener_resultados_recientes(limite: int = 100):
    """
    Obtiene todos los planillas de pedidos_medical (cada documento es una planilla).
    Sin filtro por usuario, cualquiera puede consultar.
    """
    try:
        logger.info(f"[OBTENER RESULTADOS] Limite: {limite}")

        # Contar total de documentos
        total_docs = coleccion_pedidos_medical.count_documents({})
        logger.info(f"[OBTENER RESULTADOS] Total documentos en coleccion: {total_docs}")

        # Traer todas las planillas (documentos independientes), ordenadas por fecha reciente
        planillas = list(coleccion_pedidos_medical.find(
            {}
        ).sort("fecha_creacion", -1).limit(limite))

        # Convertir ObjectId a string
        for planilla in planillas:
            planilla["_id"] = str(planilla["_id"])

        logger.info(f"[OBTENER RESULTADOS] Planillas encontradas: {len(planillas)}")

        return {
            "planillas": planillas,
            "total": len(planillas)
        }

    except Exception as e:
        logger.error(f"Error al obtener resultados recientes: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error al obtener resultados: {str(e)}"
        )

        busquedas = list(coleccion_pedidos_medical.find(
            {"usuario": usuario}
        ).sort("fecha_creacion", -1).limit(limite))

        logger.info(f"[OBTENER BUSQUEDAS] Busquedas encontradas para usuario {usuario}: {len(busquedas)}")

        # Convertir ObjectId a string
        for bus in busquedas:
            bus["_id"] = str(bus["_id"])

        return {
            "busquedas": busquedas,
            "total": len(busquedas)
        }

    except Exception as e:
        logger.error(f"Error al obtener búsquedas recientes: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error al obtener búsquedas: {str(e)}"
        )
