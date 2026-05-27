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
coleccion_causales = db["causales"]

# Configuración WS Siscore V3 (misma que en pedidos_v3)
SISCORE_V3_ENDPOINT = "https://integra-wms.appsiscore.com/app/ws/informe_v3.php"
SISCORE_V3_TOKEN = "n0ML0cFGhJwtq4lsAeUcMzrqkn94gX4TDaPuFbbXpoA"


class ConsultaPlanillasRequest(BaseModel):
    planillas: List[str]
    fecha_inicio: str
    fecha_fin: str
    perfil: Optional[str] = None
    centro_distribucion: Optional[str] = None


class VerificarFusionadasRequest(BaseModel):
    """Modelo para verificar planillas fusionadas (no requiere fechas)"""
    planillas: List[str]
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
    aforo: Optional[float] = None
    placa: Optional[str] = None
    tipo_veh_sicetac: Optional[str] = None
    solicitud_id: Optional[str] = None


class ActualizarSolicitudRequest(BaseModel):
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
    aforo: Optional[float] = None
    placa: Optional[str] = None
    tipo_veh_sicetac: Optional[str] = None
    solicitud_id: str


class EnviarTramiteRequest(BaseModel):
    solicitud_id: str
    usuario: str


class ActualizarPlanillaPedidosRequest(BaseModel):
    """Modelo para actualizar una planilla en pedidos_medical"""
    planilla: str
    tarifa_base: Optional[float] = None
    requiere_descargue: Optional[float] = 0  # Valor numérico del descargue
    punto_adicional: Optional[float] = 0     # Valor numérico del punto adicional
    desvio: Optional[float] = 0              # Valor numérico del desvío
    aforo: Optional[float] = None            # Valor numérico del aforo
    placa: Optional[str] = None
    tipo_veh_sicetac: Optional[str] = None
    total_solicitado: float
    causal: Optional[str] = None
    estado: Optional[str] = None  # 'PREAPROBADO', 'REQUIERE_APROBACION_COORDINADOR', 'REQUIERE_APROBACION_CONTROL' o 'APROBADO'
    aprobado_por: Optional[str] = None
    fecha_aprobacion: Optional[str] = None
    usuario_modificacion: str  # Usuario que está editando (trazabilidad)


class ActualizarEstadoPlanillaRequest(BaseModel):
    """Modelo para actualizar el estado de aprobación de una planilla"""
    planilla: str
    estado: str  # 'PREAPROBADO', 'REQUIERE_APROBACION_COORDINADOR', 'REQUIERE_APROBACION_CONTROL' o 'APROBADO'
    aprobado_por: str  # Usuario que aprueba


# Modelos para gestión de causales
class CausalRequest(BaseModel):
    """Modelo para crear/actualizar una causal"""
    nombre: str
    activo: bool = True


class CausalResponse(BaseModel):
    """Modelo de respuesta para causales"""
    _id: str
    nombre: str
    activo: bool
    fecha_creacion: Optional[datetime] = None


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
    planillas_a_eliminar: Optional[List[str]] = None  # Planillas a eliminar (para fusión)


def _generar_consecutivo(regional: str, fecha: datetime, es_fusion: bool = False, num_fusion: int = 1, fusion_id: Optional[str] = None, numeros_planillas_a_fusionar: Optional[List[int]] = None) -> dict:
    """
    Genera consecutivos únicos para planillas.

    Args:
        regional: Nombre de la regional (ej: "FUNZA")
        fecha: Fecha de la consulta
        es_fusion: Si es una fusión de planillas
        num_fusion: Número de planillas en la fusión
        fusion_id: ID del grupo de fusión (para reutilizar huecos)

    Returns:
        Dict con los consecutivos generados:
        - Para planilla individual: {"consecutivo": "FUNZA-20260527-1", "consecutivo_base": "FUNZA-20260527-1", "numero": 1, "letra": None}
        - Para fusión: [{"consecutivo": "FUNZA-20260527-1A", ...}, {"consecutivo": "FUNZA-20260527-1B", ...}]
    """
    fecha_str = fecha.strftime("%Y%m%d")
    prefijo = f"{regional}-{fecha_str}"

    # Buscar todos los consecutivos existentes para esta regional y fecha
    # EXCLUYENDO planillas fusionadas (marcadas como fusionada: true)
    regex_pattern = f"^{prefijo}-\\d+[A-Z]?$"
    existentes = list(coleccion_pedidos_medical.find(
        {"consecutivo": {"$regex": regex_pattern}, "fusionada": {"$ne": True}},
        {"consecutivo": 1, "consecutivo_base": 1, "numero_consecutivo": 1, "letra_consecutivo": 1}
    ))

    # Extraer números y letras usadas
    numeros_usados = set()
    fusiones_activas = {}  # {numero_base: [letras_usadas]}

    for doc in existentes:
        cons = doc.get("consecutivo", "")
        if not cons:
            continue

        # Intentar obtener numero y letra directamente de los campos
        numero = doc.get("numero_consecutivo")
        letra = doc.get("letra_consecutivo")

        # Si no están disponibles, parsear del consecutivo
        if numero is None:
            parts = cons.split("-")
            if len(parts) >= 3:
                numero_letra = parts[2]

                # Encontrar dónde empieza la letra
                for i, char in enumerate(numero_letra):
                    if char.isalpha():
                        numero = int(numero_letra[:i]) if i > 0 else None
                        letra = numero_letra[i:]
                        break
                else:
                    # No hay letra, es todo el número
                    numero = int(numero_letra) if numero_letra.isdigit() else None

        if numero is not None:
            if letra:
                # Es una planilla fusionada
                if numero not in fusiones_activas:
                    fusiones_activas[numero] = []
                fusiones_activas[numero].append(letra)
            else:
                # Es una planilla individual
                numeros_usados.add(numero)

    logger.info(f"[CONSECUTIVO] Regional: {regional}, Fecha: {fecha_str}")
    logger.info(f"[CONSECUTIVO] Números usados (individuales): {sorted(numeros_usados)}")
    logger.info(f"[CONSECUTIVO] Fusiones activas: {fusiones_activas}")

    if es_fusion:
        # Lógica para fusiones: usar el mismo número base con letras
        if fusion_id:
            # Reutilizar un número base de fusión existente
            # Buscar el fusion_id en documentos existentes
            fusion_existente = coleccion_pedidos_medical.find_one({
                "fusion_info.fusion_id": fusion_id
            })
            if fusion_existente:
                cons_base = fusion_existente.get("consecutivo_base", "")
                if cons_base:
                    parts = cons_base.split("-")
                    if len(parts) >= 3:
                        numero_base = int(''.join([c for c in parts[2] if c.isdigit()]))
                        # Obtener letras ya usadas en esta fusión
                        letras_usadas = fusiones_activas.get(numero_base, [])
                        # Generar letras para las planillas
                        letras = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                        consecutivos = []
                        letra_idx = 0

                        for i in range(num_fusion):
                            # Encontrar la siguiente letra disponible
                            while letra_idx < len(letras) and letras[letra_idx] in letras_usadas:
                                letra_idx += 1

                            if letra_idx >= len(letras):
                                raise HTTPException(
                                    status_code=500,
                                    detail=f"No hay más letras disponibles para el consecutivo base {prefijo}-{numero_base}"
                                )

                            letra = letras[letra_idx]
                            cons_completo = f"{prefijo}-{numero_base}{letra}"

                            consecutivos.append({
                                "consecutivo": cons_completo,
                                "consecutivo_base": f"{prefijo}-{numero_base}",
                                "numero": numero_base,
                                "letra": letra
                            })

                            letras_usadas.append(letra)
                            letra_idx += 1

                        return {"consecutivos": consecutivos, "numero_base": numero_base}

        # Nueva fusión: usar el número más pequeño de las planillas que se van a fusionar
        # Esto permite recuperar los números originales al dividir
        if numeros_planillas_a_fusionar and len(numeros_planillas_a_fusionar) > 0:
            # Usar el número más pequeño de las planillas que se van a fusionar
            numero_disponible = min(numeros_planillas_a_fusionar)
            logger.info(f"[CONSECUTIVO] Fusión usando número base {numero_disponible} de las planillas originales {numeros_planillas_a_fusionar}")
        else:
            # Si no se proporcionan números, usar el MÁXIMO número existente + 1 (no reutilizar huecos)
            max_numero = 0
            if numeros_usados:
                max_numero = max(numeros_usados)
            if fusiones_activas:
                max_numero = max(max_numero, max(fusiones_activas.keys()))

            numero_disponible = max_numero + 1

        # Generar letras para la fusión
        letras = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        consecutivos = []

        for i in range(num_fusion):
            if i >= len(letras):
                raise HTTPException(
                    status_code=500,
                    detail=f"No hay suficientes letras para fusionar {num_fusion} planillas"
                )

            letra = letras[i]
            cons_completo = f"{prefijo}-{numero_disponible}{letra}"

            consecutivos.append({
                "consecutivo": cons_completo,
                "consecutivo_base": f"{prefijo}-{numero_disponible}",
                "numero": numero_disponible,
                "letra": letra
            })

        fusiones_activas[numero_disponible] = [letras[i] for i in range(num_fusion)]

        return {
            "consecutivos": consecutivos,
            "numero_base": numero_disponible,
            "fusiones_activas": fusiones_activas
        }

    else:
        # Lógica para planillas individuales: usar el MÁXIMO número existente + 1
        # Esto asegura que los consecutivos no se reutilicen
        max_numero = 0
        if numeros_usados:
            max_numero = max(numeros_usados)
        if fusiones_activas:
            max_numero = max(max_numero, max(fusiones_activas.keys()))

        numero_disponible = max_numero + 1
        cons_completo = f"{prefijo}-{numero_disponible}"

        return {
            "consecutivo": cons_completo,
            "consecutivo_base": cons_completo,
            "numero": numero_disponible,
            "letra": None
        }


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

    timeout = httpx.Timeout(600.0, connect=120.0)  # 10 minutos total, 2 minutos para conectar
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


@router.post("/verificar-planillas-fusionadas")
async def verificar_planillas_fusionadas(request: VerificarFusionadasRequest):
    """
    Verifica si alguna de las planillas está fusionada ANTES de consultar Siscore.
    Evita perder tiempo consultando la API si las planillas ya están fusionadas.
    """
    try:
        logger.info(f"=== VERIFICANDO PLANILLAS FUSIONADAS ===")
        logger.info(f"Planillas a verificar: {request.planillas}")

        planillas_fusionadas = []
        for planilla_num in request.planillas:
            doc_fusionada = coleccion_pedidos_medical.find_one({
                "planilla": planilla_num,
                "fusionada": True
            })
            if doc_fusionada:
                fusion_info = doc_fusionada.get("fusionada_en", {})
                planilla_fusionada = fusion_info.get("planilla_fusionada", "")
                consecutivo_fusionada = fusion_info.get("consecutivo_fusionada", "")
                planillas_fusionadas.append({
                    "planilla": planilla_num,
                    "fusionada_en": planilla_fusionada,
                    "consecutivo_fusionada": consecutivo_fusionada
                })
                logger.warning(f"⚠️ Planilla {planilla_num} está fusionada en {planilla_fusionada} (consecutivo: {consecutivo_fusionada})")

        return {
            "planillas_fusionadas": planillas_fusionadas,
            "total_fusionadas": len(planillas_fusionadas)
        }

    except Exception as e:
        logger.error(f"Error al verificar planillas fusionadas: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error al verificar planillas fusionadas: {str(e)}"
        )


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
        logger.info(f"[GUARDAR SOLICITUD] Planilla: {request.planilla}, Placa: {request.placa}, Aforo: {request.aforo}")

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
            "aforo": request.aforo,
            "placa": request.placa,
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


@router.put("/actualizar-solicitud")
async def actualizar_solicitud(request: ActualizarSolicitudRequest):
    """
    Actualiza una solicitud existente en la colección solicitud_veh_medical.
    """
    try:
        from bson import ObjectId

        logger.info(f"[ACTUALIZAR SOLICITUD] ID: {request.solicitud_id}, Placa: {request.placa}, Aforo: {request.aforo}")

        if not ObjectId.is_valid(request.solicitud_id):
            raise HTTPException(status_code=400, detail="ID de solicitud inválido")

        # Campos a actualizar
        campos_actualizar = {
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
            "aforo": request.aforo,
            "placa": request.placa,
            "tipo_veh_sicetac": request.tipo_veh_sicetac,
            "fecha_actualizacion": datetime.now()
        }

        # Actualizar el documento
        resultado = coleccion_solicitudes.update_one(
            {"_id": ObjectId(request.solicitud_id)},
            {"$set": campos_actualizar}
        )

        if resultado.matched_count == 0:
            raise HTTPException(status_code=404, detail="Solicitud no encontrada")

        logger.info(f"Solicitud actualizada: {request.solicitud_id} por usuario {request.usuario}")

        # Obtener el documento actualizado para retornarlo
        solicitud_actualizada = coleccion_solicitudes.find_one({"_id": ObjectId(request.solicitud_id)})
        solicitud_actualizada["_id"] = str(solicitud_actualizada["_id"])

        return {
            "mensaje": "Solicitud actualizada exitosamente",
            "solicitud": solicitud_actualizada
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al actualizar solicitud: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error al actualizar solicitud: {str(e)}"
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
        logger.info(f"=== GUARDAR BÚSQUEDA ===")
        logger.info(f"Usuario: {request.usuario}")
        logger.info(f"Perfil: {request.perfil}")
        logger.info(f"Planillas buscadas: {request.planillas_buscadas}")
        logger.info(f"Cantidad de resultados: {len(request.resultados_consolidados)}")

        # VERIFICAR si alguna de las planillas buscadas está fusionada
        planillas_fusionadas_detectadas = []
        for planilla_num in request.planillas_buscadas:
            doc_fusionada = coleccion_pedidos_medical.find_one({"planilla": planilla_num, "fusionada": True})
            if doc_fusionada:
                fusion_info = doc_fusionada.get("fusionada_en", {})
                planilla_fusionada = fusion_info.get("planilla_fusionada", "")
                consecutivo_fusionada = fusion_info.get("consecutivo_fusionada", "")
                planillas_fusionadas_detectadas.append({
                    "planilla": planilla_num,
                    "fusionada_en": planilla_fusionada,
                    "consecutivo_fusionada": consecutivo_fusionada
                })
                logger.warning(f"⚠️ Planilla {planilla_num} está fusionada en {planilla_fusionada} (consecutivo: {consecutivo_fusionada})")

        fecha_creacion = datetime.now()

        # Agrupar resultados por regional y identificar fusiones
        resultados_por_regional = {}
        fusiones_por_regional = {}  # {regional: [resultados_fusionados]}

        for resultado in request.resultados_consolidados:
            # Obtener regional prioritizando:
            # 1. Regional del resultado (que no sea '-' ni 'TODOS' ni vacía)
            # 2. Centro de distribución del resultado (que no sea '-' ni 'TODOS' ni vacío)
            # 3. Centro de costo del resultado
            # 4. Centro de distribución del USUARIO (fallback)
            regional_resultado = resultado.get("regional") or resultado.get("centro_distribucion") or resultado.get("centro_costo")

            # Si la regional del resultado es inválida ('-', 'TODOS', vacía), usar la del usuario
            if not regional_resultado or regional_resultado in ['-', 'TODOS', '']:
                regional_calculada = request.centro_distribucion or "TODOS"
            else:
                regional_calculada = regional_resultado

            # Limpiar y normalizar el nombre de la regional
            regional_calculada = regional_calculada.upper().strip()

            # Si aún queda en inválida, usar TODOS
            if regional_calculada in ['-', 'TODOS', '']:
                regional_calculada = "TODOS"

            # Guardar la regional calculada en el resultado para usarla después
            resultado["regional_calculada"] = regional_calculada

            fusion_info = resultado.get("fusion_info")

            if fusion_info and fusion_info.get("es_fusionada"):
                # Es una planilla fusionada
                if regional_calculada not in fusiones_por_regional:
                    fusiones_por_regional[regional_calculada] = []
                fusiones_por_regional[regional_calculada].append(resultado)
            else:
                # Es una planilla individual
                if regional_calculada not in resultados_por_regional:
                    resultados_por_regional[regional_calculada] = []
                resultados_por_regional[regional_calculada].append(resultado)

        # Procesar fusiones primero (para asignar un solo número base con letras)
        fusiones_procesadas = []  # [(resultado, consecutivo_info), ...]

        for regional, fusionados in fusiones_por_regional.items():
            # Obtener el fusion_id si existe (para reutilizar huecos)
            fusion_id = fusionados[0].get("fusion_info", {}).get("fusion_id") if fusionados else None

            # Extraer los números de las planillas que se van a fusionar
            # Buscar los consecutivos de las planillas originales en MongoDB
            numeros_planillas_a_fusionar = []
            for resultado in fusionados:
                fusion_info = resultado.get("fusion_info", {})
                planillas_originales = fusion_info.get("planillas_originales", [])

                # Buscar cada planilla original en MongoDB para obtener su consecutivo
                for planilla_num in planillas_originales:
                    doc_original = coleccion_pedidos_medical.find_one({"planilla": planilla_num})
                    if doc_original and doc_original.get("consecutivo"):
                        cons = doc_original.get("consecutivo")
                        parts = cons.split("-")
                        if len(parts) >= 3:
                            numero_letra = parts[2]
                            # Extraer número (sin letra)
                            numero = None
                            for i, char in enumerate(numero_letra):
                                if char.isalpha():
                                    numero = int(numero_letra[:i]) if i > 0 else None
                                    break
                            else:
                                # No hay letra, es todo el número
                                numero = int(numero_letra) if numero_letra.isdigit() else None

                            if numero is not None:
                                numeros_planillas_a_fusionar.append(numero)
                                logger.info(f"[CONSECUTIVO] Planilla original {planilla_num} tiene número {numero}")

            logger.info(f"[CONSECUTIVO] Números de planillas a fusionar: {numeros_planillas_a_fusionar}")

            # Generar consecutivos para la fusión
            try:
                consecutivo_info = _generar_consecutivo(
                    regional=regional,
                    fecha=fecha_creacion,
                    es_fusion=True,
                    num_fusion=len(fusionados),
                    fusion_id=fusion_id,
                    numeros_planillas_a_fusionar=numeros_planillas_a_fusionar if numeros_planillas_a_fusionar else None
                )

                logger.info(f"[CONSECUTIVO] Fusión {regional}: {len(fusionados)} planillas → número base {consecutivo_info.get('numero_base')}")

                # Asignar consecutivos a cada planilla fusionada
                letras = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                for i, resultado in enumerate(fusionados):
                    cons_info = {
                        "consecutivo": f"{regional}-{fecha_creacion.strftime('%Y%m%d')}-{consecutivo_info.get('numero_base')}{letras[i]}",
                        "consecutivo_base": f"{regional}-{fecha_creacion.strftime('%Y%m%d')}-{consecutivo_info.get('numero_base')}",
                        "numero": consecutivo_info.get('numero_base'),
                        "letra": letras[i],
                        "es_fusionada": True
                    }
                    fusiones_procesadas.append((resultado, cons_info))

            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Error generando consecutivo para fusión: {str(e)}")
                raise HTTPException(status_code=500, detail=f"Error generando consecutivo: {str(e)}")

        # Procesar planillas individuales por lotes (por regional)
        individuales_procesadas = []  # [(resultado, consecutivo_info), ...]

        for regional, resultados in resultados_por_regional.items():
            # Generar consecutivos para todas las planillas individuales de esta regional de una vez
            try:
                # Obtener el próximo número base disponible
                consecutivo_base = _generar_consecutivo(
                    regional=regional,
                    fecha=fecha_creacion,
                    es_fusion=False
                )
                numero_inicial = consecutivo_base["numero"]

                logger.info(f"[CONSECUTIVO] Individual {regional}: {len(resultados)} planillas comenzando desde {numero_inicial}")

                # Asignar consecutivos secuenciales
                for i, resultado in enumerate(resultados):
                    # Verificar si el resultado ya tiene un consecutivo (por ejemplo, al dividir una fusión)
                    if resultado.get("consecutivo"):
                        # Usar el consecutivo existente
                        cons_completo = resultado.get("consecutivo")
                        cons_base = resultado.get("consecutivo_base", cons_completo)

                        # Extraer número del consecutivo existente
                        numero = None
                        parts = cons_completo.split("-")
                        if len(parts) >= 3:
                            numero_letra = parts[2]
                            for j, char in enumerate(numero_letra):
                                if char.isalpha():
                                    numero = int(numero_letra[:j]) if j > 0 else None
                                    break
                            else:
                                numero = int(numero_letra) if numero_letra.isdigit() else None

                        cons_info_completo = {
                            "consecutivo": cons_completo,
                            "consecutivo_base": cons_base,
                            "numero": numero,
                            "letra": None,
                            "es_fusionada": False
                        }
                        individuales_procesadas.append((resultado, cons_info_completo))
                        logger.info(f"[CONSECUTIVO] Planilla {resultado.get('planilla')}: usando consecutivo existente {cons_completo}")
                    else:
                        # Generar nuevo consecutivo
                        numero = numero_inicial + i
                        cons_completo = f"{regional}-{fecha_creacion.strftime('%Y%m%d')}-{numero}"

                        cons_info_completo = {
                            "consecutivo": cons_completo,
                            "consecutivo_base": cons_completo,
                            "numero": numero,
                            "letra": None,
                            "es_fusionada": False
                        }
                        individuales_procesadas.append((resultado, cons_info_completo))
                        logger.info(f"[CONSECUTIVO] Planilla {resultado.get('planilla')}: {cons_completo}")

            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Error generando consecutivos para planillas individuales: {str(e)}")
                raise HTTPException(status_code=500, detail=f"Error generando consecutivos: {str(e)}")

        # Combinar todos los resultados procesados
        todos_procesados = fusiones_procesadas + individuales_procesadas

        # Guardar cada resultado como un documento independiente
        for resultado, cons_info in todos_procesados:
            logger.info(f"Procesando planilla: {resultado.get('planilla')} con consecutivo: {cons_info['consecutivo']}")

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
                "regional": resultado.get("regional_calculada"),  # Usar la regional calculada con fallback
                "centro_costo": resultado.get("centro_costo"),
                "tarifa_calculada": resultado.get("tarifa_calculada", 0),
                "tipo_vehiculo": resultado.get("tipo_vehiculo"),
                "total_solicitado": resultado.get("total_solicitado", 0),
                "cantidad_destinos": resultado.get("cantidad_destinos", 0),
                "municipios_destino_lista": resultado.get("municipios_destino_lista", "-"),
                "municipios_con_pedidos": resultado.get("municipios_con_pedidos", {}),
                "fusion_info": resultado.get("fusion_info"),  # Historial de fusión
                "tarifa_base": resultado.get("tarifa_base"),
                "requiere_descargue": resultado.get("requiere_descargue", "NO"),
                "punto_adicional": resultado.get("punto_adicional", False),
                "desvio": resultado.get("desvio", False),
                "aforo": resultado.get("aforo"),
                "placa": resultado.get("placa"),
                "tipo_veh_sicetac": resultado.get("tipo_veh_sicetac"),
                "fecha_creacion": fecha_creacion,
                "estado": resultado.get("estado", "PREAPROBADO"),  # Estado por defecto (puede ser PREAPROBADO, REQUIERE_APROBACION_COORDINADOR, REQUIERE_APROBACION_CONTROL o APROBADO)
                "aprobado_por": resultado.get("aprobado_por"),
                "fecha_aprobacion": resultado.get("fecha_aprobacion"),
                # Campos de consecutivo
                "consecutivo": cons_info["consecutivo"],
                "consecutivo_base": cons_info["consecutivo_base"],
                "numero_consecutivo": cons_info["numero"],
                "letra_consecutivo": cons_info["letra"],
                "es_fusionada_consecutivo": cons_info["es_fusionada"]
            }

            # Verificar si ya existe un documento con esta planilla
            existente = coleccion_pedidos_medical.find_one({"planilla": resultado.get("planilla")})

            if existente:
                # Actualizar el existente (conservar el consecutivo si ya tiene uno)
                if existente.get("consecutivo"):
                    # Mantener el consecutivo existente y ACTUALIZAR cons_info para devolver al frontend
                    planilla_doc["consecutivo"] = existente.get("consecutivo")
                    planilla_doc["consecutivo_base"] = existente.get("consecutivo_base")
                    planilla_doc["numero_consecutivo"] = existente.get("numero_consecutivo")
                    planilla_doc["letra_consecutivo"] = existente.get("letra_consecutivo")
                    planilla_doc["es_fusionada_consecutivo"] = existente.get("es_fusionada_consecutivo", False)

                    # Actualizar cons_info con el consecutivo existente para devolver al frontend
                    cons_info["consecutivo"] = planilla_doc["consecutivo"]
                    cons_info["consecutivo_base"] = planilla_doc["consecutivo_base"]
                    cons_info["numero"] = planilla_doc["numero_consecutivo"]

                coleccion_pedidos_medical.update_one(
                    {"_id": existente["_id"]},
                    {"$set": planilla_doc}
                )
                logger.info(f"Planilla {resultado.get('planilla')}: actualizada con consecutivo {planilla_doc['consecutivo']}")
            else:
                # Insertar nuevo
                coleccion_pedidos_medical.insert_one(planilla_doc)
                logger.info(f"Planilla {resultado.get('planilla')}: guardada con consecutivo {planilla_doc['consecutivo']}")

        logger.info(f"Total guardado: {len(request.resultados_consolidados)} planillas en pedidos_medical")

        # MARCAR como fusionadas las planillas que se indicaron (en lugar de eliminarlas)
        # Esto permite reservar sus consecutivos y recuperarlos al dividir
        if request.planillas_a_eliminar and len(request.planillas_a_eliminar) > 0:
            logger.info(f"Planillas a marcar como fusionadas: {request.planillas_a_eliminar}")

            # Buscar la planilla fusionada que contiene estas planillas
            planilla_fusionada = None
            for resultado, cons_info in todos_procesados:
                if resultado.get("fusion_info", {}).get("es_fusionada"):
                    planilla_fusionada = resultado
                    fusion_consecutivo = cons_info.get("consecutivo", "")
                    break

            # Marcar las planillas originales como fusionadas
            resultado_update = coleccion_pedidos_medical.update_many(
                {"planilla": {"$in": request.planillas_a_eliminar}},
                {"$set": {
                    "fusionada": True,
                    "fusionada_en": {
                        "planilla_fusionada": planilla_fusionada.get("planilla") if planilla_fusionada else None,
                        "consecutivo_fusionada": fusion_consecutivo if planilla_fusionada else None,
                        "fecha_fusion": fecha_creacion,
                        "usuario_fusion": request.usuario
                    }
                }}
            )
            logger.info(f"Marcadas {resultado_update.modified_count} planillas como fusionadas (reservando sus consecutivos)")

        # Crear mapeo de planilla → consecutivo para devolver al frontend
        planillas_consecutivos = {}
        for resultado, cons_info in todos_procesados:
            planillas_consecutivos[resultado.get('planilla')] = {
                "consecutivo": cons_info['consecutivo'],
                "consecutivo_base": cons_info['consecutivo_base'],
                "numero": cons_info['numero'],
                "letra": cons_info['letra'],
                "es_fusionada": cons_info['es_fusionada']
            }

        return {
            "mensaje": f"Se guardaron/actualizaron {len(request.resultados_consolidados)} planillas",
            "total": len(request.resultados_consolidados),
            "consecutivos": planillas_consecutivos,
            "planillas_fusionadas_detectadas": planillas_fusionadas_detectadas
        }

    except Exception as e:
        logger.error(f"Error al guardar búsqueda: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error al guardar búsqueda: {str(e)}"
        )


class DividirFusionRequest(BaseModel):
    planilla_fusionada: str
    usuario: str


@router.post("/dividir-fusion")
async def dividir_fusion(request: DividirFusionRequest):
    """
    Divide una planilla fusionada reactivando las planillas originales.
    """
    try:
        logger.info(f"=== DIVIDIR FUSIÓN ===")
        logger.info(f"Planilla fusionada: {request.planilla_fusionada}")
        logger.info(f"Usuario: {request.usuario}")

        # Buscar la planilla fusionada
        fusionada = coleccion_pedidos_medical.find_one({"planilla": request.planilla_fusionada})
        if not fusionada:
            raise HTTPException(status_code=404, detail="Planilla fusionada no encontrada")

        fusion_info = fusionada.get("fusion_info", {})
        if not fusion_info.get("es_fusionada"):
            raise HTTPException(status_code=400, detail="Esta planilla no es una fusión")

        planillas_originales = fusion_info.get("planillas_originales", [])
        logger.info(f"Planillas originales a reactivar: {planillas_originales}")

        # Reactivar las planillas originales (quitar marca de fusionada)
        resultado_reactivar = coleccion_pedidos_medical.update_many(
            {
                "planilla": {"$in": planillas_originales},
                "fusionada": True
            },
            {"$unset": {"fusionada": "", "fusionada_en": ""}}
        )
        logger.info(f"Reactivadas {resultado_reactivar.modified_count} planillas originales")

        # Obtener las planillas reactivadas para devolverlas al frontend
        planillas_reactivadas = list(coleccion_pedidos_medical.find(
            {"planilla": {"$in": planillas_originales}}
        ))

        # Eliminar la planilla fusionada
        coleccion_pedidos_medical.delete_one({"planilla": request.planilla_fusionada})
        logger.info(f"Eliminada planilla fusionada: {request.planilla_fusionada}")

        # Convertir al formato que espera el frontend
        resultados_frontend = []
        for doc in planillas_reactivadas:
            resultado = {
                "planilla": doc.get("planilla"),
                "encontrada": doc.get("encontrada", True),
                "piezas": doc.get("piezas", 0),
                "peso_real": doc.get("peso_real", 0),
                "ruta": doc.get("ruta", "-"),
                "codigo_pedido": doc.get("codigo_pedido", "-"),
                "cantidad_pedidos": doc.get("cantidad_pedidos", 0),
                "cliente_origen": doc.get("cliente_origen", "-"),
                "municipio_destino": doc.get("municipio_destino", "-"),
                "departamento_destino": doc.get("departamento_destino", "-"),
                "regional": doc.get("regional"),
                "centro_costo": doc.get("centro_costo"),
                "tarifa_calculada": doc.get("tarifa_calculada", 0),
                "tipo_vehiculo": doc.get("tipo_vehiculo", "-"),
                "total_solicitado": doc.get("total_solicitado", 0),
                "tarifa_base": doc.get("tarifa_base"),
                "requiere_descargue": doc.get("requiere_descargue", 0),
                "punto_adicional": doc.get("punto_adicional", False),
                "desvio": doc.get("desvio", False),
                "aforo": doc.get("aforo"),
                "placa": doc.get("placa"),
                "tipo_veh_sicetac": doc.get("tipo_veh_sicetac"),
                "causal": doc.get("causal", ""),
                "cantidad_destinos": doc.get("cantidad_destinos", 0),
                "municipios_destino_lista": doc.get("municipios_destino_lista", "-"),
                "municipios_con_pedidos": doc.get("municipios_con_pedidos", {}),
                "fusion_info": None,  # Eliminar fusion_info
                "estado": doc.get("estado", "PREAPROBADO"),
                "aprobado_por": doc.get("aprobado_por"),
                "fecha_aprobacion": doc.get("fecha_aprobacion"),
                "consecutivo": doc.get("consecutivo"),
                "consecutivo_base": doc.get("consecutivo_base"),
                "guardado": True
            }
            resultados_frontend.append(resultado)

        # Crear mapeo de consecutivos
        planillas_consecutivos = {}
        for resultado in resultados_frontend:
            if resultado["consecutivo"]:
                planillas_consecutivos[resultado["planilla"]] = {
                    "consecutivo": resultado["consecutivo"],
                    "consecutivo_base": resultado["consecutivo_base"],
                    "numero": int(resultado["consecutivo"].split("-")[-1]) if resultado["consecutivo"] else None,
                    "letra": None,
                    "es_fusionada": False
                }

        logger.info(f"División completada: {len(resultados_frontend)} planillas reactivadas")

        return {
            "mensaje": f"Se han restaurado {len(resultados_frontend)} planillas originales",
            "planillas": resultados_frontend,
            "consecutivos": planillas_consecutivos
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al dividir fusión: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al dividir fusión: {str(e)}")


@router.put("/actualizar-planilla-pedidos")
async def actualizar_planilla_pedidos(request: ActualizarPlanillaPedidosRequest):
    """
    Actualiza una planilla específica en la colección pedidos_medical.
    Incluye trazabilidad completa de modificaciones.
    """
    try:
        logger.info(f"[ACTUALIZAR PLANILLA PEDIDOS] Planilla: {request.planilla}, Usuario: {request.usuario_modificacion}")

        # Obtener el documento actual antes de actualizar (para trazabilidad)
        doc_actual = coleccion_pedidos_medical.find_one({"planilla": request.planilla})
        if not doc_actual:
            logger.warning(f"[ACTUALIZAR PLANILLA PEDIDOS] No se encontró planilla: {request.planilla}")
            raise HTTPException(status_code=404, detail=f"Planilla {request.planilla} no encontrada en pedidos_medical")

        fecha_actual = datetime.now()

        # Crear registro de historial de cambios
        historial_cambios = doc_actual.get("historial_cambios", [])

        # Detectar qué campos cambiaron y registrar en historial
        campos_modificados = []
        if request.tarifa_base is not None and request.tarifa_base != doc_actual.get("tarifa_base"):
            campos_modificados.append({
                "campo": "tarifa_base",
                "valor_anterior": doc_actual.get("tarifa_base"),
                "valor_nuevo": request.tarifa_base
            })
        if request.requiere_descargue != doc_actual.get("requiere_descargue"):
            campos_modificados.append({
                "campo": "requiere_descargue",
                "valor_anterior": doc_actual.get("requiere_descargue"),
                "valor_nuevo": request.requiere_descargue
            })
        if request.punto_adicional != doc_actual.get("punto_adicional"):
            campos_modificados.append({
                "campo": "punto_adicional",
                "valor_anterior": doc_actual.get("punto_adicional"),
                "valor_nuevo": request.punto_adicional
            })
        if request.desvio != doc_actual.get("desvio"):
            campos_modificados.append({
                "campo": "desvio",
                "valor_anterior": doc_actual.get("desvio"),
                "valor_nuevo": request.desvio
            })
        if request.aforo is not None and request.aforo != doc_actual.get("aforo"):
            campos_modificados.append({
                "campo": "aforo",
                "valor_anterior": doc_actual.get("aforo"),
                "valor_nuevo": request.aforo
            })
        if request.placa is not None and request.placa != doc_actual.get("placa"):
            campos_modificados.append({
                "campo": "placa",
                "valor_anterior": doc_actual.get("placa"),
                "valor_nuevo": request.placa
            })
        if request.tipo_veh_sicetac is not None and request.tipo_veh_sicetac != doc_actual.get("tipo_veh_sicetac"):
            campos_modificados.append({
                "campo": "tipo_veh_sicetac",
                "valor_anterior": doc_actual.get("tipo_veh_sicetac"),
                "valor_nuevo": request.tipo_veh_sicetac
            })
        if request.causal != doc_actual.get("causal"):
            campos_modificados.append({
                "campo": "causal",
                "valor_anterior": doc_actual.get("causal"),
                "valor_nuevo": request.causal
            })

        # Registrar cambio de estado
        estado_anterior = doc_actual.get("estado", "PREAPROBADO")
        if request.estado is not None and request.estado != estado_anterior:
            campos_modificados.append({
                "campo": "estado",
                "valor_anterior": estado_anterior,
                "valor_nuevo": request.estado
            })

        # Si hay cambios, agregar al historial
        if campos_modificados:
            nuevo_historial = {
                "fecha": fecha_actual,
                "usuario": request.usuario_modificacion,
                "accion": "edicion",
                "campos_modificados": campos_modificados,
                "causal": request.causal
            }
            historial_cambios.append(nuevo_historial)

        # Campos a actualizar
        campos_actualizar = {
            "tarifa_base": request.tarifa_base,
            "requiere_descargue": request.requiere_descargue,
            "punto_adicional": request.punto_adicional,
            "desvio": request.desvio,
            "aforo": request.aforo,
            "placa": request.placa,
            "tipo_veh_sicetac": request.tipo_veh_sicetac,
            "total_solicitado": request.total_solicitado,
            "causal": request.causal,
            # Trazabilidad de modificación
            "usuario_modificacion": request.usuario_modificacion,
            "fecha_modificacion": fecha_actual,
            "historial_cambios": historial_cambios
        }

        # Si se envía estado, actualizarlo
        if request.estado is not None:
            campos_actualizar["estado"] = request.estado
            if request.estado == "REQUIERE_APROBACION" and estado_anterior != "REQUIERE_APROBACION":
                # Si cambia a REQUIERE_APROBACION, registrar quién solicitó autorización
                campos_actualizar["usuario_solicitud_autorizacion"] = request.usuario_modificacion
                campos_actualizar["fecha_solicitud_autorizacion"] = fecha_actual
            if request.estado != "APROBADO":
                # Limpiar campos de aprobación si no está aprobada
                campos_actualizar["aprobado_por"] = None
                campos_actualizar["fecha_aprobacion"] = None
            else:
                campos_actualizar["aprobado_por"] = request.aprobado_por
                campos_actualizar["fecha_aprobacion"] = request.fecha_aprobacion

        # Actualizar el documento
        resultado = coleccion_pedidos_medical.update_one(
            {"planilla": request.planilla},
            {"$set": campos_actualizar}
        )

        if resultado.matched_count == 0:
            logger.warning(f"[ACTUALIZAR PLANILLA PEDIDOS] No se encontró planilla: {request.planilla}")
            raise HTTPException(status_code=404, detail=f"Planilla {request.planilla} no encontrada en pedidos_medical")

        logger.info(f"[ACTUALIZAR PLANILLA PEDIDOS] Planilla {request.planilla} actualizada - Modified: {resultado.modified_count}")
        logger.info(f"[TRAZABILIDAD] Usuario: {request.usuario_modificacion}, Fecha: {fecha_actual}")

        return {
            "mensaje": "Planilla actualizada exitosamente en pedidos_medical",
            "planilla": request.planilla,
            "modified_count": resultado.modified_count,
            "trazabilidad": {
                "usuario_modificacion": request.usuario_modificacion,
                "fecha_modificacion": fecha_actual.isoformat(),
                "estado_anterior": estado_anterior,
                "estado_nuevo": request.estado
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al actualizar planilla en pedidos_medical: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error al actualizar planilla: {str(e)}"
        )


@router.put("/actualizar-estado-planilla")
async def actualizar_estado_planilla(request: ActualizarEstadoPlanillaRequest):
    """
    Actualiza el estado de aprobación de una planilla en pedidos_medical.
    Incluye trazabilidad de aprobación en el historial.
    """
    try:
        logger.info(f"=== ACTUALIZAR ESTADO PLANILLA ===")
        logger.info(f"Planilla: {request.planilla}")
        logger.info(f"Estado: {request.estado}")
        logger.info(f"Aprobado por: {request.aprobado_por}")

        # Obtener el documento actual antes de actualizar (para trazabilidad)
        doc_actual = coleccion_pedidos_medical.find_one({"planilla": request.planilla})
        if not doc_actual:
            logger.warning(f"[ACTUALIZAR ESTADO PLANILLA] No se encontró planilla: {request.planilla}")
            raise HTTPException(status_code=404, detail=f"Planilla {request.planilla} no encontrada en pedidos_medical")

        fecha_actual = datetime.now()
        estado_anterior = doc_actual.get("estado", "PREAPROBADO")

        # Obtener historial existente
        historial_cambios = doc_actual.get("historial_cambios", [])

        # Si el estado cambió, agregar al historial
        if request.estado != estado_anterior:
            nuevo_historial = {
                "fecha": fecha_actual,
                "usuario": request.aprobado_por,
                "accion": "cambio_estado",
                "campos_modificados": [
                    {
                        "campo": "estado",
                        "valor_anterior": estado_anterior,
                        "valor_nuevo": request.estado
                    }
                ]
            }
            historial_cambios.append(nuevo_historial)

            logger.info(f"[TRAZABILIDAD] Cambio de estado: {estado_anterior} → {request.estado} por {request.aprobado_por}")

        # Campos a actualizar
        campos_actualizar = {
            "estado": request.estado,
            "aprobado_por": request.aprobado_por,
            "fecha_aprobacion": datetime.now() if request.estado == "APROBADO" else None,
            "historial_cambios": historial_cambios
        }

        # Actualizar el documento
        resultado = coleccion_pedidos_medical.update_one(
            {"planilla": request.planilla},
            {"$set": campos_actualizar}
        )

        if resultado.matched_count == 0:
            logger.warning(f"No se encontró planilla: {request.planilla}")
            raise HTTPException(status_code=404, detail=f"Planilla {request.planilla} no encontrada en pedidos_medical")

        logger.info(f"Planilla {request.planilla} actualizada - Estado: {request.estado}")

        return {
            "mensaje": f"Planilla actualizada a estado {request.estado}",
            "planilla": request.planilla,
            "estado": request.estado,
            "estado_anterior": estado_anterior,
            "modified_count": resultado.modified_count
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al actualizar estado de planilla: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error al actualizar estado: {str(e)}"
        )


class EliminarPlanillaRequest(BaseModel):
    """Modelo para eliminar una planilla"""
    planilla: str
    usuario: str  # Usuario que elimina (trazabilidad)


@router.delete("/eliminar-planilla")
async def eliminar_planilla(request: EliminarPlanillaRequest):
    """
    Elimina una planilla de la colección pedidos_medical.
    Si es una planilla fusionada, también elimina las planillas originales marcadas como fusionada: true.
    Incluye trazabilidad de quién eliminó.
    """
    try:
        logger.info(f"=== ELIMINAR PLANILLA ===")
        logger.info(f"Planilla: {request.planilla}")
        logger.info(f"Usuario: {request.usuario}")

        # Verificar si existe la planilla
        doc_actual = coleccion_pedidos_medical.find_one({"planilla": request.planilla})
        if not doc_actual:
            logger.warning(f"[ELIMINAR PLANILLA] No se encontró planilla: {request.planilla}")
            raise HTTPException(status_code=404, detail=f"Planilla {request.planilla} no encontrada en pedidos_medical")

        # Verificar si es una planilla fusionada
        fusion_info = doc_actual.get("fusion_info")
        es_fusionada = fusion_info and fusion_info.get("es_fusionada") == True

        planillas_eliminadas = [request.planilla]

        # Si es una planilla fusionada, eliminar también las planillas originales marcadas como fusionada: true
        if es_fusionada:
            logger.info(f"[ELIMINAR PLANILLA] La planilla {request.planilla} es una fusión, eliminando originales también")

            # Buscar planillas originales marcadas como fusionada: true que apuntan a esta planilla
            planillas_originales_fusionadas = list(coleccion_pedidos_medical.find({
                "fusionada": True,
                "fusionada_en.planilla_fusionada": request.planilla
            }))

            logger.info(f"[ELIMINAR PLANILLA] Encontradas {len(planillas_originales_fusionadas)} planillas originales fusionadas")

            # Eliminar cada planilla original
            for doc_original in planillas_originales_fusionadas:
                planilla_original = doc_original.get("planilla")
                logger.info(f"[ELIMINAR PLANILLA] Eliminando planilla original fusionada: {planilla_original}")
                coleccion_pedidos_medical.delete_one({"planilla": planilla_original})
                planillas_eliminadas.append(planilla_original)

        # Eliminar la planilla principal
        resultado = coleccion_pedidos_medical.delete_one({"planilla": request.planilla})

        if resultado.deleted_count == 0:
            logger.warning(f"[ELIMINAR PLANILLA] No se pudo eliminar planilla: {request.planilla}")
            raise HTTPException(status_code=500, detail=f"No se pudo eliminar la planilla {request.planilla}")

        logger.info(f"[ELIMINAR PLANILLA] Planilla {request.planilla} eliminada por {request.usuario}")
        if es_fusionada:
            logger.info(f"[ELIMINAR PLANILLA] Total planillas eliminadas: {len(planillas_eliminadas)} - {planillas_eliminadas}")

        return {
            "mensaje": f"Planilla {request.planilla} eliminada exitosamente",
            "planilla": request.planilla,
            "deleted_count": resultado.deleted_count,
            "eliminado_por": request.usuario,
            "es_fusionada": es_fusionada,
            "planillas_eliminadas": planillas_eliminadas,
            "total_eliminadas": len(planillas_eliminadas)
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al eliminar planilla: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error al eliminar planilla: {str(e)}"
        )


class ExportarPlanillasExcelRequest(BaseModel):
    """Modelo para exportar planillas a Excel"""
    planillas: List[str]
    perfil: str
    centro_distribucion: Optional[str] = None


@router.post("/exportar-planillas-excel")
async def exportar_planillas_excel(request: ExportarPlanillasExcelRequest):
    """
    Exporta planillas a Excel con todos sus datos.
    Filtra por regional si el perfil es OPERATIVO.
    """
    try:
        from fastapi.responses import Response
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        from datetime import datetime
        import io

        logger.info(f"=== EXPORTAR PLANILLAS EXCEL ===")
        logger.info(f"Perfil: {request.perfil}")
        logger.info(f"Centro distribución: {request.centro_distribucion}")
        logger.info(f"Planillas solicitadas: {len(request.planillas)}")
        logger.info(f"Planillas: {request.planillas}")

        # Consultar planillas de MongoDB
        consulta = {"planilla": {"$in": request.planillas}}

        # Si es OPERATIVO, filtrar por regional
        if request.perfil == "OPERATIVO" and request.centro_distribucion:
            # Mapear centro de distribución a códigos de bodega
            regional_map = {
                "BARRANQUILLA": "CO04",
                "CALI": "CO05",
                "BUCARAMANGA": "CO06",
                "FUNZA": "CO07",
                "MEDELLIN": "CO09"
            }
            bodega = regional_map.get(request.centro_distribucion, "")
            if bodega:
                consulta["centro_costo"] = bodega

        planillas_db = list(coleccion_pedidos_medical.find(consulta))

        logger.info(f"Planillas encontradas en BD: {len(planillas_db)}")

        if not planillas_db:
            raise HTTPException(status_code=404, detail="No se encontraron planillas para exportar")

        # Crear workbook y worksheet
        wb = Workbook()
        ws = wb.active
        ws.title = "Planillas"

        # Definir columnas
        columnas = [
            "Planilla", "Estado", "Regional", "Cliente Origen", "Ruta",
            "Municipio Principal", "Municipios Destino", "Cant. Destinos",
            "Piezas", "Peso Real", "Cant. Pedidos", "Código Pedido",
            "Flete Teórico", "Flete Base", "Total Solicitado",
            "Diferencia", "% Diferencia", "Vehículo", "Placa",
            "Descargue", "Punto Adic.", "Desvío", "Aforo",
            "Fusionada", "Planillas Originales", "Causal Fusión",
            "Observaciones",
            "Usuario Registro", "Usuario Modificación", "Fecha Modificación",
            "Usuario Solicitud Aut.", "Fecha Solicitud Aut.",
            "Aprobado Por", "Fecha Aprobación"
        ]

        # Estilos
        header_font = Font(bold=True, color="FFFFFF")
        header_fill = PatternFill(start_color="004d40", end_color="004d40", fill_type="solid")
        header_alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        thin_border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )

        # Escribir cabeceras
        for col_idx, columna in enumerate(columnas, 1):
            cell = ws.cell(row=1, column=col_idx, value=columna)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = header_alignment
            cell.border = thin_border

        # Ajustar ancho de columnas
        ws.column_dimensions['A'].width = 15  # Planilla
        ws.column_dimensions['B'].width = 12  # Estado
        ws.column_dimensions['C'].width = 12  # Regional
        ws.column_dimensions['D'].width = 20  # Cliente Origen
        ws.column_dimensions['E'].width = 8   # Ruta
        ws.column_dimensions['F'].width = 18  # Municipio Principal
        ws.column_dimensions['G'].width = 30  # Municipios Destino
        ws.column_dimensions['H'].width = 12  # Cant. Destinos
        ws.column_dimensions['I'].width = 10  # Piezas
        ws.column_dimensions['J'].width = 12  # Peso Real
        ws.column_dimensions['K'].width = 12  # Cant. Pedidos
        ws.column_dimensions['L'].width = 40  # Código Pedido
        ws.column_dimensions['M'].width = 12  # Flete Teórico
        ws.column_dimensions['N'].width = 12  # Flete Base
        ws.column_dimensions['O'].width = 12  # Total Solicitado
        ws.column_dimensions['P'].width = 12  # Diferencia
        ws.column_dimensions['Q'].width = 12  # % Diferencia
        ws.column_dimensions['R'].width = 12  # Vehículo
        ws.column_dimensions['S'].width = 12  # Placa
        ws.column_dimensions['T'].width = 12  # Descargue
        ws.column_dimensions['U'].width = 12  # Punto Adic.
        ws.column_dimensions['V'].width = 12  # Desvío
        ws.column_dimensions['W'].width = 12  # Aforo
        ws.column_dimensions['X'].width = 12  # Fusionada
        ws.column_dimensions['Y'].width = 20  # Planillas Originales
        ws.column_dimensions['Z'].width = 20  # Causal Fusión
        ws.column_dimensions['AA'].width = 15  # Observaciones
        ws.column_dimensions['AB'].width = 18  # Usuario Registro
        ws.column_dimensions['AC'].width = 18  # Usuario Modificación
        ws.column_dimensions['AD'].width = 18  # Fecha Modificación
        ws.column_dimensions['AE'].width = 18  # Usuario Solicitud Aut.
        ws.column_dimensions['AF'].width = 18  # Fecha Solicitud Aut.
        ws.column_dimensions['AG'].width = 15  # Aprobado Por
        ws.column_dimensions['AH'].width = 18  # Fecha Aprobación

        # Escribir datos
        row_num = 2
        for doc in planillas_db:
            try:
                teorico = doc.get("tarifa_calculada", 0)
                total = doc.get("total_solicitado", 0)

                # Asegurar que sean numéricos
                try:
                    teorico = float(teorico) if teorico else 0
                except:
                    teorico = 0

                try:
                    total = float(total) if total else 0
                except:
                    total = 0

                diferencia = total - teorico

                # Calcular porcentaje de forma segura
                if teorico > 0:
                    porc_diferencia = (diferencia / teorico) * 100
                else:
                    porc_diferencia = 0

                datos = [
                    doc.get("planilla", ""),
                    doc.get("estado", "PREAPROBADO"),
                    doc.get("regional", ""),
                    doc.get("cliente_origen", ""),
                    doc.get("ruta", ""),
                    doc.get("municipio_destino", ""),
                    doc.get("municipios_destino_lista", ""),
                    doc.get("cantidad_destinos", 0),
                    doc.get("piezas", 0),
                    doc.get("peso_real", 0),
                    doc.get("cantidad_pedidos", 0),
                    doc.get("codigo_pedido", ""),
                    teorico,
                    doc.get("tarifa_base", 0),
                    total,
                    diferencia,
                    f"{porc_diferencia:.1f}%",
                    doc.get("tipo_vehiculo", ""),
                    doc.get("placa", ""),
                    doc.get("requiere_descargue", 0),
                    doc.get("punto_adicional", 0),
                    doc.get("desvio", 0),
                    doc.get("aforo", 0),
                    "Sí" if doc.get("fusion_info", {}).get("es_fusionada") else "No",
                    ", ".join(doc.get("fusion_info", {}).get("planillas_originales", [])),
                    doc.get("fusion_info", {}).get("causal", ""),
                    doc.get("causal", ""),  # Causal de la modificación (Observaciones)
                    # Trazabilidad completa
                    doc.get("usuario_registro", ""),
                    doc.get("usuario_modificacion", ""),
                    doc.get("fecha_modificacion", ""),
                    doc.get("usuario_solicitud_autorizacion", ""),
                    doc.get("fecha_solicitud_autorizacion", ""),
                    doc.get("aprobado_por", ""),
                    doc.get("fecha_aprobacion", "")
                ]

                for col_idx, valor in enumerate(datos, 1):
                    cell = ws.cell(row=row_num, column=col_idx, value=valor)
                    cell.border = thin_border

                    # Colorear según estado
                    if col_idx == 2:  # Columna Estado
                        if valor == "APROBADO":
                            cell.fill = PatternFill(start_color="d1fae5", end_color="d1fae5", fill_type="solid")
                        elif valor == "REQUIERE_APROBACION" or (valor == "PREAPROBADO" and diferencia > 0):
                            cell.fill = PatternFill(start_color="fef3c7", end_color="fef3c7", fill_type="solid")

                row_num += 1

            except Exception as e:
                logger.error(f"Error procesando planilla {doc.get('planilla', 'desconocido')}: {str(e)}")
                continue  # Saltar esta fila y continuar con la siguiente

        # Guardar en memoria
        output = io.BytesIO()
        wb.save(output)
        output.seek(0)

        logger.info(f"Excel generado con {row_num - 1} filas de datos")

        # Retornar archivo
        return Response(
            content=output.read(),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename=planillas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al exportar Excel: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error al exportar Excel: {str(e)}"
        )


@router.get("/obtener-resultados-recientes")
async def obtener_resultados_recientes(limite: int = 100):
    """
    Obtiene todos los planillas de pedidos_medical (cada documento es una planilla).
    Sin filtro por usuario, cualquiera puede consultar.
    """
    try:
        logger.info(f"[OBTENER RESULTADOS] Limite: {limite}")

        # Contar total de documentos (excluyendo fusionadas)
        total_docs = coleccion_pedidos_medical.count_documents({"fusionada": {"$ne": True}})
        logger.info(f"[OBTENER RESULTADOS] Total documentos en coleccion (excluyendo fusionadas): {total_docs}")

        # Traer todas las planillas (documentos independientes), ordenadas por fecha reciente
        # EXCLUYENDO las planillas fusionadas (fusionada: true)
        planillas = list(coleccion_pedidos_medical.find(
            {"fusionada": {"$ne": True}}
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


# ============= ENDPOINTS PARA GESTIÓN DE CAUSALES =============

@router.get("/causales")
async def obtener_causales():
    """
    Obtiene todas las causales activas para fusión de planillas.
    """
    try:
        causales = list(coleccion_causales.find({"activo": True}))
        for c in causales:
            c["_id"] = str(c["_id"])
        return {"causales": causales}
    except Exception as e:
        logger.error(f"Error al obtener causales: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al obtener causales: {str(e)}")


@router.get("/causales/todas")
async def obtener_todas_causales():
    """
    Obtiene todas las causales (activas e inactivas) - solo para admin.
    """
    try:
        causales = list(coleccion_causales.find({}).sort("fecha_creacion", -1))
        for c in causales:
            c["_id"] = str(c["_id"])
        return {"causales": causales}
    except Exception as e:
        logger.error(f"Error al obtener todas las causales: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al obtener causales: {str(e)}")


@router.post("/causales")
async def crear_causal(request: CausalRequest):
    """
    Crea una nueva causal de fusión.
    """
    try:
        # Verificar si ya existe una causal con ese nombre
        existente = coleccion_causales.find_one({"nombre": {"$regex": f"^{request.nombre}$", "$options": "i"}})
        if existente:
            raise HTTPException(status_code=400, detail="Ya existe una causal con ese nombre")

        nueva_causal = {
            "nombre": request.nombre,
            "activo": request.activo,
            "fecha_creacion": datetime.now()
        }
        result = coleccion_causales.insert_one(nueva_causal)
        nueva_causal["_id"] = str(result.inserted_id)
        logger.info(f"Causal creada: {nueva_causal['_id']} - {request.nombre}")
        return {"mensaje": "Causal creada exitosamente", "causal": nueva_causal}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al crear causal: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al crear causal: {str(e)}")


@router.put("/causales/{causal_id}")
async def actualizar_causal(causal_id: str, request: CausalRequest):
    """
    Actualiza una causal existente.
    """
    try:
        from bson import ObjectId
        if not ObjectId.is_valid(causal_id):
            raise HTTPException(status_code=400, detail="ID de causal inválido")

        campos_actualizar = {
            "nombre": request.nombre,
            "activo": request.activo
        }
        resultado = coleccion_causales.update_one(
            {"_id": ObjectId(causal_id)},
            {"$set": campos_actualizar}
        )

        if resultado.matched_count == 0:
            raise HTTPException(status_code=404, detail="Causal no encontrada")

        logger.info(f"Causal actualizada: {causal_id}")
        return {"mensaje": "Causal actualizada exitosamente"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al actualizar causal: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al actualizar causal: {str(e)}")


@router.delete("/causales/{causal_id}")
async def eliminar_causal(causal_id: str):
    """
    Elimina (desactiva) una causal.
    """
    try:
        from bson import ObjectId
        if not ObjectId.is_valid(causal_id):
            raise HTTPException(status_code=400, detail="ID de causal inválido")

        resultado = coleccion_causales.update_one(
            {"_id": ObjectId(causal_id)},
            {"$set": {"activo": False}}
        )

        if resultado.matched_count == 0:
            raise HTTPException(status_code=404, detail="Causal no encontrada")

        logger.info(f"Causal eliminada (desactivada): {causal_id}")
        return {"mensaje": "Causal eliminada exitosamente"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al eliminar causal: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al eliminar causal: {str(e)}")


@router.post("/causales/inicializar")
async def inicializar_causales():
    """
    Inicializa las causales por defecto si no existen.
    """
    try:
        causales_por_defecto = [
            {"nombre": "lleva paqueteo", "activo": True},
            {"nombre": "no se consiguio vehiculo", "activo": True}
        ]

        creadas = []
        for causal_def in causales_por_defecto:
            existente = coleccion_causales.find_one({"nombre": {"$regex": f"^{causal_def['nombre']}$", "$options": "i"}})
            if not existente:
                nueva_causal = {
                    "nombre": causal_def["nombre"],
                    "activo": causal_def["activo"],
                    "fecha_creacion": datetime.now()
                }
                result = coleccion_causales.insert_one(nueva_causal)
                nueva_causal["_id"] = str(result.inserted_id)
                creadas.append(nueva_causal)
                logger.info(f"Causal inicializada: {nueva_causal['_id']} - {causal_def['nombre']}")

        if creadas:
            return {"mensaje": f"Se inicializaron {len(creadas)} causales", "causales": creadas}
        else:
            return {"mensaje": "Las causales por defecto ya existen", "causales": []}
    except Exception as e:
        logger.error(f"Error al inicializar causales: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al inicializar causales: {str(e)}")
