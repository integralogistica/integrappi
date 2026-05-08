from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import httpx
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

load_dotenv()

router = APIRouter(prefix="/siscore", tags=["Siscore"])
logger = logging.getLogger(__name__)

# Configuración WS Siscore V3 (misma que en pedidos_v3)
SISCORE_V3_ENDPOINT = "https://integra-wms.appsiscore.com/app/ws/informe_v3.php"
SISCORE_V3_TOKEN = "n0ML0cFGhJwtq4lsAeUcMzrqkn94gX4TDaPuFbbXpoA"


class ConsultaPlanillasRequest(BaseModel):
    planillas: List[str]
    fecha_inicio: str
    fecha_fin: str
    perfil: Optional[str] = None
    centro_distribucion: Optional[str] = None


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
        registros = respuesta_api.get('data', [])

        logger.info(f"Total registros recibidos de Siscore: {len(registros)}")

        # LOG: Mostrar primeros registros para depurar
        if registros:
            logger.info(f"=== PRIMEROS 3 REGISTROS DE SISCORE ===")
            for i, reg in enumerate(registros[:3]):
                logger.info(f"Registro {i}: Planilla={reg.get('Planilla', 'N/A')}, Codigo Pedido={reg.get('Codigo Pedido', 'N/A')}")
                if i == 0:
                    logger.info(f"  Campos del primer registro: {list(reg.keys()) if isinstance(reg, dict) else 'No es dict'}")

        # Devolver todos los registros tal como vienen de Siscore
        return {
            "registros": registros,
            "total_registros": len(registros),
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
