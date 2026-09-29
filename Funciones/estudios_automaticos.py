# ============================================================
# ESTUDIOS DE SEGURIDAD AUTOMÁTICOS (por ahora vía TusDatos)
# ============================================================
# Cuando el conductor termina de subir la documentación y envía
# el vehículo a revisión (`completado_revision`), este módulo
# dispara en background estudios de seguridad para las personas
# involucradas y el vehículo:
#
#   - cédula del CONDUCTOR   (condCedulaCiudadania)
#   - cédula del TENEDOR     (tenedDocumento)
#   - cédula del PROPIETARIO (propDocumento)
#   - la PLACA del vehículo  (con la cédula del propietario)
#
# Si dos o tres figuras comparten cédula, UN solo estudio con los
# roles combinados (misma deduplicación de `_figuras_iguales`).
#
# Motor: por ahora la API comercial TusDatos vía el wrapper
# `rutas/tusdatos.py` (ver TUSDATOS.md). El shape del doc es
# agnóstico del proveedor (`proveedor`) para poder migrar después
# a la herramienta propia (seguriDatia, ver SEGURIDAD.md).
#
# Persistencia: array `estudiosSeguridadAuto` EN el doc del
# vehículo (colección `vehiculos`). El PDF del reporte NO se
# guarda (≈2.5 MB): se sirve por demanda desde el wrapper con el
# `reporte_id`. Reglas:
#   - disparo SIEMPRE al llegar a completado_revision;
#   - RE-REVISIÓN (aprobado editado): solo re-dispara si
#     cambiaron cédulas o placa (huella de sujetos);
#   - sin credenciales TUSDATOS_* → estudios en error
#     `configuracion_faltante` (nunca tumba el endpoint).
# ============================================================
import asyncio
import re
import uuid
from datetime import datetime

from bd.bd_cliente import bd_cliente
from pydantic import ValidationError
from fastapi import HTTPException

# Handlers del wrapper (funciones async planas, no HTTP interno).
# Import a nivel de módulo para que los tests puedan patchearlos
# por nombre en este namespace.
from rutas.tusdatos import (
    ConsultaCompletaIn,
    VehiculoCompletaIn,
    _configurado,
    _esperar as _td_esperar,
    consulta_completa,
    consulta_vehiculo,
    reintentar_fuentes as _td_reintentar,
)

bd = bd_cliente['integra']
coleccion_vehiculos = bd['vehiculos']

# Placas que el wrapper de vehículo acepta (VehiculoCompletaIn exige
# exactamente 6 caracteres; el registro de vehículos acepta 4-7).
_PLACA_VEHICULO_RE = re.compile(r"^[A-Z0-9]{6}$")

# Guard de re-entrada: una sola corrida de estudios por placa a la
# vez (el disparo es fire-and-forget; dos transiciones rápidas no
# deben lanzar tasks paralelas que se pisen los $set).
_EN_CURSO = set()

# Corridas anteriores conservadas en `historialEstudios` (append por el
# frente, tope para no crecer sin límite).
MAX_CORRIDAS_HISTORIAL = 10


def _archivar_corrida_anterior(placa: str, vehiculo: dict) -> None:
    """La corrida vigente pasa al historial antes de ser reemplazada
    (cada estudio conserva su fecha y su reporte_id → la tabla de
    históricos de /revision puede listarlos y abrirlos)."""
    previos = vehiculo.get("estudiosSeguridadAuto")
    if not previos:
        return
    try:
        coleccion_vehiculos.update_one(
            {"placa": placa},
            {"$push": {"historialEstudios": {
                "$each": [{"fecha": datetime.utcnow(), "estudios": previos}],
                "$position": 0,
                "$slice": MAX_CORRIDAS_HISTORIAL,
            }}})
    except Exception as e:
        print(f"[estudios-auto] No se pudo archivar la corrida de {placa}: {e}")


def _digitos(valor) -> str:
    """Copia local de `_solo_digitos` (rutas/vehiculos.py) para no
    arrastrar el import circular ni la cadena GCS/PIL de ese módulo."""
    return re.sub(r"\D", "", str(valor or ""))


# Roles en orden de presentación. El tercer elemento es el campo de la
# fecha de expedición del documento (TusDatos la acepta opcional para CC
# y afina el match en cédulas comunes); hoy solo el conductor la tiene.
_ROLES = (("conductor", "condCedulaCiudadania", "condFechaExpedicion"),
          ("propietario", "propDocumento", None),
          ("tenedor", "tenedDocumento", None))


def _fecha_exp_tusdatos(valor) -> str:
    """Normaliza la fecha de expedición a dd/mm/yyyy (formato fechaE de
    TusDatos). Acepta ISO aaaa-mm-dd (como la guarda la IA); devuelve ""
    si no es parseable."""
    texto = str(valor or "").strip()[:10]
    if len(texto) == 10 and texto[4] == "-" and texto[7] == "-":
        return f"{texto[8:10]}/{texto[5:7]}/{texto[0:4]}"
    return ""


def _tdoc_tusdatos(vehiculo: dict) -> str:
    """Tipo de documento del PROPIETARIO para el estudio de vehículo,
    derivado del `propTipoDocumento` que lee la IA del RUT (CC/CE/PP/TI/NIT).
    Default CC (persona natural con cédula)."""
    tipo = str(vehiculo.get("propTipoDocumento") or "").upper()
    if "NIT" in tipo:
        return "NIT"
    if "EXTRANJER" in tipo:
        return "CE"
    if "PASAPORTE" in tipo:
        return "PP"
    if "TARJETA" in tipo:
        return "TI"
    return "CC"


def sujetos_estudio(vehiculo: dict) -> list:
    """
    Sujetos a consultar con deduplicación de personas: cédula
    normalizada → roles combinados. Además del sujeto vehículo
    (placa + cédula del propietario que exige el portal, con el tipo de
    documento derivado del RUT).
    """
    por_cedula = {}
    for rol, campo, campo_fecha in _ROLES:
        cedula = _digitos(vehiculo.get(campo))
        if not cedula:
            continue
        por_cedula.setdefault(cedula, {"roles": [], "fecha": ""})
        if rol not in por_cedula[cedula]["roles"]:
            por_cedula[cedula]["roles"].append(rol)
        if campo_fecha and not por_cedula[cedula]["fecha"]:
            por_cedula[cedula]["fecha"] = _fecha_exp_tusdatos(vehiculo.get(campo_fecha))

    sujetos = [
        {"tipo": "persona", "cedula": cedula,
         "roles": datos["roles"], "fecha_expedicion": datos["fecha"],
         "clave": f"persona:{cedula}"}
        for cedula, datos in sorted(por_cedula.items())
    ]

    placa = str(vehiculo.get("placa") or "").strip().upper()
    cedula_prop = _digitos(vehiculo.get("propDocumento"))
    if placa:
        sujetos.append({
            "tipo": "vehiculo", "placa": placa,
            "cedula_propietario": cedula_prop,
            "tdoc_propietario": _tdoc_tusdatos(vehiculo),
            "clave": f"vehiculo:{placa}",
        })
    return sujetos


def _huella_sujetos(sujetos: list) -> str:
    """Firma comparable entre corridas (regla de re-revisión)."""
    return "|".join(sorted(s["clave"] for s in sujetos))


def _huella_estudios_previos(estudios: list) -> str:
    """Huella de los sujetos que YA tienen estudio persistido."""
    if not isinstance(estudios, list):
        return ""
    claves = []
    for e in estudios:
        if not isinstance(e, dict):
            continue
        if e.get("tipo") == "vehiculo":
            claves.append(f"vehiculo:{e.get('placa')}")
        elif e.get("cedula"):
            claves.append(f"persona:{e.get('cedula')}")
    return "|".join(sorted(claves))


def _estudio_base(sujeto: dict, estado: str, error: str = None) -> dict:
    estudio = {
        "id": uuid.uuid4().hex[:8],
        "tipo": sujeto["tipo"],
        "estado": estado,  # pendiente | en_curso | finalizado | error
        "proveedor": "tusdatos",
        "iniciado_en": datetime.utcnow(),
    }
    if sujeto["tipo"] == "persona":
        estudio["cedula"] = sujeto["cedula"]
        estudio["roles"] = sujeto["roles"]
    else:
        estudio["placa"] = sujeto["placa"]
        estudio["cedula_propietario"] = sujeto["cedula_propietario"]
    if error:
        estudio["error"] = error[:300]
    return estudio


def _actualizar_estudio(placa: str, estudio_id: str, cambios: dict) -> None:
    """$set posicional sobre el elemento del array con `id` coincidente."""
    operacion = {"$set": {
        f"estudiosSeguridadAuto.$[e].{campo}": valor
        for campo, valor in cambios.items()
    }}
    coleccion_vehiculos.update_one(
        {"placa": placa},
        operacion,
        array_filters=[{"e.id": estudio_id}],
    )


async def disparar_estudios(placa: str, re_revision: bool = False) -> None:
    """
    Escribe el array de estudios (pendiente) en el doc del vehículo y
    lanza la ejecución en background. Fire-and-forget: JAMÁS lanza
    (el llamador la envuelve en try/except de todos modos).
    """
    placa = str(placa or "").strip().upper()
    if not placa or placa in _EN_CURSO:
        return

    try:
        vehiculo = coleccion_vehiculos.find_one({"placa": placa})
        if not vehiculo:
            return

        sujetos = sujetos_estudio(vehiculo)

        # Re-revisión (aprobado editado): si las cédulas y la placa no
        # cambiaron, se conservan los estudios de la corrida anterior.
        if re_revision:
            previa = _huella_estudios_previos(vehiculo.get("estudiosSeguridadAuto"))
            if previa and previa == _huella_sujetos(sujetos):
                return

        _EN_CURSO.add(placa)

        # La corrida anterior (si la hay) pasa al historial ANTES de pisar.
        _archivar_corrida_anterior(placa, vehiculo)

        if not _configurado():
            # Sin credenciales: estudios marcados en error accionable
            # (visible en /revision; no toca los portales ni gasta nada).
            estudios = [_estudio_base(s, "error", "configuracion_faltante")
                        for s in sujetos]
            for e in estudios:
                e["finalizado_en"] = e["iniciado_en"]
            coleccion_vehiculos.update_one(
                {"placa": placa}, {"$set": {"estudiosSeguridadAuto": estudios}})
            return

        coleccion_vehiculos.update_one(
            {"placa": placa},
            {"$set": {"estudiosSeguridadAuto": [
                _estudio_base(s, "pendiente") for s in sujetos]}})

        await _ejecutar_estudios(placa, sujetos)
    except Exception as e:  # best-effort total: jamás tumba el endpoint
        print(f"[estudios-auto] Error disparando estudios de {placa}: {e}")
    finally:
        _EN_CURSO.discard(placa)


async def _ejecutar_estudios(placa: str, sujetos: list) -> None:
    """
    Corre todos los estudios concurrentemente (máx 4 sujetos; el límite
    de 3 simultáneas de TusDatos aplica a LOTES, no a consultas
    individuales — si el upstream llegara a responder 429 se verá como
    un estudio en error y la siguiente corrida lo re-intenta).
    """
    # Recuperar los ids asignados en disparar_estudios para actualizar
    # por id (los sujetos y el array comparten orden).
    vehiculo = coleccion_vehiculos.find_one(
        {"placa": placa}, {"estudiosSeguridadAuto": 1})
    persistidos = (vehiculo or {}).get("estudiosSeguridadAuto") or []
    por_clave = {f"persona:{e.get('cedula')}" if e.get("tipo") == "persona"
                 else f"vehiculo:{e.get('placa')}": e.get("id")
                 for e in persistidos if isinstance(e, dict)}

    tareas = []
    for sujeto in sujetos:
        estudio_id = por_clave.get(sujeto["clave"])
        if not estudio_id:
            continue
        tareas.append(_ejecutar_uno(placa, sujeto, estudio_id))
    if tareas:
        await asyncio.gather(*tareas, return_exceptions=True)


def _extraer_resultado(respuesta: dict) -> dict:
    """
    Normaliza la respuesta {lanzamiento, resultado} del wrapper al shape
    persistido. `_esperar` ante timeout NO lanza: devuelve el resultado
    con `_aviso` y estado no-finalizado → eso se trata como error.
    """
    resultado = (respuesta or {}).get("resultado") or {}
    estado = str(resultado.get("estado", "")).lower()
    if estado != "finalizado":
        aviso = resultado.get("_aviso") or f"estado={estado or 'desconocido'}"
        raise ValueError(str(aviso)[:300])

    return {
        "estado": "finalizado",
        "hallazgo": resultado.get("hallazgo"),
        # hallazgos = categoría del hallazgo de mayor severidad
        # (alto|medio|bajo|info|"").
        "categoria": resultado.get("hallazgos") or "",
        "fuentes": resultado.get("results") or {},
        "reporte_id": resultado.get("id"),
        "jobid": (respuesta.get("lanzamiento") or {}).get("jobid"),
        "nombre_lanzamiento": (respuesta.get("lanzamiento") or {}).get("nombre"),
        "finalizado_en": datetime.utcnow(),
    }


async def reintentar_fuentes_estudio(placa: str, estudio_id: str) -> dict:
    """
    Reintenta SOLO las fuentes que quedaron en "Error" de un estudio
    finalizado (botón de /revision): `GET /api/retry/{id}` relanza esas
    fuentes sobre el MISMO reporte (mismo reporte_id). Espera el resultado
    (≤180 s) y persiste el mapa de fuentes actualizado. Devuelve los
    cambios aplicados al estudio.
    """
    placa = str(placa or "").strip().upper()
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")
    estudio = next(
        (e for e in (vehiculo.get("estudiosSeguridadAuto") or [])
         if isinstance(e, dict) and e.get("id") == estudio_id), None)
    if not estudio:
        raise HTTPException(
            status_code=404, detail="Estudio no encontrado en la corrida vigente.")
    if estudio.get("estado") != "finalizado" or not estudio.get("reporte_id"):
        raise HTTPException(
            status_code=422,
            detail="Solo se pueden reintentar las fuentes de un estudio finalizado con reporte.")
    if not any(v == "Error" for v in (estudio.get("fuentes") or {}).values()):
        raise HTTPException(status_code=422, detail="El estudio no tiene fuentes fallidas.")

    respuesta = await _td_reintentar(estudio["reporte_id"], typedoc="CC")
    # El retry puede devolver un jobid nuevo (sondear) o el resultado directo.
    jobid = respuesta.get("jobid") if isinstance(respuesta, dict) else None
    resultado = (await _td_esperar(jobid, 180, 5)) if jobid else (
        respuesta if isinstance(respuesta, dict) else {})

    try:
        cambios = _extraer_resultado({"lanzamiento": respuesta, "resultado": resultado})
    except ValueError as e:
        raise HTTPException(status_code=502, detail=str(e)[:300])
    cambios["reporte_id"] = resultado.get("id") or estudio["reporte_id"]
    _actualizar_estudio(placa, estudio_id, cambios)
    return cambios


def _es_fallo_lanzamiento(exc: HTTPException) -> bool:
    """El upstream falló al INICIAR la consulta (sin jobid): 'realice la
    consulta nuevamente' — transitorio, admite un reintento."""
    detalle = exc.detail
    if isinstance(detalle, dict):
        return "lanzamiento" in detalle or "jobid" in str(detalle.get("detalle", "")).lower()
    return "jobid" in str(detalle).lower()


async def _llamar_proveedor(sujeto: dict) -> dict:
    """Llama al handler del wrapper según el tipo de sujeto. Un fallo de
    LANZAMIENTO se reintenta UNA vez (el propio TusDatos lo sugiere)."""
    ultimo_error = None
    for intento in range(2):
        try:
            return await _invocar_handler(sujeto)
        except HTTPException as e:
            if intento == 0 and _es_fallo_lanzamiento(e):
                ultimo_error = e
                await asyncio.sleep(5)  # transitorio: reintentar una vez
                continue
            raise
    raise ultimo_error  # inalcanzable en la práctica


async def _invocar_handler(sujeto: dict) -> dict:
    """Una llamada (sin reintentos) al handler del wrapper correspondiente."""
    if sujeto["tipo"] == "persona":
        kwargs = {"typedoc": "CC", "doc": sujeto["cedula"]}
        # fechaE opcional para CC: afina el match (cédulas comunes).
        if sujeto.get("fecha_expedicion"):
            kwargs["fechaE"] = sujeto["fecha_expedicion"]
        return await consulta_completa(ConsultaCompletaIn(**kwargs))
    # El estudio de vehículo exige placa de EXACTAMENTE 6 caracteres y el
    # documento del propietario: pre-validar para no construir un modelo que
    # reviente con ValidationError (placas del registro aceptan 4-7) ni
    # gastar una consulta imposible.
    placa_sujeto = str(sujeto.get("placa") or "").upper()
    cedula_prop = _digitos(sujeto.get("cedula_propietario"))
    if not _PLACA_VEHICULO_RE.match(placa_sujeto):
        raise ValueError("placa_invalida")
    if not cedula_prop:
        raise ValueError("cedula_propietario_faltante")
    return await consulta_vehiculo(VehiculoCompletaIn(
        placa=placa_sujeto, doc=int(cedula_prop),
        tdoc=sujeto.get("tdoc_propietario") or "CC"))


async def _ejecutar_uno(placa: str, sujeto: dict, estudio_id: str) -> None:
    """Ejecuta UN estudio y persiste su resultado (o su error)."""
    _actualizar_estudio(placa, estudio_id, {"estado": "en_curso"})
    try:
        respuesta = await _llamar_proveedor(sujeto)
        _actualizar_estudio(placa, estudio_id, _extraer_resultado(respuesta))
    except (HTTPException, ValidationError, ValueError) as e:
        detalle = e.detail if isinstance(e, HTTPException) else str(e)
        if isinstance(detalle, dict):
            # El wrapper anida {detalle, lanzamiento}: mostrar solo lo legible.
            detalle = detalle.get("detalle") or str(detalle)
        _actualizar_estudio(placa, estudio_id, {
            "estado": "error",
            "error": str(detalle)[:300],
            "finalizado_en": datetime.utcnow(),
        })
    except Exception as e:
        _actualizar_estudio(placa, estudio_id, {
            "estado": "error",
            "error": str(e)[:300],
            "finalizado_en": datetime.utcnow(),
        })
