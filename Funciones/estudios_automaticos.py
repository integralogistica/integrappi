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
import os
import re
import uuid
from datetime import datetime, timedelta

import pytz
from google.cloud import storage

from bd.bd_cliente import bd_cliente
from pydantic import ValidationError
from fastapi import HTTPException

# Handlers del wrapper (funciones async planas, no HTTP interno).
# Import a nivel de módulo para que los tests puedan patchearlos
# por nombre en este namespace.
from rutas.tusdatos import (
    ConsultaCompletaIn,
    NitIn,
    VehiculoCompletaIn,
    _bajar_reporte_car_pdf as _td_reporte_car_pdf,
    _bajar_reporte_nit_pdf as _td_reporte_nit_pdf,
    _bajar_reporte_pdf as _td_reporte_pdf,
    _configurado,
    _esperar as _td_esperar,
    consulta_completa,
    consulta_vehiculo,
    reintentar_fuentes as _td_reintentar,
    verificar_nit as _td_verificar_nit,
)

# Archivo de los PDFs de reporte en el bucket PRIVADO (2026-09-28, pedido del
# usuario: las descargas deben quedar garantizadas aunque la cuenta del
# proveedor se cancele). Misma carpeta Vehiculos/ del módulo → las URLs
# firmadas y exclusiones existentes aplican igual.
PDF_BUCKET = os.getenv("VEHICULOS_BUCKET", "integrapp-privado")
PDF_CARPETA = "Vehiculos"
_TZ_BOGOTA = pytz.timezone("America/Bogota")

# ── VIGENCIA DE LA CORRIDA (2026-09-28) ──
# La corrida de estudios vence a los N meses. La renovación AUTOMÁTICA fue
# eliminada (2026-10-05, orden del usuario): la actualización es manual desde
# el módulo «Estudios por antigüedad» de /revision.
VIGENCIA_MESES = int(os.getenv("ESTUDIOS_VIGENCIA_MESES", "12"))


def _fecha_vencimiento(desde: datetime) -> datetime:
    """Vence a los VIGENCIA_MESES de la corrida (aprox mes calendario)."""
    meses = max(1, VIGENCIA_MESES)
    anio = desde.year + (desde.month - 1 + meses) // 12
    mes = (desde.month - 1 + meses) % 12 + 1
    try:
        return desde.replace(year=anio, month=mes)
    except ValueError:  # 31 de un mes corto → día 28
        return desde.replace(year=anio, month=mes, day=28)

_cliente_storage = None


def _obtener_cliente_storage():
    """Cliente GCS perezoso y compartido (patrón de vehiculos.py)."""
    global _cliente_storage
    if _cliente_storage is None:
        _cliente_storage = storage.Client()
    return _cliente_storage


def _subir_blob_pdf(ruta_blob: str, contenido: bytes) -> None:
    bucket = _obtener_cliente_storage().bucket(PDF_BUCKET)
    bucket.blob(ruta_blob).upload_from_string(contenido, content_type="application/pdf")


async def _archivar_pdf_reporte(placa: str, sujeto: dict, reporte_id) -> dict | None:
    """
    Descarga el PDF del reporte del proveedor y lo archiva EN el bucket
    privado: `Vehiculos/{PLACA}/{AAAA-MM-DD}/estudioAuto_{sujeto}_{reporte_id}.pdf`
    (misma nomenclatura/fechas del módulo, zona Bogotá). BEST-EFFORT: un fallo
    JAMÁS tumba el estudio (el reporte sigue servible en vivo por reporte_id).
    La ruta es determinística por reporte_id → el reintento de fuentes
    SOBREESCRIBE el blob con la versión regenerada.
    """
    try:
        # Cada tipo de reporte tiene SU endpoint de PDF en el proveedor:
        # empresa → report_nit_pdf, vehículo (launch/car) → report_car_pdf,
        # persona → report_pdf. Los handlers devuelven BYTES.
        tipo_sujeto = sujeto.get("tipo")
        if tipo_sujeto == "empresa":
            handler_pdf = _td_reporte_nit_pdf
        elif tipo_sujeto == "vehiculo":
            handler_pdf = _td_reporte_car_pdf
        else:
            handler_pdf = _td_reporte_pdf
        contenido = await handler_pdf(str(reporte_id))
        if not contenido:
            return None
        if sujeto.get("tipo") == "persona":
            sufijo = f"persona_{_digitos(sujeto.get('cedula'))}"
        elif sujeto.get("tipo") == "empresa":
            sufijo = f"empresa_{_digitos(sujeto.get('nit'))}"
        else:
            sufijo = f"vehiculo_{str(sujeto.get('placa') or '').upper()}"
        fecha = datetime.now(_TZ_BOGOTA).strftime("%Y-%m-%d")
        # La placa va AL FINAL del nombre (2026-10-02, pedido del usuario):
        # estudioAuto_persona_1003823519_{reporte_id}_{placa}.pdf — el archivo
        # identifica su vehículo aunque se descargue o mueva de la carpeta.
        ruta_blob = (f"{PDF_CARPETA}/{placa}/{fecha}/"
                     f"estudioAuto_{sufijo}_{reporte_id}_{placa.lower()}.pdf")
        _subir_blob_pdf(ruta_blob, contenido)
        return {"ruta": ruta_blob, "tamano": len(contenido),
                "archivado_en": datetime.utcnow()}
    except Exception as e:
        print(f"[estudios-auto] No se pudo archivar el PDF de {placa} "
              f"(reporte {reporte_id}): {e}")
        return None

bd = bd_cliente['integra']
coleccion_vehiculos = bd['vehiculos']

# ── SWITCH DEL DISPARO AUTOMÁTICO (2026-10-05, pedido del usuario) ──
# Permite pausar TEMPORALMENTE el disparo automático de estudios cuando el
# conductor envía el vehículo a revisión (caso de uso: ingreso masivo de ~300
# conductores históricos que se registran solos — el estudio lo corre
# Seguridad manualmente desde /revision con «Volver a consultar» o el módulo
# «Estudios por antigüedad»). El switch vive en la colección
# `config_estudios` (doc `_id: "global"`, campo `auto_disparo`), se cambia
# con un BOTÓN en /revision y aplica al instante sin redeploy. Kill-switch
# adicional por env: ESTUDIOS_AUTO_DISPARAR=false lo apaga SIEMPRE.
coleccion_config = bd['config_estudios']


def auto_disparo_habilitado() -> bool:
    """True si el disparo automático está activo. Prioridad:
    env ESTUDIOS_AUTO_DISPARAR=false (off duro) > switch en BD > default ON."""
    env = os.getenv("ESTUDIOS_AUTO_DISPARAR", "true").strip().lower()
    if env in ("0", "false", "no", "off"):
        return False
    try:
        doc = coleccion_config.find_one({"_id": "global"})
        return bool(doc.get("auto_disparo", True)) if doc else True
    except Exception as e:
        print(f"[estudios-auto] No se pudo leer config_estudios ({e}); disparo ON")
        return True


def fijar_auto_disparo(habilitado: bool) -> bool:
    """Escribe el switch y devuelve el estado efectivo resultante."""
    coleccion_config.update_one(
        {"_id": "global"},
        {"$set": {"auto_disparo": bool(habilitado), "actualizado_en": datetime.utcnow()}},
        upsert=True,
    )
    return auto_disparo_habilitado()

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


# Roles en orden de presentación. Tercer elemento: campo de la fecha de
# expedición del documento (opcional para CC, afina el match); cuarto: campo
# del TIPO de documento — propietario/tenedor pueden ser EMPRESA (NIT) y van
# por el flujo de validación de empresa del proveedor (2026-09-28).
# (2026-10-05) Dueño del remolque: figura OPCIONAL — solo entra como sujeto
# si `RemolDuenoDocumento` viene diligenciado (dedup por dígitos igual que
# las demás: si es la misma persona, combina roles en un solo estudio).
_ROLES = (("conductor", "condCedulaCiudadania", "condFechaExpedicion", None),
          ("propietario", "propDocumento", None, "propTipoDocumento"),
          ("tenedor", "tenedDocumento", None, "tenedTipoDocumento"),
          ("dueño_remolque", "RemolDuenoDocumento", None, "RemolDuenoTipoDocumento"))


def _es_nit(vehiculo: dict, campo_tipo: str) -> bool:
    """La figura es una EMPRESA: su tipo de documento (leído del RUT por la
    IA o elegido en el form) es NIT."""
    return "NIT" in str(vehiculo.get(campo_tipo) or "").upper()


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
    Sujetos a consultar: personas por cédula (deduplicadas, roles
    combinados) + EMPRESAS por NIT (propietario/tenedor con tipo NIT — van
    por el flujo de validación de empresa) + el sujeto vehículo (placa con
    el documento del propietario y tdoc derivado del RUT).
    """
    personas, empresas = {}, {}
    for rol, campo, campo_fecha, campo_tipo in _ROLES:
        numero = _digitos(vehiculo.get(campo))
        if not numero:
            continue
        if campo_tipo and _es_nit(vehiculo, campo_tipo):
            if rol not in empresas.setdefault(numero, []):
                empresas[numero].append(rol)
            continue
        datos = personas.setdefault(numero, {"roles": [], "fecha": ""})
        if rol not in datos["roles"]:
            datos["roles"].append(rol)
        if campo_fecha and not datos["fecha"]:
            datos["fecha"] = _fecha_exp_tusdatos(vehiculo.get(campo_fecha))

    sujetos = [
        {"tipo": "persona", "cedula": cedula,
         "roles": datos["roles"], "fecha_expedicion": datos["fecha"],
         "clave": f"persona:{cedula}"}
        for cedula, datos in sorted(personas.items())
    ] + [
        {"tipo": "empresa", "nit": nit, "roles": roles,
         "clave": f"empresa:{nit}"}
        for nit, roles in sorted(empresas.items())
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
        elif e.get("tipo") == "empresa" and e.get("nit"):
            claves.append(f"empresa:{e.get('nit')}")
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
    elif sujeto["tipo"] == "empresa":
        estudio["nit"] = sujeto["nit"]
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


async def disparar_estudios(placa: str, re_revision: bool = False,
                            forzar: bool = False) -> None:
    """
    Escribe el array de estudios (pendiente) en el doc del vehículo y
    lanza la ejecución en background. Fire-and-forget: JAMÁS lanza
    (el llamador la envuelve en try/except de todos modos).
    `forzar=True` → force del proveedor (re-consulta REAL, no su caché por
    cédula: lo usan la renovación por vigencia y el botón Volver a consultar).
    Además sella `estudiosVigencia {desde, vence}` (la corrida vence a los
    ESTUDIOS_VIGENCIA_MESES — la renueva el barrido automático).
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

        ahora = datetime.utcnow()
        vigencia = {"desde": ahora, "vence": _fecha_vencimiento(ahora)}

        if not _configurado():
            # Sin credenciales: estudios marcados en error accionable
            # (visible en /revision; no toca los portales ni gasta nada).
            estudios = [_estudio_base(s, "error", "configuracion_faltante")
                        for s in sujetos]
            for e in estudios:
                e["finalizado_en"] = e["iniciado_en"]
            coleccion_vehiculos.update_one(
                {"placa": placa},
                {"$set": {"estudiosSeguridadAuto": estudios,
                          "estudiosVigencia": vigencia}})
            return

        if forzar:
            for s in sujetos:
                if s["tipo"] == "persona":
                    s["force"] = True

        coleccion_vehiculos.update_one(
            {"placa": placa},
            {"$set": {"estudiosSeguridadAuto": [
                _estudio_base(s, "pendiente") for s in sujetos],
                "estudiosVigencia": vigencia}})

        await _ejecutar_estudios(placa, sujetos)
    except Exception as e:  # best-effort total: jamás tumba el endpoint
        print(f"[estudios-auto] Error disparando estudios de {placa}: {e}")
    finally:
        _EN_CURSO.discard(placa)


# ── RENOVACIÓN POR VIGENCIA ────────────────────────────────────────────────
# (2026-10-05, orden del usuario) La renovación AUTOMÁTICA fue ELIMINADA: los
# estudios se actualizan MANUALMENTE desde el módulo «Estudios por antigüedad»
# de /revision (botón por placa → POST /vehiculos/estudios-seguridad/{placa}/
# disparar, force=true). El sello `estudiosVigencia` se sigue escribiendo en
# cada corrida: alimenta el chip de vigencia, el Excel y la reutilización
# entre vehículos.


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

    def _clave_de_estudio(e: dict) -> str:
        if e.get("tipo") == "vehiculo":
            return f"vehiculo:{e.get('placa')}"
        if e.get("tipo") == "empresa":
            return f"empresa:{e.get('nit')}"
        return f"persona:{e.get('cedula')}"

    por_clave = {_clave_de_estudio(e): e.get("id")
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
    # Fallida = valor no booleano con texto ('Error', 'Página no disponible'…);
    # la cadena vacía significa que la fuente NO APLICÓ a la consulta.
    def _es_fallida(v):
        return v is not True and v is not False and bool(str(v if v is not None else "").strip())

    if not any(_es_fallida(v) for v in (estudio.get("fuentes") or {}).values()):
        raise HTTPException(status_code=422, detail="El estudio no tiene fuentes fallidas.")

    # El proveedor NO soporta reintentar fuentes de reportes de VEHÍCULO
    # (launch/car): /api/retry/{id} responde {} para sus ids (probado en vivo
    # 2026-10-07 — antes eso caía al 502 engañoso "estado=desconocido").
    if estudio.get("tipo") == "vehiculo":
        raise HTTPException(
            status_code=422,
            detail="El proveedor no soporta reintentar fuentes de estudios de "
                   "VEHÍCULO. Usa «Volver a consultar» para regenerarlo "
                   "(consume una consulta).",
        )
    # typedoc por tipo: empresa relanza con NIT (retry persona con CC).
    typedoc = "NIT" if estudio.get("tipo") == "empresa" else "CC"
    respuesta = await _td_reintentar(estudio["reporte_id"], typedoc=typedoc)
    # El retry puede devolver un jobid nuevo (sondear) o el resultado directo.
    jobid = respuesta.get("jobid") if isinstance(respuesta, dict) else None
    if not jobid and not (isinstance(respuesta, dict) and respuesta.get("estado")):
        # Respuesta vacía (el proveedor rechazó el retry en silencio).
        raise HTTPException(
            status_code=422,
            detail="El proveedor no aceptó el reintento (respondió vacío). "
                   "Usa «Volver a consultar» para regenerar el estudio.",
        )
    resultado = (await _td_esperar(jobid, 180, 5)) if jobid else respuesta

    try:
        cambios = _extraer_resultado({"lanzamiento": respuesta, "resultado": resultado})
    except ValueError as e:
        raise HTTPException(status_code=502, detail=str(e)[:300])
    cambios["reporte_id"] = resultado.get("id") or estudio["reporte_id"]
    # El reporte se regeneró: re-archivar el PDF (la ruta determinística por
    # reporte_id SOBREESCRIBE el blob con la versión nueva).
    sujeto = {"tipo": estudio.get("tipo"), "cedula": estudio.get("cedula"),
              "placa": estudio.get("placa")}
    pdf_gcs = await _archivar_pdf_reporte(placa, sujeto, cambios["reporte_id"])
    if pdf_gcs:
        cambios["pdf_gcs"] = pdf_gcs
    _actualizar_estudio(placa, estudio_id, cambios)
    return cambios


def _log(mensaje: str) -> None:
    """print SEGURO: en consolas sin UTF-8 (Windows/cp1252) el emoji del
    mensaje de reúso reventaba con UnicodeEncodeError DENTRO del try y el
    estudio quedaba marcado 'error' pese a haber salido bien. Un fallo de
    stdout jamás es un fallo del estudio."""
    try:
        print(mensaje)
    except UnicodeEncodeError:
        print(mensaje.encode("ascii", "replace").decode("ascii"))


def _buscar_estudio_reutilizable(sujeto: dict) -> tuple:
    """
    Reutilización ENTRE VEHÍCULOS (2026-09-28, pedido del usuario: no gastar
    consultas de una cédula ya estudiada): busca en TODAS las placas el
    estudio FINALIZADO más reciente de este sujeto (cédula, NIT o placa)
    cuya antigüedad no supere la vigencia configurada. Devuelve
    (placa_origen, estudio) o (None, None).
    """
    if sujeto["tipo"] == "vehiculo":
        campo, valor = "placa", sujeto.get("placa")
    elif sujeto["tipo"] == "empresa":
        campo, valor = "nit", sujeto.get("nit")
    else:
        campo, valor = "cedula", sujeto["cedula"]
    cond = {campo: valor, "estado": "finalizado"}
    try:
        limite = datetime.utcnow() - timedelta(days=int(VIGENCIA_MESES * 30.44))
        mejor, placa_mejor = None, None
        for doc in coleccion_vehiculos.find(
                {"estudiosSeguridadAuto": {"$elemMatch": cond}},
                {"placa": 1, "estudiosSeguridadAuto": 1}):
            for e in doc.get("estudiosSeguridadAuto") or []:
                if not isinstance(e, dict) or e.get("estado") != "finalizado":
                    continue
                if e.get(campo) != valor:
                    continue
                fecha = e.get("finalizado_en") or e.get("iniciado_en")
                if not isinstance(fecha, datetime) or fecha < limite:
                    continue
                if mejor is None or fecha > (mejor.get("finalizado_en") or datetime.min):
                    mejor, placa_mejor = e, doc.get("placa")
        return placa_mejor, mejor
    except Exception as e:
        print(f"[estudios-auto] Buscando estudio reutilizable: {e}")
        return None, None


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
    if sujeto["tipo"] == "empresa":
        # Validación de EMPRESA por NIT (flujos propios del proveedor):
        # launch/verify/nit → jobid → sondeo hasta el resultado.
        lanzamiento = await _td_verificar_nit(NitIn(nit=int(sujeto["nit"])))
        jobid = lanzamiento.get("jobid") if isinstance(lanzamiento, dict) else None
        resultado = ((await _td_esperar(jobid, 300, 5)) if jobid
                     else (lanzamiento if isinstance(lanzamiento, dict) else {}))
        return {"lanzamiento": lanzamiento, "resultado": resultado}
    if sujeto["tipo"] == "persona":
        kwargs = {"typedoc": "CC", "doc": sujeto["cedula"]}
        # fechaE opcional para CC: afina el match (cédulas comunes).
        if sujeto.get("fecha_expedicion"):
            kwargs["fechaE"] = sujeto["fecha_expedicion"]
        # force=True (renovación/Volver a consultar): re-consulta REAL,
        # no la caché por cédula del proveedor.
        if sujeto.get("force"):
            kwargs["force"] = True
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
        # Reutilización (sin force): si OTRA placa ya estudió este sujeto
        # dentro de la vigencia, se copia el resultado — cero gasto.
        if not sujeto.get("force"):
            placa_origen, previo = _buscar_estudio_reutilizable(sujeto)
            if previo:
                cambios = {k: previo[k] for k in
                           ("hallazgo", "categoria", "fuentes", "reporte_id", "pdf_gcs")
                           if previo.get(k) is not None}
                cambios.update({
                    "estado": "finalizado",
                    "reutilizado_de": placa_origen,
                    "finalizado_en": datetime.utcnow(),
                })
                _actualizar_estudio(placa, estudio_id, cambios)
                _log(f"[estudios-auto] ♻️ {placa}: estudio de "
                     f"{sujeto.get('cedula') or sujeto.get('placa')} "
                     f"reutilizado de {placa_origen} (sin gasto)")
                return

        respuesta = await _llamar_proveedor(sujeto)
        cambios = _extraer_resultado(respuesta)
        # Archivo del PDF en el bucket privado (best-effort: sin él el
        # estudio sigue sirviendo el reporte en vivo por reporte_id).
        if cambios.get("reporte_id"):
            pdf_gcs = await _archivar_pdf_reporte(placa, sujeto, cambios["reporte_id"])
            if pdf_gcs:
                cambios["pdf_gcs"] = pdf_gcs
        _actualizar_estudio(placa, estudio_id, cambios)
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
