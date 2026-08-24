from __future__ import annotations

import hashlib
import json
import logging
import unicodedata
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from .config import (COMBINACIONES, HORAS_TOTALES_CARGUE_DEFAULT,
                     HORAS_TOTALES_DESCARGUE_DEFAULT, MESES_RETROCESO_PERIODO)
from .models import calcular_costo_total, decimal_rndc
from .errors import RNDCBusinessError, RNDCCredentialsError, RNDCNoDataError

logger = logging.getLogger(__name__)


def periodo_actual(): return datetime.now(ZoneInfo("America/Bogota")).strftime("%Y%m")


def periodo_anterior(periodo):
    year, month = int(periodo[:4]), int(periodo[4:]) - 1
    if month == 0: year, month = year - 1, 12
    return f"{year:04d}{month:02d}"


def normalizar(value):
    text = unicodedata.normalize("NFKD", str(value or ""))
    return " ".join("".join(c for c in text if not unicodedata.combining(c)).upper().split())


def normalizar_divipola(value):
    text = str(value or "").strip()
    return text.zfill(8) if text.isdigit() else normalizar(text)


def consulta_id(document):
    fields = ("periodo_aplicado", "origen", "destino", "configuracion", "condicion_carga", "tipo_carga", "unidad_transporte", "rutasid")
    raw = "\x1f".join(normalizar(document.get(x)) for x in fields)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _coincide(document, combinacion):
    for field, configured in (("origen", "origen_codigo"), ("destino", "destino_codigo")):
        if document.get(field) and normalizar_divipola(document[field]) != normalizar_divipola(combinacion[configured]):
            return False
    if document.get("configuracion"):
        configuracion_rndc = normalizar(document["configuracion"]).split(" ", 1)[0]
        if configuracion_rndc != normalizar(combinacion["configuracion_codigo"]):
            return False
    condicion_esperada = {"1": "CARGADO", "2": "VACIO"}.get(str(combinacion["condicion_carga_codigo"]), combinacion["condicion_carga"])
    if document.get("condicioncarga") and normalizar(document["condicioncarga"]) != normalizar(condicion_esperada):
        return False
    for field, configured in (("nombretipocarga", "tipo_carga"), ("nombreunidadtransporte", "unidad_transporte")):
        if document.get(field) and normalizar(document[field]) != normalizar(combinacion[configured]):
            return False
    return True


def transformar(document, combinacion, solicitado, aplicado,
                horas_totales_cargue=HORAS_TOTALES_CARGUE_DEFAULT,
                horas_totales_descargue=HORAS_TOTALES_DESCARGUE_DEFAULT):
    valor_moviliza, valor_hora = decimal_rndc(document.get("valormoviliza")), decimal_rndc(document.get("valorhora"))
    horas_cargue = decimal_rndc(horas_totales_cargue)
    horas_descargue = decimal_rndc(horas_totales_descargue)
    horas_total = horas_cargue + horas_descargue
    identity = {**document, "periodo_aplicado": document.get("periodo") or aplicado}
    result = {
        "consulta_id": consulta_id(identity), "periodo_solicitado": solicitado,
        "periodo_aplicado": document.get("periodo") or aplicado,
        "fecha_ingreso_rndc": document.get("fechaingreso"),
        "origen": {"codigo": combinacion["origen_codigo"], "nombre_configurado": combinacion["origen"], "nombre_rndc": document.get("nomorigen")},
        "destino": {"codigo": combinacion["destino_codigo"], "nombre_configurado": combinacion["destino"], "nombre_rndc": document.get("nomdestino")},
        "configuracion": {"codigo": combinacion["configuracion_codigo"], "nombre_configurado": combinacion["configuracion"], "valor_rndc": document.get("configuracion")},
        "condicion_carga": document.get("condicioncarga") or combinacion["condicion_carga_codigo"],
        "tipo_carga": {"codigo": document.get("tipocarga"), "nombre": document.get("nombretipocarga")},
        "unidad_transporte": {"codigo": document.get("unidadtransporte"), "nombre": document.get("nombreunidadtransporte")},
        "ruta": {"id": document.get("rutasid"), "via_estandar": normalizar(document.get("viaestandar")) in {"1", "SI", "TRUE", "S"}, "descripcion": document.get("via"), "kilometros": decimal_rndc(document.get("kilometros")), "horas_recorrido": document.get("horasrecorrido")},
        "costos": {"valor_moviliza": valor_moviliza, "valor_hora": valor_hora, "horas_totales_cargue": horas_cargue, "horas_totales_descargue": horas_descargue, "horas_logisticas_total": horas_total, "costo_total_calculado": calcular_costo_total(valor_moviliza, valor_hora, horas_cargue, horas_descargue)},
        "respuesta_rndc": document, "fuente": "RNDC_SICETAC_WS", "consultado_en": datetime.now(ZoneInfo("America/Bogota")),
    }
    return result


class SicetacService:
    def __init__(self, client, repository): self.client, self.repository = client, repository

    def ejecutar(self, solicitado=None, dry_run=False, progreso=None,
                 horas_totales_cargue=HORAS_TOTALES_CARGUE_DEFAULT,
                 horas_totales_descargue=HORAS_TOTALES_DESCARGUE_DEFAULT):
        solicitado = solicitado or periodo_actual()
        summary = {"combinaciones_configuradas": len(COMBINACIONES), "consultas_exitosas": 0, "documentos_recibidos": 0, "documentos_insertados": 0, "documentos_actualizados": 0, "combinaciones_sin_resultado": 0, "errores": []}
        self.repository.comprobar()
        broad_cache = {}
        for index, combination in enumerate(COMBINACIONES, 1):
            current, documents, valid, received_count = solicitado, [], [], 0
            try:
                for _ in range(MESES_RETROCESO_PERIODO + 1):
                    try:
                        documents = self.client.consultar(current, combination)
                    except (RNDCNoDataError, RNDCBusinessError) as exc:
                        if not isinstance(exc, RNDCNoDataError) and "RNDC13" not in str(exc).upper():
                            raise
                        cache_key = (current, combination["configuracion_codigo"], combination["origen_codigo"])
                        if cache_key not in broad_cache:
                            try:
                                broad_cache[cache_key] = self.client.consultar_amplia(current, combination)
                            except RNDCNoDataError:
                                broad_cache[cache_key] = []
                            except RNDCBusinessError as broad_exc:
                                if "RNDC13" not in str(broad_exc).upper():
                                    raise
                                broad_cache[cache_key] = []
                        documents = broad_cache[cache_key]
                    received_count += len(documents)
                    valid = [x for x in documents if _coincide(x, combination)]
                    if valid: break
                    current = periodo_anterior(current)
                transformed = [transformar(
                    x, combination, solicitado, current,
                    horas_totales_cargue, horas_totales_descargue
                ) for x in valid]
                summary["consultas_exitosas"] += 1
                summary["documentos_recibidos"] += received_count
                if not transformed: summary["combinaciones_sin_resultado"] += 1
                elif not dry_run:
                    inserted, updated = self.repository.upsert_many(transformed)
                    summary["documentos_insertados"] += inserted
                    summary["documentos_actualizados"] += updated
            except RNDCCredentialsError:
                # Las mismas credenciales se usan en todas las combinaciones;
                # continuar solo multiplicaría un error no transitorio.
                raise
            except Exception as exc:
                context = f"{solicitado} {combination['origen_codigo']}->{combination['destino_codigo']} {combination['configuracion_codigo']}"
                logger.error("Consulta SICE-TAC fallida [%s]: %s", context, exc)
                summary["errores"].append({"combinacion": context, "tipo": type(exc).__name__, "mensaje": str(exc)})
            if progreso: progreso(index, summary)
        if summary["consultas_exitosas"] == 0:
            raise RuntimeError("Todas las combinaciones SICE-TAC fallaron")
        return summary
