from __future__ import annotations

import hashlib
import json
import logging
import unicodedata
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from .config import (COMBINACIONES, HORAS_CARGUE, HORAS_DESCARGUE, HORAS_ESPERA,
                     MESES_RETROCESO_PERIODO)
from .models import calcular_costo_total, decimal_rndc
from .errors import RNDCCredentialsError, RNDCNoDataError

logger = logging.getLogger(__name__)


def periodo_actual(): return datetime.now(ZoneInfo("America/Bogota")).strftime("%Y%m")


def periodo_anterior(periodo):
    year, month = int(periodo[:4]), int(periodo[4:]) - 1
    if month == 0: year, month = year - 1, 12
    return f"{year:04d}{month:02d}"


def normalizar(value):
    text = unicodedata.normalize("NFKD", str(value or ""))
    return " ".join("".join(c for c in text if not unicodedata.combining(c)).upper().split())


def consulta_id(document):
    fields = ("periodo_aplicado", "origen", "destino", "configuracion", "condicion_carga", "tipo_carga", "unidad_transporte", "rutasid")
    raw = "\x1f".join(normalizar(document.get(x)) for x in fields)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _coincide(document, combinacion):
    comparisons = (
        ("origen", "origen_codigo"), ("destino", "destino_codigo"),
        ("configuracion", "configuracion_codigo"), ("condicioncarga", "condicion_carga_codigo"),
    )
    for field, configured in comparisons:
        if document.get(field) and normalizar(document[field]) != normalizar(combinacion[configured]):
            return False
    for field, configured in (("nombretipocarga", "tipo_carga"), ("nombreunidadtransporte", "unidad_transporte")):
        if document.get(field) and normalizar(document[field]) != normalizar(combinacion[configured]):
            return False
    return True


def transformar(document, combinacion, solicitado, aplicado):
    valor_moviliza, valor_hora = decimal_rndc(document.get("valormoviliza")), decimal_rndc(document.get("valorhora"))
    horas_total = Decimal(HORAS_CARGUE + HORAS_DESCARGUE + HORAS_ESPERA)
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
        "costos": {"valor_moviliza": valor_moviliza, "valor_hora": valor_hora, "horas_cargue": Decimal(HORAS_CARGUE), "horas_descargue": Decimal(HORAS_DESCARGUE), "horas_espera": Decimal(HORAS_ESPERA), "horas_logisticas_total": horas_total, "costo_total_calculado": calcular_costo_total(valor_moviliza, valor_hora, HORAS_CARGUE, HORAS_DESCARGUE, HORAS_ESPERA)},
        "respuesta_rndc": document, "fuente": "RNDC_SICETAC_WS", "consultado_en": datetime.now(ZoneInfo("America/Bogota")),
    }
    return result


class SicetacService:
    def __init__(self, client, repository): self.client, self.repository = client, repository

    def ejecutar(self, solicitado=None, dry_run=False, progreso=None):
        solicitado = solicitado or periodo_actual()
        summary = {"combinaciones_configuradas": len(COMBINACIONES), "consultas_exitosas": 0, "documentos_recibidos": 0, "documentos_insertados": 0, "documentos_actualizados": 0, "combinaciones_sin_resultado": 0, "errores": []}
        self.repository.comprobar()
        for index, combination in enumerate(COMBINACIONES, 1):
            current, documents = solicitado, []
            try:
                for _ in range(MESES_RETROCESO_PERIODO + 1):
                    try:
                        documents = self.client.consultar(current, combination)
                    except RNDCNoDataError:
                        documents = []
                    if documents: break
                    current = periodo_anterior(current)
                valid = [x for x in documents if _coincide(x, combination)]
                transformed = [transformar(x, combination, solicitado, current) for x in valid]
                summary["consultas_exitosas"] += 1
                summary["documentos_recibidos"] += len(documents)
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
