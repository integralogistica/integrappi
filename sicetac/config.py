from __future__ import annotations

import os
from dataclasses import dataclass

from .errors import ConfigurationError

HORAS_CARGUE = 2
HORAS_DESCARGUE = 2
HORAS_ESPERA = 2
MESES_RETROCESO_PERIODO = 3

COMBINACIONES = [
    {"origen": "GALAPA - GALAPA - ATLÁNTICO", "origen_codigo": "08296000", "destino": "YUMBO", "destino_codigo": "76892000", "configuracion": "Camión dos ejes - Livianos PBV 7500-8000 Kg", "configuracion_codigo": "2L3", "unidad_transporte": "FURGON", "tipo_carga": "General", "condicion_carga": "CARGADO", "condicion_carga_codigo": "1"},
    {"origen": "BOGOTÁ", "origen_codigo": "11001000", "destino": "TOCANCIPÁ", "destino_codigo": "25817000", "configuracion": "Tractocamión tres ejes con semiremolque de tres ejes", "configuracion_codigo": "3S3", "unidad_transporte": "FURGON", "tipo_carga": "General", "condicion_carga": "CARGADO", "condicion_carga_codigo": "1"},
    {"origen": "BUCARAMANGA", "origen_codigo": "68001000", "destino": "BARRANQUILLA", "destino_codigo": "08001000", "configuracion": "Camión dos ejes - PBV más de 10500 Kg", "configuracion_codigo": "2", "unidad_transporte": "FURGON", "tipo_carga": "General", "condicion_carga": "CARGADO", "condicion_carga_codigo": "1"},
    {"origen": "YUMBO", "origen_codigo": "76892000", "destino": "BOGOTÁ", "destino_codigo": "11001000", "configuracion": "Tractocamión tres ejes con semiremolque de tres ejes", "configuracion_codigo": "3S3", "unidad_transporte": "FURGON", "tipo_carga": "General", "condicion_carga": "CARGADO", "condicion_carga_codigo": "1"},
    {"origen": "GALAPA - GALAPA - ATLÁNTICO", "origen_codigo": "08296000", "destino": "SABANALARGA-ATLÁNTICO", "destino_codigo": "08638000", "configuracion": "Camión dos ejes - Livianos PBV 7500-8000 Kg", "configuracion_codigo": "2L3", "unidad_transporte": "FURGON", "tipo_carga": "General", "condicion_carga": "CARGADO", "condicion_carga_codigo": "1"},
]

ENDPOINTS = {
    "production": {"wsdl": "http://plc.mintransporte.gov.co:8080/wsdl/IBPMServices", "soap": "http://plc.mintransporte.gov.co:8080/soap/IBPMServices"},
    "test": {"wsdl": "http://rndcpruebas.mintransporte.gov.co:8080/wsdl/IBPMServices", "soap": ""},
}
CONFIGURACIONES_VALIDAS = {"3S3", "3S2", "2S3", "2S2", "3", "2", "2L1", "2L2", "2L3", "V2", "V3", "V4"}


def validar_combinaciones(combinaciones=COMBINACIONES):
    vistos = set()
    campos_texto = ("origen", "destino", "configuracion", "unidad_transporte", "tipo_carga", "condicion_carga")
    for i, item in enumerate(combinaciones, 1):
        for campo in campos_texto:
            if not str(item.get(campo, "")).strip():
                raise ConfigurationError(f"COMBINACIONES[{i}].{campo} es obligatorio")
        for campo in ("origen_codigo", "destino_codigo"):
            if len(str(item.get(campo, ""))) != 8 or not str(item[campo]).isdigit():
                raise ConfigurationError(f"COMBINACIONES[{i}].{campo} debe tener ocho dígitos")
        if item.get("configuracion_codigo") not in CONFIGURACIONES_VALIDAS:
            raise ConfigurationError(f"COMBINACIONES[{i}].configuracion_codigo no es válido")
        if item.get("condicion_carga_codigo") not in {"1", "2"}:
            raise ConfigurationError(f"COMBINACIONES[{i}].condicion_carga_codigo no es válido")
        clave = tuple(sorted(item.items()))
        if clave in vistos:
            raise ConfigurationError(f"COMBINACIONES[{i}] está duplicada")
        vistos.add(clave)


@dataclass(frozen=True)
class Settings:
    username: str
    password: str
    mongodb_uri: str
    mongodb_database: str
    mongodb_collection: str
    environment: str
    soap_url: str

    @classmethod
    def from_env(cls):
        obligatorias = ("RNDC_USERNAME", "RNDC_PASSWORD", "MONGODB_URI")
        faltantes = [x for x in obligatorias if not os.getenv(x, "").strip()]
        if faltantes:
            raise ConfigurationError("Faltan variables obligatorias: " + ", ".join(faltantes))
        environment = os.getenv("RNDC_ENVIRONMENT", "production").strip().lower()
        if environment not in ENDPOINTS:
            raise ConfigurationError("RNDC_ENVIRONMENT debe ser production o test")
        soap_url = os.getenv("RNDC_SOAP_URL", "").strip() or ENDPOINTS[environment]["soap"]
        if not soap_url:
            raise ConfigurationError("RNDC_SOAP_URL es obligatoria en el ambiente test; confírmela en su WSDL")
        validar_combinaciones()
        return cls(os.environ["RNDC_USERNAME"], os.environ["RNDC_PASSWORD"], os.environ["MONGODB_URI"], os.getenv("MONGODB_DATABASE", "sicetac"), os.getenv("MONGODB_COLLECTION", "consultas"), environment, soap_url)

