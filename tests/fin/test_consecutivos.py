"""Consecutivos atómicos: formato, secuencia y arranque de la OC en 386.

# puerto de tests/test_consecutivos_concurrencia.py del módulo externo
(aquí sin hilos: StaticPool serializa; la concurrencia real se prueba con
@pytest.mark.postgres contra Postgres antes de cada deploy).
"""
from datetime import datetime, timezone

from financiero.services.consecutivo_service import ConsecutivoService


def test_formatos_por_tipo(sesion):
    anio = datetime.now(timezone.utc).year
    assert ConsecutivoService.generar_consecutivo(sesion, "req") == f"REQ-{anio}-0001"
    assert ConsecutivoService.generar_consecutivo(sesion, "tr") == f"TRP-{anio}-0001"
    assert ConsecutivoService.generar_consecutivo(sesion, "vj") == f"VJ-{anio}-0001"


def test_oc_arranca_en_386(sesion):
    anio = datetime.now(timezone.utc).year
    # La 385 fue la última orden de compra emitida a mano: la primera del sistema es 2026-386
    assert ConsecutivoService.generar_consecutivo(sesion, "oc") == f"{anio}-386"
    assert ConsecutivoService.generar_consecutivo(sesion, "oc") == f"{anio}-387"


def test_secuencia_contigua_sin_duplicados(sesion):
    anio = datetime.now(timezone.utc).year
    consecutivos = [ConsecutivoService.generar_consecutivo(sesion, "req") for _ in range(25)]
    numeros = [int(c.split("-")[-1]) for c in consecutivos]
    assert numeros == sorted(numeros)
    assert len(set(consecutivos)) == 25
    assert consecutivos[0].startswith(f"REQ-{anio}-")


def test_anio_independiente(sesion):
    assert ConsecutivoService.generar_consecutivo(sesion, "req", anio=2030) == "REQ-2030-0001"
    assert ConsecutivoService.generar_consecutivo(sesion, "req", anio=2031) == "REQ-2031-0001"


def test_año_explicito_reanuda_donde_quedaba(sesion):
    assert ConsecutivoService.generar_consecutivo(sesion, "tr", anio=2029) == "TRP-2029-0001"
    assert ConsecutivoService.generar_consecutivo(sesion, "tr", anio=2029) == "TRP-2029-0002"
