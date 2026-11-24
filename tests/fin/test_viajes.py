"""Flujo completo de viajes vía API + enmascaramiento de tarifa + configuración."""
import uuid as uuidlib

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from conftest import token_modulo
from financiero.models.usuario import RolUsuario, Usuario

CORREO_VIAJERO = "viajero.test@integralogistica.com"   # rol requisicion
CORREO_CTRL = "controller.test@integralogistica.com"
CORREO_COMPRAS = "compras.test@integralogistica.com"
CORREO_TESORERIA = "tesoreria.test@integralogistica.com"
CORREO_CONTA = "conta.test@integralogistica.com"


@pytest.fixture()
def gente(sesion):
    from financiero.models.usuario import Usuario as U
    for correo, rol, extra in [
        (CORREO_VIAJERO, RolUsuario.REQUISICION, {"nombre": "Viajero Test", "area": "Tecnología"}),
        (CORREO_CTRL, RolUsuario.CONTROLLER, {"nombre": "Controller Test"}),
        (CORREO_COMPRAS, RolUsuario.COMPRAS, {"nombre": "Compras Test"}),
        (CORREO_TESORERIA, RolUsuario.TESORERIA, {"nombre": "Tesoreria Test", "es_aprobador_pagos": True}),
        (CORREO_CONTA, RolUsuario.CONTABILIDAD, {"nombre": "Conta Test"}),
    ]:
        sesion.add(U(username=correo.split("@")[0], correo=correo, rol=rol, activo=True, **extra))
    sesion.commit()
    return {u.correo: u for u in sesion.query(U).all()}


@pytest.fixture()
def api(engine, gente):
    from rutas.fin_viajes import router as ruta_fin_viajes
    from rutas.fin_configuracion import router as ruta_fin_configuracion
    app = FastAPI()
    app.include_router(ruta_fin_viajes)
    app.include_router(ruta_fin_configuracion)
    return TestClient(app)


def _h(correo: str) -> dict:
    return {"Authorization": f"Bearer {token_modulo(correo)}"}


def _payload(gente) -> dict:
    return {
        "viajero_nombre": "Viajero Test",
        "origen": "BARRANQUILLA",
        "destino": "BOGOTA",
        "fecha_inicio": "2026-12-01T08:00:00",
        "fecha_fin": "2026-12-03T18:00:00",
        "objetivo": "Visita a cliente",
        "autorizador_id": str(gente[CORREO_CTRL].id),
    }


def _transicion(api, id, accion, correo, version, datos=None, observacion=None):
    return api.post(f"/fin/viajes/{id}/transicion", headers=_h(correo),
                    json={"accion": accion, "version": version, "datos_adicionales": datos, "observacion": observacion})


class TestViajes:
    def test_flujo_completo_hasta_cerrado(self, api, gente):
        # 1. crear
        r = api.post("/fin/viajes", json=_payload(gente), headers=_h(CORREO_VIAJERO))
        assert r.status_code == 200, r.text
        vj = r.json()
        assert vj["consecutivo"].startswith("VJ-")
        assert vj["estado"] == "SOLICITADO"

        # 2. autorizar (controller designado)
        r = _transicion(api, vj["id"], "AUTORIZAR", CORREO_CTRL, vj["version"])
        assert r.status_code == 200, r.text
        vj = r.json()
        assert vj["estado"] == "AUTORIZADO"
        assert vj["firmas"]["autorizo"]["nombre"] == "Controller Test"

        # 3. presupuesto con categoría A
        ppto = {"categoria_tarifa": "A", "items": [{"concepto": "Hospedaje", "dias": 2, "valor_diario": 200000, "total": 400000}], "total": 400000}
        r = _transicion(api, vj["id"], "ASIGNAR_PRESUPUESTO", CORREO_CTRL, vj["version"], datos=ppto)
        assert r.status_code == 200, r.text
        vj = r.json()
        assert vj["estado"] == "PRESUPUESTO_ASIGNADO"

        # 4. tiquetes (compras)
        r = _transicion(api, vj["id"], "REGISTRAR_TIQUETES", CORREO_COMPRAS, vj["version"], datos={"aerolinea": "AV"})
        assert r.status_code == 200, r.text
        vj = r.json()
        assert vj["estado"] == "TIQUETES_GESTIONADOS"

        # 5. anticipo (tesorería)
        r = _transicion(api, vj["id"], "PAGAR_ANTICIPO", CORREO_TESORERIA, vj["version"], datos={"valor": 400000})
        assert r.status_code == 200, r.text
        vj = r.json()
        assert vj["estado"] == "ANTICIPO_PAGADO"
        assert vj["anticipo_detalle"]["por"] == "Tesoreria Test"

        # 6. legalizar (solicitante)
        r = _transicion(api, vj["id"], "LEGALIZAR", CORREO_VIAJERO, vj["version"], datos={"gasto_real": 380000})
        assert r.status_code == 200, r.text
        assert r.json()["estado"] == "LEGALIZADO"

        # 7. cerrar (contabilidad)
        vj = r.json()
        r = _transicion(api, vj["id"], "CERRAR", CORREO_CONTA, vj["version"])
        assert r.json()["estado"] == "CERRADO"

    def test_tarifa_enmascarada_para_el_viajero(self, api, gente):
        r = api.post("/fin/viajes", json=_payload(gente), headers=_h(CORREO_VIAJERO))
        vj = r.json()
        vj = _transicion(api, vj["id"], "AUTORIZAR", CORREO_CTRL, vj["version"]).json()
        r = _transicion(api, vj["id"], "ASIGNAR_PRESUPUESTO", CORREO_CTRL,
                        vj["version"], datos={"categoria_tarifa": "B", "total": 100})
        assert r.status_code == 200, r.text
        vj = r.json()
        # el viajero NO ve la categoría
        r = api.get(f"/fin/viajes/{vj['id']}", headers=_h(CORREO_VIAJERO))
        assert r.json()["categoria_tarifa"] is None
        # el controller sí
        r = api.get(f"/fin/viajes/{vj['id']}", headers=_h(CORREO_CTRL))
        assert r.json()["categoria_tarifa"] == "B"

    def test_viajero_propio_obligatorio(self, api, gente):
        p = _payload(gente)
        p["viajero_nombre"] = "Otra Persona"
        r = api.post("/fin/viajes", json=p, headers=_h(CORREO_VIAJERO))
        assert r.status_code == 403

    def test_autorizador_invalido_rechazado(self, api, gente):
        p = _payload(gente)
        p["autorizador_id"] = str(gente[CORREO_COMPRAS].id)  # compras no autoriza
        r = api.post("/fin/viajes", json=p, headers=_h(CORREO_VIAJERO))
        assert r.status_code == 400

    def test_rechazo_y_correccion(self, api, gente):
        r = api.post("/fin/viajes", json=_payload(gente), headers=_h(CORREO_VIAJERO))
        vj = r.json()
        r = _transicion(api, vj["id"], "RECHAZAR", CORREO_CTRL, vj["version"], observacion="Fechas mal")
        assert r.json()["estado"] == "RECHAZADO"
        vj = r.json()
        r = api.put(f"/fin/viajes/{vj['id']}", headers=_h(CORREO_VIAJERO),
                    json={**_payload(gente), "objetivo": "Visita corregida"}, params={"version": vj["version"]})
        assert r.status_code == 200, r.text
        assert r.json()["estado"] == "SOLICITADO"

    def test_editar_paso_presupuesto(self, api, gente):
        r = api.post("/fin/viajes", json=_payload(gente), headers=_h(CORREO_VIAJERO))
        vj = r.json()
        vj = _transicion(api, vj["id"], "AUTORIZAR", CORREO_CTRL, vj["version"]).json()
        r = _transicion(api, vj["id"], "ASIGNAR_PRESUPUESTO", CORREO_CTRL, vj["version"],
                        datos={"categoria_tarifa": "C", "total": 50})
        assert r.status_code == 200, r.text
        vj = api.get(f"/fin/viajes/{vj['id']}", headers=_h(CORREO_CTRL)).json()
        # compras no puede editar presupuesto
        r = api.post(f"/fin/viajes/{vj['id']}/editar-paso", headers=_h(CORREO_COMPRAS),
                     json={"paso": "presupuesto", "datos": {"total": 1}, "version": vj["version"]})
        assert r.status_code == 403
        # el controller sí
        r = api.post(f"/fin/viajes/{vj['id']}/editar-paso", headers=_h(CORREO_CTRL),
                     json={"paso": "presupuesto", "datos": {"categoria_tarifa": "A", "total": 999}, "version": vj["version"]})
        assert r.status_code == 200
        assert float(r.json()["total_presupuesto"]) == 999

    def test_visibilidad_restringida(self, api, gente):
        r = api.post("/fin/viajes", json=_payload(gente), headers=_h(CORREO_VIAJERO))
        vj = r.json()
        # contabilidad NO ve un viaje SOLICITADO ajeno
        r = api.get("/fin/viajes", headers=_h(CORREO_CONTA))
        assert all(v["id"] != vj["id"] for v in r.json())
        r = api.get(f"/fin/viajes/{vj['id']}", headers=_h(CORREO_CONTA))
        assert r.status_code == 404

    def test_endpoint_autorizadores(self, api, gente):
        r = api.get("/fin/viajes/autorizadores", headers=_h(CORREO_VIAJERO))
        assert r.status_code == 200
        ids = [a["id"] for a in r.json()]
        assert str(gente[CORREO_CTRL].id) in ids
        assert str(gente[CORREO_COMPRAS].id) not in ids


class TestConfiguracion:
    def test_leer_y_cambiar(self, api, gente):
        r = api.get("/fin/configuracion", headers=_h(CORREO_VIAJERO))
        assert r.status_code == 200
        assert r.json()["limite_controller"] == 100_000_000  # default

        # solo admin cambia
        r = api.put("/fin/configuracion/limite_controller", headers=_h(CORREO_VIAJERO),
                    json={"valor": 5000000})
        assert r.status_code == 403

    def test_admin_cambia_y_versiona(self, api, gente, sesion):
        from financiero.models.usuario import Usuario as U
        admin = U(username="admin.cfg", nombre="Admin Cfg", correo="admin.cfg@integralogistica.com",
                  rol=RolUsuario.ADMIN, activo=True)
        sesion.add(admin)
        sesion.commit()
        r = api.put("/fin/configuracion/limite_controller", headers=_h("admin.cfg@integralogistica.com"),
                    json={"valor": 5000000})
        assert r.status_code == 200, r.text
        # otra vez con la versión vieja → 409
        r2 = api.put("/fin/configuracion/limite_controller", headers=_h("admin.cfg@integralogistica.com"),
                     json={"valor": 6000000, "version": r.json()["version"] - 1})
        assert r2.status_code == 409
