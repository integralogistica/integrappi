"""Catálogos de presupuesto-responsables y personal-viajes + reglas nuevas de
viajes (origen≠destino, 60 días, responsables de anticipo por configuración)."""
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from conftest import token_modulo
from financiero.models.usuario import RolUsuario, Usuario

CORREO_JEFE = "jefe.cat@integralogistica.com"
CORREO_PRESI = "presi.cat@integralogistica.com"
CORREO_VIAJERO = "viajero.cat@integralogistica.com"
CORREO_CTRL = "ctrl.cat@integralogistica.com"
CORREO_COMPRAS = "compras.cat@integralogistica.com"
CORREO_RESP_ADICIONAL = "resp.adicional@integralogistica.com"


@pytest.fixture()
def gente(sesion):
    from financiero.models.usuario import Usuario as U
    for correo, rol, extra in [
        (CORREO_JEFE, RolUsuario.JEFE_AREA, {"nombre": "Jefe Cat", "area": "Tecnología"}),
        (CORREO_PRESI, RolUsuario.PRESIDENCIA, {"nombre": "Presi Cat"}),
        (CORREO_VIAJERO, RolUsuario.REQUISICION, {"nombre": "Viajero Cat", "area": "Tecnología"}),
        (CORREO_CTRL, RolUsuario.CONTROLLER, {"nombre": "Ctrl Cat"}),
        (CORREO_COMPRAS, RolUsuario.COMPRAS, {"nombre": "Compras Cat"}),
        (CORREO_RESP_ADICIONAL, RolUsuario.REQUISICION, {"nombre": "Resp Adicional", "area": "Comercial"}),
    ]:
        sesion.add(U(username=correo.split("@")[0], correo=correo, rol=rol, activo=True, **extra))
    from financiero.models.catalogo import PersonalViaje, PresupuestoResponsable
    sesion.add_all([
        PresupuestoResponsable(periodo=2026, responsable="Presidencia", rubro="Reserva De Presidencia",
                               mes=1, presupuesto=100_000_000, ejecutado=10_000_000),
        PresupuestoResponsable(periodo=2026, responsable="Tecnología", rubro="GASTOS › PAPELERÍA",
                               mes=1, presupuesto=5_000_000, ejecutado=1_000_000),
        PersonalViaje(nombre="VIAJERO CAT", cargo="Analista", categoria="B"),
        PersonalViaje(nombre="OTRA PERSONA", cargo="Coordinador", categoria="A"),
    ])
    sesion.commit()
    # ids de usuarios para las aserciones de configuración
    global _IDS
    _IDS = {u.correo: str(u.id) for u in sesion.query(U).all()}


@pytest.fixture()
def api(engine, gente):
    from rutas.fin_catalogos import router as ruta_fin_catalogos
    from rutas.fin_viajes import router as ruta_fin_viajes
    app = FastAPI()
    app.include_router(ruta_fin_catalogos)
    app.include_router(ruta_fin_viajes)
    return TestClient(app)


def _h(correo: str) -> dict:
    return {"Authorization": f"Bearer {token_modulo(correo)}"}


class TestCatalogos:
    def test_presupuesto_por_periodo(self, api):
        r = api.get("/fin/catalogos/presupuesto", params={"periodo": 2026}, headers=_h(CORREO_JEFE))
        assert r.status_code == 200, r.text
        data = r.json()
        assert data["Presidencia"]["Reserva De Presidencia"]["1"] == [100_000_000, 10_000_000]
        assert data["Tecnología"]["GASTOS › PAPELERÍA"]["1"] == [5_000_000, 1_000_000]

    def test_presupuesto_periodo_vacio(self, api):
        r = api.get("/fin/catalogos/presupuesto", params={"periodo": 1999}, headers=_h(CORREO_JEFE))
        assert r.status_code == 200
        assert r.json() == {}

    def test_personal_viajes_con_filtro(self, api):
        r = api.get("/fin/catalogos/personal-viajes", headers=_h(CORREO_VIAJERO))
        assert r.status_code == 200
        assert len(r.json()) == 2
        r2 = api.get("/fin/catalogos/personal-viajes", params={"q": "VIAJERO"}, headers=_h(CORREO_VIAJERO))
        assert len(r2.json()) == 1
        assert r2.json()[0]["categoria"] == "B"


class TestReglasViajes:
    def _autorizador_id(self, api):
        auts = api.get("/fin/viajes/autorizadores", headers=_h(CORREO_VIAJERO)).json()
        return next(a["id"] for a in auts if a["rol"] == "controller")

    def _payload(self, api, **extra):
        base = {
            "viajero_nombre": "Viajero Cat",
            "origen": "BARRANQUILLA", "destino": "BOGOTA",
            "fecha_inicio": "2026-12-01T08:00:00", "fecha_fin": "2026-12-03T18:00:00",
            "objetivo": "Visita cliente", "autorizador_id": self._autorizador_id(api),
        }
        base.update(extra)
        return base

    def test_origen_igual_destino_rechazado(self, api):
        r = api.post("/fin/viajes", json=self._payload(api, destino="BARRANQUILLA"), headers=_h(CORREO_VIAJERO))
        assert r.status_code == 422

    def test_viaje_mas_de_60_dias_rechazado(self, api):
        r = api.post("/fin/viajes", json=self._payload(api, fecha_fin="2027-02-15T18:00:00"), headers=_h(CORREO_VIAJERO))
        assert r.status_code == 422

    def test_viaje_valido_se_crea(self, api):
        r = api.post("/fin/viajes", json=self._payload(api), headers=_h(CORREO_VIAJERO))
        assert r.status_code == 200, r.text
        assert r.json()["estado"] == "SOLICITADO"

    def test_responsable_anticipo_por_configuracion(self, api, sesion):
        """Un usuario sin rol de Tesorería pero en viajes_responsables_anticipo ve el viaje
        cuando llega al anticipo y puede pagarlo."""
        from financiero.models.configuracion import Configuracion
        from financiero.models.viaje import EstadoViaje, Viaje
        sesion.add(Configuracion(clave="viajes_responsables_anticipo",
                                 valor={"v": [_IDS[CORREO_RESP_ADICIONAL]]}))
        sesion.commit()
        # crea y avanza el viaje hasta TIQUETES_GESTIONADOS
        vj = api.post("/fin/viajes", json=self._payload(api), headers=_h(CORREO_VIAJERO)).json()
        api.post(f"/fin/viajes/{vj['id']}/transicion", headers=_h(CORREO_CTRL),
                 json={"accion": "AUTORIZAR", "version": vj["version"]})
        vj = api.get(f"/fin/viajes/{vj['id']}", headers=_h(CORREO_VIAJERO)).json()
        api.post(f"/fin/viajes/{vj['id']}/transicion", headers=_h(CORREO_CTRL),
                 json={"accion": "ASIGNAR_PRESUPUESTO", "version": vj["version"],
                       "datos_adicionales": {"categoria_tarifa": "B", "total": 500000}})
        vj = api.get(f"/fin/viajes/{vj['id']}", headers=_h(CORREO_VIAJERO)).json()
        api.post(f"/fin/viajes/{vj['id']}/transicion", headers=_h(CORREO_COMPRAS),
                 json={"accion": "REGISTRAR_TIQUETES", "version": vj["version"],
                       "datos_adicionales": {"no_aplica": True}})
        vj = api.get(f"/fin/viajes/{vj['id']}", headers=_h(CORREO_VIAJERO)).json()
        assert vj["estado"] == "TIQUETES_GESTIONADOS"
        # el responsable adicional LO VE en el listado (antes no lo veía)
        visibles = api.get("/fin/viajes", headers=_h(CORREO_RESP_ADICIONAL)).json()
        assert any(x["id"] == vj["id"] for x in visibles)
        # y puede pagar el anticipo
        r = api.post(f"/fin/viajes/{vj['id']}/transicion", headers=_h(CORREO_RESP_ADICIONAL),
                     json={"accion": "PAGAR_ANTICIPO", "version": vj["version"],
                           "datos_adicionales": {"valor": 500000}})
        assert r.status_code == 200, r.text
        assert r.json()["estado"] == "ANTICIPO_PAGADO"
