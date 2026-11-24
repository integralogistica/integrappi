"""Flujo completo de traslados vía API (TestClient sobre SQLite)."""
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from conftest import token_modulo
from financiero.models.usuario import RolUsuario, Usuario

CORREO_JEFE = "jefe.test@integralogistica.com"
CORREO_PRESI = "presidencia.test@integralogistica.com"
CORREO_CONTROLLER = "controller.test@integralogistica.com"


@pytest.fixture()
def gente(sesion):
    from datetime import datetime, timezone
    ahora = datetime.now(timezone.utc)
    for correo, rol, area in [
        (CORREO_JEFE, RolUsuario.JEFE_AREA, "Tecnología"),
        (CORREO_PRESI, RolUsuario.PRESIDENCIA, None),
        (CORREO_CONTROLLER, RolUsuario.CONTROLLER, None),
    ]:
        sesion.add(Usuario(username=correo.split("@")[0], nombre=correo.split("@")[0].title(),
                           correo=correo, rol=rol, area=area, activo=True))
    sesion.commit()
    # Configuración con áreas (la crea la siembra; en tests la sembramos a mano)
    from financiero.models.configuracion import Configuracion
    if sesion.get(Configuracion, "areas") is None:
        sesion.add(Configuracion(clave="areas", valor={"v": ["Tecnología", "Compras"]}, version=1))
        sesion.commit()


@pytest.fixture()
def api(engine, gente):
    from rutas.fin_traslados import router as ruta_fin_traslados
    from rutas.fin_auth import router as ruta_fin_auth
    app = FastAPI()
    app.include_router(ruta_fin_auth)
    app.include_router(ruta_fin_traslados)
    return TestClient(app)


def _h(correo: str) -> dict:
    return {"Authorization": f"Bearer {token_modulo(correo)}"}


LINEAS = [{"destinoRubro": "Gastos › Papelería", "monto": 150000, "motivo": "Compra de papelería"}]


class TestFlujoTraslados:
    def test_flujo_completo(self, api):
        # 1. El jefe crea
        r = api.post("/fin/traslados", json={"lineas": LINEAS}, headers=_h(CORREO_JEFE))
        assert r.status_code == 200, r.text
        tr = r.json()
        assert tr["estado"] == "PENDIENTE_PRESIDENCIA"
        assert tr["consecutivo"].startswith("TRP-")
        assert float(tr["total"]) == 150000.0
        assert tr["historial_cambios"][0]["accion"] == "RADICACION"

        # 2. El controller NO lo ve (solo lo APROBADO)
        r = api.get("/fin/traslados", headers=_h(CORREO_CONTROLLER))
        assert r.status_code == 200
        assert r.json() == []

        # 3. Presidencia aprueba
        r = api.post(f"/fin/traslados/{tr['id']}/transicion",
                     json={"accion": "APROBAR", "version": tr["version"], "observacion": "OK"},
                     headers=_h(CORREO_PRESI))
        assert r.status_code == 200, r.text
        aprobado = r.json()
        assert aprobado["estado"] == "APROBADO"
        assert aprobado["aprobacion"]["cargo"] == "PRESIDENTE"
        assert aprobado["version"] == tr["version"] + 1

        # 4. Ahora el controller SÍ lo ve
        r = api.get("/fin/traslados", headers=_h(CORREO_CONTROLLER))
        assert len(r.json()) == 1

    def test_jefe_no_puede_aprobar(self, api):
        r = api.post("/fin/traslados", json={"lineas": LINEAS}, headers=_h(CORREO_JEFE))
        tr = r.json()
        r = api.post(f"/fin/traslados/{tr['id']}/transicion",
                     json={"accion": "APROBAR", "version": tr["version"]}, headers=_h(CORREO_JEFE))
        assert r.status_code == 403

    def test_presidencia_no_crea(self, api):
        r = api.post("/fin/traslados", json={"lineas": LINEAS}, headers=_h(CORREO_PRESI))
        assert r.status_code == 403

    def test_rechazo_exige_motivo_y_permite_corregir(self, api):
        r = api.post("/fin/traslados", json={"lineas": LINEAS}, headers=_h(CORREO_JEFE))
        tr = r.json()
        # sin motivo → 422
        r = api.post(f"/fin/traslados/{tr['id']}/transicion",
                     json={"accion": "RECHAZAR", "version": tr["version"]}, headers=_h(CORREO_PRESI))
        assert r.status_code == 422
        # con motivo → RECHAZADO
        r = api.post(f"/fin/traslados/{tr['id']}/transicion",
                     json={"accion": "RECHAZAR", "version": tr["version"], "observacion": "Sin cupo"},
                     headers=_h(CORREO_PRESI))
        assert r.json()["estado"] == "RECHAZADO"
        # el jefe corrige y reenvía
        nuevas = [{"destinoRubro": "Gastos › Otros", "monto": 90000, "motivo": "Ajustado"}]
        r = api.put(f"/fin/traslados/{tr['id']}",
                    json={"lineas": nuevas, "version": r.json()["version"]}, headers=_h(CORREO_JEFE))
        assert r.status_code == 200, r.text
        assert r.json()["estado"] == "PENDIENTE_PRESIDENCIA"
        assert float(r.json()["total"]) == 90000.0

    def test_version_vieja_da_409(self, api):
        r = api.post("/fin/traslados", json={"lineas": LINEAS}, headers=_h(CORREO_JEFE))
        tr = r.json()
        # alguien más la aprueba antes
        api.post(f"/fin/traslados/{tr['id']}/transicion",
                 json={"accion": "APROBAR", "version": tr["version"]}, headers=_h(CORREO_PRESI))
        # con la versión vieja → 409
        r = api.post(f"/fin/traslados/{tr['id']}/transicion",
                     json={"accion": "ANULAR", "version": tr["version"]}, headers=_h(CORREO_JEFE))
        assert r.status_code == 409

    def test_anular_por_creador(self, api):
        r = api.post("/fin/traslados", json={"lineas": LINEAS}, headers=_h(CORREO_JEFE))
        tr = r.json()
        r = api.post(f"/fin/traslados/{tr['id']}/transicion",
                     json={"accion": "ANULAR", "version": tr["version"]}, headers=_h(CORREO_JEFE))
        assert r.status_code == 200
        assert r.json()["estado"] == "ANULADO"

    def test_linea_invalida_rechazada(self, api):
        r = api.post("/fin/traslados", json={"lineas": [{"destinoRubro": "x", "monto": 0, "motivo": "y"}]},
                     headers=_h(CORREO_JEFE))
        assert r.status_code == 422
