"""Permisos finos por usuario: matriz rol→secciones por defecto + overrides."""
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from conftest import token_modulo
from financiero.models.usuario import RolUsuario, Usuario

CORREO_ADMIN = "admin.perm@integralogistica.com"
CORREO_CONTA = "conta.perm@integralogistica.com"
CORREO_CALIDAD = "calidad.perm@integralogistica.com"
CORREO_TESO = "teso.perm@integralogistica.com"


@pytest.fixture()
def gente(sesion):
    from financiero.models.usuario import Usuario as U
    for correo, rol in [
        (CORREO_ADMIN, RolUsuario.ADMIN),
        (CORREO_CONTA, RolUsuario.CONTABILIDAD),
        (CORREO_CALIDAD, RolUsuario.CALIDAD),
        (CORREO_TESO, RolUsuario.TESORERIA),
    ]:
        sesion.add(U(username=correo.split("@")[0], nombre=correo.split("@")[0].title(),
                     correo=correo, rol=rol, activo=True))
    sesion.commit()


@pytest.fixture()
def api(engine, gente):
    from rutas.fin_servicios import router as rs
    from rutas.fin_cajas import router as rc
    from rutas.fin_traslados import router as rt
    from rutas.fin_usuarios import router as ru
    app = FastAPI()
    for r in (rs, rc, rt, ru):
        app.include_router(r)
    return TestClient(app)


def _h(correo: str) -> dict:
    return {"Authorization": f"Bearer {token_modulo(correo)}"}


class TestDefaultsPorRol:
    def test_calidad_solo_ve_servicios(self, api):
        assert api.get("/fin/servicios", headers=_h(CORREO_CALIDAD)).status_code == 200
        assert api.get("/fin/cajas", headers=_h(CORREO_CALIDAD)).status_code == 403
        assert api.get("/fin/traslados", headers=_h(CORREO_CALIDAD)).status_code == 403

    def test_contabilidad_ve_cajas_y_facturas_no_traslados(self, api):
        assert api.get("/fin/cajas", headers=_h(CORREO_CONTA)).status_code == 200
        assert api.get("/fin/servicios", headers=_h(CORREO_CONTA)).status_code == 200
        assert api.get("/fin/traslados", headers=_h(CORREO_CONTA)).status_code == 403

    def test_admin_ve_todo(self, api):
        assert api.get("/fin/cajas", headers=_h(CORREO_ADMIN)).status_code == 200
        assert api.get("/fin/traslados", headers=_h(CORREO_ADMIN)).status_code == 200


class TestOverridesPorUsuario:
    def test_habilitar_seccion_a_quien_no_la_tiene(self, api):
        # calidad no ve cajas → el admin se la habilita solo a él
        r = api.get("/fin/usuarios", headers=_h(CORREO_ADMIN)).json()
        u = [x for x in r if x["correo"] == CORREO_CALIDAD][0]
        r = api.put(f"/fin/usuarios/{u['id']}", headers=_h(CORREO_ADMIN),
                    json={"permisos": {"cajas": True, "servicios": True}})
        assert r.status_code == 200, r.text
        assert "cajas" in r.json()["secciones_efectivas"]
        # ahora sí ve cajas
        assert api.get("/fin/cajas", headers=_h(CORREO_CALIDAD)).status_code == 200

    def test_quitar_seccion_a_quien_la_tiene(self, api):
        r = api.get("/fin/usuarios", headers=_h(CORREO_ADMIN)).json()
        u = [x for x in r if x["correo"] == CORREO_CONTA][0]
        r = api.put(f"/fin/usuarios/{u['id']}", headers=_h(CORREO_ADMIN),
                    json={"permisos": {"cajas": False}})
        assert r.status_code == 200
        assert "cajas" not in r.json()["secciones_efectivas"]
        assert api.get("/fin/cajas", headers=_h(CORREO_CONTA)).status_code == 403
        # los demás con su mismo rol no se ven afectados
        assert api.get("/fin/cajas", headers=_h(CORREO_TESO)).status_code == 200

    def test_permisos_no_recortan_al_admin(self, api):
        r = api.get("/fin/usuarios", headers=_h(CORREO_ADMIN)).json()
        u = [x for x in r if x["correo"] == CORREO_ADMIN][0]
        r = api.put(f"/fin/usuarios/{u['id']}", headers=_h(CORREO_ADMIN),
                    json={"permisos": {"traslados": False}})
        assert r.status_code == 200
        assert "traslados" in r.json()["secciones_efectivas"]  # el admin conserva todo
        assert api.get("/fin/traslados", headers=_h(CORREO_ADMIN)).status_code == 200

    def test_seccion_invalida_422(self, api):
        r = api.get("/fin/usuarios", headers=_h(CORREO_ADMIN)).json()
        u = [x for x in r if x["correo"] == CORREO_TESO][0]
        r = api.put(f"/fin/usuarios/{u['id']}", headers=_h(CORREO_ADMIN),
                    json={"permisos": {"nave-espacial": True}})
        assert r.status_code == 422

    def test_al_crear_tambien_se_pueden_definir(self, api):
        r = api.post("/fin/usuarios", headers=_h(CORREO_ADMIN), json={
            "nombre": "Invitado Test", "correo": "invitado.perm@integralogistica.com",
            "rol": "consulta", "permisos": {"facturas": True},
        })
        assert r.status_code == 200, r.text
        assert r.json()["secciones_efectivas"] == ["facturas"]
