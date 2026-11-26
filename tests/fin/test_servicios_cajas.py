"""Servicios públicos y cajas menores de punta a punta."""
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from conftest import token_modulo
from financiero.models.usuario import RolUsuario, Usuario

CORREO_TESO = "teso.sc@integralogistica.com"
CORREO_CONTA = "conta.sc@integralogistica.com"
CORREO_OPER = "oper.sc@integralogistica.com"
CORREO_CTRL = "ctrl.sc@integralogistica.com"
CORREO_ADMIN = "admin.sc@integralogistica.com"


@pytest.fixture()
def gente(sesion):
    from financiero.models.usuario import Usuario as U
    from financiero.models.catalogo import SedeCajaMenor
    for correo, rol in [
        (CORREO_TESO, RolUsuario.TESORERIA),
        (CORREO_CONTA, RolUsuario.CONTABILIDAD),
        (CORREO_OPER, RolUsuario.REQUISICION),
        (CORREO_CTRL, RolUsuario.CONTROLLER),
        (CORREO_ADMIN, RolUsuario.ADMIN),
    ]:
        sesion.add(U(username=correo.split("@")[0], nombre=correo.split("@")[0].title(),
                     correo=correo, rol=rol, activo=True, area="Tecnología"))
    # Sede del maestro cuyo jefe inmediato es el admin (para la 1ª aprobación)
    sesion.add(SedeCajaMenor(nombre="BOGOTA-TEST", etiqueta="CEDI Prueba", responsable="Teso Sc",
                             identificacion="123", jefe_inmediato="Admin Sc", cco="519104003", cuenta="11051003"))
    sesion.commit()


@pytest.fixture()
def api(engine, gente):
    from rutas.fin_servicios import router as rs
    from rutas.fin_cajas import router as rc
    from rutas.fin_contabilidad import router as rcont
    app = FastAPI()
    app.include_router(rs)
    app.include_router(rc)
    app.include_router(rcont)
    return TestClient(app)


def _h(correo: str) -> dict:
    return {"Authorization": f"Bearer {token_modulo(correo)}"}


class TestServicios:
    def test_crear_transiciones_y_resumen(self, api):
        r = api.post("/fin/servicios", headers=_h(CORREO_TESO), json={
            "servicio": "Energía", "inmueble": "Sede Principal", "periodo": "2026-11",
            "valor": 1250000, "fecha_limite": "2026-11-30",
            "datos": {"contrato": "E-123", "portal": "https://pagos.ejemplo.com"},
        })
        assert r.status_code == 200, r.text
        s = r.json()
        assert s["estado"] == "PENDIENTE"

        # un rol operativo no puede cambiar estado
        r = api.post(f"/fin/servicios/{s['id']}/transicion", headers=_h(CORREO_OPER),
                     json={"accion": "CAUSAR", "version": s["version"]})
        assert r.status_code == 403

        # causar → pagar
        r = api.post(f"/fin/servicios/{s['id']}/transicion", headers=_h(CORREO_CONTA),
                     json={"accion": "CAUSAR", "version": s["version"]})
        assert r.json()["estado"] == "CAUSADO"
        r = api.post(f"/fin/servicios/{s['id']}/transicion", headers=_h(CORREO_TESO),
                     json={"accion": "PAGAR", "version": r.json()["version"], "fecha_pago": "2026-11-28"})
        assert r.json()["estado"] == "PAGADO"
        assert r.json()["fecha_pago"] == "2026-11-28"

        # resumen
        r = api.get("/fin/servicios?periodo=2026-11", headers=_h(CORREO_TESO))
        assert r.json()["resumen"]["cantidad"] == 1
        assert r.json()["resumen"]["pendiente"] == 0

    def test_periodo_invalido(self, api):
        r = api.post("/fin/servicios", headers=_h(CORREO_TESO), json={
            "servicio": "Agua", "inmueble": "Sede", "periodo": "202611", "valor": 100})
        assert r.status_code == 422


MOVIMIENTOS = [
    {"concepto": "Aseo oficina", "valor": 80000, "tipo": "gasto", "centro_costo": "100130",
     "tipo_documento": "recibo_caja"},
    {"concepto": "Reposición caja", "valor": 80000, "tipo": "reposicion"},
]


class TestCajas:
    def test_circuito_completo_hasta_reembolso(self, api):
        # crear caja con movimientos (borrador)
        r = api.post("/fin/cajas", headers=_h(CORREO_TESO), json={
            "sede": "BOGOTA-TEST", "responsable": "Teso Sc", "periodo": "2026-11",
            "saldo_inicial": 300000, "movimientos": MOVIMIENTOS,
        })
        assert r.status_code == 200, r.text
        c = r.json()
        assert c["estado"] == "EN_EDICION"
        assert c["total_gastos"] == 160000
        assert [m["folio"] for m in c["movimientos"]] == [1, 2]

        # radicar: consecutivo CM + política semanal
        r = api.post(f"/fin/cajas/{c['id']}/radicar", headers=_h(CORREO_TESO))
        assert r.status_code == 200, r.text
        c = r.json()
        assert c["estado"] == "RADICADA"
        assert c["consecutivo"].startswith("CM-")
        assert "aTiempoParaViernes" in c["politica"]

        # contabilidad NO aprueba el jefe
        r = api.post(f"/fin/cajas/{c['id']}/transicion", headers=_h(CORREO_CONTA),
                     json={"accion": "APROBAR_JEFE", "version": c["version"]})
        assert r.status_code == 403

        # jefe inmediato de la sede (maestro: 'Admin Sc') → admin pasa
        r = api.post(f"/fin/cajas/{c['id']}/transicion", headers=_h(CORREO_ADMIN),
                     json={"accion": "APROBAR_JEFE", "version": c["version"]})
        assert r.status_code == 200, r.text
        c = r.json()
        assert c["estado"] == "APROBADA_JEFE"

        # controller aprueba
        r = api.post(f"/fin/cajas/{c['id']}/transicion", headers=_h(CORREO_CTRL),
                     json={"accion": "APROBAR_CONTROLLER", "version": c["version"]})
        c = r.json()
        assert c["estado"] == "APROBADA_CONTROLLER"

        # tesorería NO registra el FC
        r = api.post(f"/fin/cajas/{c['id']}/transicion", headers=_h(CORREO_TESO),
                     json={"accion": "CONTABILIZAR", "version": c["version"], "no_comprobante_fc": "FC-1"})
        assert r.status_code == 403

        # contabilizar sin nº FC → 422
        r = api.post(f"/fin/cajas/{c['id']}/transicion", headers=_h(CORREO_CONTA),
                     json={"accion": "CONTABILIZAR", "version": c["version"]})
        assert r.status_code == 422

        # contabilizar: sin FE/CxC el auto-avance la deja lista para egreso (CRUZADA)
        r = api.post(f"/fin/cajas/{c['id']}/transicion", headers=_h(CORREO_CONTA),
                     json={"accion": "CONTABILIZAR", "version": c["version"], "no_comprobante_fc": "FC-123"})
        assert r.status_code == 200, r.text
        c = r.json()
        assert c["estado"] == "CRUZADA"  # auto-avance: no hay items de cruce

        # el asiento cuadra: 2 renglones débito (160.000) + crédito 160.000
        r = api.get("/fin/contabilidad/movimientos?periodo=2026-11", headers=_h(CORREO_CONTA))
        mov = r.json()
        assert mov["cuadra"] is True
        assert mov["total_debito"] >= 160000

        # egreso sin nº → 422; con nº → REEMBOLSADA
        r = api.post(f"/fin/cajas/{c['id']}/transicion", headers=_h(CORREO_TESO),
                     json={"accion": "EGRESAR", "version": c["version"]})
        assert r.status_code == 422
        r = api.post(f"/fin/cajas/{c['id']}/transicion", headers=_h(CORREO_TESO),
                     json={"accion": "EGRESAR", "version": c["version"], "no_egreso_banco": "EGR-10492", "banco": "Davivienda"})
        assert r.status_code == 200, r.text
        assert r.json()["estado"] == "REEMBOLSADA"

        # no se elimina una caja contabilizada
        r = api.delete(f"/fin/cajas/{c['id']}", headers=_h(CORREO_ADMIN))
        assert r.status_code == 400

    def test_radicar_vacia_da_422(self, api):
        r = api.post("/fin/cajas", headers=_h(CORREO_TESO), json={
            "sede": "CALI-TEST", "responsable": "Teso Sc", "periodo": "2026-11", "saldo_inicial": 0})
        vacia = r.json()
        r = api.post(f"/fin/cajas/{vacia['id']}/radicar", headers=_h(CORREO_TESO))
        assert r.status_code == 422

    def test_duplicada_mismo_periodo(self, api):
        api.post("/fin/cajas", headers=_h(CORREO_TESO), json={
            "sede": "Medellín", "responsable": "Teso Sc", "periodo": "2026-10"})
        r = api.post("/fin/cajas", headers=_h(CORREO_TESO), json={
            "sede": "Medellín", "responsable": "Teso Sc", "periodo": "2026-10"})
        assert r.status_code == 409
