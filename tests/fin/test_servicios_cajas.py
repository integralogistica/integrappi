"""Servicios públicos y cajas menores de punta a punta."""
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from conftest import token_modulo
from financiero.models.usuario import RolUsuario, Usuario

CORREO_TESO = "teso.sc@integralogistica.com"
CORREO_CONTA = "conta.sc@integralogistica.com"
CORREO_OPER = "oper.sc@integralogistica.com"


@pytest.fixture()
def gente(sesion):
    from financiero.models.usuario import Usuario as U
    for correo, rol in [
        (CORREO_TESO, RolUsuario.TESORERIA),
        (CORREO_CONTA, RolUsuario.CONTABILIDAD),
        (CORREO_OPER, RolUsuario.REQUISICION),
    ]:
        sesion.add(U(username=correo.split("@")[0], nombre=correo.split("@")[0].title(),
                     correo=correo, rol=rol, activo=True, area="Tecnología"))
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
    {"concepto": "Aseo oficina", "valor": 80000, "tipo": "gasto", "centro_costo": "100130"},
    {"concepto": "Reposición caja", "valor": 80000, "tipo": "reposicion"},
]


class TestCajas:
    def test_flujo_completo_con_asiento(self, api):
        # crear caja con movimientos
        r = api.post("/fin/cajas", headers=_h(CORREO_TESO), json={
            "sede": "Bogotá", "responsable": "Teso Sc", "periodo": "2026-11",
            "saldo_inicial": 300000, "movimientos": MOVIMIENTOS,
        })
        assert r.status_code == 200, r.text
        c = r.json()
        assert c["estado"] == "EN_EDICION"
        assert c["total_gastos"] == 160000

        # legalizar sin movimientos → 422 (probamos con otra caja vacía)
        r = api.post("/fin/cajas", headers=_h(CORREO_TESO), json={
            "sede": "Cali", "responsable": "Teso Sc", "periodo": "2026-11", "saldo_inicial": 0})
        vacia = r.json()
        r = api.post(f"/fin/cajas/{vacia['id']}/transicion", headers=_h(CORREO_TESO),
                     json={"accion": "LEGALIZAR", "version": vacia["version"]})
        assert r.status_code == 422

        # legalizar la buena
        r = api.post(f"/fin/cajas/{c['id']}/transicion", headers=_h(CORREO_TESO),
                     json={"accion": "LEGALIZAR", "version": c["version"]})
        assert r.status_code == 200, r.text
        c = r.json()
        assert c["estado"] == "LEGALIZADA"

        # solo contabilidad contabiliza
        r = api.post(f"/fin/cajas/{c['id']}/transicion", headers=_h(CORREO_TESO),
                     json={"accion": "CONTABILIZAR", "version": c["version"]})
        assert r.status_code == 403

        r = api.post(f"/fin/cajas/{c['id']}/transicion", headers=_h(CORREO_CONTA),
                     json={"accion": "CONTABILIZAR", "version": c["version"]})
        assert r.status_code == 200, r.text
        assert r.json()["estado"] == "CONTABILIZADA"

        # el asiento cuadra: 2 renglones débito (160.000) + crédito 160.000
        r = api.get("/fin/contabilidad/movimientos?periodo=2026-11", headers=_h(CORREO_CONTA))
        mov = r.json()
        assert mov["cuadra"] is True
        assert mov["total_debito"] >= 160000

        # no se elimina una caja contabilizada
        r = api.delete(f"/fin/cajas/{c['id']}", headers=_h(CORREO_TESO))
        assert r.status_code in (400, 403)

    def test_duplicada_mismo_periodo(self, api):
        api.post("/fin/cajas", headers=_h(CORREO_TESO), json={
            "sede": "Medellín", "responsable": "Teso Sc", "periodo": "2026-10"})
        r = api.post("/fin/cajas", headers=_h(CORREO_TESO), json={
            "sede": "Medellín", "responsable": "Teso Sc", "periodo": "2026-10"})
        assert r.status_code == 409
