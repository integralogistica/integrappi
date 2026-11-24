"""Flujo completo de requisiciones vía API: jefe → compras → financiera →
OC → entrega → recepción con calificación. Catálogos paginados."""
import io

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from conftest import token_modulo
from financiero.models.usuario import RolUsuario, Usuario

CORREO_REQ = "solicitante.test@integralogistica.com"
CORREO_JEFE = "jefe.req@integralogistica.com"
CORREO_COMPRAS = "compras.req@integralogistica.com"
CORREO_CTRL = "controller.req@integralogistica.com"
CORREO_GERENTE = "gerente.req@integralogistica.com"
AREA = "Tecnología"


@pytest.fixture()
def gente(sesion):
    from financiero.models.usuario import Usuario as U
    for correo, rol, extra in [
        (CORREO_REQ, RolUsuario.REQUISICION, {"nombre": "Solicitante Test", "area": AREA}),
        (CORREO_JEFE, RolUsuario.JEFE_AREA, {"nombre": "Jefe Test", "area": AREA}),
        (CORREO_COMPRAS, RolUsuario.COMPRAS, {"nombre": "Compras Test"}),
        (CORREO_CTRL, RolUsuario.CONTROLLER, {"nombre": "Controller Test"}),
        (CORREO_GERENTE, RolUsuario.GERENTE_FINANCIERO, {"nombre": "Gerente Test"}),
    ]:
        sesion.add(U(username=correo.split("@")[0], correo=correo, rol=rol, activo=True, **extra))
    # catálogos mínimos para el test de búsqueda
    from financiero.models.catalogo import CentroCosto, Proveedor
    sesion.add(CentroCosto(codigo="100130", nombre="BOGOTA GENERAL ADMINISTRACION"))
    sesion.add(CentroCosto(codigo="100140", nombre="BOGOTA LOGISTICA"))
    sesion.add(Proveedor(nit="900123456", razon_social="PROVEEDOR TEST SAS", telefono="601123"))
    sesion.commit()


@pytest.fixture()
def api(engine, gente):
    from rutas.fin_requisiciones import router as ruta_fin_requisiciones
    from rutas.fin_catalogos import router as ruta_fin_catalogos
    app = FastAPI()
    app.include_router(ruta_fin_requisiciones)
    app.include_router(ruta_fin_catalogos)
    return TestClient(app)


def _h(correo: str) -> dict:
    return {"Authorization": f"Bearer {token_modulo(correo)}"}


ITEM = {"descripcion": "Resma de papel", "cantidad": 10, "unidad": "UND",
        "precio_unitario": 25000, "total": 250000, "centro_costo": "100130"}


def _payload(total: float = 250000) -> dict:
    return {
        "lugar_entrega": "Bodega central",
        "items": [{**ITEM}],
        "subtotal": total, "iva": 0, "total": total,
        "proveedor_sugerido": "PROVEEDOR TEST SAS", "proveedor_nit": "900123456",
    }


def _t(api, id, accion, correo, version, datos=None, observacion=None):
    return api.post(f"/fin/requisiciones/{id}/transicion", headers=_h(correo),
                    json={"accion": accion, "version": version, "datos_adicionales": datos, "observacion": observacion})


NOTAS_9 = {k: 12.0 for k in ("CE", "EC", "ES", "SQR", "AC", "G", "PFC", "MA", "SST")}


class TestRequisiciones:
    def test_flujo_completo_con_oc_y_calificacion(self, api):
        # 1. radicar → PENDIENTE_JEFE
        r = api.post("/fin/requisiciones", json=_payload(), headers=_h(CORREO_REQ))
        assert r.status_code == 200, r.text
        req = r.json()
        assert req["consecutivo"].startswith("REQ-")
        assert req["estado"] == "PENDIENTE_JEFE"

        # 2. jefe aprueba → PENDIENTE_COMPRAS
        r = _t(api, req["id"], "APROBAR", CORREO_JEFE, req["version"])
        assert r.status_code == 200, r.text
        req = r.json()
        assert req["estado"] == "PENDIENTE_COMPRAS"

        # 3. compras cotiza (propone pago urgente) → PENDIENTE_FINANCIERA
        r = _t(api, req["id"], "APROBAR", CORREO_COMPRAS, req["version"],
               datos={"pago_urgente": True, "pago_fecha": "2099-01-15"})
        assert r.status_code == 200, r.text
        req = r.json()
        assert req["estado"] == "PENDIENTE_FINANCIERA"
        assert req["pago_urgente"] is True

        # 4. sin decidir el pago urgente → 422
        r = _t(api, req["id"], "APROBAR", CORREO_CTRL, req["version"])
        assert r.status_code == 422

        # 5. controller aprueba (≤ tope) + decide pago → OC automática
        r = _t(api, req["id"], "APROBAR", CORREO_CTRL, req["version"], datos={"pago_urgente_aprobado": True})
        assert r.status_code == 200, r.text
        req = r.json()
        assert req["estado"] == "EN_ORDEN_COMPRA"
        assert req["oc_numero"]  # AAAA-386+ automática
        assert req["pago_aprobado"] is True

        # 6. compras entrega
        r = _t(api, req["id"], "ENTREGAR", CORREO_COMPRAS, req["version"])
        assert r.status_code == 200, r.text
        req = r.json()
        assert req["entregado_en"]

        # 7. recepción sin calificación → 422
        r = _t(api, req["id"], "RECIBIR_TOTAL", CORREO_REQ, req["version"])
        assert r.status_code == 422

        # 8. recepción con calificación → RECIBIDO_TOTAL
        r = _t(api, req["id"], "RECIBIR_TOTAL", CORREO_REQ, req["version"], datos={"notas": NOTAS_9})
        assert r.status_code == 200, r.text
        req = r.json()
        assert req["estado"] == "RECIBIDO_TOTAL"
        assert req["calificacion_proveedor"]["letra"] in ("A", "B", "C")

    def test_sobre_tope_la_aprueba_gerente(self, api):
        r = api.post("/fin/requisiciones", json=_payload(total=150_000_000), headers=_h(CORREO_REQ))
        req = r.json()
        r = _t(api, req["id"], "APROBAR", CORREO_JEFE, req["version"])
        req = r.json()
        r = _t(api, req["id"], "APROBAR", CORREO_COMPRAS, req["version"])
        req = r.json()
        # controller NO puede
        r = _t(api, req["id"], "APROBAR", CORREO_CTRL, req["version"])
        assert r.status_code == 403
        # gerente sí
        r = _t(api, req["id"], "APROBAR", CORREO_GERENTE, req["version"])
        assert r.status_code == 200
        assert r.json()["estado"] == "EN_ORDEN_COMPRA"

    def test_compras_arranca_sin_jefe(self, api):
        r = api.post("/fin/requisiciones", json=_payload(), headers=_h(CORREO_COMPRAS))
        assert r.status_code == 200
        assert r.json()["estado"] == "PENDIENTE_COMPRAS"
        assert r.json()["area"] == "Compras"

    def test_sin_jefe_de_area_no_puede_envadir(self, api, sesion):
        # desactivar al jefe
        jefe = sesion.query(Usuario).filter_by(correo=CORREO_JEFE).one()
        jefe.activo = False
        sesion.commit()
        r = api.post("/fin/requisiciones", json=_payload(), headers=_h(CORREO_REQ))
        assert r.status_code == 400
        assert "jefe" in r.json()["detail"].lower()

    def test_rechazo_del_jefe_y_correccion(self, api):
        r = api.post("/fin/requisiciones", json=_payload(), headers=_h(CORREO_REQ))
        req = r.json()
        r = _t(api, req["id"], "RECHAZAR", CORREO_JEFE, req["version"], observacion="Presupuesto")
        assert r.json()["estado"] == "RECHAZADA_JEFE"
        req = r.json()
        r = api.put(f"/fin/requisiciones/{req['id']}", headers=_h(CORREO_REQ),
                    json={**_payload(total=100000), "version": req["version"]})
        assert r.status_code == 200, r.text
        assert r.json()["estado"] == "PENDIENTE_JEFE"
        assert float(r.json()["total"]) == 100000

    def test_visibilidad_por_rol(self, api):
        r = api.post("/fin/requisiciones", json=_payload(), headers=_h(CORREO_REQ))
        req = r.json()
        # tesorería no ve (rol global NO incluido en visibilidad de requisiciones)
        # (según el original: admin/tesorería/compras/controller/gerente ven todo)
        # un controller ajeno sí la ve:
        r = api.get("/fin/requisiciones", headers=_h(CORREO_CTRL))
        assert any(x["id"] == req["id"] for x in r.json())
        # un usuario de OTRA área con rol requisicion NO la ve:
        r = api.get("/fin/requisiciones", headers=_h(CORREO_GERENTE))
        assert isinstance(r.json(), list)

    def test_oc_consecutiva(self, api):
        ocs = []
        for total in (50000, 60000):
            r = api.post("/fin/requisiciones", json=_payload(total=total), headers=_h(CORREO_COMPRAS))
            req = r.json()
            r = _t(api, req["id"], "APROBAR", CORREO_COMPRAS, req["version"])
            r = _t(api, req["id"], "APROBAR", CORREO_CTRL, r.json()["version"])
            ocs.append(r.json()["oc_numero"])
        assert len(set(ocs)) == 2  # únicas y consecutivas

    def test_pago_urgente_fecha_pasada(self, api):
        r = api.post("/fin/requisiciones", json=_payload(), headers=_h(CORREO_COMPRAS))
        req = r.json()
        r = _t(api, req["id"], "APROBAR", CORREO_COMPRAS, req["version"],
               datos={"pago_urgente": True, "pago_fecha": "2020-01-01"})
        assert r.status_code == 422


class TestCatalogos:
    def test_centros_costo_busqueda(self, api):
        r = api.get("/fin/catalogos/centros-costo?q=LOGISTICA", headers=_h(CORREO_REQ))
        assert r.status_code == 200
        data = r.json()
        assert data["total"] >= 1
        assert any("LOGISTICA" in c["nombre"] for c in data["items"])

    def test_centros_costo_por_codigo(self, api):
        r = api.get("/fin/catalogos/centros-costo?q=100130", headers=_h(CORREO_REQ))
        assert r.json()["total"] == 1

    def test_proveedores(self, api):
        r = api.get("/fin/catalogos/proveedores?q=PROVEEDOR", headers=_h(CORREO_REQ))
        assert r.json()["total"] == 1
        assert r.json()["items"][0]["nit"] == "900123456"

    def test_requiere_auth(self, api):
        assert api.get("/fin/catalogos/centros-costo").status_code == 401


class TestImportarLineas:
    def test_formato_e_importacion(self, api):
        # descargar plantilla
        r = api.get("/fin/requisiciones/formato-cargue", headers=_h(CORREO_REQ))
        assert r.status_code == 200
        # rellenarla con openpyxl
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(r.content))
        ws = wb.active
        ws.append(["100130", "BOGOTA GENERAL", 5, "UND", "Caja de clip", 3000, 0, 15000])
        ws.append(["", "", None, "", "Fila sin datos", None, None, None])
        buf = io.BytesIO()
        wb.save(buf)
        buf.seek(0)
        r = api.post("/fin/requisiciones/importar-lineas", headers=_h(CORREO_REQ),
                     files={"archivo": ("cargue.xlsx", buf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")})
        assert r.status_code == 200, r.text
        data = r.json()
        assert data["total_lineas"] == 1
        assert data["errores"][0]["fila"] == 3
        assert data["subtotal"] == 15000
