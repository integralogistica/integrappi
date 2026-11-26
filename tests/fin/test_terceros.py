"""Terceros de Compras (proveedores nuevos): creación desde requisición,
aprobación de Compras, rechazo con motivo, check de Contabilidad (software
contable) y permisos por sección."""
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from conftest import token_modulo
from financiero.models.usuario import RolUsuario, Usuario

CORREO_REQ = "solicitante.ter@integralogistica.com"
CORREO_JEFE = "jefe.ter@integralogistica.com"
CORREO_COMPRAS = "compras.ter@integralogistica.com"
CORREO_CONTA = "conta.ter@integralogistica.com"
CORREO_CONSULTA = "consulta.ter@integralogistica.com"
AREA = "Tecnología"


@pytest.fixture()
def gente(sesion):
    from financiero.models.usuario import Usuario as U
    for correo, rol, extra in [
        (CORREO_REQ, RolUsuario.REQUISICION, {"nombre": "Solicitante Terc", "area": AREA}),
        (CORREO_JEFE, RolUsuario.JEFE_AREA, {"nombre": "Jefe Terc", "area": AREA}),
        (CORREO_COMPRAS, RolUsuario.COMPRAS, {"nombre": "Compras Terc"}),
        (CORREO_CONTA, RolUsuario.CONTABILIDAD, {"nombre": "Conta Terc"}),
        (CORREO_CONSULTA, RolUsuario.CONSULTA, {"nombre": "Consulta Terc"}),
    ]:
        sesion.add(U(username=correo.split("@")[0], correo=correo, rol=rol, activo=True, **extra))
    sesion.commit()


@pytest.fixture()
def api(engine, gente):
    from rutas.fin_terceros import router as ruta_fin_terceros
    app = FastAPI()
    app.include_router(ruta_fin_terceros)
    return TestClient(app)


def _h(correo: str) -> dict:
    return {"Authorization": f"Bearer {token_modulo(correo)}"}


def _payload(**extra) -> dict:
    base = {
        "razon_social": "PROVEEDOR NUEVO SAS",
        "nit": "901234567",
        "nit_pago": "901234567",
        "tipo_cuenta": "Ahorros",
        "banco": "Bancolombia",
        "no_cuenta": "123456789",
        "area": AREA,
    }
    base.update(extra)
    return base


class TestTerceros:
    def test_crear_queda_pendiente_de_compras(self, api):
        r = api.post("/fin/terceros", json=_payload(), headers=_h(CORREO_REQ))
        assert r.status_code == 201, r.text
        ter = r.json()
        assert ter["estado_compras"] == "pendiente_compras"
        assert ter["razon_social"] == "PROVEEDOR NUEVO SAS"
        assert ter["nit"] == "901234567"
        assert ter["origen_requisicion"] is True
        assert ter["version"] == 1

    def test_nit_pendiente_duplicado_da_409(self, api):
        r = api.post("/fin/terceros", json=_payload(), headers=_h(CORREO_REQ))
        assert r.status_code == 201
        # mismo NIT con puntos/ceros: se limpia a dígitos
        r2 = api.post("/fin/terceros", json=_payload(nit="0901.234.567"), headers=_h(CORREO_REQ))
        assert r2.status_code == 409
        assert r2.json()["detail"]

    def test_creacion_directa_solo_compras(self, api):
        # un solicitante no puede crear tercero "manual"
        r = api.post("/fin/terceros", json=_payload(origen_requisicion=False), headers=_h(CORREO_REQ))
        assert r.status_code == 403
        # compras sí
        r2 = api.post("/fin/terceros", json=_payload(origen_requisicion=False), headers=_h(CORREO_COMPRAS))
        assert r2.status_code == 201

    def test_rol_consulta_no_crea(self, api):
        r = api.post("/fin/terceros", json=_payload(), headers=_h(CORREO_CONSULTA))
        assert r.status_code == 403

    def test_aprobar_y_rechazar_solo_compras(self, api):
        ter = api.post("/fin/terceros", json=_payload(), headers=_h(CORREO_REQ)).json()
        # contabilidad no aprueba
        r = api.post(f"/fin/terceros/{ter['id']}/aprobar", headers=_h(CORREO_CONTA))
        assert r.status_code == 403
        # compras aprueba
        r = api.post(f"/fin/terceros/{ter['id']}/aprobar", headers=_h(CORREO_COMPRAS))
        assert r.status_code == 200, r.text
        ter = r.json()
        assert ter["estado_compras"] == "aprobado"
        assert ter["aprobado_por"] == "Compras Terc"
        assert ter["fecha_aprobacion"]
        # ya aprobado no se vuelve a aprobar
        r2 = api.post(f"/fin/terceros/{ter['id']}/aprobar", headers=_h(CORREO_COMPRAS))
        assert r2.status_code == 400

    def test_rechazar_exige_motivo(self, api):
        ter = api.post("/fin/terceros", json=_payload(), headers=_h(CORREO_REQ)).json()
        r = api.post(f"/fin/terceros/{ter['id']}/rechazar", headers=_h(CORREO_COMPRAS), json={"motivo": "no"})
        assert r.status_code == 422
        r = api.post(f"/fin/terceros/{ter['id']}/rechazar", headers=_h(CORREO_COMPRAS),
                     json={"motivo": "El proveedor no cumple los requisitos"})
        assert r.status_code == 200, r.text
        assert r.json()["estado_compras"] == "rechazado_compras"
        assert "no cumple" in r.json()["motivo_rechazo_compras"]

    def test_cargado_software_solo_contabilidad_y_bloqueado_si_pendiente(self, api):
        ter = api.post("/fin/terceros", json=_payload(), headers=_h(CORREO_REQ)).json()
        # compras no puede marcar
        r = api.post(f"/fin/terceros/{ter['id']}/cargado-software", headers=_h(CORREO_COMPRAS), json={})
        assert r.status_code == 403
        # contabilidad no puede marcar si sigue pendiente de Compras
        r = api.post(f"/fin/terceros/{ter['id']}/cargado-software", headers=_h(CORREO_CONTA), json={})
        assert r.status_code == 400
        # aprueba Compras → contabilidad marca (toggle)
        api.post(f"/fin/terceros/{ter['id']}/aprobar", headers=_h(CORREO_COMPRAS))
        r = api.post(f"/fin/terceros/{ter['id']}/cargado-software", headers=_h(CORREO_CONTA), json={})
        assert r.status_code == 200, r.text
        ter = r.json()
        assert ter["creado_en_software"] is True
        assert ter["creado_en_software_por"] == "Conta Terc"
        # toggle inverso limpia
        r = api.post(f"/fin/terceros/{ter['id']}/cargado-software", headers=_h(CORREO_CONTA), json={"marcar": False})
        assert r.json()["creado_en_software"] is False
        assert r.json()["creado_en_software_por"] is None

    def test_listar_con_filtros(self, api):
        api.post("/fin/terceros", json=_payload(), headers=_h(CORREO_REQ))
        api.post("/fin/terceros", json=_payload(razon_social="OTRO PROVEEDOR", nit="909998887"), headers=_h(CORREO_REQ))
        # por sección: contabilidad ve la lista (tiene "proveedores")
        r = api.get("/fin/terceros", headers=_h(CORREO_CONTA))
        assert r.status_code == 200
        assert len(r.json()) == 2
        # filtro por estado
        r = api.get("/fin/terceros", params={"estado": "aprobado"}, headers=_h(CORREO_CONTA))
        assert r.json() == []
        # búsqueda por NIT
        r = api.get("/fin/terceros", params={"q": "909998"}, headers=_h(CORREO_CONTA))
        assert len(r.json()) == 1
        assert r.json()[0]["razon_social"] == "OTRO PROVEEDOR"
        # pendientes_software: nada aprobado aún
        r = api.get("/fin/terceros", params={"pendientes_software": "true"}, headers=_h(CORREO_CONTA))
        assert r.json() == []
        # consulta NO tiene la sección proveedores → 403
        r = api.get("/fin/terceros", headers=_h(CORREO_CONSULTA))
        assert r.status_code == 403

    def test_editar_datos_bancarios(self, api):
        ter = api.post("/fin/terceros", json=_payload(), headers=_h(CORREO_REQ)).json()
        r = api.patch(f"/fin/terceros/{ter['id']}", headers=_h(CORREO_COMPRAS),
                      json={"banco": "Davivienda", "no_cuenta": "987654321"})
        assert r.status_code == 200, r.text
        assert r.json()["banco"] == "Davivienda"
        assert r.json()["no_cuenta"] == "987654321"
        assert r.json()["version"] == 2

    def test_eliminar_solo_admin(self, api, sesion):
        from financiero.models.usuario import Usuario as U
        sesion.add(U(username="admin.ter", correo="admin.ter@integralogistica.com",
                     rol=RolUsuario.ADMIN, activo=True, nombre="Admin Terc"))
        sesion.commit()
        ter = api.post("/fin/terceros", json=_payload(), headers=_h(CORREO_REQ)).json()
        r = api.delete(f"/fin/terceros/{ter['id']}", headers=_h(CORREO_COMPRAS))
        assert r.status_code == 403
        r = api.delete(f"/fin/terceros/{ter['id']}", headers=_h("admin.ter@integralogistica.com"))
        assert r.status_code == 200, r.text
        assert api.get("/fin/terceros", headers=_h(CORREO_COMPRAS)).json() == []

    def test_requisicion_enlaza_tercero(self, api, engine):
        from rutas.fin_requisiciones import router as ruta_fin_requisiciones
        from rutas.fin_terceros import router as ruta_fin_terceros
        app = FastAPI()
        app.include_router(ruta_fin_terceros)
        app.include_router(ruta_fin_requisiciones)
        cliente = TestClient(app)

        ter = cliente.post("/fin/terceros", json=_payload(), headers=_h(CORREO_REQ)).json()
        item = {"descripcion": "Teclado", "cantidad": 2, "unidad": "UND",
                "precio_unitario": 50000, "total": 100000, "centro_costo": "100130"}
        payload = {
            "lugar_entrega": "CEDI Montevideo", "items": [item],
            "subtotal": 100000, "iva": 0, "total": 100000,
            "proveedor_sugerido": "PROVEEDOR NUEVO SAS", "proveedor_nit": "901234567",
            "datos_front": {"tercero_id": ter["id"], "proveedor_nuevo": True},
            "proveedor_datos": {"receptor": "Juan", "cotizacion_ref": "COT-1"},
        }
        r = cliente.post("/fin/requisiciones", json=payload, headers=_h(CORREO_REQ))
        assert r.status_code == 200, r.text
        req = r.json()
        assert req["datos_front"]["tercero_id"] == ter["id"]
        assert req["proveedor_datos"]["receptor"] == "Juan"
        # el tercero quedó enlazado a la requisición
        r2 = cliente.get("/fin/terceros", headers=_h(CORREO_COMPRAS))
        ter_db = next(t for t in r2.json() if t["id"] == ter["id"])
        assert ter_db["requisicion_id"] == req["id"]
        assert ter_db["consecutivo_req"] == req["consecutivo"]
