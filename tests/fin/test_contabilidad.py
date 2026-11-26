"""Contabilidad: carga DIAN (Excel/ZIP-XML), flujo tramitar→radicar→causar
(con movimientos que cuadran), glosas, dedup; y pagos urgentes de punta a punta."""
import io
import zipfile

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from conftest import token_modulo
from financiero.models.usuario import RolUsuario, Usuario

CORREO_CONTA = "conta.fin@integralogistica.com"
CORREO_TESO = "teso.fin@integralogistica.com"
CORREO_REQ = "sol.fin@integralogistica.com"
CORREO_JEFE = "jefec.fin@integralogistica.com"
CORREO_COMPRAS = "comprasc.fin@integralogistica.com"
CORREO_CTRL = "ctrlc.fin@integralogistica.com"


@pytest.fixture()
def gente(sesion):
    from financiero.models.usuario import Usuario as U
    for correo, rol, extra in [
        (CORREO_CONTA, RolUsuario.CONTABILIDAD, {"nombre": "Conta Fin"}),
        (CORREO_TESO, RolUsuario.TESORERIA, {"nombre": "Teso Fin", "es_aprobador_pagos": True}),
        (CORREO_REQ, RolUsuario.REQUISICION, {"nombre": "Sol Fin", "area": "Tecnología"}),
        (CORREO_JEFE, RolUsuario.JEFE_AREA, {"nombre": "Jefec Fin", "area": "Tecnología"}),
        (CORREO_COMPRAS, RolUsuario.COMPRAS, {"nombre": "Comprasc Fin"}),
        (CORREO_CTRL, RolUsuario.CONTROLLER, {"nombre": "Ctrlc Fin"}),
    ]:
        sesion.add(U(username=correo.split("@")[0], correo=correo, rol=rol, activo=True, **extra))
    sesion.commit()


@pytest.fixture()
def api(engine, gente):
    from rutas.fin_contabilidad import router as rc
    from rutas.fin_pagos import router as rp
    from rutas.fin_requisiciones import router as rr
    app = FastAPI()
    app.include_router(rc)
    app.include_router(rp)
    app.include_router(rr)
    return TestClient(app)


def _h(correo: str) -> dict:
    return {"Authorization": f"Bearer {token_modulo(correo)}"}


ENCABEZADOS = ["Tipo de documento", "CUFE/CUDE", "Prefijo", "Folio", "Fecha Emisión",
               "NIT Emisor", "Nombre Emisor", "IVA", "Total"]


def _xlsx_dian(filas: list[list]) -> bytes:
    import openpyxl
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(ENCABEZADOS)
    for f in filas:
        ws.append(f)
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.read()


FILA_1 = ["Factura electrónica", "CUFE0000000000000000000000000001", "", "100", "2026-11-20",
          "900555111", "PROVEEDOR ANDINO SAS", 38000.0, 438000.0]
FILA_2 = ["Factura electrónica", "CUFE0000000000000000000000000000002", "SE", "450", "15/11/2026",
          "800222333-5", "COMERCIAL DEL VALLE", 0.0, 150000.0]
FILA_PROPIA = ["Factura electrónica", "CUFEPROPIA00000000000000000000000009", "", "900", "2026-11-20",
               "900164363", "INTEGRA CADENA DE SERVICIOS S A S", 0.0, 990000.0]
FILA_ACUSE = ["Application response", "CUFEACUSE000000000000000000000000000", "", "1", "2026-11-20",
              "", "", 0.0, 0.0]


class TestCargaExcel:
    def test_carga_con_dedup_y_exclusiones(self, api):
        r = api.post("/fin/contabilidad/facturas/cargar", headers=_h(CORREO_CONTA),
                     files={"archivo": ("dian.xlsx", io.BytesIO(_xlsx_dian([FILA_1, FILA_2, FILA_PROPIA, FILA_ACUSE])),
                                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")})
        assert r.status_code == 200, r.text
        data = r.json()
        assert data["nuevas"] == 2          # propia y acuse fuera
        assert data["propias"] == 1
        assert data["omitidas"] == 1
        # re-cargar el mismo archivo → todo duplicado
        r = api.post("/fin/contabilidad/facturas/cargar", headers=_h(CORREO_CONTA),
                     files={"archivo": ("dian.xlsx", io.BytesIO(_xlsx_dian([FILA_1])),
                                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")})
        assert r.json()["nuevas"] == 0
        assert r.json()["duplicadas"] == 1

    def test_prefijo_se_concatena_al_numero(self, api):
        r = api.post("/fin/contabilidad/facturas/cargar", headers=_h(CORREO_CONTA),
                     files={"archivo": ("dian.xlsx", io.BytesIO(_xlsx_dian([FILA_2])),
                                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")})
        f = r.json()["facturas"][0]
        assert f["numero_factura"] == "SE450"
        assert f["nit_proveedor"] == "800222333-5"  # del Excel se conserva tal cual (el XML sí quita el DV)


XML_UBL = f"""<?xml version="1.0" encoding="UTF-8"?>
<Invoice xmlns="urn:oasis:names:specification:ubl:schema:xsd:Invoice-2"
         xmlns:cac="urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2"
         xmlns:cbc="urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2">
  <cbc:ID>FE880</cbc:ID>
  <cbc:UUID>XMLCUFE0000000000000000000000000003</cbc:UUID>
  <cbc:IssueDate>2026-11-21</cbc:IssueDate>
  <cac:AccountingSupplierParty>
    <cac:Party>
      <cac:PartyTaxScheme>
        <cbc:CompanyID>901777888-1</cbc:CompanyID>
        <cbc:RegistrationName>Servicios Generales del Norte SAS</cbc:RegistrationName>
      </cac:PartyTaxScheme>
    </cac:Party>
  </cac:AccountingSupplierParty>
  <cac:TaxTotal>
    <cbc:ID>01</cbc:ID>
    <cbc:TaxAmount>95000.00</cbc:TaxAmount>
  </cac:TaxTotal>
  <cac:LegalMonetaryTotal>
    <cbc:PayableAmount>745000.00</cbc:PayableAmount>
  </cac:LegalMonetaryTotal>
</Invoice>"""


class TestCargaZip:
    def test_zip_con_xml(self, api):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as z:
            z.writestr("factura880.xml", XML_UBL)
            z.writestr("factura880.pdf", b"%PDF-falso-de-prueba")
        buf.seek(0)
        r = api.post("/fin/contabilidad/facturas/cargar", headers=_h(CORREO_CONTA),
                     files={"archivo": ("proveedor.zip", buf, "application/zip")})
        assert r.status_code == 200, r.text
        data = r.json()
        assert data["nuevas"] == 1
        f = data["facturas"][0]
        assert f["origen"] == "xml"
        assert f["proveedor"] == "SERVICIOS GENERALES DEL NORTE SAS"
        assert f["total"] == 745000
        assert f["iva"] == 95000


class TestFlujoFacturas:
    def test_tramitar_radicar_causar_cuadra(self, api):
        # cargar
        r = api.post("/fin/contabilidad/facturas/cargar", headers=_h(CORREO_CONTA),
                     files={"archivo": ("dian.xlsx", io.BytesIO(_xlsx_dian([FILA_1])),
                                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")})
        f = r.json()["facturas"][0]

        # conta no puede saltarse el trámite
        r = api.post(f"/fin/contabilidad/facturas/{f['id']}/transicion",
                     headers=_h(CORREO_CONTA), json={"accion": "RADICAR", "version": f["version"]})
        assert r.status_code == 400

        # tramitar
        r = api.post(f"/fin/contabilidad/facturas/{f['id']}/transicion",
                     headers=_h(CORREO_CONTA), json={"accion": "TRAMITAR", "version": f["version"]})
        assert r.status_code == 200, r.text
        f = r.json()

        # radicar con OC
        r = api.post(f"/fin/contabilidad/facturas/{f['id']}/transicion",
                     headers=_h(CORREO_CONTA),
                     json={"accion": "RADICAR", "version": f["version"], "datos_adicionales": {"oc": "2026-390"}})
        assert r.status_code == 200, r.text
        f = r.json()
        assert f["estado"] == "RADICADA"
        assert f["oc_vinculada"] == "2026-390"

        # causar (solo contabilidad) → movimientos que cuadran
        r = api.post(f"/fin/contabilidad/facturas/{f['id']}/transicion",
                     headers=_h(CORREO_CONTA), json={"accion": "CAUSAR", "version": f["version"]})
        assert r.status_code == 200, r.text
        assert r.json()["estado"] == "CAUSADA"

        r = api.get("/fin/contabilidad/movimientos", headers=_h(CORREO_CONTA))
        mov = r.json()
        assert mov["cuadra"] is True
        assert mov["total_debito"] == 438000.0

    def test_glosa_exige_motivo(self, api):
        r = api.post("/fin/contabilidad/facturas/cargar", headers=_h(CORREO_CONTA),
                     files={"archivo": ("dian.xlsx", io.BytesIO(_xlsx_dian([FILA_2])),
                                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")})
        f = r.json()["facturas"][0]
        r = api.post(f"/fin/contabilidad/facturas/{f['id']}/transicion",
                     headers=_h(CORREO_CONTA), json={"accion": "GLOSAR", "version": f["version"]})
        assert r.status_code == 422
        r = api.post(f"/fin/contabilidad/facturas/{f['id']}/transicion",
                     headers=_h(CORREO_CONTA),
                     json={"accion": "GLOSAR", "version": f["version"], "observacion": "Valor no coincide"})
        assert r.json()["estado"] == "GLOSADA"
        assert r.json()["glosas"][0]["motivo"] == "Valor no coincide"

    def test_solo_admin_anula(self, api):
        r = api.post("/fin/contabilidad/facturas/cargar", headers=_h(CORREO_CONTA),
                     files={"archivo": ("dian.xlsx", io.BytesIO(_xlsx_dian([FILA_1])),
                                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")})
        f = r.json()["facturas"][0]
        r = api.post(f"/fin/contabilidad/facturas/{f['id']}/transicion",
                     headers=_h(CORREO_CONTA), json={"accion": "ANULAR", "version": f["version"]})
        assert r.status_code == 403


ITEM = {"descripcion": "Servicio urgente", "cantidad": 1, "unidad": "UND",
        "precio_unitario": 200000, "total": 200000, "centro_costo": "100130"}


class TestPagosUrgentes:
    def test_nace_al_aprobar_y_lo_paga_tesoreria(self, api):
        # requisición con pago urgente aprobado
        r = api.post("/fin/requisiciones", headers=_h(CORREO_REQ), json={
            "lugar_entrega": "Oficina", "items": [ITEM], "subtotal": 200000, "iva": 0, "total": 200000,
        })
        req = r.json()
        for correo, datos in [
            (CORREO_JEFE, None),
            (CORREO_COMPRAS, {"pago_urgente": True, "pago_fecha": "2099-06-15"}),
            (CORREO_CTRL, {"pago_urgente_aprobado": True}),
        ]:
            r = api.post(f"/fin/requisiciones/{req['id']}/transicion", headers=_h(correo),
                         json={"accion": "APROBAR", "version": r.json()["version"], "datos_adicionales": datos})
            assert r.status_code == 200, r.text

        # la bandeja de pagos tiene el pendiente
        r = api.get("/fin/pagos?estado=PENDIENTE", headers=_h(CORREO_TESO))
        pagos = [p for p in r.json()["pagos"] if p["consecutivo_req"] == req["consecutivo"]]
        assert len(pagos) == 1
        p = pagos[0]
        assert p["valor"] == 200000
        assert p["oc_numero"]

        # un rol cualquiera no puede pagar
        r = api.post(f"/fin/pagos/{p['id']}/pagar", headers=_h(CORREO_REQ), json={"version": p["version"]})
        assert r.status_code == 403

        # tesorería lo paga con referencia
        r = api.post(f"/fin/pagos/{p['id']}/pagar", headers=_h(CORREO_TESO),
                     json={"version": p["version"], "referencia": "TRANSFER-001"})
        assert r.status_code == 200, r.text
        assert r.json()["estado"] == "PAGADO"
        assert r.json()["referencia"] == "TRANSFER-001"

        # doble pago → 400
        r = api.post(f"/fin/pagos/{p['id']}/pagar", headers=_h(CORREO_TESO), json={})
        assert r.status_code == 400

    def test_rechazo_exige_motivo(self, api):
        r = api.post("/fin/requisiciones", headers=_h(CORREO_REQ), json={
            "lugar_entrega": "Oficina", "items": [ITEM], "subtotal": 100000, "iva": 0, "total": 100000,
        })
        req = r.json()
        for correo, datos in [
            (CORREO_JEFE, None),
            (CORREO_COMPRAS, {"pago_urgente": True, "pago_fecha": "2099-06-15"}),
            (CORREO_CTRL, {"pago_urgente_aprobado": True}),
        ]:
            r = api.post(f"/fin/requisiciones/{req['id']}/transicion", headers=_h(correo),
                         json={"accion": "APROBAR", "version": r.json()["version"], "datos_adicionales": datos})
        r = api.get("/fin/pagos?estado=PENDIENTE", headers=_h(CORREO_TESO))
        p = [x for x in r.json()["pagos"] if x["consecutivo_req"] == req["consecutivo"]][0]
        r = api.post(f"/fin/pagos/{p['id']}/rechazar", headers=_h(CORREO_TESO), json={"version": p["version"]})
        assert r.status_code == 422
        r = api.post(f"/fin/pagos/{p['id']}/rechazar", headers=_h(CORREO_TESO),
                     json={"version": p["version"], "motivo": "Datos bancarios errados"})
        assert r.json()["estado"] == "RECHAZADO"
