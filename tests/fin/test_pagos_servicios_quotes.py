"""Paridad Pagos urgentes, Cotizaciones/Anticipos y Servicios públicos:
herencia bancaria, contabilizada que bloquea, reversiones, pago manual,
generar-periodo, PATCH celda, contratos + propagación, quotes con pago
asociado y cruce por cercanía de valor."""
import pytest
from datetime import datetime, timezone
from fastapi import FastAPI
from fastapi.testclient import TestClient

from conftest import token_modulo
from financiero.models.usuario import RolUsuario, Usuario

CORREO_TESO = "teso.pq@integralogistica.com"
CORREO_CONTA = "conta.pq@integralogistica.com"
CORREO_OPER = "oper.pq@integralogistica.com"
CORREO_ADMIN = "admin.pq@integralogistica.com"


@pytest.fixture()
def gente(sesion):
    from financiero.models.usuario import Usuario as U
    from financiero.models.catalogo import ContratoServicio, Proveedor, SedeCajaMenor
    for correo, rol in [
        (CORREO_TESO, RolUsuario.TESORERIA),
        (CORREO_CONTA, RolUsuario.CONTABILIDAD),
        (CORREO_OPER, RolUsuario.REQUISICION),
        (CORREO_ADMIN, RolUsuario.ADMIN),
    ]:
        sesion.add(U(username=correo.split("@")[0], nombre=correo.split("@")[0].title(),
                     correo=correo, rol=rol, activo=True, area="Tecnología"))
    # proveedor del catálogo con datos bancarios (para la herencia)
    sesion.add(Proveedor(nit="900777001", razon_social="PROVEEDOR BANCARIO SA",
                         banco="Bancolombia", tipo_cuenta="Ahorros", numero_cuenta="000-111-222"))
    # contrato fijo de servicios
    sesion.add(ContratoServicio(servicio="Energía", inmueble="CEDI Galapa", categoria="publicos",
                                contrato="E-123", nit_tercero="900555000", tercero="ELECTRIFICADORA",
                                fecha_exp="20 de cada mes"))
    sesion.add(SedeCajaMenor(nombre="GALAPA-PQ", etiqueta="CEDI Galapa", responsable="Teso Pq",
                             identificacion="123", jefe_inmediato="Admin Pq", cco="519104007", cuenta="11051007"))
    sesion.commit()


@pytest.fixture()
def api(engine, gente):
    from rutas.fin_pagos import router as rpagos
    from rutas.fin_servicios import router as rserv
    from rutas.fin_quotes import router as rquotes
    app = FastAPI()
    app.include_router(rpagos)
    app.include_router(rserv)
    app.include_router(rquotes)
    return TestClient(app)


def _h(correo: str) -> dict:
    return {"Authorization": f"Bearer {token_modulo(correo)}"}


class TestPagosParidad:
    def test_manual_hereda_datos_bancarios(self, api):
        r = api.post("/fin/pagos", headers=_h(CORREO_TESO), json={
            "proveedor": "PROVEEDOR BANCARIO SA", "nit": "900777001",
            "tipo_doc": "Cuenta de cobro", "numero_doc": "CC-500",
            "valor": 450000, "fecha_requerida": "2099-03-15",
            "requerimiento": "CUENTA DE COBRO CC-500", "asunto": "SERVICIOS DE FEBRERO",
        })
        assert r.status_code == 200, r.text
        p = r.json()
        assert p["origen"] == "manual"
        assert p["banco"] == "Bancolombia"       # heredado del catálogo
        assert p["no_cuenta"] == "000-111-222"
        assert p["tipo_cuenta"] == "Ahorros"

    def test_contabilizada_bloquea_y_habilita_pago(self, api):
        p = api.post("/fin/pagos", headers=_h(CORREO_TESO), json={
            "proveedor": "OTRO PROVEEDOR", "nit": "900888002",
            "valor": 100000, "fecha_requerida": "2099-03-15",
        }).json()
        # operador no puede conmutar contabilizada
        r = api.post(f"/fin/pagos/{p['id']}/contabilizada", headers=_h(CORREO_OPER), json={"version": 1})
        assert r.status_code == 403
        # contabilidad la desmarca → el pago queda bloqueado
        api.post(f"/fin/pagos/{p['id']}/contabilizada", headers=_h(CORREO_CONTA), json={"version": 1})
        r = api.post(f"/fin/pagos/{p['id']}/pagar", headers=_h(CORREO_TESO),
                     json={"version": 2, "referencia": "TR-1"})
        assert r.status_code == 400
        assert "contabilizada" in r.json()["detail"].lower()
        # la vuelve a marcar → paga (el 400 de arriba NO incrementó la versión)
        api.post(f"/fin/pagos/{p['id']}/contabilizada", headers=_h(CORREO_CONTA), json={"version": 2})
        r = api.post(f"/fin/pagos/{p['id']}/pagar", headers=_h(CORREO_TESO),
                     json={"version": 3, "referencia": "TR-1"})
        assert r.status_code == 200, r.text
        assert r.json()["estado"] == "PAGADO"
        # reabrir → PENDIENTE limpio
        r = api.post(f"/fin/pagos/{p['id']}/reabrir", headers=_h(CORREO_TESO), json={"version": 4})
        assert r.json()["estado"] == "PENDIENTE"
        assert r.json()["referencia"] is None

    def test_rechazar_reconsiderar_y_filtrar(self, api):
        a = api.post("/fin/pagos", headers=_h(CORREO_TESO), json={
            "proveedor": "PAGADOR UNO", "valor": 90000, "fecha_requerida": "2020-01-10",  # vencido
        }).json()
        b = api.post("/fin/pagos", headers=_h(CORREO_TESO), json={
            "proveedor": "PAGADOR DOS", "valor": 70000, "fecha_requerida": "2099-01-10",
        }).json()
        api.post(f"/fin/pagos/{a['id']}/rechazar", headers=_h(CORREO_TESO),
                 json={"version": 1, "motivo": "Falta soporte de entrega"})
        r = api.post(f"/fin/pagos/{a['id']}/reconsiderar", headers=_h(CORREO_TESO), json={"version": 2})
        assert r.json()["estado"] == "PENDIENTE"
        assert r.json()["motivo_rechazo"] is None
        # resumen con vencidos y búsqueda
        r = api.get("/fin/pagos", headers=_h(CORREO_TESO))
        res = r.json()["resumen"]
        assert res["vencidos"] >= 1
        r = api.get("/fin/pagos?q=PAGADOR DOS", headers=_h(CORREO_TESO))
        assert len(r.json()["pagos"]) == 1
        # motivos frecuentes
        r = api.get("/fin/pagos/motivos-rechazo", headers=_h(CORREO_TESO))
        assert len(r.json()["motivos"]) >= 5
        # eliminar limpia vínculos de quotes (solo admin)
        r = api.delete(f"/fin/pagos/{b['id']}", headers=_h(CORREO_TESO))
        assert r.status_code == 403
        r = api.delete(f"/fin/pagos/{b['id']}", headers=_h(CORREO_ADMIN))
        assert r.status_code == 200


class TestQuotes:
    def test_anticipo_con_pago_asociado(self, api):
        r = api.post("/fin/quotes", headers=_h(CORREO_TESO), json={
            "tipo": "ANTICIPO", "tipo_comprobante": "EGC", "proceso": "CAJAS MENORES",
            "sede_cm": "GALAPA-PQ", "proveedor": "ANTICIPO DESTINATARIO",
            "nit": "900777001", "valor": 300000, "crear_pago": True,
        })
        assert r.status_code == 200, r.text
        q = r.json()
        assert q["estado"] == "PENDIENTE"
        assert q["pago_id"]
        # el pago asociado hereda los datos bancarios del catálogo
        pagos = api.get("/fin/pagos?q=ANTICIPO DESTINATARIO", headers=_h(CORREO_TESO)).json()["pagos"]
        assert pagos and pagos[0]["origen"] == "anticipo"
        assert pagos[0]["banco"] == "Bancolombia"

    def test_candidatos_por_cercania_y_cruce(self, api, sesion):
        from financiero.models.contabilidad import EstadoFactura, FacturaDian
        for num, total in (("F-100", 250000), ("F-200", 299000), ("F-300", 500000)):
            sesion.add(FacturaDian(numero_factura=num, nit_proveedor="900777001", proveedor="PROVEEDOR BANCARIO SA",
                                   fecha_emision=datetime.now(timezone.utc), total=total,
                                   estado=EstadoFactura.TRAMITADA.value))
        sesion.commit()
        q = api.post("/fin/quotes", headers=_h(CORREO_TESO), json={
            "tipo": "COTIZACION", "proveedor": "PROVEEDOR BANCARIO SA",
            "nit": "900777001", "valor": 300000,
        }).json()
        cands = api.get(f"/fin/quotes/{q['id']}/candidatos", headers=_h(CORREO_TESO)).json()
        assert [c["numero_factura"] for c in cands] == ["F-200", "F-100", "F-300"]  # cercanía ASC
        r = api.post(f"/fin/quotes/{q['id']}/cruzar", headers=_h(CORREO_TESO),
                     json={"factura_id": cands[0]["id"]})
        assert r.status_code == 200, r.text
        assert r.json()["estado"] == "CRUZADA"
        assert r.json()["factura_numero"] == "F-200"
        # legalizar y deshacer legalización
        r = api.post(f"/fin/quotes/{q['id']}/legalizar", headers=_h(CORREO_TESO),
                     json={"observaciones": "Con recibos R-1 y R-2", "recibos": ["R-1", "R-2"]})
        assert r.json()["estado"] == "LEGALIZADA"
        r = api.post(f"/fin/quotes/{q['id']}/deshacer-legalizacion", headers=_h(CORREO_TESO))
        assert r.json()["estado"] == "CRUZADA"
        r = api.post(f"/fin/quotes/{q['id']}/descruzar", headers=_h(CORREO_TESO))
        assert r.json()["estado"] == "PENDIENTE"

    def test_sede_invalida_y_proceso_invalido(self, api):
        r = api.post("/fin/quotes", headers=_h(CORREO_TESO), json={
            "tipo": "ANTICIPO", "tipo_comprobante": "EGC", "proceso": "INVENTADO",
            "proveedor": "X", "valor": 1,
        })
        assert r.status_code == 422
        r = api.post("/fin/quotes", headers=_h(CORREO_TESO), json={
            "tipo": "ANTICIPO", "sede_cm": "NOEXISTE", "proveedor": "X", "valor": 1,
        })
        assert r.status_code == 422


class TestServiciosParidad:
    def test_generar_periodo_idempotente(self, api):
        r = api.post("/fin/servicios/generar-periodo?periodo=2027-05", headers=_h(CORREO_TESO))
        assert r.status_code == 200, r.text
        assert r.json()["creados"] == 1          # un contrato activo en el maestro
        # idempotente
        r2 = api.post("/fin/servicios/generar-periodo?periodo=2027-05", headers=_h(CORREO_TESO))
        assert r2.json()["creados"] == 0
        datos = api.get("/fin/servicios?periodo=2027-05", headers=_h(CORREO_TESO)).json()
        s = datos["servicios"][0]
        assert s["estado"] == "NO_GENERADO"
        assert s["contrato"] == "E-123"
        assert s["nit_tercero"] == "900555000"
        assert s["fecha_exp"] == "20 de cada mes"

    def test_patch_celda_y_urgencia(self, api):
        api.post("/fin/servicios/generar-periodo?periodo=2027-06", headers=_h(CORREO_TESO))
        s = api.get("/fin/servicios?periodo=2027-06", headers=_h(CORREO_TESO)).json()["servicios"][0]
        r = api.patch(f"/fin/servicios/{s['id']}/celda", headers=_h(CORREO_TESO),
                      json={"campo": "valor", "valor": 1234500, "version": s["version"]})
        assert r.status_code == 200, r.text
        assert r.json()["valor"] == 1234500
        r = api.patch(f"/fin/servicios/{s['id']}/celda", headers=_h(CORREO_TESO),
                      json={"campo": "estado", "valor": "ENVIADO_CONTAB", "version": r.json()["version"]})
        assert r.json()["estado"] == "ENVIADO_CONTAB"
        r = api.patch(f"/fin/servicios/{s['id']}/celda", headers=_h(CORREO_TESO),
                      json={"campo": "estado", "valor": "INVENTADO", "version": r.json()["version"]})
        assert r.status_code == 422
        # urgencia cicla
        r = api.post(f"/fin/servicios/{s['id']}/urgencia", headers=_h(CORREO_TESO))
        assert r.json()["urgente_recepcion"] == "URGENTE"
        r = api.post(f"/fin/servicios/{s['id']}/urgencia", headers=_h(CORREO_TESO))
        assert r.json()["urgente_recepcion"] == "PROXIMA"
        r = api.post(f"/fin/servicios/{s['id']}/urgencia", headers=_h(CORREO_TESO))
        assert r.json()["urgente_recepcion"] is None

    def test_contrato_propagar(self, api):
        api.post("/fin/servicios/generar-periodo?periodo=2027-07", headers=_h(CORREO_TESO))
        contratos = api.get("/fin/servicios/contratos", headers=_h(CORREO_TESO)).json()
        c = contratos[0]
        api.put(f"/fin/servicios/contratos/{c['id']}", headers=_h(CORREO_TESO),
                json={**c, "fecha_exp": "25 de cada mes", "servicio": "Energía",
                      "inmueble": "CEDI Galapa", "categoria": "publicos", "contrato": "E-123"})
        r = api.put(f"/fin/servicios/contratos/{c['id']}/propagar?alcance=todos", headers=_h(CORREO_TESO))
        assert r.status_code == 200
        assert r.json()["cambiadas"] >= 1
        s = api.get("/fin/servicios?periodo=2027-07", headers=_h(CORREO_TESO)).json()["servicios"][0]
        assert s["fecha_exp"] == "25 de cada mes"

    def test_filtros_categoria_y_resumen(self, api):
        api.post("/fin/servicios/generar-periodo?periodo=2027-08", headers=_h(CORREO_TESO))
        datos = api.get("/fin/servicios?periodo=2027-08&categoria=publicos", headers=_h(CORREO_TESO)).json()
        assert len(datos["servicios"]) == 1
        assert "por_categoria" in datos["resumen"]
        assert "vencidos" in datos["resumen"]
