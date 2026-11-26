"""Paridad Cajas Menores y Facturas DIAN: circuito FORM-FIN-007, folios,
política semanal, vinculado de facturas, validación por área, rechazo con
catálogo, cruce NC↔factura, trámite enriquecido, novedades del Drive,
cuenta de cobro, preview de carga y torre de control."""
import io
from datetime import datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from conftest import token_modulo
from financiero.models.usuario import RolUsuario, Usuario

CORREO_TESO = "teso.pd@integralogistica.com"
CORREO_CONTA = "conta.pd@integralogistica.com"
CORREO_CTRL = "ctrl.pd@integralogistica.com"
CORREO_ADMIN = "admin.pd@integralogistica.com"


@pytest.fixture()
def gente(sesion):
    from financiero.models.usuario import Usuario as U
    from financiero.models.catalogo import SedeCajaMenor
    for correo, rol in [
        (CORREO_TESO, RolUsuario.TESORERIA),
        (CORREO_CONTA, RolUsuario.CONTABILIDAD),
        (CORREO_CTRL, RolUsuario.CONTROLLER),
        (CORREO_ADMIN, RolUsuario.ADMIN),
    ]:
        sesion.add(U(username=correo.split("@")[0], nombre=correo.split("@")[0].title(),
                     correo=correo, rol=rol, activo=True, area="Tecnología"))
    sesion.add(SedeCajaMenor(nombre="GALAPA-PD", etiqueta="CEDI Galapa", responsable="Teso Pd",
                             identificacion="123", jefe_inmediato="Admin Pd", cco="519104007", cuenta="11051007"))
    sesion.commit()


@pytest.fixture()
def api(engine, gente):
    from rutas.fin_cajas import router as rcajas
    from rutas.fin_contabilidad import router as rcont
    from rutas.fin_catalogos import router as rcatalogos
    app = FastAPI()
    app.include_router(rcajas)
    app.include_router(rcont)
    app.include_router(rcatalogos)
    return TestClient(app)


def _h(correo: str) -> dict:
    return {"Authorization": f"Bearer {token_modulo(correo)}"}


MOVIMIENTOS_PD = [
    {"concepto": "Aseo oficina", "valor": 80000, "tipo": "gasto", "centro_costo": "100130", "tipo_documento": "recibo_caja"},
    {"concepto": "Mantenimiento", "valor": 120000, "tipo": "gasto", "tipo_documento": "factura_electronica",
     "nit": "900123456", "proveedor": "PROVEEDOR PD", "no_factura": "PD-001"},
]


class TestCajasParidad:
    def _caja(self, api, movs=None):
        r = api.post("/fin/cajas", headers=_h(CORREO_TESO), json={
            "sede": "GALAPA-PD", "responsable": "Teso Pd", "periodo": "2026-12",
            "saldo_inicial": 300000, "movimientos": movs if movs is not None else MOVIMIENTOS_PD,
        })
        assert r.status_code == 200, r.text
        return r.json()

    def test_politica_semanal_y_folios(self, api):
        c = self._caja(api)
        assert [m["folio"] for m in c["movimientos"]] == [1, 2]
        assert c["total_reembolso"] == 200000
        r = api.post(f"/fin/cajas/{c['id']}/radicar", headers=_h(CORREO_TESO))
        assert r.status_code == 200, r.text
        c = r.json()
        assert c["consecutivo"].startswith("CM-")
        assert set(c["politica"]) == {"fechaEnvio", "diaSemanaEnvio", "aTiempoParaViernes", "fechaEstimadaReembolso"}

    def test_excluir_folio_recacula_total(self, api):
        c = self._caja(api)
        api.post(f"/fin/cajas/{c['id']}/radicar", headers=_h(CORREO_TESO))
        # contabilidad excluye el folio 1 (RCM) → total_reembolso baja a 120.000
        r = api.patch(f"/fin/cajas/{c['id']}/items", headers=_h(CORREO_CONTA),
                      json={"folio": 1, "contabilizado": False})
        assert r.status_code == 200, r.text
        assert r.json()["total_reembolso"] == 120000
        # tesorería NO puede marcar contabilizado
        r = api.patch(f"/fin/cajas/{c['id']}/items", headers=_h(CORREO_TESO),
                      json={"folio": 2, "contabilizado": False})
        assert r.status_code == 403

    def test_cruce_exige_fc_y_autoavance(self, api):
        c = self._caja(api)
        api.post(f"/fin/cajas/{c['id']}/radicar", headers=_h(CORREO_TESO))
        api.post(f"/fin/cajas/{c['id']}/transicion", headers=_h(CORREO_ADMIN),
                 json={"accion": "APROBAR_JEFE", "version": 2})
        api.post(f"/fin/cajas/{c['id']}/transicion", headers=_h(CORREO_CTRL),
                 json={"accion": "APROBAR_CONTROLLER", "version": 3})
        # cruce antes del FC → 400
        r = api.post(f"/fin/cajas/{c['id']}/transicion", headers=_h(CORREO_TESO),
                     json={"accion": "CRUZAR", "version": 4})
        assert r.status_code == 400
        # contabilizar con FE → auto-avance a CONTABILIZADA (hay items de cruce)
        r = api.post(f"/fin/cajas/{c['id']}/transicion", headers=_h(CORREO_CONTA),
                     json={"accion": "CONTABILIZAR", "version": 4, "no_comprobante_fc": "FC-777"})
        assert r.status_code == 200, r.text
        assert r.json()["estado"] == "CONTABILIZADA"
        # ahora sí cruza y luego egresa
        r = api.post(f"/fin/cajas/{c['id']}/transicion", headers=_h(CORREO_TESO),
                     json={"accion": "CRUZAR", "version": 5, "bg_ef": "BG-EF 207952"})
        assert r.json()["estado"] == "CRUZADA"
        r = api.post(f"/fin/cajas/{c['id']}/transicion", headers=_h(CORREO_TESO),
                     json={"accion": "EGRESAR", "version": 6, "no_egreso_banco": "EGR-10492", "banco": "Davivienda"})
        assert r.json()["estado"] == "REEMBOLSADA"

    def test_vincular_factura_ida_y_vuelta(self, api, sesion):
        from financiero.models.contabilidad import EstadoFactura, FacturaDian
        # factura en LEGALIZACION
        f = FacturaDian(numero_factura="PD-VIN1", nit_proveedor="900123456", proveedor="PROVEEDOR PD",
                        fecha_emision=datetime.now(timezone.utc),
                        total=120000, estado=EstadoFactura.LEGALIZACION.value)
        sesion.add(f)
        sesion.commit()
        c = self._caja(api, movs=[{"concepto": "Placeholder", "valor": 1000, "tipo": "gasto"}])
        r = api.post(f"/fin/cajas/{c['id']}/vincular-factura", headers=_h(CORREO_TESO),
                     json={"folio": 1, "factura_id": str(f.id)})
        assert r.status_code == 200, r.text
        mov = r.json()["movimientos"][0]
        assert mov["tipo_documento"] == "factura_electronica"
        assert mov["invoice_id"] == str(f.id)
        sesion.refresh(f)
        assert f.estado == EstadoFactura.TRAMITADA.value
        assert f.caja_menor_sede == "GALAPA-PD"
        # al eliminar la caja (admin), la factura vuelve a LEGALIZACION
        r = api.delete(f"/fin/cajas/{c['id']}", headers=_h(CORREO_ADMIN))
        assert r.status_code == 200
        sesion.refresh(f)
        assert f.estado == EstadoFactura.LEGALIZACION.value


def _factura_manual(sesion, **kw):
    from financiero.models.contabilidad import EstadoFactura, FacturaDian
    base = dict(numero_factura="PD-100", nit_proveedor="900555000", proveedor="PROVEEDOR MANUAL",
                fecha_emision=datetime.now(timezone.utc),
                total=500000, estado=EstadoFactura.PENDIENTE.value)
    base.update(kw)
    f = FacturaDian(**base)
    sesion.add(f)
    sesion.commit()
    return f


class TestDianParidad:
    def test_validar_area_y_quitar(self, api, sesion):
        f = _factura_manual(sesion)
        r = api.post(f"/fin/contabilidad/facturas/{f.id}/validar", headers=_h(CORREO_CONTA),
                     json={"area": "Compras", "nota": "Correo enviado"})
        assert r.status_code == 200, r.text
        assert r.json()["validada"] is True
        assert r.json()["area_aprobacion"] == "Compras"
        # área inválida
        r = api.post(f"/fin/contabilidad/facturas/{f.id}/validar", headers=_h(CORREO_CONTA),
                     json={"area": "Juridica"})
        assert r.status_code == 422
        r = api.post(f"/fin/contabilidad/facturas/{f.id}/quitar-validacion", headers=_h(CORREO_CONTA))
        assert r.json()["validada"] is False

    def test_rechazar_reactivar_y_cruce(self, api, sesion):
        from financiero.models.contabilidad import EstadoFactura
        a = _factura_manual(sesion, numero_factura="PD-200")
        b = _factura_manual(sesion, numero_factura="PD-201")
        r = api.post(f"/fin/contabilidad/facturas/{a.id}/rechazar", headers=_h(CORREO_CONTA),
                     json={"motivo": "Solicitada refacturación al proveedor"})
        assert r.status_code == 200, r.text
        assert r.json()["estado"] == "RECHAZADA"
        # cruce: b reemplaza a a
        r = api.post(f"/fin/contabilidad/facturas/{b.id}/cruce", headers=_h(CORREO_CONTA),
                     json={"cruzada_id": str(a.id), "motivo": "Factura reemplazada por nueva emisión formal"})
        assert r.status_code == 200, r.text
        assert r.json()["reemplazo"]["reemplaza_factura"] == "PD-200"
        # deshacer → la rechazada vuelve a PENDIENTE
        r = api.post(f"/fin/contabilidad/facturas/{a.id}/cruce/deshacer", headers=_h(CORREO_CONTA))
        assert r.status_code == 200
        sesion.expire_all()
        sesion.refresh(a)
        assert a.estado == EstadoFactura.PENDIENTE.value

    def test_tramite_enriquecido(self, api, sesion):
        f = _factura_manual(sesion, numero_factura="PD-300")
        r = api.post(f"/fin/contabilidad/facturas/{f.id}/transicion", headers=_h(CORREO_CONTA),
                     json={"accion": "TRAMITAR", "version": f.version,
                           "datos_adicionales": {"fecha_envio_contab": "2026-12-01T12:00:00",
                                                 "categoria_contable": "LOGÍSTICA",
                                                 "num_orden_compra": "2026-380",
                                                 "observacion_recepcion": "URG. ANTES 03/11"}})
        assert r.status_code == 200, r.text
        j = r.json()
        assert j["estado"] == "TRAMITADA"
        assert j["categoria_contable"] == "LOGÍSTICA"
        assert j["num_orden_compra"] == "2026-380"
        assert j["observacion_recepcion"] == "URG. ANTES 03/11"
        assert j["estado_contable"] == "Pendiente"

    def test_novedad_contabilizada_y_reabrir(self, api, sesion):
        f = _factura_manual(sesion, numero_factura="PD-400")
        api.post(f"/fin/contabilidad/facturas/{f.id}/transicion", headers=_h(CORREO_CONTA),
                 json={"accion": "TRAMITAR", "version": 1})
        sesion.expire_all()
        # novedad sin nº de causación → 422
        r = api.post(f"/fin/contabilidad/facturas/{f.id}/novedad", headers=_h(CORREO_CONTA),
                     json={"estado_contable": "Contabilizado"})
        assert r.status_code == 422
        r = api.post(f"/fin/contabilidad/facturas/{f.id}/novedad", headers=_h(CORREO_CONTA),
                     json={"estado_contable": "Contabilizado", "num_causacion": "JC-000111"})
        assert r.status_code == 200, r.text
        j = r.json()
        assert j["estado"] == "CAUSADA"
        assert j["num_causacion"] == "JC-000111"
        # reabrir
        r = api.post(f"/fin/contabilidad/facturas/{f.id}/reabrir", headers=_h(CORREO_CONTA))
        assert r.json()["estado"] == "RADICADA"
        assert r.json()["num_causacion"] is None

    def test_cuenta_cobro_y_enviar_caja(self, api, sesion):
        r = api.post("/fin/contabilidad/facturas/cuenta-cobro", headers=_h(CORREO_TESO), json={
            "beneficiario": "JUAN PEREZ", "nit": "79531245", "valor": 900000,
            "categoria": "HONORARIOS", "origen": "Caja Menor: GALAPA-PD", "concepto": "HONORARIOS SERVICIOS",
        })
        assert r.status_code == 200, r.text
        j = r.json()
        assert j["es_cuenta_cobro"] is True
        assert j["estado"] == "TRAMITADA"
        assert j["caja_menor_sede"] == "Caja Menor: GALAPA-PD"
        assert j["estado_contable"] == "Pendiente"

    def test_previsualizar_con_transportador(self, api, sesion):
        import openpyxl
        from financiero.models.dian import TransportadorOmitido
        sesion.add(TransportadorOmitido(nit="1098675432", nombre="TRANSPORTADOR PD SA"))
        sesion.commit()
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["Tipo Documento", "Prefijo", "Folio", "Fecha Emision", "NIT Emisor", "Razon Social Emisor",
                   "Valor Total", "CUFE", "Numero Factura"])
        ws.append(["Factura electrónica de Venta", "SE", "1", "2026-12-01", "900999001", "NUEVO PROVEEDOR", "1000", "CUFE-PD-1", "SE1"])
        ws.append(["Factura electrónica de Venta", "SE", "2", "2026-12-01", "1098675432", "TRANSPORTADOR PD SA", "2000", "CUFE-PD-2", "SE2"])
        ws.append(["Factura electrónica de Venta", "SE", "3", "2026-12-01", "900164363", "INTEGRA PROPIA", "3000", "CUFE-PD-3", "SE3"])
        ws.append(["Application Response", "", "", "2026-12-01", "900999001", "NUEVO PROVEEDOR", "0", "CUFE-PD-4", "SE4"])
        buf = io.BytesIO()
        wb.save(buf)
        buf.seek(0)
        r = api.post("/fin/contabilidad/facturas/previsualizar", headers=_h(CORREO_CONTA),
                     files={"archivo": ("token.xlsx", buf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")})
        assert r.status_code == 200, r.text
        c = r.json()["conteos"]
        assert c["nuevas"] == 1          # NUEVO PROVEEDOR (el AR entra en otros)
        assert c["transportadores"] == 1
        assert c["propias"] == 1
        assert c["otros"] == 1           # Application Response

    def test_uploads_y_torre(self, api):
        r = api.get("/fin/contabilidad/uploads", headers=_h(CORREO_CONTA))
        assert r.status_code == 200
        r = api.get("/fin/contabilidad/torre", headers=_h(CORREO_CONTA))
        assert r.status_code == 200, r.text
        assert "kpis" in r.json()
        assert "agenda" in r.json()

    def test_correo_notificar(self, api):
        r = api.post("/fin/contabilidad/correos/notificar", headers=_h(CORREO_CONTA),
                     json={"destinatarios": ["prueba@integralogistica.com"], "asunto": "PD", "lineas": ["una línea"]})
        assert r.status_code == 200, r.text
