# -*- coding: utf-8 -*-
"""Tests del INHABILITAMIENTO por planilla de seguridad social VENCIDA
(2026-10-07, pedido del usuario): cuando `planillaVencimiento` < hoy, el
vehículo NO se puede ofrecer (check-in 400), NO aparece en la bolsa y NO se
puede asignar — hasta que suban una planilla nueva. Los vehículos SIN fecha
(históricos previos al campo) no se bloquean.
"""
import sys
import types
import unittest
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

if "bd.bd_cliente" not in sys.modules:
    _stub = types.ModuleType("bd.bd_cliente")

    class _BDFake(dict):
        def __getitem__(self, clave):
            return self.setdefault(clave, MagicMock())

    _stub.bd_cliente = _BDFake()
    sys.modules["bd.bd_cliente"] = _stub

from fastapi import FastAPI
from fastapi.testclient import TestClient

from rutas import disponibilidad

AYER = (date.today() - timedelta(days=1)).isoformat()
EN_10_DIAS = (date.today() + timedelta(days=10)).isoformat()


def cliente_de_prueba() -> TestClient:
    app = FastAPI()
    app.include_router(disponibilidad.ruta_disponibilidad)
    return TestClient(app, raise_server_exceptions=False)


class FakeDisp:
    """Falsa colección disponibilidades: find(query) filtra por estado/fecha."""
    def __init__(self, docs):
        self.documents = list(docs)
        self.updates = []

    def find(self, query, *a, **k):
        res = []
        for d in self.documents:
            if query.get("estado") and d.get("estado") != query["estado"]:
                continue
            if query.get("fecha") and d.get("fecha") != query["fecha"]:
                continue
            res.append(d)
        return FakeCursor(res)

    def find_one(self, query, *a, **k):
        for d in self.documents:
            if all(d.get(k) == v for k, v in query.items()):
                return d
        return None

    def update_one(self, filtro, cambio, upsert=False):
        self.updates.append((filtro, cambio))


class FakeCursor:
    def __init__(self, items):
        self._items = items

    def sort(self, *a, **k):
        return self

    def __iter__(self):
        return iter(self._items)


class FakeVeh:
    def __init__(self, docs):
        self.documents = list(docs)

    def find_one(self, query, *a, **k):
        for d in self.documents:
            if d.get("placa") == query.get("placa"):
                return d
        return None

    def find(self, query, *a, **k):
        return FakeCursor([d for d in self.documents if d.get("estadoIntegra") == "aprobado"])


def _vehiculo(planilla=None):
    return {
        "placa": "ABC123", "estadoIntegra": "aprobado",
        "idUsuario": "u1", "condNombres": "Prueba",
        "planillaVencimiento": planilla,
    }


class PlanillaVencidaDisponibilidadTests(unittest.TestCase):

    def setUp(self):
        self.client = cliente_de_prueba()

    def _checkin(self):
        return self.client.post("/disponibilidad/checkin", data={
            "id_usuario": "u1", "placa": "ABC123",
            "origen": "YUMBO", "destinos_json": '["VALLE DEL CAUCA"]',
        })

    def test_checkin_rechazado_con_planilla_vencida(self):
        fake_disp = FakeDisp([])
        fake_veh = FakeVeh([_vehiculo(AYER)])
        with patch.object(disponibilidad, "coleccion_disponibilidades", fake_disp), \
             patch.object(disponibilidad, "coleccion_vehiculos", fake_veh):
            resp = self._checkin()
        self.assertEqual(resp.status_code, 400, resp.text)
        self.assertIn("VENCIDA", resp.json()["detail"])
        self.assertEqual(fake_disp.updates, [])  # no se escribió ningún check-in

    def test_checkin_ok_con_planilla_vigente(self):
        fake_disp = FakeDisp([])
        fake_veh = FakeVeh([_vehiculo(EN_10_DIAS)])
        with patch.object(disponibilidad, "coleccion_disponibilidades", fake_disp), \
             patch.object(disponibilidad, "coleccion_vehiculos", fake_veh):
            resp = self._checkin()
        self.assertEqual(resp.status_code, 200, resp.text)

    def test_checkin_ok_sin_fecha_historica(self):
        """Vehículos previos al campo (sin planillaVencimiento) no se bloquean."""
        fake_disp = FakeDisp([])
        fake_veh = FakeVeh([_vehiculo(None)])
        with patch.object(disponibilidad, "coleccion_disponibilidades", fake_disp), \
             patch.object(disponibilidad, "coleccion_vehiculos", fake_veh):
            resp = self._checkin()
        self.assertEqual(resp.status_code, 200, resp.text)

    def _bolsa(self, planilla):
        checkin = {"placa": "ABC123", "fecha": disponibilidad._fecha_hoy_str(),
                   "estado": "activa", "origen": "YUMBO",
                   "departamentos_destino": ["VALLE DEL CAUCA"], "idUsuario": "u1"}
        fake_disp = FakeDisp([checkin])
        fake_veh = FakeVeh([_vehiculo(planilla)])
        with patch.object(disponibilidad, "coleccion_disponibilidades", fake_disp), \
             patch.object(disponibilidad, "coleccion_vehiculos", fake_veh):
            return self.client.get("/disponibilidad/bolsa")

    def test_bolsa_excluye_la_planilla_vencida(self):
        resp = self._bolsa(AYER)
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(resp.json()["total"], 0)

    def test_bolsa_muestra_la_planilla_vigente(self):
        resp = self._bolsa(EN_10_DIAS)
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(resp.json()["total"], 1)

    def test_asignar_rechazado_con_planilla_vencida(self):
        """Puede vencerse DESPUÉS del check-in: la asignación también valida."""
        hoy = disponibilidad._fecha_hoy_str()
        checkin = {"placa": "ABC123", "fecha": hoy, "estado": "activa", "origen": "YUMBO"}
        fake_disp = FakeDisp([checkin])
        fake_veh = FakeVeh([_vehiculo(AYER)])
        fake_asig = MagicMock()
        fake_asig.find_one.return_value = None
        fake_disp.find_one_and_update = MagicMock(return_value=checkin)
        with patch.object(disponibilidad, "coleccion_disponibilidades", fake_disp), \
             patch.object(disponibilidad, "coleccion_vehiculos", fake_veh), \
             patch.object(disponibilidad, "coleccion_asignaciones", fake_asig):
            resp = self.client.put("/disponibilidad/asignar", data={
                "placa": "ABC123", "asignado_por": "oper1",
            })
        self.assertEqual(resp.status_code, 400, resp.text)
        self.assertIn("VENCIDA", resp.json()["detail"])

    def test_mia_marca_la_planilla_vencida(self):
        """/mia entrega planillaVencida por vehículo para avisar al conductor
        ANTES de intentar el check-in."""
        fake_disp = FakeDisp([])
        fake_veh = FakeVeh([_vehiculo(AYER), {**_vehiculo(None), "placa": "XYZ987"}])
        with patch.object(disponibilidad, "coleccion_disponibilidades", fake_disp), \
             patch.object(disponibilidad, "coleccion_vehiculos", fake_veh):
            resp = self.client.get("/disponibilidad/mia", params={"id_usuario": "u1"})
        self.assertEqual(resp.status_code, 200, resp.text)
        por_placa = {v["placa"]: v for v in resp.json()["vehiculos_aprobados"]}
        self.assertTrue(por_placa["ABC123"]["planillaVencida"])
        self.assertFalse(por_placa["XYZ987"]["planillaVencida"])


if __name__ == "__main__":
    unittest.main()
