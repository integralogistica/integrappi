"""Tests del módulo «Indicadores» de /revision (2026-10-14): placas
registradas POR DÍA (timestamp del _id ObjectId) y el estadoIntegra ACTUAL de
cada una. El día agrupado es el de Colombia (UTC−5, regla _MS_5H)."""
import sys
import types
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

if "bd.bd_cliente" not in sys.modules:
    _stub = types.ModuleType("bd.bd_cliente")

    class _BDFake(dict):
        def __getitem__(self, clave):
            return self.setdefault(clave, MagicMock())

    _stub.bd_cliente = _BDFake()
    sys.modules["bd.bd_cliente"] = _stub

from bson import ObjectId
from fastapi import FastAPI
from fastapi.testclient import TestClient

from rutas import vehiculos
from tests.test_alta_seguridad import FakeColeccion


def cliente_de_prueba(router) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    return TestClient(app, raise_server_exceptions=False)


def _oid_en(dt: datetime) -> ObjectId:
    """ObjectId cuya generation_time es dt (UTC naive, granularidad de segundo)."""
    return ObjectId.from_datetime(dt.replace(microsecond=0))


def _veh_hace_dias(dias: int, estado: str, hora_utc=15) -> dict:
    """Vehículo creado hace `dias` días a las {hora_utc}:00 UTC (hora segura:
    con hora_utc=15 el día Colombia (UTC−5) es el MISMO día calendario UTC;
    con hora_utc=2 el día Colombia es el día ANTERIOR — caso de frontera)."""
    base = datetime.utcnow() - timedelta(days=dias)
    return {"_id": _oid_en(base.replace(hour=hora_utc, minute=0, second=0)),
            "placa": f"T{dias}{hora_utc}{estado[:3]}".upper()[:10],
            "estadoIntegra": estado}


class RegistrosPorDiaTests(unittest.TestCase):

    def test_agrupa_por_dia_colombia_y_por_estado(self):
        veh_fake = FakeColeccion([
            _veh_hace_dias(3, "aprobado"),            # 15:00 UTC → mismo día Colombia
            _veh_hace_dias(3, "completado_revision"),
            _veh_hace_dias(3, "registro_incompleto", hora_utc=2),  # 02:00 UTC → día ANTERIOR Colombia
        ])
        cliente = cliente_de_prueba(vehiculos.ruta_vehiculos)
        with patch.object(vehiculos, "coleccion_vehiculos", veh_fake):
            r = cliente.get("/vehiculos/registros-por-dia", params={"dias": 7})
        self.assertEqual(r.status_code, 200)
        series = {s["fecha"]: s for s in r.json()["series"]}
        self.assertEqual(len(r.json()["series"]), 7)  # zero-fill: una entrada por día

        hace3 = (datetime.utcnow() - timedelta(days=3)).date().isoformat()
        hace4 = (datetime.utcnow() - timedelta(days=4)).date().isoformat()
        # Los dos de las 15:00 UTC quedan en el día calendario UTC−5 igual…
        self.assertEqual(series[hace3]["conteos"]["aprobado"], 1)
        self.assertEqual(series[hace3]["conteos"]["completado_revision"], 1)
        # …y el de las 02:00 UTC CAYÓ AL DÍA ANTERIOR de Colombia.
        self.assertEqual(series[hace4]["conteos"]["registro_incompleto"], 1)
        self.assertEqual(series[hace3]["total"], 2)
        self.assertEqual(series[hace4]["total"], 1)

    def test_dias_sin_registros_quedan_en_cero(self):
        veh_fake = FakeColeccion([_veh_hace_dias(1, "aprobado")])
        cliente = cliente_de_prueba(vehiculos.ruta_vehiculos)
        with patch.object(vehiculos, "coleccion_vehiculos", veh_fake):
            r = cliente.get("/vehiculos/registros-por-dia", params={"dias": 5})
        series = r.json()["series"]
        self.assertEqual(len(series), 5)
        self.assertEqual(sum(1 for s in series if s["total"] > 0), 1)
        self.assertTrue(all(s["total"] == sum(s["conteos"].values()) for s in series))

    def test_fuera_de_la_ventana_no_cuenta(self):
        veh_fake = FakeColeccion([_veh_hace_dias(40, "aprobado")])
        cliente = cliente_de_prueba(vehiculos.ruta_vehiculos)
        with patch.object(vehiculos, "coleccion_vehiculos", veh_fake):
            r = cliente.get("/vehiculos/registros-por-dia", params={"dias": 30})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(sum(s["total"] for s in r.json()["series"]), 0)

    def test_default_30_dias_y_validacion_del_param(self):
        veh_fake = FakeColeccion()
        cliente = cliente_de_prueba(vehiculos.ruta_vehiculos)
        with patch.object(vehiculos, "coleccion_vehiculos", veh_fake):
            r_ok = cliente.get("/vehiculos/registros-por-dia")
            r_cero = cliente.get("/vehiculos/registros-por-dia", params={"dias": 0})
            r_grande = cliente.get("/vehiculos/registros-por-dia", params={"dias": 400})
        self.assertEqual(r_ok.status_code, 200)
        self.assertEqual(len(r_ok.json()["series"]), 30)
        self.assertEqual(r_ok.json()["dias"], 30)
        self.assertEqual(r_cero.status_code, 400)
        self.assertEqual(r_grande.status_code, 400)

    def test_borradores_y_estados_desconocidos_cuentan(self):
        """Una placa creada al alta placa-primero (responsable_pendiente) es un
        registro del día; un estadoIntegra fuera del catálogo (o ausente) no
        puede hacer perder la placa del conteo."""
        veh_fake = FakeColeccion([
            {**_veh_hace_dias(2, "registro_incompleto"), "responsable_pendiente": True},
            _veh_hace_dias(2, "estado_raro"),
            {**_veh_hace_dias(2, "aprobado"), "_id": "sin-objectid"},  # defensive: se omite
        ])
        cliente = cliente_de_prueba(vehiculos.ruta_vehiculos)
        with patch.object(vehiculos, "coleccion_vehiculos", veh_fake):
            r = cliente.get("/vehiculos/registros-por-dia", params={"dias": 7})
        hace2 = (datetime.utcnow() - timedelta(days=2)).date().isoformat()
        dia = {s["fecha"]: s for s in r.json()["series"]}[hace2]
        self.assertEqual(dia["conteos"]["registro_incompleto"], 1)
        self.assertEqual(dia["conteos"]["estado_raro"], 1)
        self.assertEqual(dia["total"], 2)  # el de _id string no tiene fecha → fuera
        # El estado desconocido entra al final del orden de apilado, con etiqueta.
        estados = [e["estado"] for e in r.json()["estados"]]
        self.assertIn("estado_raro", estados)
        self.assertEqual(estados[0], "aprobado")

    def test_orden_de_apilado_y_colores_vienen_del_backend(self):
        """El front renderiza un <Bar> por entrada de `estados` EN ORDEN: la
        respuesta debe respetar ESTADOS_REGISTRO_DIA y traer etiqueta+color."""
        veh_fake = FakeColeccion()
        cliente = cliente_de_prueba(vehiculos.ruta_vehiculos)
        with patch.object(vehiculos, "coleccion_vehiculos", veh_fake):
            r = cliente.get("/vehiculos/registros-por-dia")
        estados = r.json()["estados"]
        esperados = [e for e, _, _ in vehiculos.ESTADOS_REGISTRO_DIA]
        self.assertEqual([e["estado"] for e in estados], esperados)
        for entrada in estados:
            self.assertTrue(entrada["etiqueta"])
            self.assertRegex(entrada["color"], r"^#[0-9a-f]{6}$")


if __name__ == "__main__":
    unittest.main()
