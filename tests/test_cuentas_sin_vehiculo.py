"""Tests de las CUENTAS SIN VEHÍCULO (2026-10-13): listado de cuentas
activas sin ninguna placa inscrita (ni propia ni como conductor invitado)
+ reenvío de verificación explícito + recordatorio a verificados."""
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

if "bd.bd_cliente" not in sys.modules:
    _stub = types.ModuleType("bd.bd_cliente")

    class _BDFake(dict):
        def __getitem__(self, clave):
            from unittest.mock import MagicMock
            return self.setdefault(clave, MagicMock())

    _stub.bd_cliente = _BDFake()
    sys.modules["bd.bd_cliente"] = _stub

from bson import ObjectId
from fastapi import FastAPI
from fastapi.testclient import TestClient

from rutas import conductores
from tests.test_alta_seguridad import FakeColeccion


def cliente_de_prueba(router) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    return TestClient(app, raise_server_exceptions=False)


def _cuenta(correo, **extra):
    doc = {"_id": ObjectId(), "correo": correo, "nombre": "PEDRO PEREZ",
           "perfil": "CONDUCTOR", "activo": True, "clave": "SECRETO",
           "correo_verificado": False}
    doc.update(extra)
    return doc


class SinVehiculoTests(unittest.TestCase):

    def test_lista_solo_las_que_no_tienen_placa(self):
        """Dueño (idUsuario) y conductor invitado (idConductor) SÍ cuentan
        como placa inscrita: solo la cuenta huérfana queda en la lista."""
        sola = _cuenta("SOLA@X.COM", correo_verificado=True)
        duenhia = _cuenta("DUENHO@X.COM")
        invitado = _cuenta("INVITADO@X.COM")
        conductores_fake = FakeColeccion([sola, duenhia, invitado])
        vehiculos_fake = FakeColeccion([
            {"_id": "v1", "placa": "ABC123", "idUsuario": str(duenhia["_id"])},
            {"_id": "v2", "placa": "DEF456", "idConductor": str(invitado["_id"])},
        ])
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", conductores_fake), \
             patch.object(conductores, "coleccion_vehiculos", vehiculos_fake):
            r = cliente.get("/conductores/sin-vehiculo")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["total"], 1)
        cuenta = r.json()["cuentas"][0]
        self.assertEqual(cuenta["correo"], "SOLA@X.COM")
        self.assertTrue(cuenta["correo_verificado"])
        # La fecha viene del ObjectId (generation_time, UTC) y sin claves JAMÁS.
        self.assertIn("creado", cuenta)
        self.assertIn("+00:00", cuenta["creado"])
        self.assertNotIn("clave", cuenta)

    def test_excluye_stubs_de_invitacion(self):
        """Las cuentas stub (activo=False, esperando invitación) no cuentan."""
        stub = _cuenta("STUB@X.COM", activo=False)
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", FakeColeccion([stub])), \
             patch.object(conductores, "coleccion_vehiculos", FakeColeccion()):
            r = cliente.get("/conductores/sin-vehiculo")
        self.assertEqual(r.json()["total"], 0)

    def test_sin_verificar_primero(self):
        """Orden: pendientes de verificación primero (los que necesitan reenvío)."""
        verificada = _cuenta("V@X.COM", correo_verificado=True)
        pendiente = _cuenta("P@X.COM", correo_verificado=False)
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores",
                          FakeColeccion([verificada, pendiente])), \
             patch.object(conductores, "coleccion_vehiculos", FakeColeccion()):
            r = cliente.get("/conductores/sin-vehiculo")
        correos = [c["correo"] for c in r.json()["cuentas"]]
        self.assertEqual(correos, ["P@X.COM", "V@X.COM"])


class ReenvioVerificacionSeguridadTests(unittest.TestCase):

    def test_reenvia_y_regenera_token(self):
        doc = _cuenta("PEND@X.COM")
        fake = FakeColeccion([doc])
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", fake):
            r = cliente.post(f"/conductores/{doc['_id']}/reenviar-verificacion")
        self.assertEqual(r.status_code, 200)
        self.assertIn("PEND@X.COM", r.json()["mensaje"])
        # El token quedó regenerado (sha nuevo para el lookup O(1)).
        self.assertIn("verificacion_token_sha", doc)
        self.assertIn("verificacion_expira", doc)

    def test_409_si_ya_verificada(self):
        doc = _cuenta("OK@X.COM", correo_verificado=True)
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", FakeColeccion([doc])):
            r = cliente.post(f"/conductores/{doc['_id']}/reenviar-verificacion")
        self.assertEqual(r.status_code, 409)
        self.assertIn("recordatorio", r.json()["detail"].lower())

    def test_404_inexistente(self):
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", FakeColeccion()):
            r = cliente.post(f"/conductores/{ObjectId()}/reenviar-verificacion")
        self.assertEqual(r.status_code, 404)


class RecordatorioVehiculoTests(unittest.TestCase):

    def test_envia_a_verificada_sin_placa(self):
        doc = _cuenta("LISTO@X.COM", correo_verificado=True)
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", FakeColeccion([doc])), \
             patch.object(conductores, "coleccion_vehiculos", FakeColeccion()):
            r = cliente.post(f"/conductores/{doc['_id']}/recordatorio-vehiculo")
        self.assertEqual(r.status_code, 200)
        self.assertIn("LISTO@X.COM", r.json()["mensaje"])

    def test_409_sin_verificar(self):
        doc = _cuenta("NOVER@X.COM", correo_verificado=False)
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", FakeColeccion([doc])), \
             patch.object(conductores, "coleccion_vehiculos", FakeColeccion()):
            r = cliente.post(f"/conductores/{doc['_id']}/recordatorio-vehiculo")
        self.assertEqual(r.status_code, 409)

    def test_409_con_placas_vinculadas(self):
        doc = _cuenta("CONPLACA@X.COM", correo_verificado=True)
        vehiculos_fake = FakeColeccion(
            [{"_id": "v1", "placa": "ABC123", "idUsuario": str(doc["_id"])}])
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", FakeColeccion([doc])), \
             patch.object(conductores, "coleccion_vehiculos", vehiculos_fake):
            r = cliente.post(f"/conductores/{doc['_id']}/recordatorio-vehiculo")
        self.assertEqual(r.status_code, 409)


if __name__ == "__main__":
    unittest.main()
