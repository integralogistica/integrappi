"""Tests del alta PLACA-PRIMERO (2026-10-01): la placa se CREA al validar
(borrador sin responsable) y el responsable se vincula después con
/asignar-responsable. Lo digitado jamás se pierde al salir a medias."""
import sys
import types
import unittest
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

from rutas import vehiculos
from tests.test_alta_seguridad import FakeColeccion


def cliente_de_prueba(router) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    return TestClient(app, raise_server_exceptions=False)


OID = "507f1f77bcf86cd799439012"


def _borrador(placa="ABC123", **extra):
    doc = {"_id": "v1", "placa": placa, "estadoIntegra": "registro_incompleto",
           "idUsuario": None, "responsable_pendiente": True, "fotos": []}
    doc.update(extra)
    return doc


class CrearBorradorTests(unittest.TestCase):

    def test_crear_sin_id_usuario_crea_borrador(self):
        veh_fake = FakeColeccion()
        cliente = cliente_de_prueba(vehiculos.ruta_vehiculos)
        with patch.object(vehiculos, "coleccion_vehiculos", veh_fake), \
             patch.object(vehiculos, "coleccion_conductores_cuenta", FakeColeccion()):
            r = cliente.post("/vehiculos/crear", data={"placa": "XYZ789"})
        self.assertEqual(r.status_code, 201)
        doc = veh_fake.documents[0]
        self.assertIsNone(doc["idUsuario"])
        self.assertTrue(doc["responsable_pendiente"])
        self.assertEqual(doc["estadoIntegra"], "registro_incompleto")
        # La bitácora nace con la entrada de creación (borrador).
        self.assertEqual(doc["auditoriaVehiculo"][0]["accion"], "vehiculo_creado")

    def test_crear_con_id_usuario_sigue_siendo_del_duenio(self):
        """El flujo del conductor (panel propio) no cambia: con id_usuario la
        ficha nace con dueño y sin flag de borrador."""
        veh_fake = FakeColeccion()
        cuentas = FakeColeccion([{"_id": OID, "perfil": "TENEDOR"}])
        cliente = cliente_de_prueba(vehiculos.ruta_vehiculos)
        with patch.object(vehiculos, "coleccion_vehiculos", veh_fake), \
             patch.object(vehiculos, "coleccion_conductores_cuenta", cuentas):
            r = cliente.post("/vehiculos/crear", data={
                "id_usuario": OID, "placa": "XYZ789"})
        self.assertEqual(r.status_code, 201)
        doc = veh_fake.documents[0]
        self.assertEqual(doc["idUsuario"], OID)
        self.assertFalse(doc["responsable_pendiente"])


class AsignarResponsableTests(unittest.TestCase):

    def test_asigna_y_deja_auditoria(self):
        veh_fake = FakeColeccion([_borrador()])
        cuentas = FakeColeccion([
            {"_id": OID, "correo": "PEDRO@X.COM", "perfil": "TENEDOR"}])
        cliente = cliente_de_prueba(vehiculos.ruta_vehiculos)
        with patch.object(vehiculos, "coleccion_vehiculos", veh_fake), \
             patch.object(vehiculos, "coleccion_conductores_cuenta", cuentas):
            r = cliente.put(f"/vehiculos/asignar-responsable/abc123",
                            data={"id_usuario": OID, "asignado_por": "EDWIN"})
        self.assertEqual(r.status_code, 200)
        doc = veh_fake.documents[0]
        self.assertEqual(doc["idUsuario"], OID)
        self.assertFalse(doc["responsable_pendiente"])
        bitacora = doc["auditoriaVehiculo"]
        self.assertEqual(bitacora[-1]["accion"], "responsable_asignado")
        self.assertEqual(bitacora[-1]["actor"], "EDWIN")
        self.assertIn("PEDRO@X.COM", bitacora[-1]["detalle"])

    def test_idempotente_con_el_mismo_responsable(self):
        veh_fake = FakeColeccion([_borrador(idUsuario=OID,
                                            responsable_pendiente=False)])
        cliente = cliente_de_prueba(vehiculos.ruta_vehiculos)
        with patch.object(vehiculos, "coleccion_vehiculos", veh_fake), \
             patch.object(vehiculos, "coleccion_conductores_cuenta",
                          FakeColeccion([{"_id": OID, "perfil": "TENEDOR"}])):
            r = cliente.put("/vehiculos/asignar-responsable/ABC123",
                            data={"id_usuario": OID})
        self.assertEqual(r.status_code, 200)

    def test_rechaza_responsable_distinto(self):
        otro = "507f1f77bcf86cd799439099"
        veh_fake = FakeColeccion([_borrador(idUsuario=OID,
                                            responsable_pendiente=False)])
        cliente = cliente_de_prueba(vehiculos.ruta_vehiculos)
        with patch.object(vehiculos, "coleccion_vehiculos", veh_fake), \
             patch.object(vehiculos, "coleccion_conductores_cuenta",
                          FakeColeccion([{"_id": otro, "perfil": "TENEDOR"}])):
            r = cliente.put("/vehiculos/asignar-responsable/ABC123",
                            data={"id_usuario": otro})
        self.assertEqual(r.status_code, 409)

    def test_404_vehiculo_o_cuenta(self):
        cliente = cliente_de_prueba(vehiculos.ruta_vehiculos)
        with patch.object(vehiculos, "coleccion_vehiculos", FakeColeccion()), \
             patch.object(vehiculos, "coleccion_conductores_cuenta",
                          FakeColeccion([{"_id": OID, "perfil": "TENEDOR"}])):
            r = cliente.put("/vehiculos/asignar-responsable/NOP678",
                            data={"id_usuario": OID})
        self.assertEqual(r.status_code, 404)
        # Cuenta inexistente sobre un borrador válido.
        with patch.object(vehiculos, "coleccion_vehiculos",
                          FakeColeccion([_borrador()])), \
             patch.object(vehiculos, "coleccion_conductores_cuenta",
                          FakeColeccion()):
            r = cliente.put("/vehiculos/asignar-responsable/ABC123",
                            data={"id_usuario": OID})
        self.assertEqual(r.status_code, 404)

    def test_conductor_con_placa_rechazado_tenedor_pasa(self):
        # CONDUCTOR que ya tiene su placa → 400.
        veh_fake = FakeColeccion([
            _borrador(),
            {"_id": "v2", "placa": "OTRA01", "idUsuario": OID, "fotos": []},
        ])
        cuentas = FakeColeccion([{"_id": OID, "correo": "J@X.COM",
                                  "perfil": "CONDUCTOR"}])
        cliente = cliente_de_prueba(vehiculos.ruta_vehiculos)
        with patch.object(vehiculos, "coleccion_vehiculos", veh_fake), \
             patch.object(vehiculos, "coleccion_conductores_cuenta", cuentas):
            r = cliente.put("/vehiculos/asignar-responsable/ABC123",
                            data={"id_usuario": OID})
        self.assertEqual(r.status_code, 400)
        # El mismo caso con TENEDOR → pasa (flota).
        cuentas.documents[0]["perfil"] = "TENEDOR"
        with patch.object(vehiculos, "coleccion_vehiculos", veh_fake), \
             patch.object(vehiculos, "coleccion_conductores_cuenta", cuentas):
            r = cliente.put("/vehiculos/asignar-responsable/ABC123",
                            data={"id_usuario": OID})
        self.assertEqual(r.status_code, 200)


class ConsultarPlacaBorradorTests(unittest.TestCase):

    def test_reporta_borrador_retomable(self):
        veh_fake = FakeColeccion([_borrador()])
        cliente = cliente_de_prueba(vehiculos.ruta_vehiculos)
        with patch.object(vehiculos, "coleccion_vehiculos", veh_fake), \
             patch.object(vehiculos, "coleccion_conductores_cuenta",
                          FakeColeccion()):
            r = cliente.get("/vehiculos/consultar-placa/ABC123")
        d = r.json()
        self.assertTrue(d["existe"])
        self.assertTrue(d["borrador"])
        self.assertIsNone(d["duenio"])

    def test_con_duenio_no_es_borrador(self):
        veh_fake = FakeColeccion([_borrador(idUsuario=OID,
                                            responsable_pendiente=False)])
        cuentas = FakeColeccion([{"_id": OID, "nombre": "PEDRO",
                                  "correo": "PEDRO@X.COM", "perfil": "TENEDOR"}])
        cliente = cliente_de_prueba(vehiculos.ruta_vehiculos)
        with patch.object(vehiculos, "coleccion_vehiculos", veh_fake), \
             patch.object(vehiculos, "coleccion_conductores_cuenta", cuentas):
            r = cliente.get("/vehiculos/consultar-placa/ABC123")
        d = r.json()
        self.assertTrue(d["existe"])
        self.assertFalse(d["borrador"])
        self.assertEqual(d["duenio"]["correo"], "PEDRO@X.COM")


class BorradosFueraDeBandejasTests(unittest.TestCase):

    def test_incompletos_excluye_borradores(self):
        veh_fake = FakeColeccion([
            _borrador(),                                   # borrador: FUERA
            {"_id": "v2", "placa": "REAL01", "estadoIntegra": "registro_incompleto",
             "idUsuario": OID, "fotos": []},               # normal: DENTRO
        ])
        cliente = cliente_de_prueba(vehiculos.ruta_vehiculos)
        with patch.object(vehiculos, "coleccion_vehiculos", veh_fake):
            r = cliente.get("/vehiculos/obtener-vehiculos-incompletos")
        placas = [v["placa"] for v in r.json()["vehicles"]]
        self.assertEqual(placas, ["REAL01"])

    def test_flag_blindado(self):
        self.assertIn("responsable_pendiente", vehiculos.CLAVES_PROTEGIDAS)


if __name__ == "__main__":
    unittest.main()
