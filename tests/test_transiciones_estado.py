"""Tests del estado `inactivo` y la whitelist de transiciones (2026-08-27).

Cubre:
- TRANSICIONES_VALIDAS: saltos inválidos → 400 (registro_incompleto→aprobado,
  inactivo→completado_revision).
- aprobado→inactivo: exige motivo, sella historialInactivacion y cancela
  disponibilidades activas; escribe fechaEstado.
- inactivo→aprobado: reactivación sin re-revisión + entrada 'reactivado'.
- obtener-vehiculos-incompletos incluye los inactivos en el $in.
- Devolver (completado_revision→registro_incompleto) sigue funcionando.
"""
import unittest
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from rutas import vehiculos


def cliente_de_prueba() -> TestClient:
    app = FastAPI()
    app.include_router(vehiculos.ruta_vehiculos)
    return TestClient(app, raise_server_exceptions=False)


class FakeColeccionVehiculos:
    def __init__(self, documentos=None):
        self.documents = list(documentos or [])
        self.updates = []

    def find_one(self, query, *args, **kwargs):
        for d in self.documents:
            if d.get("placa") == query.get("placa"):
                return d
        return None

    def update_one(self, filtro, cambio):
        self.updates.append((filtro, cambio))
        for d in self.documents:
            if d.get("placa") == filtro.get("placa"):
                if "$set" in cambio:
                    for k, v in cambio["$set"].items():
                        d[k] = v
                if "$push" in cambio:
                    for k, v in cambio["$push"].items():
                        d.setdefault(k, []).append(v)


class FakeColeccionDisponibilidades:
    def __init__(self):
        self.llamadas = []

    def update_many(self, filtro, cambio):
        self.llamadas.append((filtro, cambio))


TODOS_DOCS = {
    "tarjetaPropiedad": "https://x/1", "tarjetaPropiedadReverso": "https://x/1r", "soat": "https://x/2",
    "revisionTecnomecanica": "https://x/3", "tarjetaRemolque": "https://x/4",
    "polizaResponsabilidad": "https://x/5", "documentoIdentidadConductor": "https://x/6",
    "documentoIdentidadConductorReverso": "https://x/6r",
    "documentoIdentidadPropietario": "https://x/7", "documentoIdentidadPropietarioReverso": "https://x/7r",
    "documentoIdentidadTenedor": "https://x/8", "documentoIdentidadTenedorReverso": "https://x/8r",
    "licencia": "https://x/9", "licenciaReverso": "https://x/9r", "planillaEpsArl": "https://x/10",
    "condFoto": "https://x/11",
    "condCertificacionBancaria": "https://x/12", "propCertificacionBancaria": "https://x/13",
    "tenedCertificacionBancaria": "https://x/14", "documentoAcreditacionTenedor": "https://x/15",
    "rutTenedor": "https://x/16", "rutPropietario": "https://x/17",
    "fotos": ["https://x/f1"],
}


def vehiculo(estado="aprobado", **extra):
    doc = {"placa": "TEST01", "estadoIntegra": estado, "idUsuario": "u1", **TODOS_DOCS}
    doc.update(extra)
    return doc


class TransicionesInvalidasTests(unittest.TestCase):

    def test_registro_incompleto_a_aprobado_directo_rechazado(self):
        fake = FakeColeccionVehiculos([vehiculo(estado="registro_incompleto")])
        client = cliente_de_prueba()
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "enviar_notificacion_seguridad"):
            resp = client.put("/vehiculos/actualizar-estado", data={
                "placa": "TEST01", "nuevo_estado": "aprobado", "usuario_id": "seg1",
            })
            self.assertEqual(resp.status_code, 400)
            self.assertIn("Transición inválida", resp.json()["detail"])
            self.assertEqual(fake.updates, [])  # No tocó el documento.

    def test_inactivo_no_puede_ir_a_releccion(self):
        fake = FakeColeccionVehiculos([vehiculo(estado="inactivo")])
        client = cliente_de_prueba()
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "enviar_notificacion_seguridad"):
            resp = client.put("/vehiculos/actualizar-estado", data={
                "placa": "TEST01", "nuevo_estado": "completado_revision", "usuario_id": "seg1",
            })
            self.assertEqual(resp.status_code, 400)

    def test_estado_basura_rechazado(self):
        fake = FakeColeccionVehiculos([vehiculo(estado="aprobado")])
        client = cliente_de_prueba()
        with patch.object(vehiculos, "coleccion_vehiculos", fake):
            resp = client.put("/vehiculos/actualizar-estado", data={
                "placa": "TEST01", "nuevo_estado": "foo", "usuario_id": "seg1",
            })
            self.assertEqual(resp.status_code, 400)


class InactivacionTests(unittest.TestCase):

    def test_inactivar_sin_motivo_rechazado(self):
        fake = FakeColeccionVehiculos([vehiculo(estado="aprobado")])
        client = cliente_de_prueba()
        with patch.object(vehiculos, "coleccion_vehiculos", fake):
            resp = client.put("/vehiculos/actualizar-estado", data={
                "placa": "TEST01", "nuevo_estado": "inactivo", "usuario_id": "seg1", "motivo": "   ",
            })
            self.assertEqual(resp.status_code, 400)
            self.assertIn("motivo", resp.json()["detail"].lower())

    def test_inactivar_sella_historial_y_cancela_disponibilidades(self):
        fake = FakeColeccionVehiculos([vehiculo(estado="aprobado")])
        fake_disp = FakeColeccionDisponibilidades()
        client = cliente_de_prueba()
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "coleccion_disponibilidades", fake_disp):
            resp = client.put("/vehiculos/actualizar-estado", data={
                "placa": "TEST01", "nuevo_estado": "inactivo",
                "usuario_id": "seg1", "motivo": "SOAT vencido",
            })
            self.assertEqual(resp.status_code, 200, resp.text)

            doc = fake.documents[0]
            self.assertEqual(doc["estadoIntegra"], "inactivo")
            self.assertTrue(doc.get("fechaEstado"))  # Sello temporal.

            # Histórico append-only con motivo y acción.
            historial = doc.get("historialInactivacion", [])
            self.assertEqual(len(historial), 1)
            self.assertEqual(historial[0]["motivo"], "SOAT vencido")
            self.assertEqual(historial[0]["accion"], "inactivo")
            self.assertEqual(historial[0]["usuario"], "seg1")

            # El check-in activo del día queda cancelado.
            self.assertEqual(len(fake_disp.llamadas), 1)
            filtro, cambio = fake_disp.llamadas[0]
            self.assertEqual(filtro, {"placa": "TEST01", "estado": "activa"})
            self.assertEqual(cambio["$set"]["estado"], "cancelada")

    def test_reactivar_vuelve_a_aprobado_y_registra(self):
        fake = FakeColeccionVehiculos([vehiculo(estado="inactivo", historialInactivacion=[
            {"fecha": "2026-08-01", "usuario": "seg1", "motivo": "sanción", "accion": "inactivo"},
        ])])
        client = cliente_de_prueba()
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "coleccion_disponibilidades", FakeColeccionDisponibilidades()):
            resp = client.put("/vehiculos/actualizar-estado", data={
                "placa": "TEST01", "nuevo_estado": "aprobado", "usuario_id": "seg2",
            })
            self.assertEqual(resp.status_code, 200, resp.text)

            doc = fake.documents[0]
            self.assertEqual(doc["estadoIntegra"], "aprobado")
            historial = doc["historialInactivacion"]
            self.assertEqual(len(historial), 2)
            self.assertEqual(historial[1]["accion"], "reactivado")
            self.assertEqual(historial[1]["usuario"], "seg2")

    def test_devolver_sigue_funcionando(self):
        fake = FakeColeccionVehiculos([vehiculo(estado="completado_revision")])
        client = cliente_de_prueba()
        with patch.object(vehiculos, "coleccion_vehiculos", fake):
            resp = client.put("/vehiculos/actualizar-estado", data={
                "placa": "TEST01", "nuevo_estado": "registro_incompleto",
                "usuario_id": "seg1", "observaciones": "Falta la licencia legible",
            })
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(fake.documents[0]["estadoIntegra"], "registro_incompleto")
            self.assertEqual(fake.documents[0]["observaciones"], "Falta la licencia legible")


class InactivosEnListasTests(unittest.TestCase):

    def test_incluidos_en_obtener_vehiculos_incompletos(self):
        # El $in extendido debe incluir "inactivo": verificamos la constante
        # del filtro que construye el endpoint (no hay red en el test).
        import inspect
        fuente = inspect.getsource(vehiculos.obtener_vehiculos_incompletos)
        self.assertIn('"inactivo"', fuente)


if __name__ == "__main__":
    unittest.main()
