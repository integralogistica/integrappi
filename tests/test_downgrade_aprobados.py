"""Tests del downgrade de aprobados: diff, historialCambios y claves protegidas."""
import unittest
from unittest.mock import patch

from fastapi import HTTPException

from rutas import vehiculos


class FakeColeccionVehiculos:
    """Falsa colección vehiculos con registro de updates para aserciones."""

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
                    d.update(cambio["$set"])
                if "$push" in cambio:
                    d.setdefault("historialCambios", []).append(cambio["$push"]["historialCambios"])
                if "$pull" in cambio:
                    lista = d.get(cambio["$pull"].keys() and list(cambio["$pull"].keys())[0], [])
                    clave = list(cambio["$pull"].keys())[0]
                    if cambio["$pull"][clave] in lista:
                        lista.remove(cambio["$pull"][clave])


class FakeColeccionDisponibilidades:
    def __init__(self):
        self.updates = []

    def update_many(self, filtro, cambio):
        self.updates.append((filtro, cambio))


def vehiculo_aprobado(**extra):
    doc = {
        "placa": "ABC123",
        "estadoIntegra": "aprobado",
        "condNombres": "PEDRO PEREZ",
        "condCelular": "3001112233",
        "vehMarca": "CHEVROLET",
    }
    doc.update(extra)
    return doc


class RegistrarCambioAprobadoTests(unittest.TestCase):

    def test_aprobado_baja_y_registra_diff(self):
        fake = FakeColeccionVehiculos([vehiculo_aprobado()])
        fake_disp = FakeColeccionDisponibilidades()
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "coleccion_disponibilidades", fake_disp), \
             patch.object(vehiculos, "enviar_notificacion_seguridad") as mock_notif:
            vehiculos._registrar_cambio_aprobado(
                vehiculo_aprobado(), "user-1", "datos",
                [{"campo": "vehMarca", "antes": "CHEVROLET", "despues": "VOLVO"}],
            )
        doc = fake.documents[0]
        self.assertEqual(doc["estadoIntegra"], "completado_revision")
        self.assertEqual(len(doc["historialCambios"]), 1)
        cambio = doc["historialCambios"][0]
        self.assertEqual(cambio["usuario"], "user-1")
        self.assertEqual(cambio["campos"][0]["campo"], "vehMarca")
        self.assertEqual(cambio["campos"][0]["despues"], "VOLVO")
        mock_notif.assert_called_once()
        # La disponibilidad activa del día se cancela.
        self.assertEqual(len(fake_disp.updates), 1)
        filtro_disp = fake_disp.updates[0][0]
        self.assertEqual(filtro_disp["placa"], "ABC123")
        self.assertEqual(fake_disp.updates[0][1]["$set"]["estado"], "cancelada")

    def test_no_aprobado_es_noop(self):
        fake = FakeColeccionVehiculos([vehiculo_aprobado(estadoIntegra="registro_incompleto")])
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "enviar_notificacion_seguridad") as mock_notif:
            vehiculos._registrar_cambio_aprobado(
                fake.documents[0], "user-1", "datos", [{"campo": "x", "antes": "1", "despues": "2"}]
            )
        self.assertEqual(fake.documents[0]["estadoIntegra"], "registro_incompleto")
        self.assertEqual(fake.updates, [])
        mock_notif.assert_not_called()

    def test_sin_campos_es_noop(self):
        fake = FakeColeccionVehiculos([vehiculo_aprobado()])
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "enviar_notificacion_seguridad") as mock_notif:
            vehiculos._registrar_cambio_aprobado(fake.documents[0], "user-1", "datos", [])
        self.assertEqual(fake.documents[0]["estadoIntegra"], "aprobado")
        mock_notif.assert_not_called()

    def test_fallo_notificacion_no_rompe(self):
        fake = FakeColeccionVehiculos([vehiculo_aprobado()])
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "coleccion_disponibilidades", FakeColeccionDisponibilidades()), \
             patch.object(vehiculos, "enviar_notificacion_seguridad", side_effect=Exception("boom")):
            vehiculos._registrar_cambio_aprobado(
                fake.documents[0], "user-1", "datos", [{"campo": "x", "antes": "1", "despues": "2"}]
            )
        # El downgrade ya quedó aplicado aunque la notificación falle.
        self.assertEqual(fake.documents[0]["estadoIntegra"], "completado_revision")


class ActualizarInformacionTests(unittest.TestCase):
    """La lógica de diff y blacklist de claves protegidas."""

    def test_claves_protegidas_se_ignoran(self):
        datos = {
            "vehMarca": "VOLVO",
            "estadoIntegra": "aprobado",     # protegida: debe ignorarse
            "idConductor": "intruso",        # protegida
            "historialCambios": ["fake"],    # protegida
            "_id": "xxx",                    # protegida
        }
        limpios = {k: v for k, v in datos.items() if k not in vehiculos.CLAVES_PROTEGIDAS}
        self.assertEqual(limpios, {"vehMarca": "VOLVO"})

    def test_todas_las_claves_criticas_estan_protegidas(self):
        for clave in ["_id", "placa", "idUsuario", "idConductor", "estadoIntegra",
                      "invitacionConductor", "historialCambios", "lecturasIA"]:
            self.assertIn(clave, vehiculos.CLAVES_PROTEGIDAS, f"Falta proteger {clave}")

    def test_diff_solo_campos_cambiados(self):
        vehiculo = {"vehMarca": "CHEVROLET", "vehColor": "BLANCO"}
        datos = {"vehMarca": "CHEVROLET", "vehColor": "ROJO"}  # marca igual
        cambios = [
            {"campo": k, "antes": vehiculo.get(k), "despues": v}
            for k, v in datos.items() if vehiculo.get(k) != v
        ]
        self.assertEqual(len(cambios), 1)
        self.assertEqual(cambios[0]["campo"], "vehColor")


if __name__ == "__main__":
    unittest.main()
