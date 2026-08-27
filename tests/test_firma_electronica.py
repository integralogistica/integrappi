"""Tests de la firma electrónica con evidencia sellada (2026-08-27).

Cubre:
- _hash_datos_firmados: determinista, sensible a los datos declarados e
  inmune a los campos volátiles/administrativos.
- PUT /vehiculos/firmar: sube la imagen, setea firmaUrl + firmaEvidencia y
  sella un registro append-only en firmas_conductor (hash, IP, versión).
- Re-firmar: agrega un registro nuevo (append-only), nunca reemplaza.
- GET /vehiculos/verificar-firma: coincide mientras el documento no cambie;
  deja de coincidir si un dato firmado cambió después.
- actualizar-informacion: ignora firmaEvidencia (CLAVES_PROTEGIDAS).
"""
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from rutas import vehiculos


def cliente_de_prueba() -> TestClient:
    """Monta el router en una mini-app (mismo patrón que
    test_obligatoriedad_documentos)."""
    app = FastAPI()
    app.include_router(vehiculos.ruta_vehiculos)
    return TestClient(app, raise_server_exceptions=False)


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
                    for k, v in cambio["$set"].items():
                        d[k] = v


class FakeColeccionFirmas:
    """Falsa colección append-only firmas_conductor."""

    def __init__(self):
        self.registros = []

    def insert_one(self, doc):
        self.registros.append(doc)
        return SimpleNamespace(inserted_id="id-registro-123")

    def count_documents(self, query):
        return sum(1 for r in self.registros if r.get("placa") == query.get("placa"))

    def find_one(self, query, *args, **kwargs):
        # Los registros se insertan cronológicamente: el último es el vigente.
        placa = query.get("placa")
        coincidentes = [r for r in self.registros if r.get("placa") == placa]
        return coincidentes[-1] if coincidentes else None


def vehiculo_para_firmar(**extra):
    doc = {
        "placa": "TEST01",
        "estadoIntegra": "registro_incompleto",
        "idUsuario": "u1",
        "condNombres": "PEDRO",
        "condPrimerApellido": "PEREZ",
        "condSegundoApellido": "",
        "condCedulaCiudadania": "1.020.304.050",
        "condCorreo": "pedro@ejemplo.com",
        "condCelular": "3001234567",
        "vehMarca": "CHEVROLET",
        "vehModelo": "2020",
        "tarjetaPropiedad": "https://x/1",
        "soat": "https://x/2",
    }
    doc.update(extra)
    return doc


class HashDatosFirmadosTests(unittest.TestCase):

    def test_determinista(self):
        v = vehiculo_para_firmar()
        self.assertEqual(vehiculos._hash_datos_firmados(v), vehiculos._hash_datos_firmados(dict(v)))

    def test_sensible_a_los_datos_declarados(self):
        base = vehiculo_para_firmar()
        cambiado = vehiculo_para_firmar(condNombres="JUAN")
        self.assertNotEqual(vehiculos._hash_datos_firmados(base), vehiculos._hash_datos_firmados(cambiado))
        # Un documento cargado también hace parte del contenido firmado.
        cambiado_doc = vehiculo_para_firmar(soat="https://x/otro")
        self.assertNotEqual(vehiculos._hash_datos_firmados(base), vehiculos._hash_datos_firmados(cambiado_doc))

    def test_ignora_campos_volatiles(self):
        base = vehiculo_para_firmar()
        con_volatiles = vehiculo_para_firmar(
            firmaUrl="https://x/firma.webp",
            firmaEvidencia={"hash_datos": "abc"},
            estadoIntegra="aprobado",
            observaciones="todo bien",
            historialCambios=[{"fecha": "2026-08-27"}],
            lecturasIA={"soat": {"datos": {}}},
        )
        self.assertEqual(vehiculos._hash_datos_firmados(base), vehiculos._hash_datos_firmados(con_volatiles))


class FirmarEndpointTests(unittest.TestCase):

    def _firmar(self, client, placa="TEST01", contenido=b"pixeles-firma", extra_data=None):
        data = {"placa": placa, "id_usuario": "u1"}
        if extra_data:
            data.update(extra_data)
        return client.put(
            "/vehiculos/firmar",
            files={"archivo": ("firma.webp", contenido, "image/webp")},
            data=data,
        )

    def test_firma_vacia_rechazada(self):
        client = cliente_de_prueba()
        with patch.object(vehiculos, "coleccion_vehiculos", FakeColeccionVehiculos([vehiculo_para_firmar()])), \
             patch.object(vehiculos, "coleccion_firmas", FakeColeccionFirmas()):
            resp = self._firmar(client, contenido=b"")
            self.assertEqual(resp.status_code, 400)
            self.assertIn("vacía", resp.json()["detail"])

    def test_sella_evidencia_y_actualiza_vehiculo(self):
        fake_veh = FakeColeccionVehiculos([vehiculo_para_firmar()])
        fake_firmas = FakeColeccionFirmas()
        client = cliente_de_prueba()
        with patch.object(vehiculos, "coleccion_vehiculos", fake_veh), \
             patch.object(vehiculos, "coleccion_firmas", fake_firmas), \
             patch.object(vehiculos, "subir_a_google_storage", return_value="https://x/firma.webp") as mock_subida:
            resp = self._firmar(client)

            self.assertEqual(resp.status_code, 200, resp.text)
            cuerpo = resp.json()
            self.assertEqual(cuerpo["url"], "https://x/firma.webp")
            self.assertEqual(cuerpo["version"], 1)
            self.assertTrue(cuerpo["hash_datos"])
            self.assertTrue(cuerpo["firmado_en"])

            # La imagen subió con la nomenclatura estándar del bucket.
            mock_subida.assert_called_once()
            self.assertIn("TEST01/", mock_subida.call_args[0][1])
            self.assertIn("firma", mock_subida.call_args[0][1])

            # El vehículo quedó con la URL y el sello de conveniencia.
            doc = fake_veh.documents[0]
            self.assertEqual(doc["firmaUrl"], "https://x/firma.webp")
            self.assertEqual(doc["firmaEvidencia"]["hash_datos"], cuerpo["hash_datos"])
            self.assertEqual(doc["firmaEvidencia"]["version"], 1)
            self.assertEqual(doc["firmaEvidencia"]["registro_id"], "id-registro-123")

            # El registro sellado (append-only) trae identidad + evidencia.
            self.assertEqual(len(fake_firmas.registros), 1)
            registro = fake_firmas.registros[0]
            self.assertEqual(registro["placa"], "TEST01")
            self.assertEqual(registro["cedula"], "1020304050")
            self.assertEqual(registro["correo"], "PEDRO@EJEMPLO.COM")
            self.assertEqual(registro["nombre"], "PEDRO PEREZ")
            self.assertEqual(registro["hash_datos"], cuerpo["hash_datos"])
            self.assertEqual(registro["firma_url"], "https://x/firma.webp")
            self.assertTrue(registro["hash_firma"])
            self.assertTrue(registro["firmado_en"])
            self.assertTrue(registro["ip"])
            self.assertTrue(registro["user_agent"])

    def test_re_firmar_agrega_registro_no_reemplaza(self):
        fake_veh = FakeColeccionVehiculos([vehiculo_para_firmar()])
        fake_firmas = FakeColeccionFirmas()
        client = cliente_de_prueba()
        with patch.object(vehiculos, "coleccion_vehiculos", fake_veh), \
             patch.object(vehiculos, "coleccion_firmas", fake_firmas), \
             patch.object(vehiculos, "subir_a_google_storage", return_value="https://x/firma.webp"):
            r1 = self._firmar(client, contenido=b"pixeles-1")
            r2 = self._firmar(client, contenido=b"pixeles-2")

            self.assertEqual(r1.json()["version"], 1)
            self.assertEqual(r2.json()["version"], 2)
            self.assertEqual(len(fake_firmas.registros), 2)
            self.assertEqual(fake_firmas.registros[0]["hash_firma"], vehiculos.hashlib.sha256(b"pixeles-1").hexdigest())
            self.assertEqual(fake_firmas.registros[1]["hash_firma"], vehiculos.hashlib.sha256(b"pixeles-2").hexdigest())


class VerificarFirmaTests(unittest.TestCase):

    def test_coincide_sin_cambios_y_falla_si_cambian_los_datos(self):
        fake_veh = FakeColeccionVehiculos([vehiculo_para_firmar()])
        fake_firmas = FakeColeccionFirmas()
        client = cliente_de_prueba()
        with patch.object(vehiculos, "coleccion_vehiculos", fake_veh), \
             patch.object(vehiculos, "coleccion_firmas", fake_firmas), \
             patch.object(vehiculos, "subir_a_google_storage", return_value="https://x/firma.webp"):
            self.assertEqual(self._firmar(client).status_code, 200)

            # Sin cambios posteriores: el hash recalculado coincide.
            resp = client.get("/vehiculos/verificar-firma/TEST01")
            self.assertEqual(resp.status_code, 200)
            self.assertTrue(resp.json()["coincide"])
            self.assertEqual(resp.json()["evidencia"]["version"], 1)

            # Un dato declarado cambia después de firmado → integridad rota.
            fake_veh.documents[0]["condCelular"] = "3119998877"
            resp2 = client.get("/vehiculos/verificar-firma/TEST01")
            self.assertFalse(resp2.json()["coincide"])

            # Un cambio puramente administrativo NO invalida la firma.
            fake_veh.documents[0]["condCelular"] = "3001234567"
            fake_veh.documents[0]["estadoIntegra"] = "aprobado"
            resp3 = client.get("/vehiculos/verificar-firma/TEST01")
            self.assertTrue(resp3.json()["coincide"])

    def _firmar(self, client, placa="TEST01"):
        return client.put(
            "/vehiculos/firmar",
            files={"archivo": ("firma.webp", b"pixeles-firma", "image/webp")},
            data={"placa": placa, "id_usuario": "u1"},
        )

    def test_404_sin_firmas(self):
        client = cliente_de_prueba()
        with patch.object(vehiculos, "coleccion_vehiculos", FakeColeccionVehiculos([vehiculo_para_firmar()])), \
             patch.object(vehiculos, "coleccion_firmas", FakeColeccionFirmas()):
            resp = client.get("/vehiculos/verificar-firma/TEST01")
            self.assertEqual(resp.status_code, 404)


class FirmaEvidenciaProtegidaTests(unittest.TestCase):

    def test_actualizar_informacion_ignora_firma_evidencia(self):
        doc = vehiculo_para_firmar(idUsuario=None)
        fake_veh = FakeColeccionVehiculos([doc])
        client = cliente_de_prueba()
        with patch.object(vehiculos, "coleccion_vehiculos", fake_veh), \
             patch.object(vehiculos, "coleccion_conductores_cuenta", FakeColeccionVehiculos()):
            resp = client.put(
                "/vehiculos/actualizar-informacion/TEST01",
                json={"condNombres": "JUAN", "firmaEvidencia": {"hash_datos": "falso"}},
            )
            self.assertEqual(resp.status_code, 200)

            # El campo de datos SÍ se guardó; el sello NO fue pisado.
            self.assertEqual(doc["condNombres"], "JUAN")
            self.assertNotIn("firmaEvidencia", doc)


if __name__ == "__main__":
    unittest.main()
