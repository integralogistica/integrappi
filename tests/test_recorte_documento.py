# -*- coding: utf-8 -*-
"""Tests del RECORTE de documentos por Seguridad (2026-10-07, pedido del
usuario: fotos muy lejos con cosas de sobra).

1. `PUT /vehiculos/recortar-documento`: guarda la copia recortada como
   versión NUEVA (`_v{N}` según el historial universal), el campo apunta a
   la nueva ruta, entra al `historialDocumentos` con marca `recorte`, los
   GEMELOS que compartían la ruta anterior también se actualizan, y NO baja
   un aprobado a re-revisión (edición cosmética: el contenido no cambió).
2. `GET /vehiculos/documento-bruto`: bytes server-side del documento actual
   (para el canvas del recorte sin CORS).
"""
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


def cliente_de_prueba() -> TestClient:
    app = FastAPI()
    app.include_router(vehiculos.ruta_vehiculos)
    return TestClient(app, raise_server_exceptions=False)


class FakeColeccion:
    """Falsa colección: find_one por placa + update_one con $set/$push
    ($position/$slice se ignoran — el orden no es lo que se prueba)."""

    def __init__(self, documentos=None):
        self.documents = list(documentos or [])

    def find_one(self, query, *a, **k):
        for d in self.documents:
            if d.get("placa") == query.get("placa"):
                return d
        return None

    def update_one(self, filtro, cambio, upsert=False):
        for d in self.documents:
            if d.get("placa") != filtro.get("placa"):
                continue
            for k, v in (cambio.get("$set") or {}).items():
                d[k] = v
            for campo, valor in (cambio.get("$push") or {}).items():
                entradas = valor["$each"] if isinstance(valor, dict) and "$each" in valor else [valor]
                d.setdefault(campo, []).extend(entradas)
            return


RUTA_TENED = "Vehiculos/TEST01/2026-01-01/documentoIdentidadTenedor_test01.webp"


def _recortar(client, campo="documentoIdentidadTenedor", nombre="recorte.webp",
              tipo="image/webp", placa="TEST01"):
    return client.put(
        "/vehiculos/recortar-documento",
        data={"placa": placa, "campo": campo, "editado_por": "Seguridad Prueba"},
        files={"archivo": (nombre, b"bytes-imagen", tipo)},
    )


class RecorteDocumentoTests(unittest.TestCase):

    def setUp(self):
        self.client = cliente_de_prueba()

    def test_recorte_guarda_version_nueva_y_entra_al_historial(self):
        fake = FakeColeccion([{
            "placa": "TEST01", "estadoIntegra": "completado_revision",
            "documentoIdentidadTenedor": RUTA_TENED,
            "historialDocumentos": [
                {"tipo": "documentoIdentidadTenedor", "ruta": RUTA_TENED},
            ],
        }])
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "subir_a_google_storage",
                          side_effect=lambda a, n: f"Vehiculos/{n}"):
            resp = _recortar(self.client)
        self.assertEqual(resp.status_code, 200, resp.text)

        doc = fake.documents[0]
        # El campo apunta a la NUEVA ruta, versionada (1 subida previa → _v2).
        self.assertIn("_v2_", doc["documentoIdentidadTenedor"])
        # La versión anterior NO se pierde: queda en el historial + la nueva
        # entra con marca de recorte y el actor.
        historial = doc["historialDocumentos"]
        recortes = [h for h in historial if h.get("recorte")]
        self.assertEqual(len(recortes), 1)
        self.assertEqual(recortes[0]["actor"], "Seguridad Prueba")
        self.assertEqual(recortes[0]["ruta"], doc["documentoIdentidadTenedor"])
        self.assertTrue(any(h.get("ruta") == RUTA_TENED for h in historial))

    def test_primer_recorte_sin_sufijo_de_version(self):
        fake = FakeColeccion([{
            "placa": "TEST01", "estadoIntegra": "completado_revision",
            "documentoIdentidadTenedor": RUTA_TENED,
        }])
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "subir_a_google_storage",
                          side_effect=lambda a, n: f"Vehiculos/{n}"):
            resp = _recortar(self.client)
        self.assertEqual(resp.status_code, 200, resp.text)
        # Sin subidas previas del tipo → nombre base (compat).
        self.assertNotIn("_v", fake.documents[0]["documentoIdentidadTenedor"])

    def test_recorte_NO_baja_un_aprobado(self):
        """El recorte es cosmético (mismo contenido, otro encuadre): no saca
        al vehículo de la bolsa ni genera diff de re-revisión."""
        fake = FakeColeccion([{
            "placa": "TEST01", "estadoIntegra": "aprobado",
            "documentoIdentidadTenedor": RUTA_TENED,
        }])
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "subir_a_google_storage",
                          side_effect=lambda a, n: f"Vehiculos/{n}"):
            resp = _recortar(self.client)
        self.assertEqual(resp.status_code, 200, resp.text)
        doc = fake.documents[0]
        self.assertEqual(doc["estadoIntegra"], "aprobado")
        self.assertNotIn("historialCambios", doc)

    def test_gemelos_con_la_misma_ruta_tambien_se_actualizan(self):
        """La réplica por figura comparte la IMAGEN: si la cédula del
        propietario apunta a la MISMA ruta que la del tenedor, el recorte
        actualiza las dos."""
        fake = FakeColeccion([{
            "placa": "TEST01", "estadoIntegra": "completado_revision",
            "documentoIdentidadTenedor": RUTA_TENED,
            "documentoIdentidadPropietario": RUTA_TENED,
        }])
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "subir_a_google_storage",
                          side_effect=lambda a, n: f"Vehiculos/{n}"):
            resp = _recortar(self.client)
        self.assertEqual(resp.status_code, 200, resp.text)
        doc = fake.documents[0]
        self.assertEqual(doc["documentoIdentidadTenedor"], doc["documentoIdentidadPropietario"])

    def test_campo_no_recortable_400(self):
        """La FIRMA no se recorta: su URL entra al hash de la firma
        electrónica. Tampoco la planilla (espejo del historial)."""
        fake = FakeColeccion([{"placa": "TEST01", "firmaUrl": "https://x/f.webp"}])
        with patch.object(vehiculos, "coleccion_vehiculos", fake):
            for campo in ("firmaUrl", "planillaEpsArl", "estadoIntegra", "inexistente"):
                resp = _recortar(self.client, campo=campo)
                self.assertEqual(resp.status_code, 400, f"{campo}: {resp.text}")

    def test_sin_documento_cargado_404(self):
        fake = FakeColeccion([{"placa": "TEST01", "estadoIntegra": "completado_revision"}])
        with patch.object(vehiculos, "coleccion_vehiculos", fake):
            resp = _recortar(self.client, campo="soat")
        self.assertEqual(resp.status_code, 404)

    def test_rechazado_no_editable_403(self):
        fake = FakeColeccion([{
            "placa": "TEST01", "estadoIntegra": "rechazado",
            "documentoIdentidadTenedor": RUTA_TENED,
        }])
        with patch.object(vehiculos, "coleccion_vehiculos", fake):
            resp = _recortar(self.client)
        self.assertEqual(resp.status_code, 403)

    def test_reverso_cuenta_las_versiones_de_su_base(self):
        """El reverso de la cédula del tenedor se guardó vía `reverso` de
        subir-documento (tipo base + reverso:True): el recorte del reverso
        debe ver esas subidas para versionar."""
        fake = FakeColeccion([{
            "placa": "TEST01", "estadoIntegra": "completado_revision",
            "documentoIdentidadTenedorReverso": "Vehiculos/TEST01/2026-01-01/rev.webp",
            "historialDocumentos": [
                {"tipo": "documentoIdentidadTenedor", "reverso": True, "ruta": "Vehiculos/TEST01/2026-01-01/rev.webp"},
            ],
        }])
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "subir_a_google_storage",
                          side_effect=lambda a, n: f"Vehiculos/{n}"):
            resp = _recortar(self.client, campo="documentoIdentidadTenedorReverso")
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertIn("_v2_", fake.documents[0]["documentoIdentidadTenedorReverso"])


class DocumentoBrutoTests(unittest.TestCase):

    def setUp(self):
        self.client = cliente_de_prueba()

    def test_devuelve_los_bytes_del_documento_actual(self):
        fake = FakeColeccion([{
            "placa": "TEST01", "documentoIdentidadTenedor": RUTA_TENED,
        }])
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "_descargar_blob", return_value=b"bytes-img") as mock_dl:
            resp = self.client.get(
                "/vehiculos/documento-bruto/TEST01",
                params={"campo": "documentoIdentidadTenedor"},
            )
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(resp.content, b"bytes-img")
        self.assertEqual(resp.headers["content-type"], "image/webp")
        mock_dl.assert_called_once_with(RUTA_TENED)

    def test_campo_invalido_400_y_sin_documento_404(self):
        fake = FakeColeccion([{"placa": "TEST01"}])
        with patch.object(vehiculos, "coleccion_vehiculos", fake):
            self.assertEqual(
                self.client.get("/vehiculos/documento-bruto/TEST01", params={"campo": "firmaUrl"}).status_code,
                400,
            )
            self.assertEqual(
                self.client.get("/vehiculos/documento-bruto/TEST01", params={"campo": "soat"}).status_code,
                404,
            )
            self.assertEqual(
                self.client.get("/vehiculos/documento-bruto/NADA99", params={"campo": "soat"}).status_code,
                404,
            )


if __name__ == "__main__":
    unittest.main()
