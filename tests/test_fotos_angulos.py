# -*- coding: utf-8 -*-
"""Tests de las FOTOS DEL VEHÍCULO POR ÁNGULO (2026-10-14, pedido del
usuario: «al menos 4 fotos — FRONTAL, LATERAL IZQ Y DERECHO, POSTERIOR, con
dibujo de ejemplo para cada foto» — el dibujo es front):

1. El gate de «Finalizar» exige los 4 ÁNGULOS en `fotosVehiculo` (el array
   plano `fotos` deja de bastar; los históricos sin catálogo no pasan).
2. subir-fotos con foto_tipo: UNA foto por ángulo, re-subirla REEMPLAZA (la
   anterior sale del espejo plano) y sin foto_tipo sigue acumulando (fotos
   adicionales, comportamiento histórico).
3. eliminar-foto saca la foto del catálogo → el ángulo vuelve a faltar.
"""
import sys
import types
import unittest
from unittest.mock import patch, MagicMock

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


TODOS_DOCS = {
    "tarjetaPropiedad": "https://x/1", "tarjetaPropiedadReverso": "https://x/1r", "soat": "https://x/2",
    "revisionTecnomecanica": "https://x/3",
    "documentoIdentidadConductor": "https://x/6", "documentoIdentidadConductorReverso": "https://x/6r",
    "documentoIdentidadPropietario": "https://x/7", "documentoIdentidadPropietarioReverso": "https://x/7r",
    "documentoIdentidadTenedor": "https://x/8", "documentoIdentidadTenedorReverso": "https://x/8r",
    "licencia": "https://x/9", "licenciaReverso": "https://x/9r", "planillaEpsArl": "https://x/10",
    "condFoto": "https://x/11", "condCertificacionBancaria": "https://x/12",
    "tenedCertificacionBancaria": "https://x/14", "documentoAcreditacionTenedor": "https://x/15",
    "rutTenedor": "https://x/16",
}

TODOS_ANGULOS = [
    {"tipo": t, "ruta": f"https://x/foto_{t}"} for t in vehiculos.ANGULOS_FOTO_VEHICULO
]


def vehiculo_completo(**extra):
    doc = {
        "placa": "TEST01", "estadoIntegra": "registro_incompleto",
        "condCedulaCiudadania": "123", "propDocumento": "123", "tenedDocumento": "123",
        "vehCapacidadCarga": "8000", "vehModelo": "2015",
        "fotos": [a["ruta"] for a in TODOS_ANGULOS],
        "fotosVehiculo": TODOS_ANGULOS,
        **TODOS_DOCS,
    }
    doc.update(extra)
    return doc


class FakeColeccion:
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
            for campo, cond in (cambio.get("$pull") or {}).items():
                if isinstance(cond, dict):
                    d[campo] = [e for e in d.get(campo, [])
                                if not (isinstance(e, dict) and all(e.get(k2) == v2 for k2, v2 in cond.items()))]
                else:
                    d[campo] = [e for e in d.get(campo, []) if e != cond]
            return


class GateAngulosTests(unittest.TestCase):

    def test_sin_catalogo_faltan_los_4_angulos(self):
        """Histórico con fotos planas pero sin `fotosVehiculo`: el gate pide
        los 4 ángulos por su nombre legible."""
        v = vehiculo_completo()
        del v["fotosVehiculo"]
        v["fotos"] = ["https://x/foto_vieja"]
        faltantes = vehiculos._documentos_faltantes(v)
        self.assertEqual(len(faltantes), 4)
        for angulo in vehiculos.ANGULOS_FOTO_VEHICULO:
            self.assertIn(angulo, faltantes)
        # El mensaje del gate los lista legibles (ETIQUETAS_DOCUMENTO).
        self.assertIn("Foto Frontal del Vehículo", vehiculos.ETIQUETAS_DOCUMENTO["frontal"])

    def test_con_los_4_angulos_el_gate_pasa(self):
        self.assertEqual(vehiculos._documentos_faltantes(vehiculo_completo()), [])

    def test_falta_un_angulo(self):
        v = vehiculo_completo()
        v["fotosVehiculo"] = TODOS_ANGULOS[:3]
        self.assertEqual(vehiculos._documentos_faltantes(v), ["posterior"])


class SubirFotosAnguloTests(unittest.TestCase):

    def setUp(self):
        self.client = cliente_de_prueba()

    def _subir(self, fake, foto_tipo=None, cantidad=1):
        data = {"placa": "TEST01"}
        if foto_tipo:
            data["foto_tipo"] = foto_tipo
        files = [("archivos", (f"foto{i}.png", b"img", "image/png")) for i in range(cantidad)]
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "subir_a_google_storage",
                          side_effect=lambda a, n: f"Vehiculos/{n}"):
            return self.client.put("/vehiculos/subir-fotos", data=data, files=files)

    def test_subida_de_angulo_cataloga_y_espeja(self):
        fake = FakeColeccion([vehiculo_completo(estadoIntegra="registro_incompleto",
                                                fotos=[], fotosVehiculo=[])])
        resp = self._subir(fake, foto_tipo="frontal")
        self.assertEqual(resp.status_code, 200, resp.text)
        doc = fake.documents[0]
        self.assertEqual([a["tipo"] for a in doc["fotosVehiculo"]], ["frontal"])
        self.assertEqual(len(doc["fotos"]), 1)  # espejo plano

    def test_re_subir_un_angulo_REEMPLAZA(self):
        fake = FakeColeccion([vehiculo_completo(estadoIntegra="registro_incompleto")])
        r1 = self._subir(fake, foto_tipo="frontal")
        r2 = self._subir(fake, foto_tipo="frontal")
        self.assertEqual(r1.status_code, 200)
        self.assertEqual(r2.status_code, 200, r2.text)
        doc = fake.documents[0]
        frontal = [a for a in doc["fotosVehiculo"] if a["tipo"] == "frontal"]
        self.assertEqual(len(frontal), 1)                       # UNA por ángulo
        self.assertEqual(frontal[0]["ruta"], r2.json()["rutas"][0])
        # La anterior salió del espejo plano (no ocupa cupo del tope).
        self.assertNotIn(r1.json()["rutas"][0], doc["fotos"])
        # Los demás ángulos siguen intactos.
        self.assertEqual(len(doc["fotosVehiculo"]), 4)

    def test_angulo_con_varias_fotos_es_400(self):
        fake = FakeColeccion([vehiculo_completo(estadoIntegra="registro_incompleto")])
        resp = self._subir(fake, foto_tipo="frontal", cantidad=2)
        self.assertEqual(resp.status_code, 400)
        self.assertIn("UNA foto", resp.json()["detail"])

    def test_sin_foto_tipo_sigue_acumulando(self):
        """Las fotos ADICIONALES (comportamiento histórico) siguen sumando al
        espejo plano sin tocar el catálogo de ángulos."""
        fake = FakeColeccion([vehiculo_completo(estadoIntegra="registro_incompleto")])
        resp = self._subir(fake, cantidad=2)
        self.assertEqual(resp.status_code, 200, resp.text)
        doc = fake.documents[0]
        self.assertEqual(len(doc["fotos"]), 6)                  # 4 ángulos + 2
        self.assertEqual(len(doc["fotosVehiculo"]), 4)          # catálogo intacto

    def test_eliminar_foto_saca_el_angulo(self):
        fake = FakeColeccion([vehiculo_completo(estadoIntegra="registro_incompleto")])
        ruta = TODOS_ANGULOS[0]["ruta"]
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "eliminar_de_google_storage"):
            resp = self.client.delete(f"/vehiculos/eliminar-foto?placa=TEST01&url={ruta}")
        self.assertEqual(resp.status_code, 200, resp.text)
        doc = fake.documents[0]
        self.assertNotIn(ruta, doc["fotos"])
        self.assertEqual([a["tipo"] for a in doc["fotosVehiculo"]],
                         ["lateral_izq", "lateral_der", "posterior"])
        self.assertEqual(vehiculos._documentos_faltantes(doc), ["frontal"])

    def test_eliminar_foto_con_URL_firmada_como_las_del_front(self):
        """Regresión (2026-10-16, caso STA796): Mongo guarda la RUTA plana
        (`Vehiculos/...`) pero el front recibe la URL FIRMADA y la devuelve
        (sin query) al eliminar — la comparación exacta rebotaba 404 y la
        foto era imposible de borrar desde el visor."""
        ruta_plana = "Vehiculos/STA796/2026-10-14/foto_006_sta796.webp"
        url_front = "https://storage.googleapis.com/integrapp-privado/" + ruta_plana
        v = vehiculo_completo(estadoIntegra="registro_incompleto")
        v["fotos"] = [ruta_plana]
        v["fotosVehiculo"] = [{"tipo": "frontal", "ruta": ruta_plana}]
        fake = FakeColeccion([v])
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "eliminar_de_google_storage") as mock_del:
            resp = self.client.delete(f"/vehiculos/eliminar-foto?placa=TEST01&url={url_front}")
        self.assertEqual(resp.status_code, 200, resp.text)
        doc = fake.documents[0]
        self.assertEqual(doc["fotos"], [])            # sacó la RUTA almacenada
        self.assertEqual(doc["fotosVehiculo"], [])    # y su ángulo
        mock_del.assert_called_once_with(ruta_plana)  # el blob por su ruta plana

    def test_eliminar_foto_con_URL_con_query(self):
        """Defensivo: si el front llegara a mandar la URL firmada COMPLETA
        (con ?X-Goog-...), también debe resolverla a la ruta almacenada."""
        ruta_plana = "Vehiculos/STA796/2026-10-14/foto_006_sta796.webp"
        url_front = (f"https://storage.googleapis.com/integrapp-privado/{ruta_plana}"
                     f"?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Signature=abc")
        v = vehiculo_completo(estadoIntegra="registro_incompleto")
        v["fotos"] = [ruta_plana]
        v["fotosVehiculo"] = [{"tipo": "frontal", "ruta": ruta_plana}]
        fake = FakeColeccion([v])
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "eliminar_de_google_storage"):
            resp = self.client.delete(f"/vehiculos/eliminar-foto?placa=TEST01&url={url_front}")
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(fake.documents[0]["fotos"], [])

    def test_eliminar_foto_inexistente_sigue_404(self):
        fake = FakeColeccion([vehiculo_completo(estadoIntegra="registro_incompleto")])
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "eliminar_de_google_storage"):
            resp = self.client.delete("/vehiculos/eliminar-foto?placa=TEST01&url=Vehiculos/OTRA/9.webp")
        self.assertEqual(resp.status_code, 404)
        self.assertEqual(len(fake.documents[0]["fotos"]), 4)  # intactas


class BlindajeTests(unittest.TestCase):

    def test_catalogo_blindado(self):
        self.assertIn("fotosVehiculo", vehiculos.CLAVES_PROTEGIDAS)
        self.assertIn("fotosVehiculo", vehiculos.CAMPOS_DOCUMENTO_PROTEGIDOS)
        self.assertIn("fotosVehiculo", vehiculos.CAMPOS_VOLATILES_FIRMA)

    def test_fotos_ya_no_es_requerido_plano(self):
        self.assertNotIn("fotos", vehiculos.DOCUMENTOS_REQUERIDOS)


if __name__ == "__main__":
    unittest.main()
