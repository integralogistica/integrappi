# -*- coding: utf-8 -*-
"""Tests de la sesión 2026-10-20:

1. TARJETA DE PROPIEDAD DEL REMOLQUE — nuevo documento leíble con IA
   (esquema `tarjeta_propiedad_remolque`, dos caras, tipo `tarjetaRemolque`
   + `tarjetaRemolqueReverso`): las medidas llegan en MILÍMETROS y el campo
   de identificación del propietario alimenta la comparación de identidad.
2. IDENTIDAD PROPIETARIO ↔ CÉDULA — la identificación de la tarjeta de
   propiedad (vehículo o remolque) debe coincidir con el número de la cédula
   de la figura, en CUALQUIER orden de subida. Mismatch → 409 en
   subir-documento (nada al bucket ni a Mongo). Figura NIT: exenta.
"""
import sys
import types
import unittest
from datetime import datetime
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


def cliente_de_prueba(router) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    return TestClient(app, raise_server_exceptions=False)


def vehiculo_completo(**extra):
    doc = {"placa": "TEST01", "estadoIntegra": "registro_incompleto"}
    doc.update(extra)
    return doc


LECTURA_CEDULA_REMOLQUE = {
    "datos": {"numero": "91221607", "nombres": "MARTIN"},
    "avisos": [], "fecha": datetime(2026, 10, 20, 12, 0, 0),
}
LECTURA_TARJETA_REMOLQUE = {
    "datos": {"placa": "S84465", "propietario_documento": "91221607",
              "largo_total_mm": "13000", "ancho_total_mm": "4100",
              "alto_total_mm": "2600"},
    "avisos": [], "fecha": datetime(2026, 10, 20, 12, 5, 0),
}


# ── 1. ESQUEMA Y CATÁLOGOS ──────────────────────────────────────────────────

class EsquemaTarjetaRemolqueTests(unittest.TestCase):

    def test_esquema_existe_y_trae_identificacion_y_medidas(self):
        esquema = vehiculos.ESQUEMAS_EXTRACCION["tarjeta_propiedad_remolque"]
        for campo in ("placa", "ano_modelo", "clase", "tipo_carroceria",
                      "largo_total_mm", "ancho_total_mm", "alto_total_mm",
                      "propietario_nombre", "propietario_documento"):
            self.assertIn(campo, esquema["campos"])
        # Las medidas se describen en MILÍMETROS y el documento es de dos caras.
        self.assertIn("milímetro", esquema["campos"]["largo_total_mm"].lower())
        self.assertIn("DOS caras", esquema["descripcion"])

    def test_tarjeta_remolque_es_leible_y_de_dos_caras(self):
        self.assertEqual(
            vehiculos.TIPOS_SUBIDA_LEIBLES["tarjetaRemolque"],
            "tarjeta_propiedad_remolque")
        self.assertIn("tarjetaRemolque", vehiculos.TIPOS_DOS_CARAS_SUBIDA)
        self.assertIn("tarjetaRemolqueReverso", vehiculos.TIPOS_SUBIDA_VALIDOS)
        self.assertIn("tarjetaRemolqueReverso", vehiculos.CAMPOS_DOCUMENTO_PROTEGIDOS)
        self.assertIn("tarjetaRemolqueReverso", vehiculos.ETIQUETAS_DOCUMENTO)

    def test_sigue_obligatoria_solo_con_remolque(self):
        """La regla de 2026-10-06 no cambia: exigida solo con remolque."""
        self.assertNotIn("tarjetaRemolque", vehiculos.DOCUMENTOS_REQUERIDOS)

    def test_campos_nuevos_vin_y_pbv(self):
        """(2026-10-20, pedido del usuario) VIN y PBV máximo del remolque:
        en CAMPOS_REMOLQUE (disparan el checkbox/tarjeta obligatoria) y con
        etiqueta legible para el diff de auditoría."""
        for campo in ("RemolVin", "RemolPbvMax"):
            self.assertIn(campo, vehiculos.CAMPOS_REMOLQUE)
            self.assertIn(campo, vehiculos.ETIQUETAS_CAMPO_AUDITORIA)


# ── 2. IDENTIDAD PROPIETARIO ↔ CÉDULA (helper) ─────────────────────────────

class VerificarIdentidadTests(unittest.TestCase):

    def test_sin_contraparte_no_compara(self):
        """Sube la cédula sin tarjeta previa (ni al revés): nada que comparar."""
        v = vehiculo_completo()
        self.assertIsNone(vehiculos._verificar_identidad_propietario(
            v, "documentoIdentidadRemolque", {"numero": "91221607"}))
        self.assertIsNone(vehiculos._verificar_identidad_propietario(
            v, "tarjetaRemolque", {"propietario_documento": "91221607"}))

    def test_coincide_devuelve_aviso_positivo(self):
        v = vehiculo_completo(lecturasIA={"documentoIdentidadRemolque": LECTURA_CEDULA_REMOLQUE})
        resultado = vehiculos._verificar_identidad_propietario(
            v, "tarjetaRemolque", LECTURA_TARJETA_REMOLQUE["datos"])
        self.assertIn("coincide", resultado)
        self.assertIn("91221607", resultado)

    def test_mismatch_subiendo_la_tarjeta_409(self):
        v = vehiculo_completo(lecturasIA={
            "documentoIdentidadRemolque": {
                **LECTURA_CEDULA_REMOLQUE, "datos": {"numero": "1020304050"}}})
        with self.assertRaises(vehiculos.HTTPException) as ctx:
            vehiculos._verificar_identidad_propietario(
                v, "tarjetaRemolque", LECTURA_TARJETA_REMOLQUE["datos"])
        self.assertEqual(ctx.exception.status_code, 409)
        self.assertIn("no coincide", ctx.exception.detail)

    def test_mismatch_subiendo_la_cedula_409(self):
        """Dirección INVERSA: la cédula sube segunda y no cuadra con la tarjeta."""
        v = vehiculo_completo(lecturasIA={"tarjetaRemolque": LECTURA_TARJETA_REMOLQUE})
        with self.assertRaises(vehiculos.HTTPException) as ctx:
            vehiculos._verificar_identidad_propietario(
                v, "documentoIdentidadRemolque", {"numero": "1020304050"})
        self.assertEqual(ctx.exception.status_code, 409)
        self.assertIn("no coincide", ctx.exception.detail)

    def test_vehiculo_tarjeta_propiedad_tambien_bloquea(self):
        """Mismo par para la tarjeta de propiedad del VEHÍCULO (decisión del
        usuario 2026-10-20: bloquear también)."""
        v = vehiculo_completo(lecturasIA={
            "documentoIdentidadPropietario": {
                "datos": {"numero": "55444333"}, "avisos": []}})
        with self.assertRaises(vehiculos.HTTPException) as ctx:
            vehiculos._verificar_identidad_propietario(
                v, "tarjetaPropiedad", {"propietario_documento": "91221607"})
        self.assertEqual(ctx.exception.status_code, 409)

    def test_figura_nit_exenta(self):
        """Dueño/propietario empresa (NIT): no hay cédula que comparar."""
        v = vehiculo_completo(
            RemolDuenoTipoDocumento="NIT",
            lecturasIA={"tarjetaRemolque": LECTURA_TARJETA_REMOLQUE})
        self.assertIsNone(vehiculos._verificar_identidad_propietario(
            v, "tarjetaRemolque", {"propietario_documento": "900123456"}))
        v2 = vehiculo_completo(
            propTipoDocumento="NIT",
            lecturasIA={"tarjetaPropiedad": {
                "datos": {"propietario_documento": "900123456"}, "avisos": []}})
        self.assertIsNone(vehiculos._verificar_identidad_propietario(
            v2, "documentoIdentidadPropietario", {"numero": "1020304050"}))


# ── 3. IDENTIDAD EN subir-documento (integración) ──────────────────────────

class SubirDocumentoIdentidadTests(unittest.TestCase):

    def setUp(self):
        self.client = cliente_de_prueba(vehiculos.ruta_vehiculos)

    def _subir_tarjeta(self, fake, lectura):
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "subir_a_google_storage",
                          side_effect=lambda a, n: f"Vehiculos/{n}") as mock_storage:
            resp = self.client.put(
                "/vehiculos/subir-documento",
                data={"placa": "TEST01", "tipo": "tarjetaRemolque",
                      "extraer": "false",
                      "lectura_datos": lectura,
                      "lectura_avisos": "[]"},
                files={"archivo": ("tarjeta.png", b"img", "image/png"),
                       "reverso": ("tarjeta-rev.png", b"img", "image/png")},
            )
        return resp, mock_storage

    def test_tarjeta_subida_con_lectura_del_front(self):
        """Flujo del paso 2 (dos caras): la lectura viaja en lectura_datos, se
        persiste en lecturasIA y el aviso ✓ de identidad se agrega."""
        import json
        v = vehiculo_completo(lecturasIA={"documentoIdentidadRemolque": LECTURA_CEDULA_REMOLQUE})
        fake = _FakeColeccion([v])
        resp, mock_storage = self._subir_tarjeta(
            fake, json.dumps(LECTURA_TARJETA_REMOLQUE["datos"]))
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertTrue(mock_storage.called)
        doc = fake.documents[0]
        self.assertTrue(str(doc["tarjetaRemolque"]).startswith("Vehiculos/"))
        self.assertTrue(str(doc["tarjetaRemolqueReverso"]).startswith("Vehiculos/"))
        avisos = doc["lecturasIA"]["tarjetaRemolque"]["avisos"]
        self.assertTrue(any("coincide" in a for a in avisos), avisos)

    def test_mismatch_no_sube_nada(self):
        """409 ANTES de tocar el bucket: cero llamadas al storage, sin $set."""
        import json
        v = vehiculo_completo(lecturasIA={
            "documentoIdentidadRemolque": {
                **LECTURA_CEDULA_REMOLQUE, "datos": {"numero": "1020304050"}}})
        fake = _FakeColeccion([v])
        datos = {**LECTURA_TARJETA_REMOLQUE["datos"], "propietario_documento": "91221607"}
        resp, mock_storage = self._subir_tarjeta(fake, json.dumps(datos))
        self.assertEqual(resp.status_code, 409, resp.text)
        self.assertIn("no coincide", resp.json()["detail"])
        self.assertFalse(mock_storage.called)
        self.assertNotIn("tarjetaRemolque", fake.documents[0])


# ── Fakes mínimos (patrón de los tests existentes) ─────────────────────────

class _CursorFake(list):
    def sort(self, *_a, **_k):
        return self

    def limit(self, _n):
        return self


class _FakeColeccion:
    def __init__(self, documentos=None):
        self.documents = list(documentos or [])
        self.updates = []

    @staticmethod
    def _match(doc, cond):
        return all(doc.get(c) == e for c, e in (cond or {}).items())

    def find(self, filtro=None, *a, **k):
        docs = self.documents
        for campo, esperado in (filtro or {}).items():
            docs = [d for d in docs if d.get(campo) == esperado]
        return _CursorFake(docs)

    def find_one(self, query, *a, **k):
        for d in self.documents:
            if self._match(d, query):
                return d
        return None

    def update_one(self, filtro, cambio, upsert=False):
        self.updates.append((filtro, cambio))
        for d in self.documents:
            if not self._match(d, filtro):
                continue
            if "$set" in cambio:
                for k, v in cambio["$set"].items():
                    partes = k.split(".")
                    actual = d
                    for p in partes[:-1]:
                        actual = actual.setdefault(p, {})
                    actual[partes[-1]] = v
            if "$push" in cambio:
                for campo, valor in cambio["$push"].items():
                    d.setdefault(campo, []).extend(
                        valor["$each"] if isinstance(valor, dict) and "$each" in valor
                        else [valor])
            if "$unset" in cambio:
                for k in cambio["$unset"]:
                    d.pop(k, None)
            return


if __name__ == "__main__":
    unittest.main()
