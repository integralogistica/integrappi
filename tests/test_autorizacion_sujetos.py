# -*- coding: utf-8 -*-
"""Tests de la AUTORIZACIÓN DE DATOS por SUJETO sin cuenta (2026-10-05):
link por correo (token 48 h → página pública) + firma en papel, evidencia
append-only por cédula en aceptaciones_politica y envío automático al pasar
el vehículo a revisión. La autorización es por PERSONA (cédula), no por rol."""
import re
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

from fastapi import FastAPI
from fastapi.testclient import TestClient

from rutas import conductores


def cliente_de_prueba(router) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    return TestClient(app, raise_server_exceptions=False)


class _CursorFake(list):
    """Cursor chainable (sort/limit) para los find de los endpoints."""

    def sort(self, clave, direccion=-1):
        return _CursorFake(sorted(self, key=lambda d: str(d.get(clave, "")),
                                  reverse=direccion < 0))

    def limit(self, _n):
        return self


class FakeCol:
    """Colección Mongo mínima: igualdad, $gt/$ne/$exists/$regex, sort/limit,
    insert/update (one y many) con claves planas."""

    def __init__(self, documentos=None):
        self.documents = list(documentos or [])
        self.contador = 0

    @staticmethod
    def _match(d, cond):
        for campo, esperado in (cond or {}).items():
            actual = d.get(campo)
            if isinstance(esperado, dict):
                if "$gt" in esperado and not (actual is not None and actual > esperado["$gt"]):
                    return False
                if "$ne" in esperado and actual == esperado["$ne"]:
                    return False
                if "$regex" in esperado:
                    opciones = re.IGNORECASE if "i" in esperado.get("$options", "") else 0
                    if not re.search(esperado["$regex"], str(actual or ""), opciones):
                        return False
            elif actual != esperado:
                return False
        return True

    def find(self, filtro=None, *a, **k):
        return _CursorFake([d for d in self.documents if self._match(d, filtro)])

    def find_one(self, query=None, *a, **k):
        for d in self.documents:
            if self._match(d, query or {}):
                return d
        return None

    def count_documents(self, filtro=None):
        return len(self.find(filtro))

    def insert_one(self, doc):
        self.contador += 1
        doc = dict(doc)
        doc.setdefault("_id", f"id-{self.contador}")
        self.documents.append(doc)
        return MagicMock(inserted_id=doc["_id"])

    def update_one(self, filtro, cambio, upsert=False):
        for d in self.documents:
            if self._match(d, filtro):
                d.update(cambio.get("$set") or {})
                return
        if upsert:
            nuevo = dict(filtro)
            nuevo.update(cambio.get("$set") or {})
            self.documents.append(nuevo)

    def update_many(self, filtro, cambio):
        for d in self.documents:
            if self._match(d, filtro):
                d.update(cambio.get("$set") or {})


POLITICA_V2 = {
    "_id": "pol-1", "version": 2, "titulo": "Declaraciones de vinculación",
    "texto_html": "<p>texto</p>", "activo": True,
    "declaraciones": [
        {"id": "origen_fondos", "titulo": "Origen de Fondos", "texto_html": "<p>a</p>"},
        {"id": "sarlaft", "titulo": "SARLAFT", "texto_html": "<p>b</p>"},
        {"id": "tratamiento_datos", "titulo": "Tratamiento de Datos", "texto_html": "<p>c</p>"},
    ],
}


class AutorizacionSujetosTests(unittest.TestCase):

    def setUp(self):
        self.client = cliente_de_prueba(conductores.ruta_conductores)
        self.politicas = FakeCol([dict(POLITICA_V2)])
        self.aceptaciones = FakeCol()
        self.tokens = FakeCol()
        self.cuentas = FakeCol()
        self.parches = [
            patch.object(conductores, "coleccion_politicas", self.politicas),
            patch.object(conductores, "coleccion_aceptaciones", self.aceptaciones),
            patch.object(conductores, "coleccion_tokens_aut", self.tokens),
            patch.object(conductores, "coleccion_conductores", self.cuentas),
        ]
        for p in self.parches:
            p.start()
            self.addCleanup(p.stop)
        # Resend moqueado: capturamos el link para extraer el token.
        self.correos_enviados = []
        resend_fake = MagicMock()
        resend_fake.api_key = "re_test"
        resend_fake.Emails.send.side_effect = lambda payload: self.correos_enviados.append(payload)
        parche_resend = patch.object(conductores, "resend", resend_fake)
        parche_resend.start()
        self.addCleanup(parche_resend.stop)

    def _token_del_correo(self) -> str:
        self.assertTrue(self.correos_enviados, "no se envió correo")
        html = self.correos_enviados[-1]["html"]
        m = re.search(r"token=([A-Za-z0-9_\-]+)", html)
        self.assertIsNotNone(m, "el correo no trae el token")
        return m.group(1)

    def test_flujo_completo_link_por_correo(self):
        r = self.client.post("/conductores/autorizacion/solicitar", json={
            "placa": "WOO453", "cedula": "55.444.333",
            "correo": "propietario@x.com", "nombre": "PEDRO PEREZ"})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["estado"], "enviado")
        # Token en BD: solo el HASH, sin usar, con cédula en dígitos.
        doc_token = self.tokens.documents[0]
        self.assertEqual(doc_token["cedula"], "55444333")
        self.assertNotIn("token_plano", doc_token)
        self.assertTrue(doc_token["token_hash"].startswith("$2"))

        token = self._token_del_correo()
        # GET verificar: pendiente + política con declaraciones.
        r = self.client.get(f"/conductores/autorizacion/verificar?token={token}")
        self.assertEqual(r.status_code, 200, r.text)
        cuerpo = r.json()
        self.assertEqual(cuerpo["estado"], "pendiente")
        self.assertEqual(cuerpo["placa"], "WOO453")
        self.assertEqual(len(cuerpo["politica"]["declaraciones"]), 3)

        # POST aceptar: exigidas marcadas (tratamiento_datos no exigida).
        r = self.client.post("/conductores/autorizacion/aceptar", json={
            "token": token, "declaraciones_aceptadas": ["origen_fondos", "sarlaft"]})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["estado"], "aceptado")
        # Evidencia: UNA entrada por declaración marcada, canal vinculo_correo.
        entradas = self.aceptaciones.documents
        self.assertEqual(len(entradas), 2)
        for e in entradas:
            self.assertEqual(e["sujeto_cedula"], "55444333")
            self.assertEqual(e["canal"], "vinculo_correo")
            self.assertIsNone(e["conductor_id"])
            self.assertIn("ip", e)
        # Token consumido: reusar el link → 400.
        r = self.client.post("/conductores/autorizacion/aceptar", json={
            "token": token, "declaraciones_aceptadas": ["origen_fondos", "sarlaft"]})
        self.assertEqual(r.status_code, 400)

    def test_aceptar_exige_todas_las_declaraciones(self):
        self.client.post("/conductores/autorizacion/solicitar", json={
            "placa": "X", "cedula": "11.111", "correo": "a@x.com"})
        token = self._token_del_correo()
        r = self.client.post("/conductores/autorizacion/aceptar", json={
            "token": token, "declaraciones_aceptadas": ["origen_fondos"]})
        self.assertEqual(r.status_code, 400)
        self.assertIn("Faltan", r.json()["detail"])

    def test_solicitar_es_idempotente_con_link_vigente(self):
        self.client.post("/conductores/autorizacion/solicitar", json={
            "placa": "X", "cedula": "22.222", "correo": "b@x.com"})
        self.assertEqual(len(self.correos_enviados), 1)
        r = self.client.post("/conductores/autorizacion/solicitar", json={
            "placa": "X", "cedula": "22.222", "correo": "b@x.com"})
        self.assertEqual(r.json()["estado"], "pendiente")
        self.assertEqual(len(self.correos_enviados), 1)  # NO se duplicó

    def test_ya_autorizado_no_envia(self):
        self.aceptaciones.insert_one({
            "conductor_id": None, "sujeto_cedula": "33333",
            "canal": "papel", "version": 2})
        r = self.client.post("/conductores/autorizacion/solicitar", json={
            "placa": "X", "cedula": "33.333", "correo": "c@x.com"})
        self.assertEqual(r.json()["estado"], "ya_autorizado")
        self.assertEqual(len(self.correos_enviados), 0)

    def test_papel_registra_entradas_con_documento(self):
        with patch("rutas.vehiculos.subir_a_google_storage",
                   return_value="Vehiculos/T1/2026-10-05/autorizacionFisica_44444_t1.pdf"), \
             patch("rutas.vehiculos._url_para_cliente", return_value="https://firma"):
            r = self.client.post("/conductores/autorizacion/papel",
                                 data={"placa": "T1", "cedula": "44.444", "registrado_por": "EDWIN"},
                                 files={"archivo": ("firma.png", b"img", "image/png")})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["documento_ruta"],
                         "Vehiculos/T1/2026-10-05/autorizacionFisica_44444_t1.pdf")
        # Una entrada POR declaración de la política vigente (firma completa).
        entradas = [e for e in self.aceptaciones.documents if e["canal"] == "papel"]
        self.assertEqual(len(entradas), 3)
        self.assertEqual(entradas[0]["registrado_por"], "EDWIN")
        self.assertTrue(entradas[0]["documento_ruta"].startswith("Vehiculos/"))

    def test_habeas_data_merguea_cuenta_y_sujeto(self):
        # Persona CON cuenta + aceptación de cuenta.
        self.cuentas.insert_one({"_id": "cid-9", "cedula": "55555", "nombre": "ANA",
                                 "correo": "ANA@X.COM",
                                 "aceptacion_politica": {"version": 2}})
        self.aceptaciones.insert_one({"conductor_id": "cid-9", "version": 2,
                                      "declaracion_titulo": "SARLAFT",
                                      "canal": "verificacion_correo",
                                      "aceptado_en": datetime(2026, 9, 1)})
        # Sujeto SIN cuenta con aceptación por link + link pendiente de otro.
        self.aceptaciones.insert_one({"conductor_id": None, "sujeto_cedula": "66666",
                                      "version": 2, "declaracion_titulo": "PTEE",
                                      "canal": "vinculo_correo",
                                      "aceptado_en": datetime(2026, 10, 5)})
        self.tokens.insert_one({"cedula": "77777", "usado_en": None,
                                "expira": datetime.utcnow() + timedelta(hours=24),
                                "creado_en": datetime.utcnow()})
        r = self.client.get("/conductores/habeas-data?cedulas=55555,66666,77777")
        self.assertEqual(r.status_code, 200, r.text)
        por_ced = {p["cedula"]: p for p in r.json()["personas"]}
        self.assertTrue(por_ced["55555"]["tiene_cuenta"])
        self.assertEqual(len(por_ced["55555"]["aceptaciones"]), 1)
        self.assertFalse(por_ced["66666"]["tiene_cuenta"])
        self.assertEqual(por_ced["66666"]["aceptaciones"][0]["canal"], "vinculo_correo")
        self.assertIsNotNone(por_ced["77777"]["token_pendiente"])

    def test_envio_automatico_al_pasar_a_revision(self):
        vehiculo = {
            "placa": "AUTO1",
            "condCedulaCiudadania": "111", "condCorreo": "cond@x.com",
            "propDocumento": "222", "propCorreo": "prop@x.com",
            "tenedDocumento": "900.123.456-7", "tenedTipoDocumento": "NIT",
            "RemolDuenoDocumento": "333",  # sin RemolDuenoCorreo
        }
        # El conductor ya tiene cuenta con aceptación → no se le escribe.
        self.cuentas.insert_one({"_id": "cid-1", "cedula": "111",
                                 "aceptacion_politica": {"version": 2}})
        resumen = conductores.enviar_autorizaciones_pendientes(vehiculo)
        self.assertEqual(resumen["ya_autorizado"], 1)      # el conductor
        self.assertEqual(resumen["enviado"], 1)            # el propietario
        self.assertEqual(resumen["sin_correo"], ["333"])   # dueño remolque
        # La empresa (NIT) no genera envío NI aparece en sin_correo.
        self.assertEqual(len(self.correos_enviados), 1)
        self.assertEqual(self.correos_enviados[0]["to"], ["prop@x.com"])
        # Re-ejecutar (re-revisión): idempotente, no duplica.
        resumen2 = conductores.enviar_autorizaciones_pendientes(vehiculo)
        self.assertEqual(resumen2["pendiente"], 1)
        self.assertEqual(len(self.correos_enviados), 1)


if __name__ == "__main__":
    unittest.main()
