# -*- coding: utf-8 -*-
"""Tests de la Planilla de Seguridad Social multi-carga (2026-10-07, pedido
del usuario): la planilla se ACTUALIZA MENSUALMENTE, así que

1. Cada subida ACUMULA en el array `documentosPlanillaSegSocial` (nada se
   reemplaza; `planillaEpsArl` queda como espejo de la ÚLTIMA).
2. Subirla NO baja un aprobado a re-revisión (excepción como hojaVidaFisica)
   — antes inhabilitaba el vehículo sin razón.
3. El array queda blindado (CLAVES_PROTEGIDAS / CAMPOS_DOCUMENTO_PROTEGIDOS /
   CAMPOS_VOLATILES_FIRMA) para que ni el front ni el hash de la firma lo toquen.
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
    ($each/$position/$slice — $position/$slice se ignoran, el orden lo
    verifican las rutas, no el índice)."""

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
                if campo.endswith(".auditoriaVehiculo") or campo == "auditoriaVehiculo":
                    d.setdefault("auditoriaVehiculo", []).extend(entradas)
                else:
                    d.setdefault(campo, []).extend(entradas)
            return
        if upsert:
            self.documents.append(dict(filtro))


def _subir(client, nombre="planilla.pdf", contenido=b"pdf"):
    return client.put(
        "/vehiculos/subir-documento",
        data={"placa": "TEST01", "tipo": "planillaEpsArl", "extraer": "false"},
        files={"archivo": (nombre, contenido, "application/pdf")},
    )


class PlanillaSeguridadSocialTests(unittest.TestCase):

    def setUp(self):
        self.client = cliente_de_prueba()

    def test_subidas_acumulan_y_el_espejo_es_la_ultima(self):
        fake = FakeColeccion([{"placa": "TEST01", "estadoIntegra": "aprobado"}])
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "subir_a_google_storage",
                          side_effect=lambda a, n: f"Vehiculos/{n}"):
            r1 = _subir(self.client, "planilla_enero.pdf")
            r2 = _subir(self.client, "planilla_febrero.pdf")
        self.assertEqual(r1.status_code, 200, r1.text)
        self.assertEqual(r2.status_code, 200, r2.text)

        doc = fake.documents[0]
        historial = doc["documentosPlanillaSegSocial"]
        self.assertEqual(len(historial), 2)  # ACUMULA: enero NO fue reemplazada
        rutas = {h["ruta"] for h in historial}
        self.assertTrue(all("planillaEpsArl" in r for r in rutas))
        # El espejo queda con la ÚLTIMA subida (compat: gate de documentos).
        self.assertEqual(doc["planillaEpsArl"], r2.json()["ruta"])
        # Cada entrada lleva su fecha y nombre de archivo.
        nombres = {h["nombre"] for h in historial}
        self.assertEqual(nombres, {"planilla_enero.pdf", "planilla_febrero.pdf"})
        self.assertTrue(all(h.get("fecha") for h in historial))

    def test_subir_no_baja_un_aprobado(self):
        """La planilla se renueva cada mes: NO debe sacar al vehículo de la
        bolsa (antes lo bajaba a completado_revision)."""
        fake = FakeColeccion([{"placa": "TEST01", "estadoIntegra": "aprobado"}])
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "subir_a_google_storage",
                          side_effect=lambda a, n: f"Vehiculos/{n}"):
            resp = _subir(self.client)
        self.assertEqual(resp.status_code, 200, resp.text)
        doc = fake.documents[0]
        self.assertEqual(doc["estadoIntegra"], "aprobado")  # SIGUE aprobado
        self.assertNotIn("historialCambios", doc)           # sin diff de re-revisión

    def test_otro_documento_si_baja_un_aprobado(self):
        """Contraste: un dato real (SOAT) subido por el conductor SÍ baja a
        re-revisión — la excepción es SOLO para la planilla."""
        fake = FakeColeccion([{"placa": "TEST01", "estadoIntegra": "aprobado"}])
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "subir_a_google_storage",
                          side_effect=lambda a, n: f"Vehiculos/{n}"):
            resp = self.client.put(
                "/vehiculos/subir-documento",
                data={"placa": "TEST01", "tipo": "soat", "extraer": "false"},
                files={"archivo": ("soat.png", b"img", "image/png")},
            )
        self.assertEqual(resp.status_code, 200, resp.text)
        doc = fake.documents[0]
        self.assertEqual(doc["estadoIntegra"], "completado_revision")

    def test_array_blindado(self):
        self.assertIn("documentosPlanillaSegSocial", vehiculos.CLAVES_PROTEGIDAS)
        self.assertIn("documentosPlanillaSegSocial", vehiculos.CAMPOS_DOCUMENTO_PROTEGIDOS)
        self.assertIn("documentosPlanillaSegSocial", vehiculos.CAMPOS_VOLATILES_FIRMA)


class HistorialDocumentosTests(unittest.TestCase):
    """Historial UNIVERSAL (2026-10-07, pedido del usuario: "todos los
    documentos son sujetos a actualización, no se pierden verdad?"): toda
    subida de CUALQUIER documento queda en `historialDocumentos` con su
    archivo propio — re-subir NUNCA pisa el blob anterior (sufijo _v{N})."""

    def setUp(self):
        self.client = cliente_de_prueba()

    def _subir_soat(self, fake, nombre="soat.png"):
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "subir_a_google_storage",
                          side_effect=lambda a, n: f"Vehiculos/{n}"):
            return self.client.put(
                "/vehiculos/subir-documento",
                data={"placa": "TEST01", "tipo": "soat", "extraer": "false"},
                files={"archivo": (nombre, b"img", "image/png")},
            )

    def test_toda_subida_entra_al_historial(self):
        fake = FakeColeccion([{"placa": "TEST01", "estadoIntegra": "aprobado"}])
        resp = self._subir_soat(fake)
        self.assertEqual(resp.status_code, 200, resp.text)
        doc = fake.documents[0]
        historial = doc["historialDocumentos"]
        self.assertEqual(len(historial), 1)
        entrada = historial[0]
        self.assertEqual(entrada["tipo"], "soat")
        self.assertEqual(entrada["etiqueta"], "SOAT")  # etiqueta legible del backend
        self.assertEqual(entrada["ruta"], doc["soat"])  # apunta al archivo subido
        self.assertTrue(entrada.get("fecha"))

    def test_re_subida_NO_pisa_y_suma_version(self):
        """La 1ª subida usa el nombre base; la 2ª crea `soat_v2_...` (antes
        re-subir el mismo día SOBRESCRIBÍA el archivo) y ambas quedan en el
        historial — la versión vieja NO se pierde."""
        fake = FakeColeccion([{"placa": "TEST01", "estadoIntegra": "registro_incompleto"}])
        r1 = self._subir_soat(fake, "soat_viejo.png")
        r2 = self._subir_soat(fake, "soat_nuevo.png")
        self.assertEqual(r1.status_code, 200)
        self.assertEqual(r2.status_code, 200)
        doc = fake.documents[0]
        self.assertIn("_v2_", doc["soat"])              # archivo NUEVO, no el mismo
        self.assertNotIn("_v2_", r1.json()["ruta"])     # el primero quedó con nombre base
        historial = doc["historialDocumentos"]
        self.assertEqual(len(historial), 2)             # ambas subidas registradas
        rutas = {h["ruta"] for h in historial}
        self.assertEqual(rutas, {r1.json()["ruta"], r2.json()["ruta"]})

    def test_historial_blindado(self):
        self.assertIn("historialDocumentos", vehiculos.CLAVES_PROTEGIDAS)
        self.assertIn("historialDocumentos", vehiculos.CAMPOS_DOCUMENTO_PROTEGIDOS)
        self.assertIn("historialDocumentos", vehiculos.CAMPOS_VOLATILES_FIRMA)


class WasapSeguridadTests(unittest.TestCase):
    """WhatsApp a SEGURIDAD (2026-10-07): cuando un conductor deja un vehículo
    pendiente de revisión (Finalizar o edición de un aprobado), además del
    correo les llega plantilla `enruta_revision_pendiente` al celular de los
    usuarios SEGURIDAD activos de baseusuarios (patrón Otros Costos)."""

    def setUp(self):
        self.client = cliente_de_prueba()

    def test_notifica_a_seguridad_con_celular(self):
        from Funciones import whatsapp_utils_integra

        class FakeBase(dict):
            TODOS = [
                {"perfil": "SEGURIDAD", "nombre": "ANA", "celular": "3001112233"},
                {"perfil": "SEGURIDAD", "nombre": "BOB", "celular": None},   # sin celular: se saltea
                {"perfil": "SEGURIDAD", "nombre": "INACTIVA", "celular": "3009998877", "activo": False},
            ]

            def find(self, filtro, *a, **k):
                # respeta el filtro de activos del backend
                return [u for u in self.TODOS if u.get("activo", True)]

        with patch.object(vehiculos, "coleccion_baseusuarios", FakeBase()), \
             patch.object(whatsapp_utils_integra, "enviar_template_sync") as envio:
            vehiculos.enviar_notificacion_seguridad("TEST01", "PEDRO PEREZ")
        envio.assert_called_once()  # solo ANA: BOB sin celular, INACTIVA desactivada
        kwargs = envio.call_args.kwargs
        self.assertEqual(kwargs["to"], "573001112233")     # normalizado con 57
        self.assertEqual(kwargs["template_name"], "enruta_revision_pendiente")
        self.assertEqual(kwargs["body_params"][2], "TEST01")  # {{3}} = placa

    def test_fallo_del_wa_no_tumba_el_correo(self):
        """Fire-and-forget: si la plantilla/envío explota, la función de
        notificación termina sin lanzar (el flujo del conductor sigue)."""
        from Funciones import whatsapp_utils_integra

        class FakeBase(dict):
            def find(self, filtro, *a, **k):
                return [{"perfil": "SEGURIDAD", "nombre": "ANA", "celular": "3001112233"}]

        with patch.object(vehiculos, "coleccion_baseusuarios", FakeBase()), \
             patch.object(whatsapp_utils_integra, "enviar_template_sync",
                          side_effect=RuntimeError("boom")):
            vehiculos.enviar_notificacion_seguridad("TEST01", "PEDRO")  # no debe lanzar


if __name__ == "__main__":
    unittest.main()
