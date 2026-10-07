# -*- coding: utf-8 -*-
"""Tests de la sesión 2026-10-05:

1. CÉDULA DEL DUEÑO DEL REMOLQUE — nuevo documento opcional
   (documentoIdentidadRemolque + Reverso): subida con dos caras,
   reutilización desde otra figura, blindaje en actualizar-informacion y
   rol «dueño_remolque» en los sujetos de los estudios automáticos.
2. HABEAS DATA — endpoint de consulta /conductores/habeas-data por cédulas
   (evidencia de aceptaciones para auditoría).
3. ESTUDIOS POR ANTIGÜEDAD — GET /vehiculos/estudios-antiguedad: listado
   ordenado por la fecha del último estudio.
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

from rutas import conductores, vehiculos
from Funciones import estudios_automaticos


def cliente_de_prueba(router) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    return TestClient(app, raise_server_exceptions=False)


class _CursorFake(list):
    """Cursor mínimo: sort/limit encadenables (listado de aceptaciones)."""

    def sort(self, clave, direccion=-1):
        return _CursorFake(sorted(self, key=lambda d: str(d.get(clave, "")),
                                  reverse=direccion < 0))

    def limit(self, _n):
        return self


def _get_punteado(doc, campo):
    actual = doc
    for parte in campo.split("."):
        if isinstance(actual, list):
            try:
                actual = actual[int(parte)]
            except (ValueError, IndexError):
                return None
        elif isinstance(actual, dict):
            actual = actual.get(parte)
        else:
            return None
    return actual


def _tiene_punteado(doc, campo):
    actual = doc
    for parte in campo.split("."):
        if isinstance(actual, list):
            try:
                actual = actual[int(parte)]
            except (ValueError, IndexError):
                return False
        elif isinstance(actual, dict) and parte in actual:
            actual = actual[parte]
        else:
            return False
    return True


class FakeColeccionVehiculos:
    """Falsa colección: find/find_one/update_one genéricos mínimos (soporta
    $or/$exists/$ne/$regex y claves punteadas como lecturasIA.tipo)."""

    def __init__(self, documentos=None):
        self.documents = list(documentos or [])
        self.updates = []

    @staticmethod
    def _match(doc, cond):
        import re as _re
        for campo, esperado in (cond or {}).items():
            if isinstance(esperado, dict) and "$exists" in esperado:
                existe = _tiene_punteado(doc, campo) and _get_punteado(doc, campo) is not None
                if existe != esperado["$exists"]:
                    return False
            elif isinstance(esperado, dict) and "$ne" in esperado:
                if _get_punteado(doc, campo) == esperado["$ne"]:
                    return False
            elif isinstance(esperado, dict) and "$gt" in esperado:
                valor = _get_punteado(doc, campo)
                if valor is None or valor <= esperado["$gt"]:
                    return False
            elif isinstance(esperado, dict) and "$regex" in esperado:
                opciones = _re.IGNORECASE if "i" in esperado.get("$options", "") else 0
                if not _re.search(esperado["$regex"], str(_get_punteado(doc, campo) or ""), opciones):
                    return False
            elif _get_punteado(doc, campo) != esperado:
                return False
        return True

    def find(self, filtro=None, *a, **k):
        docs = self.documents
        for campo, esperado in (filtro or {}).items():
            if campo == "$or":
                docs = [d for d in docs if any(self._match(d, c) for c in esperado)]
            else:
                docs = [d for d in docs if self._match(d, {campo: esperado})]
        return _CursorFake(docs)

    def find_one(self, query, *args, **kwargs):
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
            return
        if upsert:
            nuevo = dict(filtro)
            for k, v in (cambio.get("$set") or {}).items():
                nuevo[k] = v
            self.documents.append(nuevo)


def vehiculo_completo(**extra):
    doc = {"placa": "TEST01", "estadoIntegra": "registro_incompleto"}
    doc.update(extra)
    return doc


# ── 1. CÉDULA DEL DUEÑO DEL REMOLQUE ───────────────────────────────────────

class CedulaDuenoRemolqueTests(unittest.TestCase):

    def setUp(self):
        self.client = cliente_de_prueba(vehiculos.ruta_vehiculos)

    def test_subida_frente_y_reverso(self):
        fake = FakeColeccionVehiculos([vehiculo_completo()])
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "subir_a_google_storage",
                          side_effect=lambda archivo, nombre: f"Vehiculos/{nombre}"):
            resp = self.client.put(
                "/vehiculos/subir-documento",
                data={"placa": "TEST01", "tipo": "documentoIdentidadRemolque", "extraer": "false"},
                files={
                    "archivo": ("cedula.png", b"img", "image/png"),
                    "reverso": ("cedula-rev.png", b"img", "image/png"),
                },
            )
        self.assertEqual(resp.status_code, 200, resp.text)
        doc = fake.documents[0]
        self.assertTrue(str(doc["documentoIdentidadRemolque"]).startswith("Vehiculos/"))
        self.assertTrue(str(doc["documentoIdentidadRemolqueReverso"]).startswith("Vehiculos/"))
        # La nomenclatura lleva el tipo (con su reverso) como nombre de blob.
        self.assertIn("documentoIdentidadRemolqueReverso", doc["documentoIdentidadRemolqueReverso"])

    def test_campos_blindados_en_actualizar_informacion(self):
        """Las URLs del documento del remolque jamás se pisan desde
        actualizar-informacion (mismo blindaje que las demás cédulas)."""
        self.assertIn("documentoIdentidadRemolque", vehiculos.CAMPOS_DOCUMENTO_PROTEGIDOS)
        self.assertIn("documentoIdentidadRemolqueReverso", vehiculos.CAMPOS_DOCUMENTO_PROTEGIDOS)

    def test_no_es_documento_requerido(self):
        """El documento del dueño del remolque es OPCIONAL: no bloquea el
        paso a completado_revision."""
        self.assertNotIn("documentoIdentidadRemolque", vehiculos.DOCUMENTOS_REQUERIDOS)

    def test_reutilizar_desde_el_conductor(self):
        v = vehiculo_completo(
            documentoIdentidadConductor="Vehiculos/TEST01/2026-10-05/documentoIdentidadConductor.webp",
            documentoIdentidadConductorReverso="Vehiculos/TEST01/2026-10-05/documentoIdentidadConductorReverso.webp",
            lecturasIA={"documentoIdentidadConductor": {
                "datos": {"numero": "1020304050", "nombres": "MARIA"},
                "avisos": [], "fecha": datetime(2026, 10, 5, 12, 0, 0)}},
        )
        fake = FakeColeccionVehiculos([v])
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "_copiar_blob_bucket",
                          side_effect=lambda u, n: f"Vehiculos/{n}"):
            resp = self.client.put(
                "/vehiculos/reutilizar-documento",
                data={"placa": "TEST01", "figura": "remolque",
                      "documento": "cedula", "origen": "conductor"},
            )
        self.assertEqual(resp.status_code, 200, resp.text)
        doc = fake.documents[0]
        self.assertIn("documentoIdentidadRemolque", doc["documentoIdentidadRemolque"])
        self.assertIn("documentoIdentidadRemolqueReverso", doc["documentoIdentidadRemolqueReverso"])
        # La lectura IA del origen quedó replicada en el tipo destino.
        self.assertEqual(
            doc["lecturasIA"]["documentoIdentidadRemolque"]["reutilizada_de"],
            "documentoIdentidadConductor")

    def test_reutilizar_DESDE_el_remolque(self):
        """2026-10-06: la cédula del dueño del remolque también sirve de
        ORIGEN (si se subió primero, las demás figuras pueden copiarla)."""
        v = vehiculo_completo(
            documentoIdentidadRemolque="Vehiculos/TEST01/2026-10-06/documentoIdentidadRemolque.webp",
            documentoIdentidadRemolqueReverso="Vehiculos/TEST01/2026-10-06/documentoIdentidadRemolqueReverso.webp",
            lecturasIA={"documentoIdentidadRemolque": {
                "datos": {"numero": "55444333", "nombres": "PEDRO"},
                "avisos": [], "fecha": datetime(2026, 10, 6, 12, 0, 0)}},
        )
        fake = FakeColeccionVehiculos([v])
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "_copiar_blob_bucket",
                          side_effect=lambda u, n: f"Vehiculos/{n}"):
            resp = self.client.put(
                "/vehiculos/reutilizar-documento",
                data={"placa": "TEST01", "figura": "conductor",
                      "documento": "cedula", "origen": "remolque"},
            )
        self.assertEqual(resp.status_code, 200, resp.text)
        doc = fake.documents[0]
        self.assertIn("documentoIdentidadConductor_", doc["documentoIdentidadConductor"])
        self.assertIn("documentoIdentidadConductorReverso", doc["documentoIdentidadConductorReverso"])
        self.assertEqual(
            doc["lecturasIA"]["documentoIdentidadConductor"]["reutilizada_de"],
            "documentoIdentidadRemolque")


# ── 1b. ROL «dueño_remolque» EN LOS ESTUDIOS AUTOMÁTICOS ──────────────────

class SujetosDuenoRemolqueTests(unittest.TestCase):

    def test_dueno_remolque_es_sujeto(self):
        v = vehiculo_completo(
            condCedulaCiudadania="1020304050",
            RemolDuenoDocumento="55.444.333",
            RemolDuenoTipoDocumento="CÉDULA DE CIUDADANÍA",
        )
        sujetos = estudios_automaticos.sujetos_estudio(v)
        personas = [s for s in sujetos if s["tipo"] == "persona"]
        self.assertIn("55444333", [p["cedula"] for p in personas])
        rol = next(p for p in personas if p["cedula"] == "55444333")
        self.assertIn("dueño_remolque", rol["roles"])

    def test_sin_dueno_no_genera_sujeto(self):
        sujetos = estudios_automaticos.sujetos_estudio(
            vehiculo_completo(condCedulaCiudadania="1020304050"))
        self.assertEqual(len(sujetos), 2)  # conductor + vehículo

    def test_misma_persona_combina_roles(self):
        """Dueño del remolque = tenedor → UN solo estudio con ambos roles
        (dedup por dígitos, sin gasto extra)."""
        v = vehiculo_completo(
            condCedulaCiudadania="1020304050",
            tenedDocumento="55444333",
            RemolDuenoDocumento="55.444.333",
        )
        sujetos = estudios_automaticos.sujetos_estudio(v)
        personas = [s for s in sujetos if s["tipo"] == "persona"]
        self.assertEqual(len(personas), 2)  # cond + tenedor/remolque combinado
        combinado = next(p for p in personas if p["cedula"] == "55444333")
        self.assertIn("tenedor", combinado["roles"])
        self.assertIn("dueño_remolque", combinado["roles"])


# ── 2. HABEAS DATA POR CÉDULA ──────────────────────────────────────────────

class HabeasDataTests(unittest.TestCase):

    def setUp(self):
        self.client = cliente_de_prueba(conductores.ruta_conductores)

    def _fakes(self):
        cuenta = {
            "_id": "cid-1", "cedula": "1020304050", "nombre": "PEDRO PEREZ",
            "correo": "PEDRO@X.COM", "perfil": "CONDUCTOR",
            "aceptacion_politica": {"version": 2, "aceptado_en": datetime(2026, 9, 1)},
        }
        aceptaciones = [
            {"conductor_id": "cid-1", "version": 2, "declaracion_id": "origen_fondos",
             "declaracion_titulo": "Origen de Fondos", "canal": "verificacion_correo",
             "aceptado_en": datetime(2026, 9, 1), "ip": "190.0.0.1",
             "user_agent": "Mozilla/5.0"},
            {"conductor_id": "cid-1", "version": 2, "declaracion_id": "sarlaft",
             "declaracion_titulo": "SARLAFT", "canal": "verificacion_correo",
             "aceptado_en": datetime(2026, 9, 2), "ip": "190.0.0.1",
             "user_agent": "Mozilla/5.0"},
        ]
        return FakeColeccionVehiculos([cuenta]), FakeColeccionVehiculos(aceptaciones)

    def test_persona_con_aceptaciones(self):
        fake_cuentas, fake_acept = self._fakes()
        with patch.object(conductores, "coleccion_conductores", fake_cuentas), \
             patch.object(conductores, "coleccion_aceptaciones", fake_acept):
            resp = self.client.get("/conductores/habeas-data?cedulas=1.020.304.050")
        self.assertEqual(resp.status_code, 200, resp.text)
        personas = resp.json()["personas"]
        self.assertEqual(len(personas), 1)
        p = personas[0]
        self.assertTrue(p["tiene_cuenta"])
        self.assertEqual(p["nombre"], "PEDRO PEREZ")
        self.assertEqual(len(p["aceptaciones"]), 2)
        self.assertEqual(p["aceptaciones"][0]["declaracion_titulo"], "SARLAFT")  # desc
        self.assertIn("aceptado_en", p["aceptaciones"][0])

    def test_persona_sin_cuenta(self):
        with patch.object(conductores, "coleccion_conductores", FakeColeccionVehiculos()), \
             patch.object(conductores, "coleccion_aceptaciones", FakeColeccionVehiculos()):
            resp = self.client.get("/conductores/habeas-data?cedulas=99999")
        self.assertEqual(resp.status_code, 200)
        p = resp.json()["personas"][0]
        self.assertFalse(p["tiene_cuenta"])
        self.assertEqual(p["aceptaciones"], [])

    def test_sin_cedulas_lista_vacia(self):
        resp = self.client.get("/conductores/habeas-data")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["personas"], [])

    def test_token_pendiente_trae_el_correo(self):
        """2026-10-07: el link vigente sin usar debe reportar a QUÉ correo se
        envió (la tarjeta lo muestra: «Enlace enviado a … el …»)."""
        fake_cuentas, fake_acept = self._fakes()
        token = {
            "cedula": "1020304050", "correo": "PEDRO@X.COM", "usado_en": None,
            "creado_en": datetime(2026, 10, 6, 16, 29, 0),
            "expira": datetime(2027, 1, 1),
        }
        with patch.object(conductores, "coleccion_conductores", fake_cuentas), \
             patch.object(conductores, "coleccion_aceptaciones", fake_acept), \
             patch.object(conductores, "coleccion_tokens_aut",
                          FakeColeccionVehiculos([token])):
            resp = self.client.get("/conductores/habeas-data?cedulas=1020304050")
        self.assertEqual(resp.status_code, 200, resp.text)
        p = resp.json()["personas"][0]
        self.assertIn("token_pendiente", p)
        self.assertEqual(p["token_pendiente_correo"], "PEDRO@X.COM")

    def test_devuelve_declaraciones_de_la_politica(self):
        """2026-10-07: el response trae las declaraciones de la política vigente
        (con la marca de la opcional) para que el front pinte en ROJO las que
        falten — en la práctica solo puede faltar «tratamiento_datos»."""
        politica = {"version": 2, "declaraciones": conductores.DECLARACIONES_V2}
        with patch.object(conductores, "coleccion_conductores", FakeColeccionVehiculos()), \
             patch.object(conductores, "coleccion_aceptaciones", FakeColeccionVehiculos()), \
             patch.object(conductores, "_politica_vigente", return_value=politica):
            resp = self.client.get("/conductores/habeas-data?cedulas=99999")
        self.assertEqual(resp.status_code, 200, resp.text)
        decls = resp.json()["declaraciones_politica"]
        self.assertEqual(len(decls), 7)
        td = next(d for d in decls if d["id"] == "tratamiento_datos")
        self.assertTrue(td["opcional"])
        self.assertFalse(decls[0]["opcional"])


# ── 3. RECHAZO DEFINITIVO (2026-10-05) ─────────────────────────────────────

class RechazoDefinitivoTests(unittest.TestCase):
    """Estado `rechazado`: transición con motivo obligatorio, SIN salidas y
    con candado de edición (403 en todas las mutaciones de contenido)."""

    def setUp(self):
        self.client = cliente_de_prueba(vehiculos.ruta_vehiculos)

    def _rechazar(self, fake, extra=None):
        data = {"placa": "TEST01", "nuevo_estado": "rechazado", "usuario_id": "seg1"}
        data.update(extra or {})
        return self.client.put("/vehiculos/actualizar-estado", data=data)

    def test_rechazo_exige_motivo(self):
        fake = FakeColeccionVehiculos([vehiculo_completo(estadoIntegra="completado_revision")])
        with patch.object(vehiculos, "coleccion_vehiculos", fake):
            resp = self._rechazar(fake)
        self.assertEqual(resp.status_code, 400)
        self.assertIn("motivo", resp.json()["detail"].lower())

    def test_rechazo_queda_candado_con_historial(self):
        fake = FakeColeccionVehiculos([vehiculo_completo(estadoIntegra="completado_revision")])
        with patch.object(vehiculos, "coleccion_vehiculos", fake):
            resp = self._rechazar(fake, {"motivo": "Hallazgos graves en el estudio"})
        self.assertEqual(resp.status_code, 200, resp.text)
        doc = fake.documents[0]
        self.assertEqual(doc["estadoIntegra"], "rechazado")
        # El motivo viaja al histórico (acción propia) y a observaciones.
        self.assertEqual(doc["historialInactivacion"][-1]["accion"], "rechazado")
        self.assertEqual(doc["historialInactivacion"][-1]["motivo"], "Hallazgos graves en el estudio")
        self.assertIn("Hallazgos graves", doc["observaciones"])

    def test_rechazado_no_tiene_transiciones_de_salida(self):
        fake = FakeColeccionVehiculos([vehiculo_completo(estadoIntegra="rechazado")])
        with patch.object(vehiculos, "coleccion_vehiculos", fake):
            resp = self.client.put("/vehiculos/actualizar-estado", data={
                "placa": "TEST01", "nuevo_estado": "aprobado", "usuario_id": "seg1"})
        self.assertEqual(resp.status_code, 400)  # transición inválida
        # Ni devolviéndolo al conductor.
        with patch.object(vehiculos, "coleccion_vehiculos", fake):
            resp = self.client.put("/vehiculos/actualizar-estado", data={
                "placa": "TEST01", "nuevo_estado": "registro_incompleto", "usuario_id": "seg1"})
        self.assertEqual(resp.status_code, 400)

    def test_rechazado_bloquea_ediciones(self):
        fake = FakeColeccionVehiculos([vehiculo_completo(estadoIntegra="rechazado")])
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "subir_a_google_storage") as mock_storage:
            resp = self.client.put(
                "/vehiculos/subir-documento",
                data={"placa": "TEST01", "tipo": "soat", "extraer": "false"},
                files={"archivo": ("soat.png", b"img", "image/png")},
            )
        self.assertEqual(resp.status_code, 403)
        mock_storage.assert_not_called()
        # Datos también candados.
        with patch.object(vehiculos, "coleccion_vehiculos", fake):
            resp = self.client.put("/vehiculos/actualizar-informacion/TEST01", json={"condNombres": "X"})
        self.assertEqual(resp.status_code, 403)


# ── 4. SWITCH DEL DISPARO AUTOMÁTICO (2026-10-05) ──────────────────────────

class SwitchAutoDisparoTests(unittest.TestCase):
    """El disparo automático de estudios se puede pausar TEMPORALMENTE:
    switch en `config_estudios` (botón de /revision) + kill-switch por env."""

    def setUp(self):
        self.client = cliente_de_prueba(vehiculos.ruta_vehiculos)

    def test_default_encendido_sin_config(self):
        with patch.object(estudios_automaticos, "coleccion_config", FakeColeccionVehiculos()):
            self.assertTrue(estudios_automaticos.auto_disparo_habilitado())

    def test_apagado_en_bd(self):
        fake = FakeColeccionVehiculos([{"_id": "global", "auto_disparo": False}])
        with patch.object(estudios_automaticos, "coleccion_config", fake):
            self.assertFalse(estudios_automaticos.auto_disparo_habilitado())

    def test_env_apaga_siempre(self):
        """ESTUDIOS_AUTO_DISPARAR=false gana aunque la BD diga ON (kill-switch)."""
        fake = FakeColeccionVehiculos([{"_id": "global", "auto_disparo": True}])
        import os as _os
        with patch.object(estudios_automaticos, "coleccion_config", fake), \
             patch.dict(_os.environ, {"ESTUDIOS_AUTO_DISPARAR": "false"}):
            self.assertFalse(estudios_automaticos.auto_disparo_habilitado())

    def test_fijar_auto_disparo_upsertea(self):
        fake = FakeColeccionVehiculos()
        with patch.object(estudios_automaticos, "coleccion_config", fake):
            efectivo = estudios_automaticos.fijar_auto_disparo(False)
        self.assertFalse(efectivo)
        doc = next(d for d in fake.documents if d.get("_id") == "global")
        self.assertIs(doc["auto_disparo"], False)

    def test_hook_omite_disparo_con_switch_apagado(self):
        """_disparar_estudios_seguridad NO lanza la task cuando el switch
        está apagado (el vehículo llega a revisión sin estudios)."""
        with patch.object(estudios_automaticos, "auto_disparo_habilitado", return_value=False), \
             patch.object(estudios_automaticos, "disparar_estudios") as espia:
            vehiculos._disparar_estudios_seguridad("TEST01")
        espia.assert_not_called()

    def test_disparo_MANUAL_ignora_switch_apagado(self):
        """BUG 2026-10-07: el botón «Volver a consultar» (endpoint manual
        /disparar) pasaba por el switch → con el switch APAGADO respondía 200
        en silencio y no lanzaba nada (reporte del usuario en MVX48E)."""
        fake = FakeColeccionVehiculos([vehiculo_completo(placa="MVX48E")])
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(estudios_automaticos, "auto_disparo_habilitado", return_value=False), \
             patch.object(estudios_automaticos, "disparar_estudios") as espia:
            resp = self.client.post("/vehiculos/estudios-seguridad/MVX48E/disparar")
        self.assertEqual(resp.status_code, 200, resp.text)
        espia.assert_called()

    def test_endpoints_del_switch(self):
        fake = FakeColeccionVehiculos()
        with patch.object(estudios_automaticos, "coleccion_config", fake):
            r = self.client.get("/vehiculos/estudios-config")
            self.assertEqual(r.status_code, 200)
            self.assertTrue(r.json()["auto_disparo"])  # default ON

            r = self.client.put("/vehiculos/estudios-config", data={"auto_disparo": "false"})
            self.assertEqual(r.status_code, 200, r.text)
            self.assertFalse(r.json()["auto_disparo"])

            r = self.client.get("/vehiculos/estudios-config")
            self.assertFalse(r.json()["auto_disparo"])


# ── 5. ESTUDIOS POR ANTIGÜEDAD ─────────────────────────────────────────────

class EstudiosAntiguedadTests(unittest.TestCase):

    def setUp(self):
        self.client = cliente_de_prueba(vehiculos.ruta_vehiculos)

    def test_lista_ordenada_por_antiguedad(self):
        docs = [
            vehiculo_completo(placa="NUEVA", estadoIntegra="aprobado",
                              condCedulaCiudadania="1",
                              estudiosSeguridadAuto=[{"estado": "finalizado",
                                                      "finalizado_en": datetime(2026, 10, 1)}],
                              estudiosVigencia={"desde": datetime(2026, 10, 1),
                                                "vence": datetime(2027, 10, 1)}),
            vehiculo_completo(placa="VIEJA", estadoIntegra="aprobado",
                              condCedulaCiudadania="2",
                              estudiosSeguridadAuto=[{"estado": "finalizado",
                                                      "finalizado_en": datetime(2025, 3, 1)}],
                              estudiosVigencia={"desde": datetime(2025, 3, 1),
                                                "vence": datetime(2026, 3, 1)}),
            vehiculo_completo(placa="SINES"),  # sin estudios → no se lista
        ]
        fake = FakeColeccionVehiculos(docs)
        with patch.object(vehiculos, "coleccion_vehiculos", fake):
            resp = self.client.get("/vehiculos/estudios-antiguedad")
        self.assertEqual(resp.status_code, 200, resp.text)
        cuerpo = resp.json()
        placas = [f["placa"] for f in cuerpo["vehiculos"]]
        self.assertEqual(placas, ["VIEJA", "NUEVA"])  # la más antigua PRIMERO
        self.assertNotIn("SINES", placas)
        self.assertEqual(cuerpo["vencidas"], 1)  # VIEJA venció
        vieja = cuerpo["vehiculos"][0]
        self.assertTrue(vieja["vencida"])
        # Sujetos deducidos del documento: la cédula del conductor + la placa.
        documentos = [s["documento"] for s in vieja["sujetos"]]
        self.assertIn("2", documentos)      # cédula del conductor
        self.assertIn("VIEJA", documentos)  # sujeto vehículo

    # ── 2026-10-06: pendientes de AUTORIZACIÓN + filtros del backend ──

    def _dos_placas(self):
        return [
            vehiculo_completo(placa="PEND", estadoIntegra="aprobado",
                              condCedulaCiudadania="111",
                              estudiosSeguridadAuto=[{"estado": "finalizado",
                                                      "finalizado_en": datetime(2026, 9, 1)}]),
            vehiculo_completo(placa="OK", estadoIntegra="aprobado",
                              condCedulaCiudadania="222",
                              estudiosSeguridadAuto=[{"estado": "finalizado",
                                                      "finalizado_en": datetime(2026, 9, 2)}]),
        ]

    def test_pendientes_autorizacion_por_fila(self):
        fake = FakeColeccionVehiculos(self._dos_placas())
        # 111 SIN autorización (ninguna cuenta ni aceptación); 222 autorizada
        # (aceptación canal vinculo_correo).
        aceptaciones = FakeColeccionVehiculos(
            [{"sujeto_cedula": "222", "canal": "vinculo_correo"}])
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(conductores, "coleccion_conductores", FakeColeccionVehiculos()), \
             patch.object(conductores, "coleccion_aceptaciones", aceptaciones):
            resp = self.client.get("/vehiculos/estudios-antiguedad")
        self.assertEqual(resp.status_code, 200, resp.text)
        cuerpo = resp.json()
        por_placa = {f["placa"]: f for f in cuerpo["vehiculos"]}
        self.assertEqual(por_placa["PEND"]["pendientes_autorizacion"], 1)
        self.assertEqual(por_placa["PEND"]["personas"], 1)
        self.assertFalse(por_placa["PEND"]["sujetos"][0]["autorizado"])
        self.assertEqual(por_placa["OK"]["pendientes_autorizacion"], 0)
        self.assertTrue(por_placa["OK"]["sujetos"][0]["autorizado"])
        self.assertEqual(cuerpo["con_pendientes"], 1)

    def test_filtro_solo_pendientes(self):
        fake = FakeColeccionVehiculos(self._dos_placas())
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(conductores, "coleccion_conductores", FakeColeccionVehiculos()), \
             patch.object(conductores, "coleccion_aceptaciones",
                          FakeColeccionVehiculos([{"sujeto_cedula": "222"}])):
            resp = self.client.get("/vehiculos/estudios-antiguedad?solo_pendientes=true")
        self.assertEqual(resp.status_code, 200, resp.text)
        placas = [f["placa"] for f in resp.json()["vehiculos"]]
        self.assertEqual(placas, ["PEND"])  # solo la que tiene pendientes

    def test_filtro_placa_en_el_backend(self):
        fake = FakeColeccionVehiculos(self._dos_placas())
        with patch.object(vehiculos, "coleccion_vehiculos", fake):
            resp = self.client.get("/vehiculos/estudios-antiguedad?placa=pen")
        self.assertEqual(resp.status_code, 200, resp.text)
        placas = [f["placa"] for f in resp.json()["vehiculos"]]
        self.assertEqual(placas, ["PEND"])  # filtro por placa (contiene, i)


if __name__ == "__main__":
    unittest.main()
