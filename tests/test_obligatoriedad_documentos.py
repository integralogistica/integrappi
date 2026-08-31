"""Tests de obligatoriedad de documentos por figura (gemelos) y replicar_en.

Cubre:
- _figuras_iguales: flags persistidos e inferencia por dígitos.
- _gemelos_documento / _documentos_faltantes: deduplicación por figura.
- actualizar-estado: valida SOLO al pasar a completado_revision.
- subir-documento con replicar_en: setea gemelos y copia lecturasIA.
- eliminar-documento: limpia gemelos con la misma URL.
"""
import json
import unittest
from unittest.mock import patch

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from rutas import vehiculos


def cliente_de_prueba() -> TestClient:
    """Monta el router en una mini-app: los exception handlers de HTTPException
    los registra FastAPI (un APIRouter pelado los convertiría en 500)."""
    app = FastAPI()
    app.include_router(vehiculos.ruta_vehiculos)
    return TestClient(app, raise_server_exceptions=False)


class FakeColeccionVehiculos:
    """Falsa colección vehiculos con registro de updates para aserciones.
    Soporta $set/$unset con claves punteadas (lecturasIA.tipo)."""

    def __init__(self, documentos=None):
        self.documents = list(documentos or [])
        self.updates = []

    def find_one(self, query, *args, **kwargs):
        for d in self.documents:
            if d.get("placa") == query.get("placa"):
                return d
        return None

    @staticmethod
    def _set_punteado(doc: dict, clave: str, valor):
        partes = clave.split(".")
        actual = doc
        for p in partes[:-1]:
            actual = actual.setdefault(p, {})
        actual[partes[-1]] = valor

    def update_one(self, filtro, cambio):
        self.updates.append((filtro, cambio))
        for d in self.documents:
            if d.get("placa") == filtro.get("placa"):
                if "$set" in cambio:
                    for k, v in cambio["$set"].items():
                        self._set_punteado(d, k, v)
                if "$unset" in cambio:
                    for k in cambio["$unset"]:
                        partes = k.split(".")
                        actual = d
                        for p in partes[:-1]:
                            actual = actual.get(p, {}) if isinstance(actual, dict) else {}
                        if isinstance(actual, dict):
                            actual.pop(partes[-1], None)


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


def vehiculo_completo(**extra):
    doc = {"placa": "TEST01", "estadoIntegra": "registro_incompleto", **TODOS_DOCS}
    doc.update(extra)
    return doc


class FigurasIgualesTests(unittest.TestCase):

    def test_flag_persistido_tiene_prioridad(self):
        v = {"propietarioIgualConductor": True, "tenedorIgualPropietario": False}
        fig = vehiculos._figuras_iguales(v)
        self.assertTrue(fig["prop_igual_cond"])
        self.assertFalse(fig["tened_igual_prop"])

    def test_inferencia_por_digitos_sin_flags(self):
        v = {
            "condCedulaCiudadania": "1.020.304.050",
            "propDocumento": "1020304050",       # mismos dígitos → igual
            "tenedDocumento": "987654321",       # distinto → diferente
        }
        fig = vehiculos._figuras_iguales(v)
        self.assertTrue(fig["prop_igual_cond"])
        self.assertFalse(fig["tened_igual_prop"])

    def test_inferencia_vacio_no_infiere_igual(self):
        fig = vehiculos._figuras_iguales({"condCedulaCiudadania": "", "propDocumento": ""})
        self.assertFalse(fig["prop_igual_cond"])


class GemelosDocumentoTests(unittest.TestCase):

    def test_transitividad_tened_igual_cond(self):
        v = {"propietarioIgualConductor": True, "tenedorIgualPropietario": True}
        gemelos = vehiculos._gemelos_documento("documentoIdentidadConductor", v)
        self.assertIn("documentoIdentidadPropietario", gemelos)
        self.assertIn("documentoIdentidadTenedor", gemelos)

    def test_rut_conductor_no_existe(self):
        # La familia RUT no tiene conductor: subir rutPropietario con tened==prop
        # replica solo a rutTenedor.
        v = {"propietarioIgualConductor": True, "tenedorIgualPropietario": True}
        gemelos = vehiculos._gemelos_documento("rutPropietario", v)
        self.assertEqual(gemelos, ["rutTenedor"])


class DocumentosFaltantesTests(unittest.TestCase):

    def test_vehiculo_completo_no_falta_nada(self):
        self.assertEqual(vehiculos._documentos_faltantes(vehiculo_completo()), [])

    def test_falta_rut_tenedor_cubierto_por_figura_igual(self):
        v = vehiculo_completo(rutTenedor=None, tenedorIgualPropietario=True)
        # tened==prop y rutPropietario lleno → rutTenedor cubierto.
        self.assertEqual(vehiculos._documentos_faltantes(v), [])

    def test_falta_rut_tenedor_con_figuras_distintas(self):
        v = vehiculo_completo(rutTenedor=None, tenedorIgualPropietario=False)
        faltantes = vehiculos._documentos_faltantes(v)
        self.assertIn("rutTenedor", faltantes)

    def test_inferencia_por_digitos_cubre_identidad(self):
        v = vehiculo_completo(
            documentoIdentidadPropietario=None,
            condCedulaCiudadania="1020304050",
            propDocumento="1020304050",  # sin flags: inferencia por dígitos
        )
        self.assertEqual(vehiculos._documentos_faltantes(v), [])

    def test_array_fotos_vacio_cuenta_como_faltante(self):
        v = vehiculo_completo(fotos=[])
        self.assertIn("fotos", vehiculos._documentos_faltantes(v))

    def test_url_basura_cuenta_como_faltante(self):
        v = vehiculo_completo(licencia="null")
        self.assertIn("licencia", vehiculos._documentos_faltantes(v))

    def test_reverso_de_licencia_y_tarjeta_son_obligatorios(self):
        # Sin los reversos, el frente solo NO satisface el requisito (2026-08-27).
        v = vehiculo_completo(licenciaReverso=None, tarjetaPropiedadReverso=None)
        faltantes = vehiculos._documentos_faltantes(v)
        self.assertIn("licenciaReverso", faltantes)
        self.assertIn("tarjetaPropiedadReverso", faltantes)

    def test_tarjeta_remolque_es_opcional(self):
        # No todo vehículo arrastra remolque: sin ella NO hay faltante (2026-08-27).
        v = vehiculo_completo(tarjetaRemolque=None)
        self.assertEqual(vehiculos._documentos_faltantes(v), [])

    def test_fotos_minimo_una(self):
        # Fotos del vehículo: se exige al menos 1 (máximo 10 lo valida subir-fotos).
        v = vehiculo_completo(fotos=None)
        self.assertIn("fotos", vehiculos._documentos_faltantes(v))


class JsonSeguroTests(unittest.TestCase):
    """Los docs de vehículo llevan datetime (lecturasIA, historialCambios) y
    ObjectId: _json_seguro los convierte para que los endpoints no revienten
    con 500 (bug real 2026-08-27: un vehículo con lecturas IA rompía
    obtener-vehiculos y el conductor dejaba de ver sus placas)."""

    def test_datetime_y_objectid_anidados_se_serializan(self):
        from datetime import datetime as dt
        from bson import ObjectId
        doc = {
            "placa": "TEST01",
            "_id": ObjectId("64b000000000000000000000"),
            "lecturasIA": {"licencia": {"fecha": dt(2026, 8, 27, 12, 0, 0)}},
            "historialCambios": [{"fecha": dt(2026, 8, 27), "campos": []}],
        }
        seguro = vehiculos._json_seguro(doc)
        json.dumps(seguro)  # no debe lanzar
        self.assertIsInstance(seguro["_id"], str)
        self.assertEqual(seguro["lecturasIA"]["licencia"]["fecha"], "2026-08-27T12:00:00")
        self.assertEqual(seguro["historialCambios"][0]["fecha"], "2026-08-27T00:00:00")

    def test_lista_y_tipos_simples_intactos(self):
        seguro = vehiculos._json_seguro({"a": [1, "x", None, True], "b": 2.5})
        self.assertEqual(seguro, {"a": [1, "x", None, True], "b": 2.5})


class NombreDocBucketTests(unittest.TestCase):
    """Nomenclatura de archivos en el bucket: {PLACA}/{AAAA-MM-DD}/{tipo}{sufijo}.{ext}
    (Desde 2026-08-31 SIN cédula en el nombre: minimización — las rutas llegan
    a logs de GCS y proxies, mismo criterio que los estudios de seguridad.)"""

    def test_cedula_no_va_en_el_nombre(self):
        v = {"condCedulaCiudadania": "1.020.304.050"}
        nombre = vehiculos._nombre_doc_bucket("mx48e", "soat", "pdf", v)
        self.assertRegex(nombre, r"^MX48E/\d{4}-\d{2}-\d{2}/soat\.pdf$")

    def test_nombre_simple(self):
        nombre = vehiculos._nombre_doc_bucket("MX48E", "firma", "webp", {})
        self.assertRegex(nombre, r"^MX48E/\d{4}-\d{2}-\d{2}/firma\.webp$")

    def test_sufijo_fotos(self):
        nombre = vehiculos._nombre_doc_bucket("ABC123", "foto", "webp", None, sufijo="_002")
        self.assertRegex(nombre, r"^ABC123/\d{4}-\d{2}-\d{2}/foto_002\.webp$")


class CamposDocumentoProtegidosTests(unittest.TestCase):
    """actualizar-informacion jamás pisa las URLs de documentos (bug del
    autoguardado: un form con nulls del montaje vaciaba lo recién subido)."""

    def test_claves_de_documentos_se_ignoran(self):
        datos = {
            "vehMarca": "VOLVO",
            "documentoIdentidadConductor": "",   # debe ignorarse
            "licencia": None,                   # debe ignorarse
            "soat": "https://x/falsa",          # debe ignorarse
            "firmaUrl": "",                     # debe ignorarse
        }
        limpios = {
            k: v for k, v in datos.items()
            if k not in vehiculos.CLAVES_PROTEGIDAS and k not in vehiculos.CAMPOS_DOCUMENTO_PROTEGIDOS
        }
        self.assertEqual(limpios, {"vehMarca": "VOLVO"})

    def test_todos_los_documentos_estan_blindados(self):
        for clave in ["documentoIdentidadConductor", "licencia", "tarjetaPropiedad",
                      "soat", "rutTenedor", "rutPropietario", "condCertificacionBancaria",
                      "propCertificacionBancaria", "tenedCertificacionBancaria", "firmaUrl",
                      "licenciaReverso", "tarjetaPropiedadReverso", "documentoIdentidadConductorReverso"]:
            self.assertIn(clave, vehiculos.CAMPOS_DOCUMENTO_PROTEGIDOS, f"Falta blindar {clave}")


class ActualizarEstadoValidacionTests(unittest.TestCase):
    """actualizar-estado valida documentos SOLO al pasar a completado_revision."""

    def setUp(self):
        # raise_server_exceptions=False: las HTTPException del endpoint se
        # ven como respuestas HTTP (lo que recibe el front) en vez de explotar.
        self.client = cliente_de_prueba()

    def test_completado_revision_con_faltantes_devuelve_400(self):
        fake = FakeColeccionVehiculos([vehiculo_completo(soat=None)])
        with patch.object(vehiculos, "coleccion_vehiculos", fake):
            resp = self.client.put(
                "/vehiculos/actualizar-estado",
                data={"placa": "TEST01", "nuevo_estado": "completado_revision", "usuario_id": "u1"},
            )
        self.assertEqual(resp.status_code, 400)
        self.assertIn("Faltan documentos", resp.json()["detail"])
        self.assertIn("SOAT", resp.json()["detail"])

    def test_aprobado_no_valida_documentos(self):
        # Seguridad aprueba con el mismo endpoint: no debe exigir documentos.
        # (Desde completado_revision — la transición real del flujo; desde
        # registro_incompleto directo ya no se permite, ver test_transiciones.)
        fake = FakeColeccionVehiculos([vehiculo_completo(estadoIntegra="completado_revision", soat=None)])
        with patch.object(vehiculos, "coleccion_vehiculos", fake):
            resp = self.client.put(
                "/vehiculos/actualizar-estado",
                data={"placa": "TEST01", "nuevo_estado": "aprobado", "usuario_id": "seg1"},
            )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(fake.documents[0]["estadoIntegra"], "aprobado")

    def test_completado_revision_completo_pasa(self):
        fake = FakeColeccionVehiculos([vehiculo_completo()])
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "enviar_notificacion_seguridad") as mock_notif:
            resp = self.client.put(
                "/vehiculos/actualizar-estado",
                data={
                    "placa": "TEST01", "nuevo_estado": "completado_revision",
                    "usuario_id": "u1", "nombre_conductor": "PEDRO",
                },
            )
        self.assertEqual(resp.status_code, 200)
        mock_notif.assert_called_once()


class DocumentoValidoTests(unittest.TestCase):
    """El veredicto documento_valido del LLM: 409 si la imagen no es el doc
    esperado (no se guarda NADA); ilegible (422) sube igual."""

    def test_prompt_pide_veredicto(self):
        for tipo in ["cedula", "rut", "soat"]:
            instruction, _ = vehiculos._prompt_extraccion(tipo)
            self.assertIn("documento_valido", instruction)

    def test_lectura_marca_documento_invalido(self):
        fake = FakeColeccionVehiculos([vehiculo_completo()])

        def _llm_falso(tipo_doc, archivos):
            raise HTTPException(status_code=409, detail="Esto no parece ser el documento esperado. Sube una foto o PDF del documento indicado.")

        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "extraer_datos_con_llm", side_effect=_llm_falso), \
             patch.object(vehiculos, "subir_a_google_storage") as mock_storage:
            client = cliente_de_prueba()
            resp = client.put(
                "/vehiculos/subir-documento",
                data={"placa": "TEST01", "tipo": "licencia"},
                files={"archivo": ("meme.png", b"img", "image/png")},
            )
        self.assertEqual(resp.status_code, 409)
        # Nada llegó al bucket ni se actualizó el campo del documento.
        mock_storage.assert_not_called()


class SubirDocumentoReplicarTests(unittest.TestCase):

    def setUp(self):
        self.client = cliente_de_prueba()

    def test_replicar_en_setea_gemelos_y_lecturas(self):
        v = vehiculo_completo(
            estadoIntegra="registro_incompleto",
            propietarioIgualConductor=True,
            documentoIdentidadPropietario=None,
        )
        fake = FakeColeccionVehiculos([v])
        lectura = {"datos": {"nombres": "PEDRO"}, "avisos": []}
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "subir_a_google_storage", return_value="https://x/nueva"), \
             patch.object(
                 vehiculos, "extraer_datos_con_llm",
                 side_effect=lambda *a, **k: lectura["datos"],
             ) as mock_llm:
            resp = self.client.put(
                "/vehiculos/subir-documento",
                data={
                    "placa": "TEST01", "tipo": "documentoIdentidadConductor",
                    "extraer": "true", "replicar_en": "documentoIdentidadPropietario",
                },
                files={"archivo": ("cedula.png", b"img", "image/png")},
            )
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["url"], "https://x/nueva")
        self.assertIn("documentoIdentidadPropietario", body["replicado_en"])
        mock_llm.assert_called_once()

        doc = fake.documents[0]
        # El gemelo quedó con la misma URL y su lecturasIA copiada.
        self.assertEqual(doc["documentoIdentidadConductor"], "https://x/nueva")
        self.assertEqual(doc["documentoIdentidadPropietario"], "https://x/nueva")
        self.assertEqual(doc["lecturasIA"]["documentoIdentidadConductor"]["datos"]["nombres"], "PEDRO")
        self.assertEqual(doc["lecturasIA"]["documentoIdentidadPropietario"]["datos"]["nombres"], "PEDRO")

    def test_gemelo_invalido_se_ignora(self):
        fake = FakeColeccionVehiculos([vehiculo_completo()])
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "subir_a_google_storage", return_value="https://x/n2"):
            resp = self.client.put(
                "/vehiculos/subir-documento",
                data={"placa": "TEST01", "tipo": "soat", "replicar_en": "campo_inexistente"},
                files={"archivo": ("soat.png", b"img", "image/png")},
            )
        self.assertEqual(resp.status_code, 200)
        self.assertNotIn("campo_inexistente", fake.documents[0])

    def test_reverso_se_guarda_con_campo_propio(self):
        fake = FakeColeccionVehiculos([vehiculo_completo()])
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "subir_a_google_storage", side_effect=lambda a, n: f"https://x/{n}"):
            client = cliente_de_prueba()
            resp = client.put(
                "/vehiculos/subir-documento",
                data={"placa": "TEST01", "tipo": "licencia", "extraer": "false"},
                files=[
                    ("archivo", ("lic_f.png", b"frente", "image/png")),
                    ("reverso", ("lic_r.png", b"reverso", "image/png")),
                ],
            )
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertIn("/licencia", body["url"])
        self.assertIsNotNone(body["url_reverso"])
        doc = fake.documents[0]
        self.assertEqual(doc["licencia"], body["url"])
        self.assertEqual(doc["licenciaReverso"], body["url_reverso"])

    def test_reverso_rechazado_para_tipo_una_cara(self):
        fake = FakeColeccionVehiculos([vehiculo_completo()])
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "subir_a_google_storage") as mock_storage:
            client = cliente_de_prueba()
            resp = client.put(
                "/vehiculos/subir-documento",
                data={"placa": "TEST01", "tipo": "soat", "extraer": "false"},
                files=[
                    ("archivo", ("soat.png", b"img", "image/png")),
                    ("reverso", ("soat_r.png", b"rev", "image/png")),
                ],
            )
        self.assertEqual(resp.status_code, 400)
        mock_storage.assert_not_called()

    def test_fallo_lectura_ia_no_rompe_subida(self):
        fake = FakeColeccionVehiculos([vehiculo_completo()])
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "subir_a_google_storage", return_value="https://x/n3"), \
             patch.object(
                 vehiculos, "extraer_datos_con_llm",
                 side_effect=HTTPException(status_code=422, detail="ilegible"),
             ):
            resp = self.client.put(
                "/vehiculos/subir-documento",
                data={"placa": "TEST01", "tipo": "licencia"},
                files={"archivo": ("lic.png", b"img", "image/png")},
            )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["lectura_ia"], None)
        self.assertEqual(fake.documents[0]["licencia"], "https://x/n3")


class EliminarDocumentoGemelosTests(unittest.TestCase):

    def setUp(self):
        self.client = cliente_de_prueba()

    def test_elimina_gemelo_con_misma_url(self):
        v = vehiculo_completo(
            propietarioIgualConductor=True,
            documentoIdentidadConductor="https://x/misma",
            documentoIdentidadPropietario="https://x/misma",
        )
        fake = FakeColeccionVehiculos([v])
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "eliminar_de_google_storage") as mock_del:
            resp = self.client.delete(
                "/vehiculos/eliminar-documento?placa=TEST01&tipo=documentoIdentidadConductor"
            )
        self.assertEqual(resp.status_code, 200)
        self.assertIn("documentoIdentidadPropietario", resp.json()["gemelos_eliminados"])
        # 2 blobs: frente + reverso (la cédula del conductor ahora SIEMPRE
        # tiene reverso cargado en TODOS_DOCS — eliminar borra ambas caras).
        self.assertEqual(mock_del.call_count, 2)
        doc = fake.documents[0]
        self.assertNotIn("documentoIdentidadConductor", doc)
        self.assertNotIn("documentoIdentidadPropietario", doc)

    def test_no_elimina_gemelo_con_url_distinta(self):
        v = vehiculo_completo(
            propietarioIgualConductor=True,
            documentoIdentidadConductor="https://x/a",
            documentoIdentidadPropietario="https://x/b",
        )
        fake = FakeColeccionVehiculos([v])
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "eliminar_de_google_storage"):
            resp = self.client.delete(
                "/vehiculos/eliminar-documento?placa=TEST01&tipo=documentoIdentidadConductor"
            )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["gemelos_eliminados"], [])
        # El documento del propietario (distinto) permanece.
        self.assertEqual(fake.documents[0]["documentoIdentidadPropietario"], "https://x/b")


class ReutilizarCedulaTests(unittest.TestCase):
    """PUT /reutilizar-cedula: copia la cédula del conductor como cédula de
    propietario/tenedor sin re-leer con IA (blob copiado server-side con
    nomenclatura propia + lecturasIA replicada)."""

    def setUp(self):
        self.client = cliente_de_prueba()

    def _vehiculo(self):
        v = vehiculo_completo(
            condCedulaCiudadania="1.020.304.050",
            documentoIdentidadConductor="Vehiculos/TEST01/2026-08-29/documentoIdentidadConductor.webp",
            documentoIdentidadConductorReverso="Vehiculos/TEST01/2026-08-29/documentoIdentidadConductorReverso.webp",
            lecturasIA={
                "documentoIdentidadConductor": {
                    "datos": {"numero": "1020304050", "nombres": "MARIA", "apellidos": "PEREZ SOTO"},
                    "avisos": [],
                }
            },
        )
        # El doc destino debe estar VACÍO antes de reutilizar (sino el test no
        # prueba que el endpoint lo crea).
        v.pop("documentoIdentidadPropietario", None)
        v.pop("documentoIdentidadPropietarioReverso", None)
        v.pop("documentoIdentidadTenedor", None)
        v.pop("documentoIdentidadTenedorReverso", None)
        return v

    @staticmethod
    def _copias_urls(copias):
        """Devuelve {nombre_destino: url} de las llamadas a _copiar_blob_bucket."""
        return {args[1]: kwargs_or_url for args, kwargs_or_url in copias}

    def test_copia_frente_reverso_y_lectura(self):
        fake = FakeColeccionVehiculos([self._vehiculo()])
        copias = []
        def copiar_mock(ruta, nombre):
            copias.append((ruta, nombre))
            return f"Vehiculos/{nombre}"
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "_copiar_blob_bucket", side_effect=copiar_mock):
            resp = self.client.put(
                "/vehiculos/reutilizar-cedula",
                data={"placa": "TEST01", "figura": "propietario"},
            )
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        # Nomenclatura propia de la figura (campo con su ruta de blob).
        self.assertIn("documentoIdentidadPropietario", body["ruta"].rsplit("/", 1)[-1])
        self.assertTrue(body["ruta"].startswith("Vehiculos/"))
        self.assertIsNotNone(body["url_reverso"])
        self.assertEqual(body["lectura_ia"]["datos"]["numero"], "1020304050")
        # 2 copias: frente y reverso.
        self.assertEqual(len(copias), 2)
        # Mongo: campo propio de la figura + lectura replicada.
        doc = fake.documents[0]
        self.assertNotEqual(doc["documentoIdentidadPropietario"], doc["documentoIdentidadConductor"])
        self.assertNotEqual(doc["documentoIdentidadPropietario"], doc["documentoIdentidadConductor"])
        self.assertIn("documentoIdentidadPropietarioReverso", doc)
        self.assertEqual(
            doc["lecturasIA"]["documentoIdentidadPropietario"]["datos"]["numero"], "1020304050"
        )
        self.assertEqual(
            doc["lecturasIA"]["documentoIdentidadPropietario"]["reutilizada_de"],
            "documentoIdentidadConductor",
        )

    def test_sin_cedula_del_conductor_da_409(self):
        v = self._vehiculo()
        v["documentoIdentidadConductor"] = None
        fake = FakeColeccionVehiculos([v])
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "_copiar_blob_bucket") as mock_copiar:
            resp = self.client.put(
                "/vehiculos/reutilizar-cedula",
                data={"placa": "TEST01", "figura": "tenedor"},
            )
        self.assertEqual(resp.status_code, 409)
        mock_copiar.assert_not_called()

    def test_figura_invalida_da_400(self):
        fake = FakeColeccionVehiculos([self._vehiculo()])
        with patch.object(vehiculos, "coleccion_vehiculos", fake):
            resp = self.client.put(
                "/vehiculos/reutilizar-cedula",
                data={"placa": "TEST01", "figura": "conductor"},
            )
        self.assertEqual(resp.status_code, 400)

    def test_tenedor_sin_reverso_del_conductor(self):
        v = self._vehiculo()
        v.pop("documentoIdentidadConductorReverso")
        fake = FakeColeccionVehiculos([v])
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "_copiar_blob_bucket", side_effect=lambda u, n: f"Vehiculos/{n}"):
            resp = self.client.put(
                "/vehiculos/reutilizar-cedula",
                data={"placa": "TEST01", "figura": "tenedor"},
            )
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertIsNone(resp.json()["url_reverso"])
        self.assertNotIn("documentoIdentidadTenedorReverso", fake.documents[0])


class StoragePrivadoTests(unittest.TestCase):
    """2026-08-31: los documentos del vehículo van al bucket PRIVADO — Mongo
    guarda la RUTA del blob (nunca una URL pública) y las respuestas al front
    llevan la URL firmada temporal."""

    def setUp(self):
        self.client = cliente_de_prueba()

    def test_subir_documento_persiste_ruta_y_responde_firmada(self):
        fake = FakeColeccionVehiculos([vehiculo_completo(estadoIntegra="registro_incompleto")])
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "subir_a_google_storage", return_value="Vehiculos/TEST01/2026-08-31/soat.pdf"), \
             patch.object(vehiculos, "extraer_datos_con_llm", side_effect=lambda *a, **k: {}), \
             patch.object(vehiculos, "_url_firmada_documento", return_value="https://firmada/soat") as mock_firmar:
            resp = self.client.put(
                "/vehiculos/subir-documento",
                data={"placa": "TEST01", "tipo": "soat", "extraer": "false"},
                files={"archivo": ("soat.pdf", b"%PDF", "application/pdf")},
            )
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        self.assertEqual(body["ruta"], "Vehiculos/TEST01/2026-08-31/soat.pdf")
        self.assertEqual(body["url"], "https://firmada/soat")
        # Mongo queda con la RUTA plana, jamás con una URL (firmada o pública).
        self.assertEqual(fake.documents[0]["soat"], "Vehiculos/TEST01/2026-08-31/soat.pdf")
        mock_firmar.assert_called_once_with("Vehiculos/TEST01/2026-08-31/soat.pdf")

    def test_firmar_documentos_convierte_solo_rutas(self):
        with patch.object(vehiculos, "_url_firmada_documento", side_effect=lambda r: f"https://firmada/{r}"):
            doc = vehiculos._firmar_documentos({
                "soat": "Vehiculos/T1/2026-08-31/soat.pdf",
                "vehMarca": "VOLVO",
                "condCorreo": "A@B.C",
                "fotos": ["Vehiculos/T1/2026-08-31/foto_001.webp", None],
            })
        self.assertEqual(doc["soat"], "https://firmada/Vehiculos/T1/2026-08-31/soat.pdf")
        self.assertEqual(doc["vehMarca"], "VOLVO")
        self.assertEqual(doc["condCorreo"], "A@B.C")
        self.assertEqual(doc["fotos"][0], "https://firmada/Vehiculos/T1/2026-08-31/foto_001.webp")
        self.assertIsNone(doc["fotos"][1])

    def test_url_para_cliente_deja_pasar_valores_externos(self):
        # Valores que no son rutas de este módulo (URLs históricas, mocks) se
        # devuelven tal cual: el signing es solo para blobs del bucket privado.
        self.assertEqual(vehiculos._url_para_cliente("https://x/legacy.webp"), "https://x/legacy.webp")
        self.assertEqual(vehiculos._url_para_cliente(""), "")

    def test_bucket_configurado_es_el_privado(self):
        self.assertEqual(vehiculos.BUCKET_NAME, "integrapp-privado")


if __name__ == "__main__":
    unittest.main()
