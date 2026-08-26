"""Tests de obligatoriedad de documentos por figura (gemelos) y replicar_en.

Cubre:
- _figuras_iguales: flags persistidos e inferencia por dígitos.
- _gemelos_documento / _documentos_faltantes: deduplicación por figura.
- actualizar-estado: valida SOLO al pasar a completado_revision.
- subir-documento con replicar_en: setea gemelos y copia lecturasIA.
- eliminar-documento: limpia gemelos con la misma URL.
"""
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
    "documentoIdentidadPropietario": "https://x/7", "documentoIdentidadTenedor": "https://x/8",
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


class NombreDocBucketTests(unittest.TestCase):
    """Nomenclatura de archivos en el bucket: {PLACA}/{AAAA-MM-DD}/{tipo}[_{cedula}][sufijo].{ext}"""

    def test_con_cedula(self):
        v = {"condCedulaCiudadania": "1.020.304.050"}
        nombre = vehiculos._nombre_doc_bucket("mx48e", "soat", "pdf", v)
        self.assertRegex(nombre, r"^MX48E/\d{4}-\d{2}-\d{2}/soat_1020304050\.pdf$")

    def test_sin_cedula_se_omite_sufijo(self):
        nombre = vehiculos._nombre_doc_bucket("MX48E", "firma", "webp", {})
        self.assertRegex(nombre, r"^MX48E/\d{4}-\d{2}-\d{2}/firma\.webp$")

    def test_sufijo_fotos(self):
        nombre = vehiculos._nombre_doc_bucket("ABC123", "foto", "webp", None, sufijo="_002")
        self.assertRegex(nombre, r"^ABC123/\d{4}-\d{2}-\d{2}/foto(_\d+)?_002\.webp$")


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
        fake = FakeColeccionVehiculos([vehiculo_completo(soat=None)])
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
        mock_del.assert_called_once()
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


if __name__ == "__main__":
    unittest.main()
