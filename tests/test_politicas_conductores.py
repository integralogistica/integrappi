import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from bson import ObjectId
from fastapi import HTTPException

from rutas import conductores
from Funciones.claves import crear_hash


class FakeCursor:
    def __init__(self, items):
        self.items = items

    def limit(self, n):
        return self

    def sort(self, *args, **kwargs):
        return self

    def __iter__(self):
        return iter(self.items)


class FakeColeccion:
    """Falsa MongoCollection con registro de escrituras para aserciones."""

    def __init__(self, documentos=None):
        self.documents = list(documentos or [])
        self.inserts = []
        self.updates = []

    def find(self, query, *args, **kwargs):
        # Solo usamos el helper de token con {"verificacion_token_hash": {"$exists": True}}.
        if "verificacion_token_hash" in query:
            resultado = [d for d in self.documents if "verificacion_token_hash" in d]
            return FakeCursor(resultado)
        return FakeCursor(list(self.documents))

    def find_one(self, query, *args, **kwargs):
        if query == {"activo": True}:
            activos = [d for d in self.documents if d.get("activo")]
            return activos[0] if activos else None
        if "version" in query:
            for d in self.documents:
                if d.get("version") == query["version"]:
                    return d
            return None
        if "_id" in query:
            for d in self.documents:
                if d["_id"] == query["_id"]:
                    return d
            return None
        return self.documents[0] if self.documents else None

    def insert_one(self, doc):
        doc = dict(doc)
        doc.setdefault("_id", ObjectId())
        self.documents.append(doc)
        self.inserts.append(doc)
        class R:
            inserted_id = doc["_id"]
        return R()

    def update_one(self, filtro, cambio):
        self.updates.append((filtro, cambio))
        for d in self.documents:
            if d.get("_id") == filtro.get("_id"):
                if "$set" in cambio:
                    d.update(cambio["$set"])
                if "$unset" in cambio:
                    for k in cambio["$unset"]:
                        d.pop(k, None)
        class R:
            modified_count = 1
        return R()

    def update_many(self, filtro, cambio):
        for d in self.documents:
            if d.get("activo"):
                d.update(cambio.get("$set", {}))
        class R:
            modified_count = 1
        return R()

    def count_documents(self, query):
        if query == {}:
            return len(self.documents)
        return sum(1 for d in self.documents if all(d.get(k) == v for k, v in query.items()))


class RequestFake:
    """Request mínimo para ip/user-agent en la evidencia de aceptación."""
    def __init__(self):
        class Client:
            host = "190.0.0.1"
        self.client = Client()
        self.headers = {"user-agent": "Mozilla/5.0 (test)"}


def conductor_pendiente(token_plano="token-prueba"):
    return {
        "_id": ObjectId(),
        "nombre": "JUAN PEREZ",
        "correo": "JUAN@X.COM",
        "perfil": "CONDUCTOR",
        "activo": True,
        "correo_verificado": False,
        "verificacion_token_hash": crear_hash(token_plano),
        "verificacion_expira": datetime.now(timezone.utc) + timedelta(hours=24),
    }


def politica_v1():
    return {
        "_id": ObjectId(),
        "version": 1,
        "titulo": "Política de Tratamiento de Datos Personales — Habeas Data",
        "texto_html": "<p>texto v1</p>",
        "activo": True,
        "publicado_en": datetime.now(timezone.utc),
        "publicado_por": "SISTEMA",
    }


class PoliticasConductoresTests(unittest.TestCase):
    def setUp(self):
        self.conductor = conductor_pendiente()
        self.politica = politica_v1()
        self.col_conductores = FakeColeccion([dict(self.conductor)])
        self.col_politicas = FakeColeccion([dict(self.politica)])
        self.col_aceptaciones = FakeColeccion([])
        self.patchers = [
            patch.object(conductores, "coleccion_conductores", self.col_conductores),
            patch.object(conductores, "coleccion_politicas", self.col_politicas),
            patch.object(conductores, "coleccion_aceptaciones", self.col_aceptaciones),
        ]
        for p in self.patchers:
            p.start()
        self.addCleanup(lambda: [p.stop() for p in self.patchers])

    # ── GET /verificar-correo ──────────────────────────────────────────────
    def test_get_token_valido_no_marca_verificado(self):
        resp = asyncio_run(conductores.verificar_correo(token="token-prueba"))
        self.assertEqual(resp["estado"], "pendiente_aceptacion")
        # La v1 sin declaraciones se auto-upgradea a v2 (declaraciones).
        self.assertEqual(resp["politica"]["version"], 2)
        self.assertEqual(len(resp["politica"]["declaraciones"]), 7)
        self.assertEqual(resp["correo"], "JUAN@X.COM")
        # El GET no marca verificado al conductor.
        self.assertFalse(self.col_conductores.documents[0]["correo_verificado"])

    def test_get_token_ya_verificado(self):
        doc = self.col_conductores.documents[0]
        doc["correo_verificado"] = True
        resp = asyncio_run(conductores.verificar_correo(token="token-prueba"))
        self.assertEqual(resp["estado"], "ya_verificado")

    def test_get_token_expirado(self):
        self.col_conductores.documents[0]["verificacion_expira"] = (
            datetime.now(timezone.utc) - timedelta(hours=1)
        )
        with self.assertRaises(HTTPException) as ctx:
            asyncio_run(conductores.verificar_correo(token="token-prueba"))
        self.assertEqual(ctx.exception.status_code, 400)

    # ── POST /aceptar-politica ─────────────────────────────────────────────
    def _ids_declaraciones(self):
        return [d["id"] for d in conductores.DECLARACIONES_V2]

    def _post(self, version=None, acepta=True, token="token-prueba", declaraciones="todas"):
        if version is None:
            # La vigente tras el auto-upgrade (v2).
            version = conductores._politica_vigente().get("version")
        decl = self._ids_declaraciones() if declaraciones == "todas" else declaraciones
        entrada = conductores.AceptarPoliticaInput(
            token=token, version_politica=version, acepta=acepta,
            declaraciones_aceptadas=decl,
        )
        return asyncio_run(conductores.aceptar_politica(entrada, RequestFake()))

    def test_post_acepta_falso(self):
        with self.assertRaises(HTTPException) as ctx:
            self._post(acepta=False)
        self.assertEqual(ctx.exception.status_code, 400)
        # Cero escrituras: ni evidencia ni verificación.
        self.assertEqual(self.col_aceptaciones.inserts, [])
        self.assertEqual(self.col_conductores.updates, [])

    def test_post_version_incorrecta(self):
        with self.assertRaises(HTTPException) as ctx:
            self._post(version=99)
        self.assertEqual(ctx.exception.status_code, 400)
        detail = ctx.exception.detail
        self.assertIn("actualizada", detail["mensaje"])
        self.assertEqual(detail["politica"]["version"], 2)

    def test_post_feliz_path(self):
        resp = self._post()
        self.assertEqual(resp["estado"], "verificado")

        # Evidencia: UNA entrada POR declaración (7).
        self.assertEqual(len(self.col_aceptaciones.inserts), 7)
        ids_declaraciones = {d["id"] for d in conductores.DECLARACIONES_V2}
        for evidencia in self.col_aceptaciones.inserts:
            self.assertEqual(evidencia["version"], 2)
            self.assertEqual(evidencia["canal"], "verificacion_correo")
            self.assertEqual(evidencia["ip"], "190.0.0.1")
            self.assertIn("Mozilla", evidencia["user_agent"])
            self.assertIn(evidencia["declaracion_id"], ids_declaraciones)

        # Conductor: verificado + aceptación embebida con declaraciones.
        doc = self.col_conductores.documents[0]
        self.assertTrue(doc["correo_verificado"])
        self.assertEqual(doc["aceptacion_politica"]["version"], 2)
        self.assertEqual(len(doc["declaraciones_aceptadas"]), 7)
        self.assertIn("verificacion_token_hash", doc)

    def test_post_faltan_declaraciones_rechaza(self):
        # Solo marca 6 de 7 → rechazo con el detalle de la que falta.
        incompletas = self._ids_declaraciones()[:6]
        with self.assertRaises(HTTPException) as ctx:
            self._post(declaraciones=incompletas)
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertIn("Debes aceptar todas", str(ctx.exception.detail))
        # Cero escrituras.
        self.assertEqual(self.col_aceptaciones.inserts, [])

    def test_post_idempotente(self):
        self._post()
        resp = self._post()  # doble click / refresh
        self.assertEqual(resp["estado"], "ya_verificado")
        self.assertEqual(len(self.col_aceptaciones.inserts), 7)

    # ── Auto-siembra y auto-upgrade ─────────────────────────────────────────
    def test_politica_datos_auto_siembra_y_upgrade(self):
        self.col_politicas.documents = []
        resp = asyncio_run(conductores.obtener_politica_datos())
        # Siembra v1 e inmediatamente la upgradea a v2 (declaraciones).
        self.assertEqual(resp["version"], 2)
        self.assertEqual(len(resp["declaraciones"]), 7)
        activos = [d for d in self.col_politicas.documents if d.get("activo")]
        self.assertEqual(len(activos), 1)


def asyncio_run(coro):
    import asyncio
    return asyncio.run(coro)


if __name__ == "__main__":
    unittest.main()
