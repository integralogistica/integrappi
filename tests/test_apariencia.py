"""Tests del módulo de apariencia (2026-11-25, 10.ª tanda): GET público con
cache y default «ninguno», PUT solo ADMIN con validación de efecto y auditoría."""
import sys
import types
import unittest
from unittest.mock import MagicMock

if "bd.bd_cliente" not in sys.modules:
    _stub = types.ModuleType("bd.bd_cliente")

    class _BDFake(dict):
        def __getitem__(self, clave):
            return self.setdefault(clave, MagicMock())

    _stub.bd_cliente = _BDFake()
    sys.modules["bd.bd_cliente"] = _stub

from fastapi import FastAPI
from fastapi.testclient import TestClient

from rutas import otros_costos, apariencia
from tests.test_alta_seguridad import FakeColeccion


def cliente_de_prueba(router) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    return TestClient(app, raise_server_exceptions=False)


class ColAparienciaFake(FakeColeccion):
    def update_one(self, filtro, update, upsert=False):
        # upsert del PUT: guarda en documents para que el GET lo vea.
        doc = dict(update["$set"])
        self.documents = [dict(d, _id="global") if d.get("_id") == "global" else d for d in self.documents]
        existe = any(d.get("_id") == "global" for d in self.documents)
        if existe:
            self.documents = [dict(d, **doc) if d.get("_id") == "global" else d for d in self.documents]
        else:
            self.documents.append({"_id": "global", **doc})
        return MagicMock(matched_count=1, modified_count=1)

    def find_one(self, filtro=None, *a, **k):
        for d in self.documents:
            if d.get("_id") == "global":
                return d
        return None


class TestApariencia(unittest.TestCase):
    def setUp(self):
        self.col = ColAparienciaFake()
        self._respaldo = apariencia.col_apariencia
        apariencia.col_apariencia = self.col
        # Reset del cache de módulo entre tests.
        apariencia._cache["datos"] = None
        apariencia._cache["ts"] = 0.0
        otros_costos.col_usuarios = FakeColeccion(
            [
                {"_id": "u1", "usuario": "ADMIN.X", "perfil": "ADMIN", "nombre": "Admin", "activo": True},
                {"_id": "u2", "usuario": "OPER.Y", "perfil": "OPERATIVO", "nombre": "Oper", "activo": True},
            ]
        )
        self.client = cliente_de_prueba(apariencia.router)

    def tearDown(self):
        apariencia.col_apariencia = self._respaldo
        apariencia._cache["datos"] = None
        apariencia._cache["ts"] = 0.0

    def test_get_sin_config_devuelve_ninguno(self):
        r = self.client.get("/apariencia")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["efecto"], "ninguno")
        self.assertEqual(r.json()["etiqueta"], "Ninguno")

    def test_put_solo_admin(self):
        r = self.client.put("/apariencia", json={"efecto": "navidad", "usuario": "OPER.Y"})
        self.assertEqual(r.status_code, 403)
        r = self.client.put("/apariencia", json={"efecto": "navidad"})  # sin usuario
        self.assertEqual(r.status_code, 403)

    def test_put_admin_actualiza_y_get_lo_ve(self):
        r = self.client.put("/apariencia", json={"efecto": "navidad", "usuario": "ADMIN.X"})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["etiqueta"], "Navidad")
        # El cache se invalida: el GET inmediato ve el efecto nuevo.
        r = self.client.get("/apariencia")
        self.assertEqual(r.json()["efecto"], "navidad")
        self.assertEqual(r.json()["actualizado_por"], "ADMIN.X")
        self.assertIsNotNone(r.json()["fecha"])

    def test_put_efecto_invalido_422(self):
        r = self.client.put("/apariencia", json={"efecto": "verano", "usuario": "ADMIN.X"})
        self.assertEqual(r.status_code, 422)
        self.assertIn("verano", r.json()["detail"])

    def test_catalogo_completo(self):
        for efecto in ("ninguno", "halloween", "patrias", "celebracion", "san_valentin", "semana_santa"):
            r = self.client.put("/apariencia", json={"efecto": efecto, "usuario": "ADMIN.X"})
            self.assertEqual(r.status_code, 200, efecto)
            r = self.client.get("/apariencia")
            self.assertEqual(r.json()["efecto"], efecto)


if __name__ == "__main__":
    unittest.main()
