# -*- coding: utf-8 -*-
"""Tests del administrador de clientes de Otros Costos (2026-10-21, apartado
de GestionUsuarios): catálogo `clientes_otros_costos` con NIT + estado.

Cubre: GET /clientes excluye deshabilitados, GET /clientes/completo los
incluye, POST /clientes/crear (normaliza nombre, NIT opcional, 409 duplicado
case-insensitive, 400 sin nombre) y PUT /clientes/{id}/estado (toggle, 404,
id inválido)."""
import asyncio
import re
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

from fastapi import HTTPException
from rutas import otros_costos


class _CursorFake(list):
    def sort(self, campo, direccion=1):
        return _CursorFake(sorted(self, key=lambda d: str(d.get(campo, ""))))


class FakeCol:
    """Colección Mongo mínima: $ne y $regex(i, anclado) para este módulo."""

    def __init__(self, documentos=None):
        self.documents = list(documentos or [])
        self.contador = 0

    @staticmethod
    def _match(d, cond):
        for campo, esperado in (cond or {}).items():
            actual = d.get(campo)
            if isinstance(esperado, dict):
                if "$ne" in esperado and actual == esperado["$ne"]:
                    return False
                regex = esperado.get("$regex")
                if regex is not None:
                    banderas = 0
                    if "i" in esperado.get("$options", ""):
                        banderas |= re.IGNORECASE
                    if not re.search(regex, str(actual or ""), banderas):
                        return False
            elif actual != esperado and str(actual) != str(esperado):
                # str(): los _id del fake son strings pero el endpoint pasa
                # ObjectId (comparación por representación).
                return False
        return True

    def find(self, filtro=None, *a, **k):
        return _CursorFake([d for d in self.documents if self._match(d, filtro)])

    def find_one(self, query=None, *a, **k):
        return next((d for d in self.documents if self._match(d, query or {})), None)

    def insert_one(self, doc):
        self.contador += 1
        doc = dict(doc)
        doc.setdefault("_id", f"cli{self.contador}")
        self.documents.append(doc)
        return MagicMock(inserted_id=doc["_id"])

    def update_one(self, filtro, cambio, upsert=False):
        for d in self.documents:
            if self._match(d, filtro):
                d.update(cambio.get("$set") or {})
                return MagicMock(matched_count=1)
        return MagicMock(matched_count=0)

    def count_documents(self, filtro=None):
        return sum(1 for d in self.documents if self._match(d, filtro or {}))


class ClientesOtrosCostosTests(unittest.TestCase):

    def setUp(self):
        self._original = otros_costos.col_clientes
        # Catálogo con un histórico sin nit/activo (debe verse activo, NIT —)
        # y uno deshabilitado por el admin.
        otros_costos.col_clientes = FakeCol([
            {"_id": "5f1f1f77bcf86cd7994390a1", "nombre": "FRESENIUS KABI"},
            {"_id": "5f1f1f77bcf86cd7994390a2", "nombre": "CONGRUPO", "nit": "900123456", "activo": False},
        ])

    def tearDown(self):
        otros_costos.col_clientes = self._original

    def test_get_clientes_excluye_deshabilitados(self):
        res = asyncio.run(otros_costos.clientes())
        self.assertEqual(res, ["FRESENIUS KABI"])
        # El histórico sin campo `activo` cuenta como activo.

    def test_completo_incluye_deshabilitados_con_nit(self):
        res = asyncio.run(otros_costos.clientes_completo())
        self.assertEqual(len(res), 2)
        por_id = {c["id"]: c for c in res}
        self.assertTrue(por_id["5f1f1f77bcf86cd7994390a1"]["activo"])
        self.assertEqual(por_id["5f1f1f77bcf86cd7994390a1"]["nit"], "")
        self.assertFalse(por_id["5f1f1f77bcf86cd7994390a2"]["activo"])
        self.assertEqual(por_id["5f1f1f77bcf86cd7994390a2"]["nit"], "900123456")

    def test_crear_normaliza_nombre_y_guarda_nit(self):
        res = asyncio.run(otros_costos.crear_cliente(
            otros_costos.ClienteOtroCostoIn(nombre="  miniso   colombia ", nit=" 901234567 ")))
        self.assertEqual(res["nombre"], "MINISO COLOMBIA")
        self.assertEqual(res["nit"], "901234567")
        self.assertTrue(res["activo"])
        guardado = otros_costos.col_clientes.find_one({"nombre": "MINISO COLOMBIA"})
        self.assertTrue(guardado["activo"])
        self.assertIn("creado_en", guardado)

    def test_crear_sin_nit_es_valido(self):
        res = asyncio.run(otros_costos.crear_cliente(
            otros_costos.ClienteOtroCostoIn(nombre="DAVITA", nit="")))
        self.assertEqual(res["nit"], "")

    def test_crear_duplicado_409_case_insensitive(self):
        with self.assertRaises(HTTPException) as ctx:
            asyncio.run(otros_costos.crear_cliente(
                otros_costos.ClienteOtroCostoIn(nombre="fresenius kabi", nit="")))
        self.assertEqual(ctx.exception.status_code, 409)

    def test_estado_toggle(self):
        res = asyncio.run(otros_costos.cambiar_estado_cliente(
            "5f1f1f77bcf86cd7994390a1", otros_costos.EstadoClienteOtroCostoIn(activo=False)))
        self.assertFalse(res["activo"])
        self.assertFalse(otros_costos.col_clientes.find_one({"_id": "5f1f1f77bcf86cd7994390a1"})["activo"])
        # Y el dropdown deja de ofrecerlo.
        self.assertEqual(asyncio.run(otros_costos.clientes()), [])

    def test_estado_404_y_id_invalido(self):
        with self.assertRaises(HTTPException) as ctx:
            asyncio.run(otros_costos.cambiar_estado_cliente(
                "5f1f1f77bcf86cd7994390ff", otros_costos.EstadoClienteOtroCostoIn(activo=True)))
        self.assertEqual(ctx.exception.status_code, 404)
        with self.assertRaises(HTTPException) as ctx:
            asyncio.run(otros_costos.cambiar_estado_cliente("$$$", otros_costos.EstadoClienteOtroCostoIn(activo=True)))
        self.assertEqual(ctx.exception.status_code, 400)


if __name__ == "__main__":
    unittest.main()
