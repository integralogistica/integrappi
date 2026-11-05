# -*- coding: utf-8 -*-
"""Tests del catálogo de aforadores de Otros Costos (2026-11-04).

Cubre: `_registrar_aforadores` (upsert por nombre, `usos` acumula, ignora
vacíos) y `GET /otros-costos/aforadores` (orden: más usados primero)."""
import asyncio
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

from rutas import otros_costos
from rutas.otros_costos import CostoConcepto, _registrar_aforadores


class ColAforadoresFake:
    """Colección mínima para el upsert de aforadores: guarda por `nombre`."""

    def __init__(self):
        self.docs = {}

    def update_one(self, filtro, update, upsert=False):
        nombre = filtro["nombre"]
        doc = self.docs.get(nombre)
        if doc is None:
            doc = {"nombre": nombre, "usos": 0}
            self.docs[nombre] = doc
        if "$setOnInsert" in update:
            doc.update({k: v for k, v in update["$setOnInsert"].items() if k not in doc})
        if "$set" in update:
            doc.update(update["$set"])
        if "$inc" in update:
            for k, v in update["$inc"].items():
                doc[k] = doc.get(k, 0) + v

    def find(self, filtro=None, projection=None):
        return _CursorFake(list(self.docs.values()))


class _CursorFake(list):
    def sort(self, clave=None, direccion=1):
        # El endpoint ordena por [("usos", -1), ("nombre", 1)]; el fake fija
        # esa misma clave multi-campo.
        return _CursorFake(sorted(self, key=lambda d: (-d.get("usos", 0), d.get("nombre", ""))))


class AforadoresOtrosCostosTests(unittest.TestCase):

    def setUp(self):
        self._col = otros_costos.col_aforadores
        self.fake = ColAforadoresFake()
        otros_costos.col_aforadores = self.fake

    def tearDown(self):
        otros_costos.col_aforadores = self._col

    def _costos(self, *aforadores):
        return [
            CostoConcepto(tipo_costo="AFORO", descripcion="Aforo", valor=50000, aforador=a)
            for a in aforadores
        ]

    def test_registra_y_acumula_usos(self):
        _registrar_aforadores(self._costos("JUAN PEREZ", "", "MARIA GOMEZ"))
        _registrar_aforadores(self._costos("JUAN PEREZ"))
        self.assertEqual(
            {d["nombre"]: d["usos"] for d in self.fake.docs.values()},
            {"JUAN PEREZ": 2, "MARIA GOMEZ": 1},
        )

    def test_aforadores_orden_por_usos(self):
        _registrar_aforadores(self._costos("ANA", "BETO", "BETO", "CARLOS", "CARLOS", "CARLOS"))
        res = asyncio.run(otros_costos.aforadores())
        self.assertEqual(res, ["CARLOS", "BETO", "ANA"])


if __name__ == "__main__":
    unittest.main()
