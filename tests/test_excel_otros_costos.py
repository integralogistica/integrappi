# -*- coding: utf-8 -*-
"""Tests del Excel de Otros Costos (POST /otros_costos/exportar-excel).

Cubre: columna «Descripción» (campo `descripcion` de cada concepto) al lado
del «Tipo de Costo», una fila por concepto con su descripción, y fila vacía
para solicitudes sin conceptos (2026-10-27)."""
import asyncio
import io
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

from openpyxl import load_workbook

from rutas import otros_costos


class _CursorFake(list):
    def sort(self, campo, direccion=1):
        return _CursorFake(sorted(self, key=lambda d: str(d.get(campo, "")), reverse=direccion < 0))

    def limit(self, n):
        return _CursorFake(self[:n])


class FakeCol:
    """Colección Mongo mínima: find/find_one devuelven todo (el export no
    depende de filtros para estos tests)."""

    def __init__(self, documentos=None):
        self.documents = list(documentos or [])

    def find(self, filtro=None, *a, **k):
        return _CursorFake(self.documents)

    def find_one(self, query=None, *a, **k):
        usuario = (query or {}).get("usuario")
        return next((d for d in self.documents if d.get("usuario") == usuario), None)


class ExcelOtrosCostosTests(unittest.TestCase):

    def setUp(self):
        self._cols = (otros_costos.col_usuarios, otros_costos.col_historico, otros_costos.col_activos)
        otros_costos.col_usuarios = FakeCol([
            {"_id": "u1", "usuario": "ADMIN1", "perfil": "ADMIN", "activo": True},
        ])
        otros_costos.col_historico = FakeCol([
            {
                "consecutivo": "MEDELLIN-OC-0001",
                "datos_servicio": {"cliente": "FRESENIUS KABI", "placa": "ABC123"},
                "costos": [
                    {"tipo_costo": "AFORO", "descripcion": "Aforo en patios", "valor": 50000},
                    {"tipo_costo": "DESCARGUE", "descripcion": "Descargue nocturno", "valor": 80000},
                ],
                "created_at": "2026-10-20T10:00:00",
                "estado": "PAGADO",
            },
            {
                # Sin conceptos: una fila con tipo/descripción vacíos (como antes).
                "consecutivo": "BUCARAMANGA-OC-0002",
                "datos_servicio": {"cliente": "CONGRUPO", "placa": "XYZ789"},
                "costos": [],
                "created_at": "2026-10-21T10:00:00",
                "estado": "APROBADO",
            },
        ])
        otros_costos.col_activos = FakeCol([])

    def tearDown(self):
        otros_costos.col_usuarios, otros_costos.col_historico, otros_costos.col_activos = self._cols

    def _hoja(self):
        res = asyncio.run(otros_costos.exportar_excel(
            otros_costos.ExportarExcelRequest(usuario="admin1", origen="historico")))
        ws = load_workbook(io.BytesIO(res.body)).active
        return ws, [c.value for c in ws[1]]

    def test_columna_descripcion_al_lado_del_tipo(self):
        _, headers = self._hoja()
        self.assertIn("Descripción", headers)
        self.assertEqual(headers.index("Descripción"), headers.index("Tipo de Costo") + 1)

    def test_fila_por_concepto_con_su_descripcion(self):
        ws, headers = self._hoja()
        col_desc = headers.index("Descripción") + 1
        col_tipo = headers.index("Tipo de Costo") + 1
        # Orden por created_at desc: BUCARAMANGA (sin conceptos) primero.
        # openpyxl guarda "" como None al escribir la celda.
        self.assertEqual(ws.cell(row=2, column=col_tipo).value or "", "")
        self.assertEqual(ws.cell(row=2, column=col_desc).value or "", "")
        self.assertEqual(ws.cell(row=3, column=col_tipo).value, "AFORO")
        self.assertEqual(ws.cell(row=3, column=col_desc).value, "Aforo en patios")
        self.assertEqual(ws.cell(row=4, column=col_tipo).value, "DESCARGUE")
        self.assertEqual(ws.cell(row=4, column=col_desc).value, "Descargue nocturno")


if __name__ == "__main__":
    unittest.main()
