"""Corre los módulos de unittest indicados con el stub de bd.bd_cliente
inyectado (Atlas inalcanzable intermitentemente desde la red local).
Uso: python _correr_tests_stub.py tests.test_obligatoriedad_documentos ...
"""
import sys
import types
import unittest
from unittest.mock import MagicMock

_stub = types.ModuleType("bd.bd_cliente")


class _BDFake(dict):
    def __getitem__(self, clave):
        return self.setdefault(clave, MagicMock())


_stub.bd_cliente = _BDFake()
sys.modules["bd.bd_cliente"] = _stub

if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for nombre in sys.argv[1:]:
        suite.addTests(loader.loadTestsFromName(nombre))
    runner = unittest.TextTestRunner(verbosity=1)
    resultado = runner.run(suite)
    sys.exit(0 if resultado.wasSuccessful() else 1)
