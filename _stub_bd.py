"""Stub de `bd.bd_cliente` para correr la suite de tests con Atlas
inalcanzable desde la red local (patrón ya usado en las sesiones de firma y
reutilizar-cédula). USO:

    python _stub_bd.py tests.test_politicas_conductores tests.test_padrino ...

Reemplaza el cliente Mongo por objetos que no hacen NADA de red; los tests
parchean las colecciones con sus FakeColeccion de siempre.
"""
import sys
import types
import unittest


class _ResultadoFake(list):
    """Resultado de cursor vacío pero ENCADENABLE (.sort/.limit/.skip)."""

    def sort(self, *args, **kwargs):
        return self

    def limit(self, *args, **kwargs):
        return self

    def skip(self, *args, **kwargs):
        return self


class _FakeColeccion:
    """Cualquier método/índice devuelve algo inofensivo (los tests parchean).
    Los métodos no definidos devuelven un cursor vacío encadenable para que
    el bd_cliente directo (indicadores/OT/citas) no reviente con None."""

    def __getattr__(self, nombre):
        def _no_op(*args, **kwargs):
            return _ResultadoFake()
        return _no_op

    def __getitem__(self, clave):
        return _FakeColeccion()


class _FakeDb:
    def __getitem__(self, clave):
        return _FakeColeccion()

    def __getattr__(self, nombre):
        return _FakeColeccion()


class _FakeCliente:
    def __getitem__(self, nombre):
        return _FakeDb()

    def __getattr__(self, nombre):
        return _FakeColeccion()

    def close(self):
        pass


mod = types.ModuleType("bd.bd_cliente")
mod.bd_cliente = _FakeCliente()
sys.modules["bd.bd_cliente"] = mod

if __name__ == "__main__":
    loader = unittest.defaultTestLoader
    suite = unittest.TestSuite()
    for nombre in sys.argv[1:]:
        suite.addTests(loader.loadTestsFromName(nombre))
    runner = unittest.TextTestRunner(verbosity=1)
    resultado = runner.run(suite)
    sys.exit(0 if resultado.wasSuccessful() else 1)
