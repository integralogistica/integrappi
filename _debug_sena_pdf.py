import io
import sys
import unittest

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, ".")

loader = unittest.TestLoader()
suite = loader.loadTestsFromName("tests.test_pdf_estudio_seguridad.TestSeccionSena")
runner = unittest.TextTestRunner(verbosity=0, stream=io.StringIO())
resultado = runner.run(suite)
for _, tb in resultado.failures + resultado.errors:
    print(tb.splitlines()[-1][:400])
