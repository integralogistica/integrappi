# Vacío a propósito: convierte `tests` en paquete regular para que
# `from tests.test_alta_seguridad import ...` resuelva al DIRECTORIO LOCAL y
# no al paquete `tests` de site-packages que lo tapaba (namespace package).
