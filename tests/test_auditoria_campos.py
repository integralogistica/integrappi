"""Bitácora legible de `datos_actualizados` (2026-10-14, caso LCM768).

La auditoría decía solo "N campo(s)" y no se sabía QUÉ había cambiado. Ahora
el detalle lleva las etiquetas legibles de los campos (espejo de la pestaña
Datos de /revision), con resumen cuando son muchos.
"""
import sys
import types
import unittest

# Stub de BD (patrón del repo): correr standalone sin Atlas.
if "bd.bd_cliente" not in sys.modules:
    _stub = types.ModuleType("bd.bd_cliente")

    class _BDFake(dict):
        def __getitem__(self, clave):
            from unittest.mock import MagicMock
            return self.setdefault(clave, MagicMock())

    _stub.bd_cliente = _BDFake()
    sys.modules["bd.bd_cliente"] = _stub

from rutas import vehiculos


class EtiquetaCampoTests(unittest.TestCase):

    def test_campos_curados_del_mapa(self):
        self.assertEqual(vehiculos._etiqueta_campo_auditoria("tenedDocumento"), "Doc. Tenedor")
        self.assertEqual(vehiculos._etiqueta_campo_auditoria("propDocumento"), "Doc. Propietario")
        self.assertEqual(vehiculos._etiqueta_campo_auditoria("RemolDuenoDocumento"), "Doc. Dueño Remolque")
        self.assertEqual(vehiculos._etiqueta_campo_auditoria("condCedulaCiudadania"), "Cédula Conductor")

    def test_campo_fuera_del_mapa_cae_al_prefijo_de_figura(self):
        # Campo nuevo del form (aún sin etiqueta curada): figura legible.
        etiqueta = vehiculos._etiqueta_campo_auditoria("condCampoFuturo")
        self.assertIn("Conductor", etiqueta)

    def test_campo_desconocido_sin_prefijo_queda_cruo(self):
        self.assertEqual(vehiculos._etiqueta_campo_auditoria("campoLibre"), "campoLibre")


class DetalleCamposTests(unittest.TestCase):

    def test_detalle_lleva_conteo_y_etiquetas(self):
        detalle = vehiculos._detalle_campos_auditoria(
            [{"campo": "tenedDocumento"}, {"campo": "tenedNombre"},
             {"campo": "tenedTipoDocumento"}])
        self.assertIn("3 campo(s)", detalle)
        self.assertIn("Doc. Tenedor", detalle)
        self.assertIn("Nombre Tenedor", detalle)
        self.assertIn("Tipo Doc. Tenedor", detalle)

    def test_detalle_resumido_cuando_son_muchos(self):
        cambios = [{"campo": f"condCampo{i}"} for i in range(10)]
        detalle = vehiculos._detalle_campos_auditoria(cambios)
        self.assertIn("10 campo(s)", detalle)
        self.assertIn("y 4 más", detalle)
        self.assertLessEqual(len(detalle), 300)  # tope de la entrada de auditoría


if __name__ == "__main__":
    unittest.main()
