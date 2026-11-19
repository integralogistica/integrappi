# -*- coding: utf-8 -*-
"""Tests de la regla «uso del vehículo < 50% → autorización CONTROL» (2026-11-19).

Aplica a Pedidos y SolicitudVehiculos: aunque el flete cumpla (PREAUTORIZADO /
PREAPROBADO o solo COORDINADOR), un vehículo con % de uso menor al 50% sube al
tier más restrictivo. Sin tipo evaluable la regla NO aplica (no se puede medir).
"""
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

from rutas import pedidos, siscore_consultas

# Topes reales (indicadores_costo_operacion): TURBO=4500, SENCILLO=9000.
TURBO = 4500
SENSILLO = 9000


class ReglaUsoPedidosTests(unittest.TestCase):
    """pedidos.aplicar_regla_uso (carga, ajustar, fusión y división)."""

    def test_preautorizado_con_uso_bajo_sube_a_control(self):
        # 1.800 kg en un TURBO (tope 4.500) = 40% < 50%.
        self.assertEqual(
            pedidos.aplicar_regla_uso("PREAUTORIZADO", 0.4 * TURBO, "TURBO", None),
            "REQUIERE AUTORIZACION CONTROL",
        )

    def test_coordinador_con_uso_bajo_sube_a_control(self):
        self.assertEqual(
            pedidos.aplicar_regla_uso("REQUIERE AUTORIZACION COORDINADOR", 2000, "SENCILLO", None),
            "REQUIERE AUTORIZACION CONTROL",
        )

    def test_uso_ok_no_cambia_el_estado(self):
        # 4.050 kg en un TURBO = 90% ≥ 50%: el flete manda.
        for estado in ("PREAUTORIZADO", "REQUIERE AUTORIZACION COORDINADOR"):
            self.assertEqual(pedidos.aplicar_regla_uso(estado, 0.9 * TURBO, "TURBO", None), estado)

    def test_sin_tipo_evaluable_no_aplica(self):
        # Tipo desconocido → uso None → la regla no puede medir, no aplica.
        for estado in ("PREAUTORIZADO", "REQUIERE AUTORIZACION COORDINADOR"):
            self.assertEqual(pedidos.aplicar_regla_uso(estado, 100, "MISTERIOSO", None), estado)

    def test_control_y_borde_50(self):
        # Ya CONTROL (o superior) no cambia; exactamente 50% NO aplica (es "<").
        self.assertEqual(pedidos.aplicar_regla_uso("REQUIERE AUTORIZACION CONTROL", 100, "TURBO", None),
                         "REQUIERE AUTORIZACION CONTROL")
        self.assertEqual(pedidos.aplicar_regla_uso("PREAUTORIZADO", TURBO * 0.5, "TURBO", None),
                         "PREAUTORIZADO")

    def test_respaldo_del_tipo_sugerido(self):
        # Sin SICETAC, el tipo sugerido define el tope (mismo criterio del % Uso).
        self.assertEqual(
            pedidos.aplicar_regla_uso("PREAUTORIZADO", 0.2 * TURBO, None, "TURBO"),
            "REQUIERE AUTORIZACION CONTROL",
        )


class ReglaUsoSolicitudVehiculosTests(unittest.TestCase):
    """siscore_consultas._aplicar_regla_uso_control (guardar, re-consulta,
    edición y enviar)."""

    def _doc(self, peso, tipo="TURBO"):
        return {"planilla": "846001", "consecutivo": "FUNZA-20261119-1",
                "peso_real": peso, "tipo_veh_sicetac": tipo}

    def test_preaprobado_con_uso_bajo_sube_a_control(self):
        res = siscore_consultas._aplicar_regla_uso_control("PREAPROBADO", self._doc(0.3 * TURBO))
        self.assertEqual(res, "REQUIERE_APROBACION_CONTROL")

    def test_coordinador_con_uso_bajo_sube_a_control(self):
        res = siscore_consultas._aplicar_regla_uso_control(
            "REQUIERE_APROBACION_COORDINADOR", self._doc(1000))
        self.assertEqual(res, "REQUIERE_APROBACION_CONTROL")

    def test_creado_aprobado_y_control_pasan_sin_cambio(self):
        for estado in ("CREADO", "APROBADO", "REQUIERE_APROBACION_CONTROL"):
            res = siscore_consultas._aplicar_regla_uso_control(estado, self._doc(10))
            self.assertEqual(res, estado)

    def test_uso_ok_y_sin_tipo_no_aplican(self):
        self.assertEqual(
            siscore_consultas._aplicar_regla_uso_control("PREAPROBADO", self._doc(TURBO)), "PREAPROBADO")
        # Sin tipo evaluable (tipo_vehiculo tampoco tiene tope): no aplica.
        doc = {"planilla": "1", "peso_real": 100, "tipo_veh_sicetac": "MISTERIOSO"}
        self.assertEqual(
            siscore_consultas._aplicar_regla_uso_control("PREAPROBADO", doc), "PREAPROBADO")


if __name__ == "__main__":
    unittest.main()
