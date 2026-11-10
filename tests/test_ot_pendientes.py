"""Tests del segmento «Pendientes» del OT (2026-11-10): las guías EN CURSO
(sin fecha de entrega) ya NO se juzgan contra HOY — van a los segmentos
Pendientes / Pendientes vencidas y quedan FUERA del % OT (ot viaja None).
Las en curso CON novedad (reporte Siscore o justificada manual) siguen
auto-cumpliendo («Con novedad»)."""
import sys
import types
import unittest
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

if "bd.bd_cliente" not in sys.modules:
    _stub = types.ModuleType("bd.bd_cliente")

    class _BDFake(dict):
        def __getitem__(self, clave):
            return self.setdefault(clave, MagicMock())

    _stub.bd_cliente = _BDFake()
    sys.modules["bd.bd_cliente"] = _stub

from rutas import indicadores_cliente


def _fila_ot(**extra) -> dict:
    """Fila CARGA de Bogotá (tiempos BOGOTA DC = 1 día hábil)."""
    fila = {
        "guia": "999123",
        "nombre_cliente": "FRESENIUS KABI",
        "destinatario": "CLINICA X",
        "ciudad_destino": "BOGOTA DC - BOGOTA DC",
        "ciudad_origen": "",
        "estado": "ENTREGADA",
        "fecha_emision": "2026-11-01",
        "fecha_entrega": "2026-11-10",
    }
    fila.update(extra)
    return fila


class CalcularOTPendientesTests(unittest.TestCase):
    """Parchea los mapas de tablas (nada de Mongo) y llama _calcular_ot_filas."""

    def _calcular(self, fila: dict, novedades: dict | None = None, anuladas: dict | None = None) -> dict:
        with patch.object(indicadores_cliente, "_mapa_citas_agendadas", lambda: {}), \
             patch.object(indicadores_cliente, "_mapa_citas_entrega", lambda: {}), \
             patch.object(indicadores_cliente, "_mapa_novedades_siscore", lambda: novedades or {}), \
             patch.object(indicadores_cliente, "_mapa_guias_anuladas", lambda: anuladas or {}), \
             patch.object(indicadores_cliente, "_mapa_tiempos_carga",
                          lambda: {"par": {}, "mun": {"BOGOTA DC": 1}}), \
             patch.object(indicadores_cliente, "_mapa_tiempos_st", lambda: {"par": {}, "mun": {}}), \
             patch.object(indicadores_cliente, "_ciudad_y_dpto",
                          lambda f, usar_origen: ("BOGOTA DC", "CUNDINAMARCA")):
            indicadores_cliente._calcular_ot_filas([fila])
        return fila

    def test_en_curso_sin_vencer_es_pendiente(self):
        """Emitida HOY + 1 día de promesa → aún en plazo: pendiente, NO vencida,
        sin veredicto (fuera del % OT)."""
        fila = self._calcular(_fila_ot(
            fecha_emision=date.today().isoformat(), fecha_entrega=None,
        ))
        self.assertTrue(fila["ot_pendiente"])
        self.assertFalse(fila["ot_pendiente_vencida"])
        self.assertIsNone(fila["ot"])  # antes: 1 (juzgada contra HOY)

    def test_en_curso_vencida_es_pendiente_vencida(self):
        """Emitida hace meses, sin entrega → ya pasó la promesa: pendiente
        VENCIDA, pero sigue sin veredicto (fuera del % OT)."""
        fila = self._calcular(_fila_ot(
            fecha_emision="2026-01-05", fecha_entrega=None,
        ))
        self.assertTrue(fila["ot_pendiente"])
        self.assertTrue(fila["ot_pendiente_vencida"])
        self.assertIsNone(fila["ot"])  # antes: 0 (juzgada contra HOY)

    def test_entregada_tarde_sigue_sin_cambios(self):
        """Sin regresión: entregada después de la promesa → ot=0, no pendiente."""
        fila = self._calcular(_fila_ot())  # emitida 01, entregada 10, promesa ~02
        self.assertFalse(fila["ot_pendiente"])
        self.assertEqual(fila["ot"], 0)

    def test_en_curso_con_novedad_sigue_cumpliendo(self):
        """En curso CON reporte de Siscore → auto-cumple («Con novedad»): la
        precedencia la resuelven los consumidores (novedad > pendiente)."""
        fila = self._calcular(
            _fila_ot(fecha_emision="2026-01-05", fecha_entrega=None),
            novedades={"999123": "REHUSADO"},
        )
        self.assertTrue(fila["cumple_novedad"])
        self.assertTrue(fila["ot_pendiente"])  # es en curso…
        # …pero la clasificación de _serie_ot la manda a conNovedad (ver abajo)
        bucket = indicadores_cliente._serie_ot([fila], 7)[0]
        self.assertEqual(bucket["conNovedad"], 1)
        self.assertEqual(bucket["pendientes"], 0)
        self.assertEqual(bucket["pendientesVencidas"], 0)

    def test_anulada_auto_cumple_como_novedad(self):
        """(2026-11-10, caso 801232571) Guía ANULADA sin entrega: NO va a
        Pendientes — auto-cumple como «Con novedad» (cuenta como cumplida en
        el % OT), aunque no tenga novedad de Siscore ni justificación."""
        fila = self._calcular(_fila_ot(
            estado="ANULADA", fecha_emision="2026-01-05", fecha_entrega=None,
        ))
        self.assertTrue(fila["cumple_novedad"])
        self.assertEqual(fila["ot"], 1)
        self.assertFalse(fila["ot_pendiente"])  # antes: True (vencida por defecto)
        bucket = indicadores_cliente._serie_ot([fila], 7)[0]
        self.assertEqual(bucket["conNovedad"], 1)
        self.assertEqual(bucket["pendientes"], 0)
        self.assertEqual(bucket["pendientesVencidas"], 0)
        self.assertEqual(bucket["evaluables"], 1)  # entra al % como cumplida
        self.assertTrue(fila["anulada"])  # marca para chips/Excel del frontend

    def test_anulada_detectada_por_ws_though_informe_dice_transito(self):
        """(2026-11-10, caso 801232571) El informe del TMS NUNCA trae
        'ANULADA' — la guía queda en su último estado ('Transito Nacional')
        sin entrega. Si el WS de Siscore la detectó (guias_anuladas), el OT la
        trata igual: «Con novedad» que auto-cumple, NO pendiente vencida."""
        fila = self._calcular(
            _fila_ot(estado="Transito Nacional", fecha_emision="2026-01-05", fecha_entrega=None),
            anuladas={"999123": "2026-11-10T00:00:00"},
        )
        self.assertTrue(fila["anulada"])
        self.assertTrue(fila["cumple_novedad"])
        self.assertEqual(fila["ot"], 1)
        self.assertFalse(fila["ot_pendiente"])  # antes: vencida
        bucket = indicadores_cliente._serie_ot([fila], 7)[0]
        self.assertEqual(bucket["conNovedad"], 1)
        self.assertEqual(bucket["pendientesVencidas"], 0)


class SerieOTPendientesTests(unittest.TestCase):
    """Buckets del gráfico: pendientes/pendientesVencidas separadas y FUERA de
    evaluables. _serie_ot solo lee campos — no necesita mapas."""

    def test_buckets_y_evaluables(self):
        filas = [
            # En curso en plazo
            {"fecha_emision": "2026-11-01", "ot_pendiente": True, "ot_pendiente_vencida": False},
            # En curso vencida
            {"fecha_emision": "2026-11-02", "ot_pendiente": True, "ot_pendiente_vencida": True},
            # Entregadas: una cumple, una no
            {"fecha_emision": "2026-11-03", "ot": 1},
            {"fecha_emision": "2026-11-04", "ot": 0},
            # Sin promesa evaluable
            {"fecha_emision": "2026-11-05", "ot": None},
        ]
        bucket = indicadores_cliente._serie_ot(filas, 7)[0]
        self.assertEqual(bucket["pendientes"], 1)
        self.assertEqual(bucket["pendientesVencidas"], 1)
        self.assertEqual(bucket["cumplieron"], 1)
        self.assertEqual(bucket["noCumplieron"], 1)
        self.assertEqual(bucket["noAplica"], 1)
        # evaluables = cumplieron + noCumplieron + conNovedad — SIN pendientes
        self.assertEqual(bucket["evaluables"], 2)
        self.assertEqual(bucket["total"], 5)

    def test_fila_vieja_sin_campo_ot_pendiente_no_rompe(self):
        """Filas que no pasaron por el nuevo código (sin ot_pendiente) caen a
        su veredicto de siempre."""
        bucket = indicadores_cliente._serie_ot(
            [{"fecha_emision": "2026-11-01", "ot": 1}], 7
        )[0]
        self.assertEqual(bucket["cumplieron"], 1)
        self.assertEqual(bucket["pendientes"], 0)


class FilaPendienteHelperTests(unittest.TestCase):
    """(2026-11-10) Helper de /pendientes: límite = cita usada (manual o
    agendada) y si no hay, la promesa; días calendario desde HOY."""

    def test_dias_vs_cita_usada(self):
        hoy = date.today()
        fila = {"guia": "1", "fecha_cita_usada": (hoy - timedelta(days=3)).isoformat(),
                "fecha_promesa": "2000-01-01"}
        r = indicadores_cliente._fila_pendiente(fila, hoy)
        self.assertEqual(r["origen_limite"], "CITA")
        self.assertEqual(r["dias_vs_limite"], 3)

    def test_dias_vs_promesa_sin_cita(self):
        hoy = date.today()
        fila = {"guia": "2", "fecha_cita_usada": None,
                "fecha_promesa": (hoy + timedelta(days=2)).isoformat()}
        r = indicadores_cliente._fila_pendiente(fila, hoy)
        self.assertEqual(r["origen_limite"], "PROMESA")
        self.assertEqual(r["dias_vs_limite"], -2)  # aún en plazo

    def test_sin_limite_evaluable(self):
        r = indicadores_cliente._fila_pendiente({"guia": "3"}, date.today())
        self.assertIsNone(r["origen_limite"])
        self.assertIsNone(r["dias_vs_limite"])

    def test_orden_vencidas_mas_atrasada_primero(self):
        """El endpoint ordena vencidas desc (más atrasada arriba) y en curso
        asc (la que vence primero arriba); sin días evaluable al final."""
        vencidas = [
            {"dias_vs_limite": 2},
            {"dias_vs_limite": 40},
            {"dias_vs_limite": None},
            {"dias_vs_limite": 10},
        ]
        vencidas.sort(key=lambda r: -(r["dias_vs_limite"] if r["dias_vs_limite"] is not None else -10**6))
        self.assertEqual([r["dias_vs_limite"] for r in vencidas], [40, 10, 2, None])
        en_curso = [
            {"dias_vs_limite": -5},
            {"dias_vs_limite": None},
            {"dias_vs_limite": -1},
        ]
        en_curso.sort(key=lambda r: r["dias_vs_limite"] if r["dias_vs_limite"] is not None else 10**6)
        self.assertEqual([r["dias_vs_limite"] for r in en_curso], [-5, -1, None])


if __name__ == "__main__":
    unittest.main()
