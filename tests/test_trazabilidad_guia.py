# tests/test_trazabilidad_guia.py
"""
(2026-11-13) Helper COMPARTIDO de trazabilidad de guía
(Funciones/trazabilidad_guia.py) — el enriquecimiento que usa la pestaña 🧭
Trazabilidad del portal (GET /indicadores-cliente/{id}/consulta-guia) y el
bot de WhatsApp (flujo 🧾 CLIENTE).

Se parchea TODO sobre ``rutas.indicadores_cliente`` (mismo patrón de
test_monitor_ws): el helper resuelve colecciones/caches/OT vía ese módulo.
"""
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from Funciones.trazabilidad_guia import (
    enriquecer_consulta_guia,
    guia_ya_entregada,
    url_evidencia_guia,
)
from Funciones.siscore_ws_format import (
    formatear_respuesta_guia,
    formatear_trazabilidad_guia,
)


def _ws(estado: str = "EN DISTRIBUCION", movimientos: int = 0) -> dict:
    return {
        "ok": True,
        "exists": True,
        "guia": "801233440",
        "data": {
            "Estado": estado,
            "NomDes": "DISFARMA CEDI BOGOTÁ",
            "CiuDes": "BOGOTA",
            "Nombre_cliente": "FRESENIUS KABI",
            "NumPie": 12,
            "FecEnv": "2026-11-10 08:31:00",
            "Movimientos": [
                {"FecMov": f"2026-11-1{i} 0{i}:00:00", "DetalleMov": f"MOV {i}"} for i in range(movimientos)
            ],
        },
        "tracking_url": "https://ejemplo/tracking",
    }


def _tms(**extra) -> dict:
    base = {
        "placa": "SPW261",
        "ciudad_destino": "BOGOTA - SANTANDER",
        "estado": "En distribucion",
        "fecha_emision": "2026-11-10",
        "nombre_cliente": "FRESENIUS KABI",
    }
    base.update(extra)
    return {k: v for k, v in base.items() if v is not None}


def _stub_ot(**campos):
    """_calcular_ot_filas que 'calcula' lo que el test decide (muta la fila
    in-place como el real)."""
    def calc(filas):
        filas[0].update(campos)
    return calc


class ContextoParche:
    """Parcha el módulo del router como hacen los tests del endpoint."""

    def __init__(self, tms=None, ot=None, companeras=None, nov=None, cita=None,
                 anuladas_previas=False, monitor=None):
        from rutas import indicadores_cliente as ic
        self.ic = ic
        self.tms = MagicMock(return_value=tms)
        self.companeras = MagicMock(return_value=companeras or [])
        self.monitor = monitor or AsyncMock(return_value={"ok": False, "sin_credenciales": True})
        self.calc = _stub_ot(**(ot or {}))
        self.anuladas_insert = MagicMock()
        col_anuladas = MagicMock()
        # find_one: None la 1ª vez (no existe → inserta) y el doc desde
        # entonces (ya persistida → NO vuelve a insertar); con
        # anuladas_previas ya existe desde el principio.
        ya_estaba = {"guia": "X"} if anuladas_previas else None
        col_anuladas.find_one.side_effect = [ya_estaba] if anuladas_previas else [None]
        col_anuladas.insert_one.side_effect = self.anuladas_insert
        # tras insertar, la guía "queda" persistida para el siguiente find_one
        def _tras_insertar(doc):
            col_anuladas.find_one.side_effect = list(col_anuladas.find_one.side_effect) + [doc]
        self.anuladas_insert.side_effect = lambda doc: _tras_insertar(doc)
        self.col_anuladas = col_anuladas
        self._nov = nov
        self._cita = cita

    def __enter__(self):
        self._parches = [
            patch.object(self.ic, "guia_tms_placa", self.tms),
            patch.object(self.ic, "guias_placa_dia", self.companeras),
            patch.object(self.ic, "col_guias_anuladas", self.col_anuladas),
            patch.object(self.ic, "col_novedades_ot", self._col(self._nov)),
            patch.object(self.ic, "col_citas_entrega", self._col(self._cita)),
            patch.object(self.ic, "_calcular_ot_filas", self.calc),
            patch("Funciones.monitor_ws.consultar_vehiculo_monitor", self.monitor),
        ]
        for p in self._parches:
            p.start()
        return self

    def _col(self, doc):
        col = MagicMock()
        col.find_one.return_value = doc
        return col

    def __exit__(self, *exc):
        for p in self._parches:
            p.stop()


class GuiasEntregadaEvidenciaTests(unittest.TestCase):

    def test_guia_ya_entregada(self):
        self.assertTrue(guia_ya_entregada(_ws("ENTREGA DIGITALIZADA")))
        self.assertTrue(guia_ya_entregada(_ws("ENTREGADO")))
        self.assertFalse(guia_ya_entregada(_ws("EN DISTRIBUCION")))
        self.assertFalse(guia_ya_entregada({"data": {}}))

    def test_url_evidencia_digitalizada(self):
        p = _ws("ENTREGA DIGITALIZADA")
        p["ot"] = {"fecha_emision": "2026-11-10"}
        url = url_evidencia_guia(p)
        self.assertEqual(
            url,
            "https://integra.appsiscore.com/app/xMr3hVYKKkVzL0gGjnCFySIRO6RLPLb1qUb1bwop94Zq8Of"
            "/xMr3hVYKKkVzL0gGjnCFySIRO6RLPLb1qUb1bwop94Zq8Of/imagenes/2026/11/10/801233440.png",
        )

    def test_url_evidencia_fallback_fecenv(self):
        p = _ws("DEVOLUCION DIGITALIZADA")
        p["data"]["FecEnv"] = "2026-03-05"
        self.assertIn("/imagenes/2026/03/05/801233440.png", url_evidencia_guia(p))

    def test_url_evidencia_solo_digitalizada(self):
        self.assertIsNone(url_evidencia_guia(_ws("ENTREGADO")))

    def test_url_evidencia_fecha_no_iso(self):
        p = _ws("ENTREGA DIGITALIZADA")
        p["data"]["FecEnv"] = "10/11/2026"
        self.assertIsNone(url_evidencia_guia(p))


class EnriquecerTests(unittest.TestCase):

    def run_enriquecer(self, payload, **ctx):
        import asyncio
        with ContextoParche(**ctx) as c:
            out = asyncio.run(enriquecer_consulta_guia("801233440", payload))
        return out, c

    def test_placa_y_ciudad_del_tms(self):
        out, _ = self.run_enriquecer(_ws(), tms=_tms())
        self.assertEqual(out["placa"], "SPW261")
        self.assertEqual(out["ciudad_destino_tms"], "BOGOTA - SANTANDER")

    def test_bloque_ot_mapea_la_fila(self):
        out, _ = self.run_enriquecer(
            _ws(), tms=_tms(),
            ot={"ot": 1, "fecha_promesa": "2026-11-12", "origen_promesa": "TARIFA",
                "fecha_entrega": "2026-11-11", "ot_pendiente": False},
        )
        self.assertEqual(out["ot"]["ot"], 1)
        self.assertEqual(out["ot"]["fecha_promesa"], "2026-11-12")
        self.assertFalse(out["ot"]["pendiente"])

    def test_misma_placa_sin_filtro_en_bot(self):
        compa = [{"guia": "801233381", "estado": "ENTREGADO"}]
        out, c = self.run_enriquecer(_ws(), tms=_tms(), companeras=compa)
        self.assertEqual(out["misma_placa"][0]["guia"], "801233381")
        # El bot (nombres_cliente=None) NO filtra por cliente
        self.assertIsNone(c.companeras.call_args[0][3])

    def test_misma_placa_con_filtro_del_portal(self):
        compa = [{"guia": "801233381", "estado": "ENTREGADO"}]
        import asyncio
        with ContextoParche(tms=_tms(), companeras=compa) as c:
            asyncio.run(enriquecer_consulta_guia("801233440", _ws(), nombres_cliente=["FRESENIUS KABI"]))
        self.assertEqual(c.companeras.call_args[0][3], ["FRESENIUS KABI"])

    def test_vehiculo_para_guia_en_curso(self):
        monitor = AsyncMock(return_value={
            "ok": True, "existe": True,
            "vehiculo": {"placa": "SPW261", "ubicacion": "Vía Funza", "lat": 4.71, "lng": -74.15},
        })
        out, _ = self.run_enriquecer(_ws(), tms=_tms(), monitor=monitor)
        self.assertEqual(out["vehiculo"]["vehiculo"]["ubicacion"], "Vía Funza")
        # valida el manifiesto contra las guías del viaje
        gui = monitor.await_args.kwargs.get("guias") or monitor.await_args.args[1:] or set()
        self.assertIn("801233440", gui if isinstance(gui, set) else gui[0])

    def test_entregada_NO_consulta_monitor(self):
        monitor = AsyncMock()
        out, c = self.run_enriquecer(_ws("ENTREGA DIGITALIZADA"), tms=_tms(), monitor=monitor)
        self.assertEqual(monitor.await_count, 0)
        self.assertNotIn("vehiculo", out)

    def test_anulada_por_ws_se_persiste_una_vez(self):
        import asyncio
        with ContextoParche(tms=_tms()) as c:
            # primera consulta: persiste + limpia caches
            c.ic._CACHE_GUIAS["x"] = 1
            c.ic._CACHE_PENDIENTES["y"] = 1
            c.ic._CACHE_TABLAS["guias_anuladas"] = object()
            asyncio.run(enriquecer_consulta_guia("801233440", _ws("ANULADA"), detectada_por="wa:573001112223"))
            self.assertEqual(c.anuladas_insert.call_count, 1)
            self.assertFalse(c.ic._CACHE_GUIAS)
            self.assertFalse(c.ic._CACHE_PENDIENTES)
            self.assertNotIn("guias_anuladas", c.ic._CACHE_TABLAS)
            doc = c.anuladas_insert.call_args[0][0]
            self.assertEqual(doc["detectada_por"], "wa:573001112223")
            # segunda consulta (ya existe): NO vuelve a insertar
            asyncio.run(enriquecer_consulta_guia("801233440", _ws("ANULADA")))
            self.assertEqual(c.anuladas_insert.call_count, 1)

    def test_justificada_y_cita_viajan(self):
        nov = {"causal": "DERRUMBE", "justificacion": "vía cerrada",
               "creado_por": {"nombre": "Ana", "fecha": None}}
        cita = {"fecha": "2026-11-12", "hora_inicio": "08:00", "hora_fin": "12:00",
                "creado_por": {"nombre": "Ana"}, "historial": []}
        out, _ = self.run_enriquecer(_ws(), tms=_tms(), nov=nov, cita=cita)
        self.assertEqual(out["justificada"]["causal"], "DERRUMBE")
        self.assertEqual(out["cita_entrega"]["fecha"], "2026-11-12")
        self.assertEqual(out["cita_entrega"]["hora_inicio"], "08:00")

    def test_evidencia_url_en_payload(self):
        out, _ = self.run_enriquecer(_ws("ENTREGA DIGITALIZADA"), tms=_tms(estado="ENTREGADO", fecha_entrega="2026-11-11"))
        self.assertIn("/imagenes/2026/11/10/801233440.png", out["evidencia_url"])

    def test_guia_sin_tms_degrada(self):
        out, _ = self.run_enriquecer(_ws(), tms=None)
        self.assertNotIn("ot", out)
        self.assertNotIn("placa", out)
        self.assertIsNone(out["evidencia_url"])


class FormatearTrazabilidadTests(unittest.TestCase):

    def test_payload_plano_degrada_al_formato_viejo(self):
        p = _ws("EN DISTRIBUCION")
        self.assertEqual(formatear_trazabilidad_guia(p), formatear_respuesta_guia(p))

    def test_error_degrada(self):
        p = {"ok": False, "error": "timeout"}
        self.assertEqual(formatear_trazabilidad_guia(p), formatear_respuesta_guia(p))

    def _enriquecido(self, **extra):
        p = _ws("EN DISTRIBUCION", movimientos=10)
        p.update({
            "placa": "SPW261",
            "ot": {"ot": 1, "origen_promesa": "CITA", "fecha_cita_usada": "2026-11-12",
                   "fecha_promesa": "2026-11-12", "fecha_entrega": "2026-11-11"},
            "cita_entrega": {"fecha": "2026-11-12", "hora_inicio": "08:00",
                             "hora_fin": "12:00", "anteriores": []},
            "evidencia_url": None,
        })
        p.update(extra)
        return p

    def test_bloques_del_mensaje(self):
        p = self._enriquecido()
        p["vehiculo"] = {"ok": True, "existe": True, "vehiculo": {
            "ubicacion": "Vía Funza", "estado_viaje": "En curso", "lat": 4.71, "lng": -74.15,
            "velocidad": 45, "eta_fecha": "2026-11-13 18:00:00", "eta_distancia_km": 12,
        }}
        p["misma_placa"] = [{"guia": "801233381", "estado": "ENTREGADO"}]
        msg = formatear_trazabilidad_guia(p)
        for esperado in (
            "*Guía:* 801233440",
            "*Placa:* SPW261",
            "*On Time:* ✔ Cumplió — entrega 11 nov 2026 ≤ cita 12 nov 2026",
            "*Cita de entrega:* 12 nov 2026 · 08:00–12:00",
            "*Vehículo en curso*",
            "45 km/h",
            "⏱ Llegada estimada: 13 nov 2026 18:00 (12 km)",
            "maps.google.com/?q=4.71,-74.15",
            "*Otras guías del vehículo:* 801233381 (ENTREGADO)",
            "…y 4 movimientos más",
        ):
            self.assertIn(esperado, msg, esperado)
        self.assertLessEqual(len(msg), 4096)

    def test_chip_ot_orden_y_variantes(self):
        casos = [
            ({"anulada": True}, "⊘ Guía anulada"),
            ({"devolucion_total": True}, "↩ Devolución total"),
            ({"cumple_novedad": True}, "✔ Con novedad"),
            ({"ot": 0}, "✘ No cumplió"),
        ]
        for campos, chip in casos:
            p = self._enriquecido()
            p["ot"] = {"origen_promesa": "TARIFA", "fecha_promesa": "2026-11-12", **campos}
            msg = formatear_trazabilidad_guia(p)
            self.assertIn(chip, msg, chip)

    def test_chip_pendiente_vencida(self):
        p = self._enriquecido()
        p["ot"] = {"ot": None, "pendiente": True, "pendiente_vencida": True,
                   "origen_promesa": "CITA", "fecha_cita_usada": "2026-11-12"}
        msg = formatear_trazabilidad_guia(p)
        self.assertIn("🕒 Pendiente (vencida) — cita 12 nov 2026 ya venció", msg)

    def test_justificada_rescata_no_cumplida(self):
        p = self._enriquecido()
        p["ot"] = {"ot": 0, "origen_promesa": "TARIFA"}
        p["justificada"] = {"causal": "DERRUMBE", "justificacion": "vía cerrada"}
        msg = formatear_trazabilidad_guia(p)
        self.assertIn("✔ Justificada — DERRUMBE", msg)
        self.assertIn("🚧 *Novedad:* DERRUMBE — vía cerrada", msg)

    def test_vehiculo_finalizado_no_aparece(self):
        p = self._enriquecido()
        p["vehiculo"] = {"ok": True, "existe": True,
                         "vehiculo": {"ubicacion": "Patio", "estado_viaje": "Finalizado"}}
        msg = formatear_trazabilidad_guia(p)
        self.assertNotIn("Vehículo en curso", msg)

    def test_evidencia_reemplaza_tracking(self):
        p = self._enriquecido()
        p["evidencia_url"] = "https://integra.appsiscore.com/app/t/t/imagenes/2026/11/10/801233440.png"
        msg = formatear_trazabilidad_guia(p)
        self.assertIn("🖼️ *Evidencia de entrega digitalizada:*", msg)
        self.assertNotIn("Seguimiento en línea", msg)

    def test_coincide_guia_false_NO_muestra_vehiculo(self):
        """El bot es de cara a CLIENTES: sin coincidencia de guías en el
        manifiesto, NADA de Monitor (igual que el perfil CLIENTE del portal;
        el aviso «carro sin planillar» es solo para perfiles internos)."""
        p = self._enriquecido()
        p["vehiculo"] = {"ok": True, "existe": True, "vehiculo": {
            "ubicacion": "Vía Funza", "estado_viaje": "En curso", "coincide_guia": False}}
        msg = formatear_trazabilidad_guia(p)
        self.assertNotIn("Vehículo en curso", msg)
        self.assertNotIn("Vía Funza", msg)
        self.assertNotIn("planillar", msg)


if __name__ == "__main__":
    unittest.main()
