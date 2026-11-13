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
        "tracking_url": "https://integra.appsiscore.com/app/app-cliente/cons_publica.php?GUIA=801233440",
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
    """Mensaje WA rediseñado (2026-11-13): secciones + enlace de seguimiento
    SOLO con la guía ENTREGADA (escenarios de validación de la spec)."""

    def test_error_degrada(self):
        p = {"ok": False, "error": "timeout"}
        self.assertEqual(formatear_trazabilidad_guia(p), formatear_respuesta_guia(p))

    def test_guia_inexistente_degrada(self):
        p = {"ok": True, "exists": False, "guia": "123", "not_found": True}
        self.assertEqual(formatear_trazabilidad_guia(p), formatear_respuesta_guia(p))

    def _payload(self, estado="EN DISTRIBUCION", movimientos=10, **extra):
        p = _ws(estado, movimientos=movimientos)
        p["data"].update({
            "Nombre_cliente": "FRESENIUS KABI", "Servicio": "CARGA", "NumPie": 66,
            "NomRem": "FRESENIUS KABI", "CiuRem": "FUNZA",
            "NomDes": "CLINICA BELO HORIZONTE", "CiuDes": "NEIVA",
        })
        p.update({
            "placa": "JTZ665",
            "ot": {"ot": None, "pendiente": True, "origen_promesa": "TARIFA",
                   "fecha_promesa": "2026-11-17"},
            "evidencia_url": None,
        })
        p.update(extra)
        return p

    # ── Escenario 2: EN DISTRIBUCIÓN → sin enlace, con GPS ──
    def test_en_distribucion_sin_enlace_con_gps(self):
        p = self._payload("EN DISTRIBUCION")
        p["vehiculo"] = {"ok": True, "existe": True, "vehiculo": {
            "ubicacion": "Calle 51, Manchester, Bello, Valle de Aburrá, Antioquia, RAP del Agua y la Montaña, 051053, Colombia",
            "estado_viaje": "En ruta", "lat": 6.33, "lng": -75.55, "velocidad": 45,
            "eta_fecha": "2099-01-15 10:03:00", "eta_distancia_km": 5,
            "fecha_reporte_gps": "2099-01-15 09:36:00", "coincide_guia": True,
            "conductor": "JUAN DAVID CORNELIO URIZA",
        }}
        msg = formatear_trazabilidad_guia(p)
        self.assertIn("📦 *Seguimiento de envío*", msg)
        self.assertIn("Guía #801233440", msg)
        self.assertIn("🚚 *EN DISTRIBUCIÓN*", msg)
        self.assertIn("📍 *Origen y destino*", msg)
        self.assertIn("• Origen: FRESENIUS KABI, FUNZA", msg)
        self.assertIn("• Destino: CLINICA BELO HORIZONTE — NEIVA", msg)
        # GPS: ubicación ACORTADA (3 componentes), ETA, distancia, GPS, mapa
        self.assertIn("• Ubicación: Calle 51, Manchester, Bello · 45 km/h", msg)
        self.assertNotIn("Valle de Aburrá", msg)
        self.assertIn("• Llegada estimada: 15 ene · 10:03 a. m.", msg)
        self.assertIn("• Distancia restante: 5 km", msg)
        self.assertIn("• Última actualización GPS: 15 ene · 9:36 a. m.", msg)
        self.assertIn("https://maps.google.com/?q=6.33,-75.55", msg)
        # cumplimiento en curso dentro del plazo + compromiso
        self.assertIn("• Fecha compromiso: 17 nov 2026", msg)
        self.assertIn("• Estado: Dentro del plazo de entrega", msg)
        # detalles
        self.assertIn("• Vehículo: JTZ665", msg)
        self.assertIn("• Conductor: JUAN DAVID CORNELIO URIZA", msg)
        # historial acotado
        self.assertIn("…y 4 movimientos más", msg)
        # ⚠️ REGLA: sin enlace de seguimiento (no está entregada)
        self.assertNotIn("Seguimiento en línea", msg)
        self.assertNotIn("cons_publica", msg)
        self.assertLessEqual(len(msg), 4096)

    # ── Escenario 1: ENTREGADA → enlace de seguimiento ──
    def test_entregada_muestra_enlace(self):
        p = self._payload("ENTREGA DIGITALIZADA")
        p["ot"] = {"ot": 1, "entregada": True, "fecha_entrega": "2026-11-12",
                   "fecha_promesa": "2026-11-17", "origen_promesa": "TARIFA"}
        msg = formatear_trazabilidad_guia(p)
        self.assertIn("✅ *ENTREGADO*", msg)
        self.assertIn("🔗 *Seguimiento en línea:*", msg)
        self.assertIn("cons_publica.php?GUIA=801233440", msg)
        self.assertIn("• Estado: Entregado a tiempo", msg)
        # entregada no lleva GPS (el enriquecimiento no lo adjunta)
        self.assertNotIn("Ubicación y llegada estimada", msg)

    def test_entregada_mayusculas_tildes_espacios_cuenta(self):
        """La comparación del estado normaliza tildes/mayúsculas/espacios."""
        p = self._payload("  Entregado ")
        msg = formatear_trazabilidad_guia(p)
        self.assertIn("🔗 *Seguimiento en línea:*", msg)

    def test_entregada_digitalizada_prefiere_evidencia(self):
        p = self._payload("ENTREGA DIGITALIZADA")
        p["evidencia_url"] = "https://integra.appsiscore.com/app/t/t/imagenes/2026/11/10/801233440.png"
        msg = formatear_trazabilidad_guia(p)
        self.assertIn("🖼️ *Evidencia de entrega digitalizada:*", msg)
        self.assertNotIn("Seguimiento en línea", msg)

    # ── Escenario 3: EN TRÁNSITO → sin enlace ──
    def test_en_transito_sin_enlace(self):
        msg = formatear_trazabilidad_guia(self._payload("TRANSITO NACIONAL"))
        self.assertIn("🚛 *EN TRÁNSITO*", msg)
        self.assertNotIn("Seguimiento en línea", msg)

    def test_pendiente_sin_enlace(self):
        msg = formatear_trazabilidad_guia(self._payload("PENDIENTE"))
        self.assertIn("🕒 *PENDIENTE*", msg)
        self.assertNotIn("Seguimiento en línea", msg)

    # ── Escenario 4: sin GPS → sin campos vacíos ──
    def test_sin_gps_sin_campos_vacios(self):
        msg = formatear_trazabilidad_guia(self._payload("EN DISTRIBUCION"))
        self.assertNotIn("Ubicación y llegada estimada", msg)
        self.assertNotIn("Conductor:", msg)
        self.assertNotIn("• Ubicación: -", msg)

    # ── Escenario 5: ETA vencida → pendiente de actualización ──
    def test_eta_vencida_pendiente_de_actualizacion(self):
        p = self._payload("EN DISTRIBUCION")
        p["vehiculo"] = {"ok": True, "existe": True, "vehiculo": {
            "ubicacion": "Vía Neiva", "estado_viaje": "En ruta",
            "eta_fecha": "2020-01-15 10:03:00", "eta_distancia_km": 2,
            "coincide_guia": True,
        }}
        msg = formatear_trazabilidad_guia(p)
        self.assertIn("• Llegada estimada: pendiente de actualización", msg)
        self.assertNotIn("10:03 a. m.", msg)

    def test_km0_se_muestra_tal_cual(self):
        p = self._payload("EN DISTRIBUCION")
        p["vehiculo"] = {"ok": True, "existe": True, "vehiculo": {
            "ubicacion": "Km 45 Autopista", "estado_viaje": "En ruta",
            "velocidad": 0, "coincide_guia": True,
        }}
        msg = formatear_trazabilidad_guia(p)
        self.assertIn("Km 45 Autopista · 0 km/h", msg)

    # ── Escenario 6: entregada fuera de plazo ──
    def test_entregada_fuera_de_tiempo(self):
        p = self._payload("ENTREGADO")
        p["ot"] = {"ot": 0, "entregada": True, "fecha_entrega": "2026-11-20",
                   "fecha_promesa": "2026-11-17", "origen_promesa": "TARIFA"}
        msg = formatear_trazabilidad_guia(p)
        self.assertIn("• Estado: Entregado fuera de tiempo", msg)
        self.assertIn("🔗 *Seguimiento en línea:*", msg)

    def test_cumplimiento_por_confirmar_sin_ot(self):
        p = self._payload("EN DISTRIBUCION")
        p.pop("ot")
        msg = formatear_trazabilidad_guia(p)
        self.assertIn("• Estado: Cumplimiento por confirmar", msg)
        self.assertNotIn("Fecha compromiso", msg)

    def test_plazo_vencido_en_curso(self):
        p = self._payload("EN DISTRIBUCION")
        p["ot"] = {"ot": None, "pendiente": True, "pendiente_vencida": True,
                   "origen_promesa": "CITA", "fecha_cita_usada": "2026-11-06"}
        msg = formatear_trazabilidad_guia(p)
        self.assertIn("• Estado: Plazo de entrega vencido", msg)
        self.assertIn("• Fecha compromiso: 06 nov 2026", msg)

    def test_anulada(self):
        p = self._payload("ANULADA")
        p["ot"] = {"anulada": True}
        msg = formatear_trazabilidad_guia(p)
        self.assertIn("⊘ *ANULADA*", msg)
        self.assertIn("Guía anulada — no cuenta como incumplimiento", msg)

    def test_justificada_rescata_fuera_de_tiempo(self):
        p = self._payload("ENTREGADO")
        p["ot"] = {"ot": 0, "entregada": True, "fecha_entrega": "2026-11-20",
                   "fecha_promesa": "2026-11-17", "origen_promesa": "TARIFA"}
        p["justificada"] = {"causal": "DERRUMBE", "justificacion": "vía cerrada"}
        msg = formatear_trazabilidad_guia(p)
        self.assertIn("Entregado fuera de tiempo (justificada)", msg)
        self.assertIn("• Novedad: DERRUMBE — vía cerrada", msg)

    def test_cita_de_entrega_con_franja(self):
        p = self._payload("EN DISTRIBUCION")
        p["cita_entrega"] = {"fecha": "2026-11-13", "hora_inicio": "08:00", "hora_fin": "12:00"}
        msg = formatear_trazabilidad_guia(p)
        self.assertIn("• Cita de entrega: 13 nov 2026 · 08:00–12:00", msg)

    def test_coincide_guia_false_NO_muestra_vehiculo(self):
        """El bot es de cara a CLIENTES: sin coincidencia de guías en el
        manifiesto, NADA de Monitor (igual que el perfil CLIENTE del portal)."""
        p = self._payload("EN DISTRIBUCION")
        p["vehiculo"] = {"ok": True, "existe": True, "vehiculo": {
            "ubicacion": "Vía Funza", "estado_viaje": "En curso", "coincide_guia": False}}
        msg = formatear_trazabilidad_guia(p)
        self.assertNotIn("Ubicación y llegada estimada", msg)
        self.assertNotIn("Vía Funza", msg)

    def test_manifiesto_finalizado_no_muestra_vehiculo(self):
        p = self._payload("EN DISTRIBUCION")
        p["vehiculo"] = {"ok": True, "existe": True,
                         "vehiculo": {"ubicacion": "Patio", "estado_viaje": "Finalizado"}}
        msg = formatear_trazabilidad_guia(p)
        self.assertNotIn("Ubicación y llegada estimada", msg)

    def test_manifiesto_sin_gps_NO_muestra_bloque_huerfano(self):
        """(caso 801233636 / NNM001) Manifiesto vigente CON la guía pero sin
        NINGUNA posición GPS: el bloque no se publica."""
        p = self._payload("EN DISTRIBUCION")
        p["vehiculo"] = {"ok": True, "existe": True, "vehiculo": {
            "placa": "NNM001", "manifiesto": "00153625", "estado_viaje": "En ruta",
            "conductor": "YONATAN OCHOA OCHOA", "ubicacion": "", "lat": None,
            "lng": None, "velocidad": None, "fecha_reporte_gps": "", "eta_fecha": "",
            "coincide_guia": True}}
        msg = formatear_trazabilidad_guia(p)
        self.assertNotIn("Ubicación y llegada estimada", msg)

    def test_otras_guias_del_vehiculo(self):
        p = self._payload("EN DISTRIBUCION")
        p["misma_placa"] = [{"guia": "801233381", "estado": "ENTREGADO"}]
        msg = formatear_trazabilidad_guia(p)
        self.assertIn("🚛 *Otras guías del vehículo:* 801233381 (ENTREGADO)", msg)

    def test_menu_final_siempre(self):
        for estado in ("ENTREGADO", "EN DISTRIBUCION", "PENDIENTE", "ANULADA"):
            msg = formatear_trazabilidad_guia(self._payload(estado))
            self.assertIn("1️⃣ Consultar otra guía", msg, estado)
            self.assertIn("2️⃣ Volver al menú principal", msg, estado)


if __name__ == "__main__":
    unittest.main()
