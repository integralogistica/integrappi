"""Tests del cliente de la API Monitor (Mayasoft, 2026-11-09): posición GPS
del vehículo de una placa para la Trazabilidad del portal de clientes.

Unitarios de Funciones/monitor_ws.py con httpx.MockTransport (login+cache de
token, re-login ante 401, selección del manifiesto vigente, saneo) y del
enganche en GET /indicadores-cliente/{id}/consulta-guia: la ubicación SOLO se
consulta si la guía NO está entregada y un fallo de Monitor NO rompe la
consulta (que ya tardó 40-90s en el SOAP de Siscore)."""
import sys
import types
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

if "bd.bd_cliente" not in sys.modules:
    _stub = types.ModuleType("bd.bd_cliente")

    class _BDFake(dict):
        def __getitem__(self, clave):
            return self.setdefault(clave, MagicMock())

    _stub.bd_cliente = _BDFake()
    sys.modules["bd.bd_cliente"] = _stub

import httpx

from Funciones import monitor_ws


def _reset(monitor_user="U.MONITOR", monitor_pass="secreto"):
    """Estado limpio por test: credenciales y caché de token."""
    monitor_ws._TOKEN = None
    monitor_ws._TOKEN_EN = 0.0
    return (
        patch.object(monitor_ws, "MONITOR_USER", monitor_user),
        patch.object(monitor_ws, "MONITOR_PASSWORD", monitor_pass),
    )


def _manifiesto(**extra) -> dict:
    base = {
        "client": "FRESENIUS KABI",
        "manifest": "0949224",
        "manifestcreationdate": "2026-11-08 06:00:00",
        "delivered": "En ruta",
        "licenceplate": "TRN399",
        "driver": "ANDRES FELIPE",
        "lastlatitude": "9.969",
        "lastlongitude": "-75.08141",
        "locationdescription": "Vía Sincelejo - Barranquilla",
        "gpslastreportdate": "2026-11-09 14:20:01",
        "etadate": "2026-11-09 19:22:41",
        "etadistance": "852.106091",
        "location": [
            {"latitude": "8.97606", "longitude": "-75.45438", "velocity": "0",
             "state": "stopped", "realdate": "2026-11-09 10:00:00"},
            {"latitude": "9.969", "longitude": "-75.08141", "velocity": "65",
             "state": "moving", "realdate": "2026-11-09 14:20:00"},
        ],
    }
    base.update(extra)
    return base


class MonitorWsUnitTests(unittest.TestCase):
    def setUp(self):
        # Credenciales y caché de token limpios para cada test.
        self._parches = _reset()
        for p in self._parches:
            p.start()
        monitor_ws._TOKEN = None
        monitor_ws._TOKEN_EN = 0.0

    def tearDown(self):
        for p in self._parches:
            p.stop()

    def _transport(self, respuestas):
        """MockTransport: respuestas por (método, sufijo de path). Registra
        los requests en `llamadas` para afirmar el cacheo/re-login."""
        llamadas = []

        def handler(request: httpx.Request) -> httpx.Response:
            llamadas.append((request.method, request.url.path))
            if request.url.path.endswith("/getToken/"):
                return respuestas["login"](request)
            return respuestas["consulta"](request)

        return httpx.MockTransport(handler), llamadas

    def test_login_cache_y_consulta(self):
        cuenta = {"logins": 0}

        def login(request):
            cuenta["logins"] += 1
            return httpx.Response(200, json={"token": "TOK1"})

        def consulta(request):
            self.assertIn("licensePlate=TRN399", str(request.url))
            # (2026-11-11) la consulta pide remittances además de location:
            # sin ellas no hay clientreferencenumber con qué validar el viaje
            self.assertIn("lists=location%2Cremittances", str(request.url))
            self.assertEqual(request.headers["Authorization"], "TOK1")
            return httpx.Response(200, json=[_manifiesto()])

        transport, llamadas = self._transport({"login": login, "consulta": consulta})
        import asyncio

        r1 = asyncio.run(monitor_ws.consultar_vehiculo_monitor("trn399", transport=transport))
        r2 = asyncio.run(monitor_ws.consultar_vehiculo_monitor("TRN399", transport=transport))
        self.assertTrue(r1["ok"] and r1["existe"])
        self.assertEqual(cuenta["logins"], 1)  # 2ª llamada usa el token cacheado
        self.assertEqual(r1, r2)
        v = r1["vehiculo"]
        self.assertEqual(v["placa"], "TRN399")
        self.assertEqual(v["manifiesto"], "0949224")
        self.assertEqual(v["ubicacion"], "Vía Sincelejo - Barranquilla")
        self.assertEqual(v["lat"], 9.969)
        self.assertEqual(v["lng"], -75.08141)
        self.assertEqual(v["velocidad"], 65)  # del ÚLTIMO location[]
        self.assertEqual(v["estado_gps"], "moving")
        self.assertEqual(v["eta_distancia_km"], 852)
        self.assertNotIn("location", v)  # saneado: nunca el historial completo
        self.assertNotIn("coincide_guia", v)  # sin guias → sin validación
        self.assertEqual(len(llamadas), 3)  # login + 2 consultas

    def test_valida_guias_contra_clientreferencenumber(self):
        """(2026-11-11) coincide_guia: True si ALGUNA de las guías del viaje
        aparece en el clientreferencenumber de las remesas (texto separado por
        coma); False si el manifiesto declara otras guías o nada."""

        def login(request):
            return httpx.Response(200, json={"token": "T"})

        def manifiesto_con(remesas):
            return httpx.Response(200, json=[_manifiesto(remittances=remesas)])

        transport, _ = self._transport({
            "login": login,
            "consulta": lambda request: manifiesto_con([
                {"number": "00257278", "clientreferencenumber": "801233381, 801233382"},
                {"number": "00257279", "clientreferencenumber": ""},
            ]),
        })
        import asyncio

        # La guía consultada está en la remesa (entre varias, por coma)
        r = asyncio.run(monitor_ws.consultar_vehiculo_monitor("TRN399", guias={"801233382"}, transport=transport))
        self.assertTrue(r["vehiculo"]["coincide_guia"])
        # Coincide una COMPAÑERA del vehículo, no la consultada
        r = asyncio.run(monitor_ws.consultar_vehiculo_monitor("TRN399", guias={"999123", "801233381"}, transport=transport))
        self.assertTrue(r["vehiculo"]["coincide_guia"])
        # El manifiesto declara OTRAS guías → sin coincidencia
        r = asyncio.run(monitor_ws.consultar_vehiculo_monitor("TRN399", guias={"801216689"}, transport=transport))
        self.assertFalse(r["vehiculo"]["coincide_guia"])

        # Todas las remesas con clientreferencenumber VACÍO (p. ej. clientes
        # que no lo reportan) → sin coincidencia: el viaje no es declarado
        transport2, _ = self._transport({
            "login": login,
            "consulta": lambda request: manifiesto_con([{"number": "1", "clientreferencenumber": ""}]),
        })
        r = asyncio.run(monitor_ws.consultar_vehiculo_monitor("TRN399", guias={"801233381"}, transport=transport2))
        self.assertFalse(r["vehiculo"]["coincide_guia"])

    def test_relogin_ante_401(self):
        estados = {"login": 0, "consulta": 0}

        def login(request):
            estados["login"] += 1
            return httpx.Response(200, json={"token": f"TOK{estados['login']}"})

        def consulta(request):
            estados["consulta"] += 1
            if estados["consulta"] == 1:
                return httpx.Response(401, json={"error": "token expirado"})
            return httpx.Response(200, json=[_manifiesto()])

        transport, _ = self._transport({"login": login, "consulta": consulta})
        import asyncio

        r = asyncio.run(monitor_ws.consultar_vehiculo_monitor("TRN399", transport=transport))
        self.assertTrue(r["ok"] and r["existe"])
        self.assertEqual(estados["login"], 2)  # re-login tras el 401
        self.assertEqual(estados["consulta"], 2)

    def test_prefiere_manifiesto_en_ruta(self):
        def login(request):
            return httpx.Response(200, json={"data": {"access_token": "T"}})

        def consulta(request):
            return httpx.Response(200, json=[
                _manifiesto(manifest="0111", delivered="Finalizado",
                            manifestcreationdate="2026-11-09 05:00:00"),
                _manifiesto(manifest="0222", delivered="En ruta",
                            manifestcreationdate="2026-11-08 06:00:00"),
            ])

        transport, _ = self._transport({"login": login, "consulta": consulta})
        import asyncio

        r = asyncio.run(monitor_ws.consultar_vehiculo_monitor("TRN399", transport=transport))
        self.assertEqual(r["vehiculo"]["manifiesto"], "0222")  # el vigente, no el último creado

    def test_todos_finalizados_toma_el_ultimo(self):
        def login(request):
            return httpx.Response(200, json="TOK")

        def consulta(request):
            return httpx.Response(200, json=[
                _manifiesto(manifest="VIEJO", delivered="Finalizado",
                            manifestcreationdate="2026-10-01 06:00:00"),
                _manifiesto(manifest="RECIENTE", delivered="Finalizado",
                            manifestcreationdate="2026-11-07 06:00:00"),
            ])

        transport, _ = self._transport({"login": login, "consulta": consulta})
        import asyncio

        r = asyncio.run(monitor_ws.consultar_vehiculo_monitor("TRN399", transport=transport))
        self.assertTrue(r["ok"] and r["existe"])
        self.assertEqual(r["vehiculo"]["manifiesto"], "RECIENTE")
        self.assertEqual(r["vehiculo"]["estado_viaje"], "Finalizado")

    def test_placa_sin_despachos(self):
        def login(request):
            return httpx.Response(200, json={"token": "T"})

        def consulta(request):
            return httpx.Response(200, json=[])

        transport, _ = self._transport({"login": login, "consulta": consulta})
        import asyncio

        r = asyncio.run(monitor_ws.consultar_vehiculo_monitor("XXX000", transport=transport))
        self.assertTrue(r["ok"])
        self.assertFalse(r["existe"])

    def test_sin_credenciales_no_consulta(self):
        import asyncio
        with patch.object(monitor_ws, "MONITOR_USER", ""), patch.object(monitor_ws, "MONITOR_PASSWORD", ""):
            r = asyncio.run(monitor_ws.consultar_vehiculo_monitor("TRN399"))
        self.assertFalse(r["ok"])
        self.assertTrue(r["sin_credenciales"])

    def test_error_http_no_lanza(self):
        def login(request):
            return httpx.Response(200, json={"token": "T"})

        def consulta(request):
            return httpx.Response(500, json={"error": "boom"})

        transport, _ = self._transport({"login": login, "consulta": consulta})
        import asyncio

        r = asyncio.run(monitor_ws.consultar_vehiculo_monitor("TRN399", transport=transport))
        self.assertFalse(r["ok"])

    def test_login_rechazado_no_lanza(self):
        def login(request):
            return httpx.Response(400, json={"description": "usuario y/o contraseña invalida"})

        def consulta(request):
            raise AssertionError("no debe llegar a consultar")

        transport, _ = self._transport({"login": login, "consulta": consulta})
        import asyncio

        r = asyncio.run(monitor_ws.consultar_vehiculo_monitor("TRN399", transport=transport))
        self.assertFalse(r["ok"])
        self.assertIn("login", r["error"])


# ── Enganche en GET /{cliente_id}/consulta-guia ──────────────────────────────


def _payload_siscore(estado: str) -> dict:
    return {
        "ok": True,
        "exists": True,
        "guia": "999123",
        "data": {"Estado": estado, "NomDes": "CLINICA X", "NumPie": 5},
        "tracking_url": "https://ejemplo/tracking",
    }


class ConsultaGuiaMonitorTests(unittest.TestCase):
    """Parchea el SOAP de Siscore y la fila TMS; valida que la ubicación se
    pida SOLO en guías no entregadas y que un fallo no rompa el endpoint."""

    def setUp(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from rutas import indicadores_cliente

        app = FastAPI()
        app.include_router(indicadores_cliente.router)
        self.client = TestClient(app, raise_server_exceptions=False)
        self.mod = indicadores_cliente

    def _consultar(self, estado_tms: str, estado_siscore: str):
        ws = AsyncMock(return_value=_payload_siscore(estado_siscore))
        tms = MagicMock(return_value={"placa": "TRN399", "estado": estado_tms})
        monitor = AsyncMock(return_value={
            "ok": True, "existe": True,
            "vehiculo": {"placa": "TRN399", "ubicacion": "Vía Sincelejo", "lat": 9.969, "lng": -75.08141},
        })
        with patch("Funciones.siscore_ws_tracking.consultar_guia_ws", ws), \
             patch.object(self.mod, "guia_tms_placa", tms), \
             patch.object(self.mod, "col_novedades_ot") as col_nov, \
             patch.object(self.mod, "col_citas_entrega") as col_cita, \
             patch("Funciones.monitor_ws.consultar_vehiculo_monitor", monitor):
            col_nov.find_one.return_value = None
            col_cita.find_one.return_value = None
            r = self.client.get("/indicadores-cliente/fresenius-kabi/consulta-guia", params={"guia": "999123"})
            monitor_llamado = monitor.await_count > 0
        return r, monitor_llamado

    def test_guia_en_curso_consulta_monitor(self):
        r, llamado = self._consultar(estado_tms="EN DISTRIBUCION", estado_siscore="EN DISTRIBUCION")
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.json()["success"])
        self.assertTrue(llamado)
        self.assertEqual(r.json()["data"]["vehiculo"]["vehiculo"]["ubicacion"], "Vía Sincelejo")

    def test_guia_entregada_NO_consulta_monitor(self):
        for estado in ("ENTREGADO", "ENTREGA DIGITALIZADA"):
            r, llamado = self._consultar(estado_tms=estado, estado_siscore=estado)
            self.assertEqual(r.status_code, 200, estado)
            self.assertFalse(llamado, estado)
            self.assertNotIn("vehiculo", r.json()["data"], estado)

    def test_fallo_monitor_no_rompe_la_consulta(self):
        ws = AsyncMock(return_value=_payload_siscore("EN DISTRIBUCION"))
        tms = MagicMock(return_value={"placa": "TRN399", "estado": "EN DISTRIBUCION"})
        monitor = AsyncMock(side_effect=RuntimeError("Monitor caído"))
        with patch("Funciones.siscore_ws_tracking.consultar_guia_ws", ws), \
             patch.object(self.mod, "guia_tms_placa", tms), \
             patch.object(self.mod, "col_novedades_ot") as col_nov, \
             patch.object(self.mod, "col_citas_entrega") as col_cita, \
             patch("Funciones.monitor_ws.consultar_vehiculo_monitor", monitor):
            col_nov.find_one.return_value = None
            col_cita.find_one.return_value = None
            r = self.client.get("/indicadores-cliente/fresenius-kabi/consulta-guia", params={"guia": "999123"})
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.json()["success"])
        self.assertNotIn("vehiculo", r.json()["data"])

    def test_sin_placa_no_consulta_monitor(self):
        ws = AsyncMock(return_value=_payload_siscore("EN DISTRIBUCION"))
        tms = MagicMock(return_value={"placa": None, "estado": "EN DISTRIBUCION"})
        monitor = AsyncMock()
        with patch("Funciones.siscore_ws_tracking.consultar_guia_ws", ws), \
             patch.object(self.mod, "guia_tms_placa", tms), \
             patch.object(self.mod, "col_novedades_ot") as col_nov, \
             patch.object(self.mod, "col_citas_entrega") as col_cita, \
             patch("Funciones.monitor_ws.consultar_vehiculo_monitor", monitor):
            col_nov.find_one.return_value = None
            col_cita.find_one.return_value = None
            r = self.client.get("/indicadores-cliente/fresenius-kabi/consulta-guia", params={"guia": "999123"})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(monitor.await_count, 0)


if __name__ == "__main__":
    unittest.main()
