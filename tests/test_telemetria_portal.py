"""Tests del módulo de telemetría de uso del portal (2026-11-19): beacon
POST /telemetria/evento (nunca falla, usuario best-effort) y resumen
GET /telemetria/resumen (solo ADMIN, KPIs derivados)."""
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

from fastapi import FastAPI
from fastapi.testclient import TestClient

from rutas import otros_costos, telemetria
from tests.test_alta_seguridad import FakeColeccion


def cliente_de_prueba(router) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    return TestClient(app, raise_server_exceptions=False)


class ColEventosFake(FakeColeccion):
    """FakeColeccion + aggregate por-pipeline: decide qué canned-result
    devolver mirando el $group _id de cada consulta del /resumen."""

    def __init__(self):
        super().__init__()
        self.pipelines = []

    def create_index(self, *a, **k):
        pass

    def aggregate(self, pipeline):
        self.pipelines.append(pipeline)
        grupo = None
        for etapa in pipeline:
            if "$group" in etapa:
                grupo = etapa["$group"]["_id"]
        if grupo == "$fecha_col":
            return [
                {"_id": "2026-11-18", "eventos": 9, "usr": ["A", "B"]},
                {"_id": "2026-11-19", "eventos": 5, "usr": ["A"]},
            ]
        if grupo == "$usuario":
            return [
                {
                    "_id": "KABI.USER",
                    "nombre": "Usuario Kabi",
                    "perfil": "CLIENTE",
                    "eventos": 12,
                    "dias": ["2026-11-18", "2026-11-19"],
                    "ultima_conexion": "2026-11-19T14:00:00",
                    "evs": ["pageview", "pageview", "consulta_guia"],
                }
            ]
        if grupo == "$evento":
            return [{"_id": "pageview", "total": 10}, {"_id": "consulta_guia", "total": 2}]
        if grupo == "$detalle.pestana":
            return [{"_id": "gerenciales", "total": 8}, {"_id": "trazabilidad", "total": 4}]
        if grupo == "$cliente_id":
            return [{"_id": "fresenius-kabi", "eventos": 12, "usr": ["KABI.USER"]}]
        # (2026-11-25) Telemetría general: por módulo (expresión $ifNull como
        # _id) y por hora del día en Colombia. (9.ª tanda) por_dia/por_hora
        # agrupan por dict {fecha|hora, modulo} — se distinguen por sus claves.
        if isinstance(grupo, dict) and "fecha" in grupo:
            return [
                {"_id": {"fecha": "2026-11-18", "modulo": "OTROS_COSTOS"}, "eventos": 6, "usr": ["A", "B"]},
                {"_id": {"fecha": "2026-11-18", "modulo": "portal_fresenius-kabi"}, "eventos": 3, "usr": ["A"]},
                {"_id": {"fecha": "2026-11-19", "modulo": "OTROS_COSTOS"}, "eventos": 5, "usr": ["A"]},
            ]
        if isinstance(grupo, dict) and "hora" in grupo:
            return [
                {"_id": {"hora": 9, "modulo": "OTROS_COSTOS"}, "eventos": 5, "usr": ["A", "B"]},
                {"_id": {"hora": 9, "modulo": "MODULOS"}, "eventos": 2, "usr": ["A"]},
                {"_id": {"hora": 14, "modulo": "OTROS_COSTOS"}, "eventos": 5, "usr": ["A"]},
            ]
        if isinstance(grupo, dict) and "$ifNull" in grupo:
            return [{"_id": "portal_fresenius-kabi", "eventos": 12, "usr": ["KABI.USER"]}]
        if grupo is None:
            return [{"s": ["ses-1", "ses-2"]}]
        raise AssertionError(f"Pipeline no esperado: {grupo}")


class TestEventoBeacon(unittest.TestCase):
    def setUp(self):
        self.col = ColEventosFake()
        self._respaldo = telemetria.col_eventos
        telemetria.col_eventos = self.col
        otros_costos.col_usuarios = FakeColeccion(
            [{"_id": "u1", "usuario": "ADMIN.X", "perfil": "ADMIN", "nombre": "Admin", "activo": True}]
        )
        self.client = cliente_de_prueba(telemetria.router)

    def tearDown(self):
        telemetria.col_eventos = self._respaldo

    def _beacon(self, **extra):
        cuerpo = {
            "usuario": "ADMIN.X",
            "cliente_id": "fresenius-kabi",
            "modo": "externo",
            "evento": "pageview",
            "detalle": {"pestana": "gerenciales"},
            "sesion_id": "ses-1",
        }
        cuerpo.update(extra)
        return self.client.post("/telemetria/evento", json=cuerpo)

    def test_beacon_usuario_conocido(self):
        r = self._beacon()
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.json()["guardado"])
        doc = self.col.documents[-1]
        self.assertEqual(doc["usuario"], "ADMIN.X")
        self.assertEqual(doc["perfil"], "ADMIN")
        self.assertEqual(doc["nombre"], "Admin")
        self.assertEqual(doc["modo"], "externo")
        self.assertIn("fecha_col", doc)  # YYYY-MM-DD Colombia
        self.assertEqual(len(doc["fecha_col"]), 10)
        self.assertEqual(doc["detalle"], {"pestana": "gerenciales"})

    def test_beacon_usuario_desconocido_no_falla(self):
        r = self._beacon(usuario="desconocido.9")
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.json()["guardado"])
        doc = self.col.documents[-1]
        self.assertEqual(doc["usuario"], "DESCONOCIDO.9")
        self.assertIsNone(doc["perfil"])

    def test_beacon_evento_invalido_se_ignora(self):
        antes = len(self.col.documents)
        r = self._beacon(evento="cosa_rara")
        self.assertEqual(r.status_code, 200)
        self.assertFalse(r.json()["guardado"])
        self.assertEqual(len(self.col.documents), antes)

    def test_beacon_detalle_se_sanea(self):
        r = self._beacon(
            evento="consulta_guia",
            detalle={"guia": 801233440, "x": "y" * 500},
        )
        self.assertTrue(r.json()["guardado"])
        doc = self.col.documents[-1]
        self.assertEqual(doc["detalle"]["guia"], "801233440")
        self.assertEqual(len(doc["detalle"]["x"]), 120)

    def test_beacon_app_general_sin_cliente_id_con_modulo(self):
        """(2026-11-25) Beacon de la app general: sin cliente_id, con módulo;
        modulo se guarda en MAYÚSCULAS y hora_col es un entero 0-23."""
        r = self._beacon(
            cliente_id=None,
            modulo="otros_costos",
            modo="interno",
            evento="accion",
            detalle={"accion": "export_excel"},
        )
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.json()["guardado"])
        doc = self.col.documents[-1]
        self.assertIsNone(doc["cliente_id"])
        self.assertEqual(doc["modulo"], "OTROS_COSTOS")
        self.assertIsInstance(doc["hora_col"], int)
        self.assertTrue(0 <= doc["hora_col"] <= 23)

    def test_beacon_login_y_accion_son_validos(self):
        for evento in ("login", "accion"):
            r = self._beacon(evento=evento)
            self.assertTrue(r.json()["guardado"], evento)


class TestResumen(unittest.TestCase):
    def setUp(self):
        self.col = ColEventosFake()
        self._respaldo = telemetria.col_eventos
        telemetria.col_eventos = self.col
        otros_costos.col_usuarios = FakeColeccion(
            [
                {"_id": "u1", "usuario": "ADMIN.X", "perfil": "ADMIN", "nombre": "Admin", "activo": True},
                {"_id": "u2", "usuario": "KABI.USER", "perfil": "CLIENTE", "nombre": "Kabi", "activo": True},
            ]
        )
        self.client = cliente_de_prueba(telemetria.router)

    def tearDown(self):
        telemetria.col_eventos = self._respaldo

    def test_resumen_solo_admin(self):
        r = self.client.get("/telemetria/resumen", params={"usuario": "KABI.USER"})
        self.assertEqual(r.status_code, 403)
        r = self.client.get("/telemetria/resumen", params={"usuario": "no.existe"})
        self.assertEqual(r.status_code, 401)

    def test_resumen_admin_kpis(self):
        r = self.client.get("/telemetria/resumen", params={"usuario": "ADMIN.X"})
        self.assertEqual(r.status_code, 200)
        data = r.json()["data"]
        self.assertEqual(data["kpi"]["eventos_total"], 12)  # 10 + 2
        self.assertEqual(data["kpi"]["usuarios_unicos"], 1)
        self.assertEqual(data["kpi"]["sesiones_unicas"], 2)
        self.assertEqual(data["kpi"]["dias_activos"], 2)
        u = data["por_usuario"][0]
        self.assertEqual(u["usuario"], "KABI.USER")
        self.assertEqual(u["dias_activos"], 2)
        self.assertEqual(u["por_evento"], {"pageview": 2, "consulta_guia": 1})
        self.assertEqual(data["por_pestana"][0]["pestana"], "gerenciales")
        self.assertEqual(data["por_cliente"][0]["cliente_id"], "fresenius-kabi")
        # (2026-11-25) Agregaciones de la telemetría general
        self.assertEqual(data["por_modulo"][0]["modulo"], "portal_fresenius-kabi")
        self.assertEqual(data["por_modulo"][0]["eventos"], 12)
        self.assertEqual(data["por_hora"][0]["hora"], 9)
        # (9.ª tanda) Desglose por módulo en cada cubeta de día/hora.
        self.assertEqual(data["por_dia"][0]["fecha"], "2026-11-18")
        self.assertEqual(data["por_dia"][0]["eventos"], 9)  # 6 OC + 3 portal
        self.assertEqual(data["por_dia"][0]["por_modulo"], {"OTROS_COSTOS": 6, "portal_fresenius-kabi": 3})
        self.assertEqual(data["por_hora"][0]["por_modulo"], {"OTROS_COSTOS": 5, "MODULOS": 2})
        self.assertIn("modulo", r.json()["rango"])
        self.assertIn("rango", r.json())

    def test_resumen_fecha_invalida(self):
        r = self.client.get(
            "/telemetria/resumen",
            params={"usuario": "ADMIN.X", "desde": "18/11/2026"},
        )
        self.assertEqual(r.status_code, 422)

    def test_resumen_cliente_filtro_llega_al_match(self):
        r = self.client.get(
            "/telemetria/resumen",
            params={"usuario": "ADMIN.X", "cliente_id": "fresenius-kabi"},
        )
        self.assertEqual(r.status_code, 200)
        for pipeline in self.col.pipelines:
            self.assertEqual(pipeline[0]["$match"].get("cliente_id"), "fresenius-kabi")

    def test_resumen_solo_portales_excluye_telemetria_general(self):
        """(2026-11-27) uso-kabi (solo_portales=true) solo ve eventos con
        cliente_id: sin el filtro, «Todos los portales» arrastraba también los
        pageviews/acciones de la telemetría general de la app (cliente_id null)."""
        r = self.client.get(
            "/telemetria/resumen",
            params={"usuario": "ADMIN.X", "solo_portales": "true"},
        )
        self.assertEqual(r.status_code, 200)
        self.assertTrue(self.col.pipelines)
        for pipeline in self.col.pipelines:
            self.assertEqual(pipeline[0]["$match"].get("cliente_id"), {"$ne": None})
        self.assertTrue(r.json()["rango"]["solo_portales"])

    def test_resumen_solo_portales_no_pisa_filtro_de_cliente(self):
        """cliente_id explícito + solo_portales: gana el filtro concreto."""
        self.client.get(
            "/telemetria/resumen",
            params={"usuario": "ADMIN.X", "solo_portales": "true", "cliente_id": "fresenius-kabi"},
        )
        for pipeline in self.col.pipelines:
            self.assertEqual(pipeline[0]["$match"].get("cliente_id"), "fresenius-kabi")

    def test_pipelines_sintaxis_valida_para_mongo(self):
        """Regresión del bug productivo 2026-11-19 (Location28689): el
        acumulador $first SOLO vale en el nivel superior del $group — dentro
        de un _id anidado Mongo lo lee como la expresión $first (pide array).
        Los fakes no ejecutan la pipeline: acá se valida su FORMA."""
        self.client.get("/telemetria/resumen", params={"usuario": "ADMIN.X"})
        self.assertTrue(self.col.pipelines)

        def _sin_acumuladores_dentro(valor, ruta=""):
            if isinstance(valor, dict):
                for k, v in valor.items():
                    if k in ("$first", "$last", "$sum", "$avg", "$max", "$min", "$push", "$addToSet"):
                        # acumulador: su argumento debe ser campo ("$nombre") o expr simple, no otro acumulador
                        if isinstance(v, dict) and any(
                            kk.startswith("$") and kk != "$literal" for kk in v
                        ):
                            raise AssertionError(f"Acumulador {k} anidado en {ruta}: {v}")
                    else:
                        _sin_acumuladores_dentro(v, f"{ruta}.{k}")
            elif isinstance(valor, list):
                for i, item in enumerate(valor):
                    _sin_acumuladores_dentro(item, f"{ruta}[{i}]")

        for pipeline in self.col.pipelines:
            _sin_acumuladores_dentro(pipeline)


if __name__ == "__main__":
    unittest.main()
