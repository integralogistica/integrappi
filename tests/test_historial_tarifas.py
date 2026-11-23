"""Auditoría de tarifarios: historial_tarifas (2026-11-23).

Cubre: resolución de identidad con perfil real, guard ADMIN/CONTROL en los
endpoints de escritura de /fletes y /fletes-rutas-fmc, diff antes→después,
registro de eventos (creacion/edicion/eliminacion/carga_masiva) y el
listado GET /historial-tarifas.
"""
import sys
import types
import unittest
from unittest.mock import MagicMock

# Stub de BD (patrón del repo): correr standalone sin Atlas.
if "bd.bd_cliente" not in sys.modules:
    _stub = types.ModuleType("bd.bd_cliente")

    class _BDFake(dict):
        def __getitem__(self, clave):
            return self.setdefault(clave, MagicMock())

    _stub.bd_cliente = _BDFake()
    sys.modules["bd.bd_cliente"] = _stub

from fastapi import HTTPException
from fastapi import FastAPI
from fastapi.testclient import TestClient

from rutas import historial_tarifas as ht
from rutas import fletes, tarifas_rutas_fmc


def _cliente_de(router) -> TestClient:
    """TestClient sobre una FastAPI app (el handler 422 de validación vive
    en la app, no en el router pelón)."""
    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


USUARIO_CONTROL = {
    "usuario": "JCONTROL",
    "perfil": "CONTROL",
    "regional": "FUNZA",
    "nombre": "Juan Control",
    "activo": True,
    "_id": "idc123",
}
USUARIO_COORDINADOR = {**USUARIO_CONTROL, "usuario": "CCOORD", "perfil": "COORDINADOR"}


class FakeFind:
    """find_one/find sobre una lista de docs en memoria."""

    def __init__(self, docs):
        self.docs = docs
        self.insertados = []

    def find_one(self, filtro):
        for d in self.docs:
            if all(d.get(k) == v for k, v in filtro.items()):
                return d
        return None

    def find(self, filtro=None):
        return list(self.docs)

    def insert_one(self, doc):
        self.insertados.append(doc)
        class R: inserted_id = "nuevo_id_123"
        return R()

    def delete_one(self, filtro):
        for i, d in enumerate(self.docs):
            if all(d.get(k) == v for k, v in filtro.items()):
                del self.docs[i]
                class R: deleted_count = 1
                return R()
        class R0: deleted_count = 0
        return R0()

    def update_one(self, filtro, update):
        # Como pymongo: reporta matched_count sin mutar la copia local que
        # ya leyó el endpoint (el diff antes→después se calcula sobre ella).
        doc = self.find_one(filtro)
        class R:
            matched_count = 1 if doc else 0
        return R()


class ResolverUsuarioTests(unittest.TestCase):

    def test_sin_usuario_da_401(self):
        with self.assertRaises(HTTPException) as ctx:
            ht._resolver_usuario("")
        self.assertEqual(ctx.exception.status_code, 401)

    def test_usuario_inexistente_da_401(self):
        col = MagicMock()
        col.find_one.return_value = None
        ht.col_usuarios = col
        with self.assertRaises(HTTPException) as ctx:
            ht._resolver_usuario("NADIE")
        self.assertEqual(ctx.exception.status_code, 401)

    def test_usuario_inactivo_da_403(self):
        col = MagicMock()
        col.find_one.return_value = {**USUARIO_CONTROL, "activo": False}
        ht.col_usuarios = col
        with self.assertRaises(HTTPException) as ctx:
            ht._resolver_usuario("JCONTROL")
        self.assertEqual(ctx.exception.status_code, 403)

    def test_usuario_ok_devuelve_perfil_upper(self):
        col = MagicMock()
        col.find_one.return_value = {**USUARIO_CONTROL, "perfil": "control"}
        ht.col_usuarios = col
        info = ht._resolver_usuario("jcontrol")
        self.assertEqual(info["perfil"], "CONTROL")
        self.assertEqual(info["nombre"], "Juan Control")


class RequiereTests(unittest.TestCase):

    def test_perfil_fuera_del_conjunto_da_403(self):
        with self.assertRaises(HTTPException) as ctx:
            ht._requiere({"perfil": "COORDINADOR"}, ht.PERFILES_EDICION_TARIFAS, "editar tarifas")
        self.assertEqual(ctx.exception.status_code, 403)
        self.assertIn("COORDINADOR", ctx.exception.detail)

    def test_admin_y_control_pasan(self):
        for perfil in ("ADMIN", "CONTROL"):
            ht._requiere({"perfil": perfil}, ht.PERFILES_EDICION_TARIFAS, "editar tarifas")


class DiffCamposTests(unittest.TestCase):

    def test_campo_cambiado(self):
        cambios = ht._diff_campos({"turbo": 850000.0}, {"turbo": 900000})
        self.assertEqual(cambios, [{"campo": "turbo", "valor_anterior": 850000, "valor_nuevo": 900000}])

    def test_campo_igual_no_aparece(self):
        self.assertEqual(ht._diff_campos({"turbo": 850000}, {"turbo": 850000.0}), [])

    def test_campo_nuevo_y_eliminado(self):
        cambios = ht._diff_campos({"a": 1}, {"b": 2})
        self.assertEqual(
            cambios,
            [
                {"campo": "a", "valor_anterior": 1, "valor_nuevo": None},
                {"campo": "b", "valor_anterior": None, "valor_nuevo": 2},
            ],
        )

    def test_id_de_mongo_no_se_reporta_como_cambio(self):
        anterior = {"_id": "abc123", "tarifas": {"TURBO": 1}}
        nuevo = {"tarifas": {"TURBO": 1}}
        self.assertEqual(ht._diff_campos(anterior, nuevo), [])

    def test_dict_anidado_tarifas(self):
        cambios = ht._diff_campos(
            {"tarifas": {"TURBO": 1.0, "NHR": 2.0}},
            {"tarifas": {"TURBO": 3.0, "NHR": 2.0}},
        )
        self.assertEqual(len(cambios), 1)
        self.assertEqual(cambios[0]["campo"], "tarifas")
        self.assertEqual(cambios[0]["valor_anterior"], {"TURBO": 1, "NHR": 2})
        self.assertEqual(cambios[0]["valor_nuevo"], {"TURBO": 3, "NHR": 2})


class RegistrarEventoTests(unittest.TestCase):

    def test_inserta_documento_correcto(self):
        col = MagicMock()
        ht.col_historial = col
        ht._registrar_evento(
            accion="edicion",
            tarifario="fletes",
            registro="A → B",
            info={"usuario": "JCONTROL", "nombre": "Juan Control", "perfil": "CONTROL"},
            campos_modificados=[{"campo": "turbo", "valor_anterior": 1, "valor_nuevo": 2}],
        )
        doc = col.insert_one.call_args[0][0]
        self.assertEqual(doc["accion"], "edicion")
        self.assertEqual(doc["tarifario"], "fletes")
        self.assertEqual(doc["usuario"], "JCONTROL")
        self.assertEqual(doc["rol"], "CONTROL")
        self.assertTrue(doc["fecha"].endswith("Z"))
        self.assertEqual(doc["campos_modificados"][0]["campo"], "turbo")

    def test_fallo_de_insert_no_rompe_flujo(self):
        col = MagicMock()
        col.insert_one.side_effect = Exception("Atlas caído")
        ht.col_historial = col
        ht._registrar_evento("creacion", "fletes", "A → B", {"usuario": "X", "perfil": "ADMIN"})


class EndpointFletesTests(unittest.TestCase):

    def setUp(self):
        self.col_fletes = FakeFind([
            {"origen": "FUNZA", "destino": "CALI", "ruta": "ANACIONAL", "tipo": "NACIONAL",
             "pago_cargue_desc": "NO", "equivalencia_centro_costo": "FUNZA",
             "tarifas": {"TURBO": 850000.0}, "promesa_entrega_dias": 1},
        ])
        self.col_historial = FakeFind([])
        fletes.coleccion_fletes = self.col_fletes
        ht.col_historial = self.col_historial
        self.client = _cliente_de(fletes.ruta_fletes)

    def _login(self, perfil="CONTROL"):
        col = MagicMock()
        col.find_one.return_value = {**USUARIO_CONTROL, "perfil": perfil}
        ht.col_usuarios = col

    def test_put_sin_usuario_da_422(self):
        r = self.client.put("/fletes/FUNZA/CALI", json={})
        self.assertEqual(r.status_code, 422)

    def test_put_con_control_registra_edicion(self):
        self._login("CONTROL")
        payload = {
            "origen": "FUNZA", "destino": "CALI", "ruta": "ANACIONAL", "tipo": "NACIONAL",
            "pago_cargue_desc": "NO", "equivalencia_centro_costo": "FUNZA",
            "tarifas": {"TURBO": 900000}, "promesa_entrega_dias": 1,
        }
        r = self.client.put("/fletes/FUNZA/CALI?usuario=JCONTROL", json=payload)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(len(self.col_historial.insertados), 1)
        evento = self.col_historial.insertados[0]
        self.assertEqual(evento["accion"], "edicion")
        self.assertEqual(evento["rol"], "CONTROL")
        self.assertEqual(
            evento["campos_modificados"],
            [{"campo": "tarifas", "valor_anterior": {"TURBO": 850000}, "valor_nuevo": {"TURBO": 900000}}],
        )

    def test_put_con_coordinador_da_403(self):
        self._login("COORDINADOR")
        payload = {
            "origen": "FUNZA", "destino": "CALI", "ruta": "ANACIONAL", "tipo": "NACIONAL",
            "pago_cargue_desc": "NO", "equivalencia_centro_costo": "FUNZA",
            "tarifas": {"TURBO": 900000}, "promesa_entrega_dias": 1,
        }
        r = self.client.put("/fletes/FUNZA/CALI?usuario=JCONTROL", json=payload)
        self.assertEqual(r.status_code, 403)
        self.assertEqual(self.col_historial.insertados, [])

    def test_delete_registra_snapshot(self):
        self._login("CONTROL")
        r = self.client.delete("/fletes/FUNZA/CALI?usuario=JCONTROL")
        self.assertEqual(r.status_code, 200)
        evento = self.col_historial.insertados[0]
        self.assertEqual(evento["accion"], "eliminacion")
        self.assertEqual(evento["snapshot"]["destino"], "CALI")

    def test_post_crear_registra_creacion(self):
        self._login("CONTROL")
        payload = {
            "origen": "BOGOTA", "destino": "MEDELLIN", "ruta": "ANACIONAL", "tipo": "NACIONAL",
            "pago_cargue_desc": "NO", "equivalencia_centro_costo": "BOGOTA",
            "tarifas": {"TURBO": 1000000}, "promesa_entrega_dias": 2,
        }
        r = self.client.post("/fletes/?usuario=JCONTROL", json=payload)
        self.assertEqual(r.status_code, 201)
        evento = self.col_historial.insertados[0]
        self.assertEqual(evento["accion"], "creacion")
        self.assertEqual(evento["registro"], "BOGOTA → MEDELLIN")


class EndpointHistorialTests(unittest.TestCase):

    def setUp(self):
        self.col_historial = MagicMock()
        self.col_historial.find.return_value.sort.return_value.limit.return_value = iter([])
        ht.col_historial = self.col_historial
        self.client = _cliente_de(ht.ruta_historial_tarifas)

    def test_sin_usuario_da_422(self):
        r = self.client.get("/historial-tarifas/")
        self.assertEqual(r.status_code, 422)

    def test_perfil_sin_permiso_da_403(self):
        col = MagicMock()
        col.find_one.return_value = USUARIO_COORDINADOR
        ht.col_usuarios = col
        r = self.client.get("/historial-tarifas/?usuario=CCOORD")
        self.assertEqual(r.status_code, 403)

    def test_con_permiso_devuelve_lista_descendente(self):
        col = MagicMock()
        col.find_one.return_value = USUARIO_CONTROL
        ht.col_usuarios = col
        r = self.client.get("/historial-tarifas/?usuario=JCONTROL")
        self.assertEqual(r.status_code, 200)
        self.col_historial.find.assert_called_once_with({})
        self.col_historial.find.return_value.sort.assert_called_once_with("fecha", -1)

    def test_tarifario_invalido_da_400(self):
        col = MagicMock()
        col.find_one.return_value = USUARIO_CONTROL
        ht.col_usuarios = col
        r = self.client.get("/historial-tarifas/?usuario=JCONTROL&tarifario=otra_cosa")
        self.assertEqual(r.status_code, 400)

    def test_filtro_por_registro(self):
        col = MagicMock()
        col.find_one.return_value = USUARIO_CONTROL
        ht.col_usuarios = col
        r = self.client.get("/historial-tarifas/?usuario=JCONTROL&registro=FUNZA%20%E2%86%92%20CALI")
        self.assertEqual(r.status_code, 200)
        self.col_historial.find.assert_called_once_with({"registro": "FUNZA → CALI"})

    def test_filtro_registro_con_masivas_requiere_tarifario(self):
        col = MagicMock()
        col.find_one.return_value = USUARIO_CONTROL
        ht.col_usuarios = col
        r = self.client.get("/historial-tarifas/?usuario=JCONTROL&registro=X&incluir_masivas=true")
        self.assertEqual(r.status_code, 400)

    def test_filtro_registro_con_masivas(self):
        col = MagicMock()
        col.find_one.return_value = USUARIO_CONTROL
        ht.col_usuarios = col
        r = self.client.get("/historial-tarifas/?usuario=JCONTROL&tarifario=fletes&registro=X&incluir_masivas=true")
        self.assertEqual(r.status_code, 200)
        esperado = {"$or": [
            {"registro": "X", "tarifario": "fletes"},
            {"accion": "carga_masiva", "tarifario": "fletes"},
        ]}
        self.col_historial.find.assert_called_once_with(esperado)

    def test_accion_invalida_da_400(self):
        col = MagicMock()
        col.find_one.return_value = USUARIO_CONTROL
        ht.col_usuarios = col
        r = self.client.get("/historial-tarifas/?usuario=JCONTROL&accion=otra")
        self.assertEqual(r.status_code, 400)


if __name__ == "__main__":
    unittest.main()
