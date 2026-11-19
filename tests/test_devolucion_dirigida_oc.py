# -*- coding: utf-8 -*-
"""Tests de la devolución dirigida de Otros Costos (2026-11-19).

Cubre:
- `/devolver` con `devolver_a="tramite"`: la solicitud SE QUEDA en 'aprobado'
  con trámite 'pendiente' (bandeja del Analista) y registra el punto de retorno.
- `/devolver` con `devolver_a="operativo"` (default): estado 'devuelto' como
  siempre, ahora también con punto de retorno (`devolucion_info`).
- `/enviar-aprobacion` con `retorno_directo=True`: restaura el estado/trámite
  que tenía cuando la devolvieron y limpia `devolucion_info`.
- `/enviar-aprobacion` por la cadena completa: limpia `devolucion_info`.
- `/marcar-tramite-vulcano` OK tras una devolución al trámite: limpia la marca.
"""
import asyncio
import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

if "bd.bd_cliente" not in sys.modules:
    _stub = types.ModuleType("bd.bd_cliente")

    class _BDFake(dict):
        def __getitem__(self, clave):
            return self.setdefault(clave, MagicMock())

    _stub.bd_cliente = _BDFake()
    sys.modules["bd.bd_cliente"] = _stub

from fastapi import HTTPException

from rutas import otros_costos
from rutas.otros_costos import (
    DevolverRequest, EnviarAprobacionRequest, MarcarTramiteVulcanoRequest,
)


class ColFake:
    """Colección Mongo mínima: find_one + update_one con $set/$unset/$push."""

    def __init__(self, docs):
        self.docs = list(docs)

    def find_one(self, filtro):
        for d in self.docs:
            if all(d.get(k) == v for k, v in filtro.items()):
                return d
        return None

    def update_one(self, filtro, update):
        doc = self.find_one(filtro)
        if doc is None:
            return SimpleNamespace(matched_count=0)
        if "$set" in update:
            doc.update(update["$set"])
        if "$unset" in update:
            for k in update["$unset"]:
                doc.pop(k, None)
        if "$push" in update:
            for k, v in update["$push"].items():
                doc.setdefault(k, []).append(v)
        return SimpleNamespace(matched_count=1)


class ColUsuariosFake:
    def __init__(self, usuarios):
        self.usuarios = {u["usuario"]: u for u in usuarios}

    def find_one(self, filtro):
        return self.usuarios.get(filtro.get("usuario"))


USUARIOS = [
    {"_id": "f1", "usuario": "FINAN1", "perfil": "FINANCIERO", "nombre": "Financiero Uno", "activo": True},
    {"_id": "c1", "usuario": "CONTROL1", "perfil": "CONTROL", "nombre": "Control Uno", "activo": True},
    {"_id": "o1", "usuario": "OPER1", "perfil": "OPERATIVO", "nombre": "Operativo Uno", "activo": True},
    {"_id": "a1", "usuario": "ANAL1", "perfil": "ANALISTA", "nombre": "Analista Uno", "activo": True},
]


def _doc_aprobado_tramite_ok():
    return {
        "consecutivo": "OC-20261119-0001",
        "estado": "aprobado",
        "tramite_vulcano": "ok",
        "usuario_registro": "OPER1",
        "valor_total": 100000,
    }


def _doc_devuelta_con_retorno():
    return {
        "consecutivo": "OC-20261119-0002",
        "estado": "devuelto",
        "tramite_vulcano": "pendiente",
        "usuario_registro": "OPER1",
        "valor_total": 100000,
        "devolucion_info": {
            "etapa": "operativo",
            "estado_retorno": "aprobado",
            "tramite_retorno": "ok",
            "devuelto_por": {"usuario": "FINAN1", "nombre": "Financiero Uno", "rol": "FINANCIERO"},
            "fecha": "2026-11-19T00:00:00",
            "motivo": "Pedido mal digitado en Vulcano",
        },
        "historial_movimientos": [],
    }


class DevolucionDirigidaTests(unittest.TestCase):

    def setUp(self):
        self._col_activos = otros_costos.col_activos
        self._col_usuarios = otros_costos.col_usuarios
        otros_costos.col_activos = MagicMock()
        otros_costos.col_usuarios = ColUsuariosFake(USUARIOS)
        # Notificaciones y validaciones fuera del alcance de estos tests.
        self._parches = {}
        for nombre in (
            "_notificar_aprobacion", "_notificar_devolucion", "_notificar_tramite_ok",
            "_notificar_envio_aprobacion", "_denegar_fuera_de_alcance", "_validar_solicitud",
        ):
            self._parches[nombre] = getattr(otros_costos, nombre)
            setattr(otros_costos, nombre, MagicMock())

    def tearDown(self):
        otros_costos.col_activos = self._col_activos
        otros_costos.col_usuarios = self._col_usuarios
        for nombre, original in self._parches.items():
            setattr(otros_costos, nombre, original)

    def _montar(self, docs):
        otros_costos.col_activos = ColFake(docs)
        return otros_costos.col_activos.docs[0]

    # ── /devolver ────────────────────────────────────────────────────────────

    def test_financiero_devuelve_al_tramite_del_analista(self):
        doc = self._montar([_doc_aprobado_tramite_ok()])
        res = asyncio.run(otros_costos.devolver_solicitud(
            DevolverRequest(consecutivo=doc["consecutivo"], usuario="FINAN1",
                            observacion="El registro en Vulcano quedó mal",
                            devolver_a="tramite"), None))
        self.assertEqual(res["estado"], "aprobado")
        self.assertEqual(res["devuelto_a"], "tramite")
        # La solicitud queda en la bandeja del Analista: trámite pendiente.
        self.assertEqual(doc["estado"], "aprobado")
        self.assertEqual(doc["tramite_vulcano"], "pendiente")
        # Punto de retorno: dónde estaba y quién la devolvió.
        info = doc["devolucion_info"]
        self.assertEqual(info["etapa"], "tramite")
        self.assertEqual(info["estado_retorno"], "aprobado")
        self.assertEqual(info["tramite_retorno"], "ok")
        self.assertEqual(info["devuelto_por"]["usuario"], "FINAN1")
        movs = doc["historial_movimientos"]
        self.assertEqual(movs[-1]["accion"], "devolucion")
        self.assertEqual(movs[-1]["devuelto_a"], "tramite")

    def test_devolver_al_tramite_solo_desde_bandeja_financiero(self):
        doc = self._montar([{"consecutivo": "OC-1", "estado": "pendiente_aprobacion",
                             "tramite_vulcano": "pendiente"}])
        with self.assertRaises(HTTPException) as ctx:
            asyncio.run(otros_costos.devolver_solicitud(
                DevolverRequest(consecutivo="OC-1", usuario="CONTROL1",
                                observacion="motivo", devolver_a="tramite"), None))
        self.assertEqual(ctx.exception.status_code, 422)

    def test_devolver_al_operativo_registra_punto_de_retorno(self):
        doc = self._montar([_doc_aprobado_tramite_ok()])
        res = asyncio.run(otros_costos.devolver_solicitud(
            DevolverRequest(consecutivo=doc["consecutivo"], usuario="FINAN1",
                            observacion="El valor del concepto está mal"), None))
        self.assertEqual(res["estado"], "devuelto")
        self.assertEqual(doc["estado"], "devuelto")
        self.assertEqual(doc["tramite_vulcano"], "pendiente")
        info = doc["devolucion_info"]
        self.assertEqual(info["etapa"], "operativo")
        self.assertEqual(info["estado_retorno"], "aprobado")
        self.assertEqual(info["tramite_retorno"], "ok")
        self.assertEqual(doc["historial_movimientos"][-1]["devuelto_a"], "operativo")

    # ── /enviar-aprobacion ───────────────────────────────────────────────────

    def test_retorno_directo_restaura_la_bandeja_de_financiero(self):
        doc = self._montar([_doc_devuelta_con_retorno()])
        res = asyncio.run(otros_costos.enviar_aprobacion(
            EnviarAprobacionRequest(consecutivo=doc["consecutivo"], usuario="OPER1",
                                    observacion="Corregido el pedido", retorno_directo=True), None))
        self.assertEqual(res["estado"], "aprobado")
        self.assertEqual(doc["estado"], "aprobado")
        self.assertEqual(doc["tramite_vulcano"], "ok")
        # La marca de devolución se consume: futuros envíos van por la cadena.
        self.assertNotIn("devolucion_info", doc)
        movs = doc["historial_movimientos"]
        self.assertEqual(movs[-1]["accion"], "retorno_directo")
        self.assertEqual(movs[-1]["estado_anterior"], "devuelto")
        self.assertEqual(movs[-1]["estado_nuevo"], "aprobado")

    def test_enviar_por_la_cadena_limpia_el_punto_de_retorno(self):
        doc = self._montar([_doc_devuelta_con_retorno()])
        res = asyncio.run(otros_costos.enviar_aprobacion(
            EnviarAprobacionRequest(consecutivo=doc["consecutivo"], usuario="OPER1",
                                    observacion="", retorno_directo=False), None))
        self.assertEqual(res["estado"], "pendiente_aprobacion")
        self.assertEqual(doc["estado"], "pendiente_aprobacion")
        self.assertNotIn("devolucion_info", doc)
        self.assertEqual(doc["historial_movimientos"][-1]["accion"], "envio_aprobacion")

    def test_retorno_directo_sin_devolucion_previa_es_422(self):
        doc = self._montar([{"consecutivo": "OC-3", "estado": "devuelto",
                             "tramite_vulcano": "pendiente", "historial_movimientos": []}])
        with self.assertRaises(HTTPException) as ctx:
            asyncio.run(otros_costos.enviar_aprobacion(
                EnviarAprobacionRequest(consecutivo="OC-3", usuario="OPER1",
                                        observacion="", retorno_directo=True), None))
        self.assertEqual(ctx.exception.status_code, 422)

    # ── /marcar-tramite-vulcano ──────────────────────────────────────────────

    def test_marcar_tramite_ok_consuma_la_devolucion_al_tramite(self):
        doc = self._montar([{
            "consecutivo": "OC-4", "estado": "aprobado", "tramite_vulcano": "pendiente",
            "devolucion_info": {"etapa": "tramite", "estado_retorno": "aprobado",
                                "tramite_retorno": "ok"}, "historial_movimientos": [],
        }])
        asyncio.run(otros_costos.marcar_tramite_vulcano(
            MarcarTramiteVulcanoRequest(consecutivo="OC-4", usuario="ANAL1",
                                        tramite_vulcano="ok"), None))
        self.assertEqual(doc["tramite_vulcano"], "ok")
        self.assertNotIn("devolucion_info", doc)


if __name__ == "__main__":
    unittest.main()
