"""Tests del flujo tenedor → invitación → conductor vinculado."""
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from bson import ObjectId
from fastapi import HTTPException

from rutas import conductores
from Funciones.claves import crear_hash, verificar_clave


class FakeCursor:
    def __init__(self, items):
        self.items = items

    def limit(self, n):
        return self

    def sort(self, *a, **k):
        return self

    def __iter__(self):
        return iter(self.items)


class FakeColeccion:
    """Colección fake mínima para conductores/vehículos/políticas."""

    def __init__(self, documentos=None):
        self.documents = list(documentos or [])
        self.updates = []

    def find(self, query=None, *a, **k):
        return FakeCursor(list(self.documents))

    def find_one(self, query=None, *a, **k):
        if not query:
            return self.documents[0] if self.documents else None
        for d in self.documents:
            if self._match(d, query):
                return d
        return None

    @staticmethod
    def _match(doc, query):
        for k, v in query.items():
            if k == "_id":
                if doc.get("_id") != v:
                    return False
            elif isinstance(v, dict) and "$regex" in v:
                import re as _re
                if not _re.match(v["$regex"], str(doc.get(k, "")), _re.IGNORECASE if "i" in v.get("$options", "") else 0):
                    return False
            elif doc.get(k) != v:
                return False
        return True

    def insert_one(self, doc):
        doc = dict(doc)
        doc.setdefault("_id", ObjectId())
        self.documents.append(doc)

        class R:
            inserted_id = doc["_id"]
        return R()

    def update_one(self, filtro, cambio):
        self.updates.append((filtro, cambio))
        for d in self.documents:
            if self._match(d, filtro):
                if "$set" in cambio:
                    # Soportar notación punto (invitacionConductor.estado).
                    for k, v in cambio["$set"].items():
                        if "." in k:
                            raiz, hijo = k.split(".", 1)
                            d.setdefault(raiz, {})[hijo] = v
                        else:
                            d[k] = v

    def count_documents(self, query=None):
        return len(self.documents)


class FakeRequest:
    def __init__(self):
        self.client = type("C", (), {"host": "127.0.0.1"})()
        self.headers = {"user-agent": "test-agent"}


def politica_vigente():
    return {
        "_id": ObjectId(), "version": 1, "titulo": "T", "texto_html": "<p>x</p>", "activo": True,
        # Modelo declaraciones (v2): 7 declaraciones como en producción.
        "declaraciones": conductores.DECLARACIONES_V2,
    }


class InvitarConductorTests(unittest.TestCase):
    def setUp(self):
        self.conductores = FakeColeccion()
        self.vehiculos = FakeColeccion([
            {"placa": "ABC123", "idUsuario": "ten-1", "idConductor": None, "invitacionConductor": None}
        ])
        self.politicas = FakeColeccion([politica_vigente()])
        self.aceptaciones = FakeColeccion()

    def _patchear(self):
        return [
            patch.object(conductores, "coleccion_conductores", self.conductores),
            patch.object(conductores, "coleccion_vehiculos", self.vehiculos),
            patch.object(conductores, "coleccion_politicas", self.politicas),
            patch.object(conductores, "coleccion_aceptaciones", self.aceptaciones),
        ]

    def test_invitar_correo_nuevo_crea_stub(self):
        from rutas.conductores import InvitarConductorInput
        datos = InvitarConductorInput(
            id_tenedor="ten-1", placa="ABC123",
            correo_conductor="nuevo@correo.com", nombre_conductor="Luis Pérez",
        )
        import asyncio
        with patch.object(conductores, "_generar_token_verificacion", return_value="token-falso"):
            with ExitStackContext(self._patchear()):
                resultado = asyncio.run(conductores.invitar_conductor(datos, BackgroundTasksFake()))

        self.assertEqual(resultado["estado"], "invitado")
        stub = self.conductores.find_one({"correo": {"$regex": "^nuevo@correo.com$", "$options": "i"}})
        self.assertIsNotNone(stub)
        self.assertFalse(stub["activo"])
        self.assertEqual(stub["invitado_por"], "ten-1")
        # El vehículo quedó con invitación pendiente.
        veh = self.vehiculos.find_one({"placa": "ABC123"})
        self.assertEqual(veh["invitacionConductor"]["estado"], "pendiente")
        self.assertIsNone(veh["idConductor"])

    def test_invitar_cuenta_activa_vincula_directo(self):
        from rutas.conductores import InvitarConductorInput
        self.conductores.documents.append({
            "_id": ObjectId(), "correo": "VIEJO@correo.com", "perfil": "CONDUCTOR",
            "activo": True, "correo_verificado": True, "nombre": "ANA",
        })
        datos = InvitarConductorInput(id_tenedor="ten-1", placa="ABC123", correo_conductor="viejo@correo.com")
        import asyncio
        with ExitStackContext(self._patchear()):
            resultado = asyncio.run(conductores.invitar_conductor(datos, BackgroundTasksFake()))
        self.assertEqual(resultado["estado"], "vinculado")
        veh = self.vehiculos.find_one({"placa": "ABC123"})
        self.assertEqual(veh["invitacionConductor"]["estado"], "aceptada")
        self.assertEqual(veh["idConductor"], str(self.conductores.documents[0]["_id"]))

    def test_invitar_a_tenedor_rechazado(self):
        from rutas.conductores import InvitarConductorInput
        self.conductores.documents.append({
            "_id": ObjectId(), "correo": "JEFE@correo.com", "perfil": "TENEDOR",
            "activo": True, "correo_verificado": True,
        })
        datos = InvitarConductorInput(id_tenedor="ten-1", placa="ABC123", correo_conductor="jefe@correo.com")
        import asyncio
        with ExitStackContext(self._patchear()):
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(conductores.invitar_conductor(datos, BackgroundTasksFake()))
        self.assertEqual(ctx.exception.status_code, 400)

    def test_vehiculo_de_otro_tenedor_rechazado(self):
        from rutas.conductores import InvitarConductorInput
        datos = InvitarConductorInput(id_tenedor="otro", placa="ABC123", correo_conductor="x@y.com")
        import asyncio
        with ExitStackContext(self._patchear()):
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(conductores.invitar_conductor(datos, BackgroundTasksFake()))
        self.assertEqual(ctx.exception.status_code, 403)


class AceptarInvitacionTests(unittest.TestCase):
    def setUp(self):
        self.stub_id = ObjectId()
        self.conductores = FakeColeccion([{
            "_id": self.stub_id, "correo": "NUEVO@correo.com", "perfil": "CONDUCTOR",
            "nombre": "LUIS", "activo": False, "correo_verificado": False,
            "invitado_por": "ten-1",
        }])
        self.vehiculos = FakeColeccion([
            {"placa": "ABC123", "idUsuario": "ten-1", "idConductor": None,
             "invitacionConductor": {"correo": "NUEVO@correo.com", "estado": "pendiente"}},
        ])
        self.politicas = FakeColeccion([politica_vigente()])
        self.aceptaciones = FakeColeccion()

    def _patchear(self):
        return [
            patch.object(conductores, "coleccion_conductores", self.conductores),
            patch.object(conductores, "coleccion_vehiculos", self.vehiculos),
            patch.object(conductores, "coleccion_politicas", self.politicas),
            patch.object(conductores, "coleccion_aceptaciones", self.aceptaciones),
        ]

    def test_aceptar_valida_y_vincula(self):
        from rutas.conductores import AceptarInvitacionInput
        ids_declaraciones = [d["id"] for d in self.politicas.documents[0]["declaraciones"]]
        datos = AceptarInvitacionInput(
            token="tok", placa="ABC123", clave="secreta1", version_politica=1,
            acepta=True, declaraciones_aceptadas=ids_declaraciones,
            celular="3001112233", cedula="1234567890",
        )
        import asyncio
        with patch.object(conductores, "_buscar_conductor_por_token", return_value=self.conductores.documents[0]), \
             ExitStackContext(self._patchear()):
            resultado = asyncio.run(conductores.aceptar_invitacion(datos, FakeRequest()))
        self.assertEqual(resultado["estado"], "aceptada")

        cond = self.conductores.find_one({"_id": self.stub_id})
        self.assertTrue(cond["activo"])
        self.assertTrue(cond["correo_verificado"])
        self.assertEqual(cond["cedula"], "1234567890")
        self.assertTrue(verificar_clave("secreta1", cond["clave"]))

        veh = self.vehiculos.find_one({"placa": "ABC123"})
        self.assertEqual(veh["idConductor"], str(self.stub_id))
        self.assertEqual(veh["invitacionConductor"]["estado"], "aceptada")

        # Evidencia append-only: una por declaración, con canal de invitación.
        self.assertEqual(len(self.aceptaciones.documents), len(ids_declaraciones))
        for evidencia in self.aceptaciones.documents:
            self.assertEqual(evidencia["canal"], "invitacion_tenedor")
            self.assertIn("declaracion_id", evidencia)

    def test_aceptar_sin_aceptar_politica_rechaza(self):
        from rutas.conductores import AceptarInvitacionInput
        datos = AceptarInvitacionInput(
            token="tok", placa="ABC123", clave="secreta1", version_politica=1, acepta=False,
        )
        import asyncio
        with patch.object(conductores, "_buscar_conductor_por_token", return_value=self.conductores.documents[0]), \
             ExitStackContext(self._patchear()):
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(conductores.aceptar_invitacion(datos, FakeRequest()))
        self.assertEqual(ctx.exception.status_code, 400)

    def test_token_invalido_rechaza(self):
        from rutas.conductores import AceptarInvitacionInput
        datos = AceptarInvitacionInput(
            token="malo", placa="ABC123", clave="secreta1", version_politica=1, acepta=True,
        )
        import asyncio
        with patch.object(conductores, "_buscar_conductor_por_token", return_value=None), \
             ExitStackContext(self._patchear()):
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(conductores.aceptar_invitacion(datos, FakeRequest()))
        self.assertEqual(ctx.exception.status_code, 400)

    def test_login_stub_inactivo_rechazado(self):
        """El login de una cuenta stub sin aceptar da 403 con mensaje claro."""
        import asyncio
        doc_stub = dict(self.conductores.documents[0])
        doc_stub["clave"] = crear_hash("loquesea")
        with ExitStackContext([
            patch.object(conductores, "coleccion_conductores", FakeColeccion([doc_stub])),
            patch.object(conductores, "coleccion_baseusuarios", FakeColeccion()),
        ]):
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(conductores.login_conductor(usuario="nuevo@correo.com", clave="loquesea"))
        self.assertEqual(ctx.exception.status_code, 403)
        self.assertIn("pendiente", ctx.exception.detail.lower())


class ExitStackContext:
    """Helper: aplica una lista de patch en un with."""

    def __init__(self, patches):
        from contextlib import ExitStack
        self._stack = ExitStack()
        self._patches = patches

    def __enter__(self):
        for p in self._patches:
            self._stack.enter_context(p)
        return self._stack

    def __exit__(self, *args):
        self._stack.__exit__(*args)


class BackgroundTasksFake:
    """BackgroundTasks que ejecuta la tarea al vuelo (no async, no espera)."""

    def add_task(self, func, *args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception:
            pass


if __name__ == "__main__":
    unittest.main()
