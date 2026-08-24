import asyncio
import unittest
from unittest.mock import patch

import jwt
from bson import ObjectId

from rutas import baseusuarios


class FakeCollection:
    def __init__(self, document):
        self.document = document

    def find_one(self, query):
        if "usuario" in query and query["usuario"] == self.document["usuario"]:
            return self.document
        if "_id" in query and query["_id"] == self.document["_id"]:
            return self.document
        return None


class BaseUsuariosAuthTests(unittest.TestCase):
    def setUp(self):
        self.user = {
            "_id": ObjectId(),
            "nombre": "Administrador",
            "email": "admin@example.com",
            "correo": "admin@example.com",
            "tenedor": "1",
            "telefono": "1",
            "celular": "1",
            "regional": "CENTRO",
            "clientes": ["KABI"],
            "usuario": "ADMIN_TEST",
            "clave": "clave-prueba",
            "perfil": "ADMIN",
            "activo": True,
        }

    def test_autentica_el_mismo_usuario_de_baseusuarios(self):
        with patch.object(baseusuarios, "coleccion_usuarios", FakeCollection(self.user)):
            encontrado = baseusuarios._buscar_baseusuario_activo("admin_test", "clave-prueba")
        self.assertEqual(encontrado["_id"], self.user["_id"])

    def test_token_identifica_fuente_y_perfil(self):
        token = baseusuarios._crear_token_baseusuario(self.user)
        payload = jwt.decode(
            token,
            baseusuarios.BASEUSUARIOS_JWT_SECRET,
            algorithms=[baseusuarios.BASEUSUARIOS_JWT_ALGORITHM],
        )
        self.assertEqual(payload["auth_source"], "baseusuarios")
        self.assertEqual(payload["perfil"], "ADMIN")
        self.assertEqual(payload["sub"], str(self.user["_id"]))

    def test_dependencia_recupera_usuario_activo(self):
        token = baseusuarios._crear_token_baseusuario(self.user)
        with patch.object(baseusuarios, "coleccion_usuarios", FakeCollection(self.user)):
            actual = asyncio.run(baseusuarios.obtener_baseusuario_actual(token))
        self.assertEqual(actual["usuario"], "ADMIN_TEST")
        self.assertEqual(actual["perfil"], "ADMIN")


if __name__ == "__main__":
    unittest.main()
