import asyncio
import re
import unittest
from unittest.mock import patch

import jwt
from bson import ObjectId

from rutas import baseusuarios
from Funciones.claves import crear_hash, verificar_clave, es_hash


def _matchea_correo(regex, correo_doc):
    """Emula $regex ^...$ con $options i (el backend manda re.escape)."""
    if regex.startswith("^") and regex.endswith("$"):
        return re.fullmatch(regex[1:-1], str(correo_doc), flags=re.IGNORECASE) is not None
    return re.search(regex, str(correo_doc), flags=re.IGNORECASE) is not None


class FakeCollection:
    """Falso MongoCollection que responde queries por usuario, correo e _id."""

    def __init__(self, documents):
        self.documents = documents if isinstance(documents, list) else [documents]

    def find_one(self, query):
        for doc in self.documents:
            if "usuario" in query and query["usuario"] == doc["usuario"]:
                return doc
            if "_id" in query and query["_id"] == doc["_id"]:
                return doc
            if "correo" in query:
                if _matchea_correo(query["correo"].get("$regex", ""), doc.get("correo") or ""):
                    return doc
        return None

    def find(self, query, *args, **kwargs):
        resultado = []
        for doc in self.documents:
            if "correo" in query:
                if _matchea_correo(query["correo"].get("$regex", ""), doc.get("correo") or ""):
                    resultado.append(doc)
        return IteradorFake(resultado)


class IteradorFake:
    def __init__(self, items):
        self.items = items

    def limit(self, n):
        return self

    def sort(self, *args, **kwargs):
        return self

    def __iter__(self):
        return iter(self.items)


class ClavesTests(unittest.TestCase):
    def test_roundtrip_crear_y_verificar(self):
        h = crear_hash("Secreto123")
        self.assertTrue(es_hash(h))
        self.assertTrue(verificar_clave("Secreto123", h))
        self.assertFalse(verificar_clave("OtraClave", h))

    def test_verificar_clave_clara_legacy(self):
        # Pre-migración: clave guardada en claro.
        self.assertTrue(verificar_clave("clave-prueba", "clave-prueba"))
        # Hack legacy: acepta la clave en MAYÚSCULAS.
        self.assertTrue(verificar_clave("CLAVE-PRUEBA", "clave-prueba"))
        # Y en minúsculas (claves guardadas en mayúsculas).
        self.assertTrue(verificar_clave("clave-prueba", "CLAVE-PRUEBA"))
        self.assertFalse(verificar_clave("mala", "clave-prueba"))

    def test_verificar_hash_case_insensitive(self):
        h = crear_hash("CLAVE-GUARDADA")
        self.assertTrue(verificar_clave("clave-guardada", h))
        self.assertTrue(verificar_clave("Clave-Guardada", h))

    def test_crear_hash_rechaza_invalidas(self):
        with self.assertRaises(ValueError):
            crear_hash("   ")
        with self.assertRaises(ValueError):
            crear_hash("x" * 100)


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

    def test_autentica_por_correo_con_clave_clara(self):
        with patch.object(baseusuarios, "coleccion_usuarios", FakeCollection(self.user)):
            encontrado = baseusuarios._buscar_baseusuario_activo("admin@example.com", "clave-prueba")
        self.assertEqual(encontrado["_id"], self.user["_id"])

    def test_autentica_por_correo_con_clave_hasheada(self):
        user = dict(self.user)
        user["clave"] = crear_hash("clave-prueba")
        with patch.object(baseusuarios, "coleccion_usuarios", FakeCollection(user)):
            encontrado = baseusuarios._buscar_baseusuario_activo("ADMIN@EXAMPLE.COM", "clave-prueba")
        self.assertEqual(encontrado["_id"], user["_id"])

    def test_clave_hasheada_rechaza_clave_incorrecta(self):
        user = dict(self.user)
        user["clave"] = crear_hash("clave-prueba")
        with patch.object(baseusuarios, "coleccion_usuarios", FakeCollection(user)):
            encontrado = baseusuarios._buscar_baseusuario_activo("admin@example.com", "incorrecta")
        self.assertIsNone(encontrado)

    def test_usuario_sin_arroba_no_autentica(self):
        # Login estricto por correo: sin @ no busca nada.
        with patch.object(baseusuarios, "coleccion_usuarios", FakeCollection(self.user)):
            encontrado = baseusuarios._buscar_baseusuario_activo("ADMIN_TEST", "clave-prueba")
        self.assertIsNone(encontrado)

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
