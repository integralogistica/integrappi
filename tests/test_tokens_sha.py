# -*- coding: utf-8 -*-
"""Tests del fix CPU 2026-10-08: lookup de tokens por SHA-256 (O(1)) en vez
del scan de hasta 200 candidatos con bcrypt (~0,4 s de CPU c/u → picos del
100% en Render por cada GET /verificar-correo con 68 tokens pendientes).

Cubre: generación con sha, fast-path sin gastar bcrypt, tokens vencidos/usados,
fallback legacy (solo bcrypt) y verificación sin variantes de case."""
import re
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

if "bd.bd_cliente" not in sys.modules:
    _stub = types.ModuleType("bd.bd_cliente")

    class _BDFake(dict):
        def __getitem__(self, clave):
            return self.setdefault(clave, MagicMock())

    _stub.bd_cliente = _BDFake()
    sys.modules["bd.bd_cliente"] = _stub

from rutas import conductores
from Funciones.claves import crear_hash, hash_token, verificar_token_hash


class _CursorFake(list):
    def limit(self, _n):
        return self


class FakeCol:
    """Colección Mongo mínima: igualdad, $ne/$exists/$gt, insert/update one."""

    def __init__(self, documentos=None):
        self.documents = list(documentos or [])
        self.contador = 0

    @staticmethod
    def _match(d, cond):
        for campo, esperado in (cond or {}).items():
            actual = d.get(campo)
            if isinstance(esperado, dict):
                if "$exists" in esperado and (actual is not None) != esperado["$exists"]:
                    return False
                if "$ne" in esperado and actual == esperado["$ne"]:
                    return False
                if "$gt" in esperado and not (actual is not None and actual > esperado["$gt"]):
                    return False
            elif actual != esperado:
                return False
        return True

    def find(self, filtro=None, *a, **k):
        return _CursorFake([d for d in self.documents if self._match(d, filtro)])

    def find_one(self, query=None, *a, **k):
        for d in self.documents:
            if self._match(d, query or {}):
                return d
        return None

    def insert_one(self, doc):
        self.contador += 1
        doc = dict(doc)
        doc.setdefault("_id", f"id-{self.contador}")
        self.documents.append(doc)
        return MagicMock(inserted_id=doc["_id"])

    def update_one(self, filtro, cambio, upsert=False):
        for d in self.documents:
            if self._match(d, filtro):
                d.update(cambio.get("$set") or {})
                return MagicMock(modified_count=1)
        if upsert:
            nuevo = dict(filtro)
            nuevo.update(cambio.get("$set") or {})
            self.documents.append(nuevo)
        return MagicMock(modified_count=0)


def _doc_verificacion(token_plano, **extra):
    """Doc de conductor con token de verificación NUEVO (hash + sha)."""
    doc = {
        "_id": "c-1", "correo": "X@Y.CO", "perfil": "CONDUCTOR",
        "verificacion_token_hash": crear_hash(token_plano),
        "verificacion_token_sha": hash_token(token_plano),
        "verificacion_expira": datetime.now(timezone.utc) + timedelta(hours=48),
    }
    doc.update(extra)
    return doc


class TokensShaTests(unittest.TestCase):
    """Fast-path SHA-256 del token de VERIFICACIÓN de correo."""

    def setUp(self):
        self.cuentas = FakeCol()
        self.parches = [
            patch.object(conductores, "coleccion_conductores", self.cuentas),
        ]
        for p in self.parches:
            p.start()
        self.addCleanup(lambda: [p.stop() for p in self.parches])

    def test_generar_token_persiste_sha(self):
        self.cuentas.documents.append({"_id": "c-1", "correo": "X@Y.CO"})
        token = conductores._generar_token_verificacion("c-1")
        doc = self.cuentas.find_one({"_id": "c-1"})
        self.assertEqual(doc["verificacion_token_sha"], hash_token(token))
        # El bcrypt sigue ahí (compat con el fallback si el sha se perdiera).
        self.assertTrue(doc["verificacion_token_hash"].startswith("$2"))

    def test_fast_path_encuentra_sin_bcrypt(self):
        token = "abc-def_ghi123"
        self.cuentas.documents.append(_doc_verificacion(token))
        with patch.object(conductores, "verificar_token_hash",
                          side_effect=AssertionError("no debia llamar bcrypt")):
            doc = conductores._buscar_conductor_por_token(token)
        self.assertIsNotNone(doc)
        self.assertEqual(doc["correo"], "X@Y.CO")

    def test_sha_vencido_no_cae_al_scan(self):
        token = "vencido-token"
        self.cuentas.documents.append(_doc_verificacion(
            token, verificacion_expira=datetime.now(timezone.utc) - timedelta(hours=1)))
        with patch.object(conductores, "verificar_token_hash",
                          side_effect=AssertionError("no debia llamar bcrypt")):
            self.assertIsNone(conductores._buscar_conductor_por_token(token))

    def test_legacy_solo_bcrypt_sigue_funcionando(self):
        token = "legacy-token-xyz"
        self.cuentas.documents.append({
            "_id": "c-2", "correo": "VIEJO@Y.CO",
            "verificacion_token_hash": crear_hash(token),  # SIN sha (pre-fix)
            "verificacion_expira": datetime.now(timezone.utc) + timedelta(hours=48),
        })
        doc = conductores._buscar_conductor_por_token(token)
        self.assertIsNotNone(doc)
        self.assertEqual(doc["correo"], "VIEJO@Y.CO")

    def test_token_invalido_none(self):
        self.cuentas.documents.append(_doc_verificacion("otro-token"))
        self.assertIsNone(conductores._buscar_conductor_por_token("no-existe"))


class TokensAutorizacionShaTests(unittest.TestCase):
    """Fast-path SHA-256 de los tokens de AUTORIZACIÓN de datos."""

    def setUp(self):
        self.tokens = FakeCol()
        self.cuentas = FakeCol()
        self.aceptaciones = FakeCol()
        self.parches = [
            patch.object(conductores, "coleccion_tokens_aut", self.tokens),
            patch.object(conductores, "coleccion_conductores", self.cuentas),
            patch.object(conductores, "coleccion_aceptaciones", self.aceptaciones),
        ]
        for p in self.parches:
            p.start()
        self.addCleanup(lambda: [p.stop() for p in self.parches])

    def _nuevo_token(self, **extra):
        token_plano = "aut-token-123"
        doc = {
            "_id": "t-1", "cedula": "1010", "correo": "P@Q.CO", "placa": "ABC123",
            "token_hash": crear_hash(token_plano),
            "token_sha": hash_token(token_plano),
            "creado_en": datetime.utcnow(),
            "expira": datetime.utcnow() + timedelta(hours=720),
            "solicitado_por": "seguridad",
        }
        doc.update(extra)
        return token_plano, doc

    def test_solicitar_inserta_sha(self):
        with patch.object(conductores, "enviar_correo_autorizacion"):
            conductores._solicitar_autorizacion("ABC123", "1010", "p@q.co", "Perico")
        doc = self.tokens.find_one({"cedula": "1010"})
        self.assertIn("token_sha", doc)
        self.assertTrue(doc["token_hash"].startswith("$2"))

    def test_fast_path_encuentra_sin_bcrypt(self):
        token, doc = self._nuevo_token()
        self.tokens.documents.append(doc)
        with patch.object(conductores, "verificar_token_hash",
                          side_effect=AssertionError("no debia llamar bcrypt")):
            encontrado = conductores._buscar_token_aut(token)
        self.assertIsNotNone(encontrado)

    def test_usado_o_vencido_none(self):
        token, doc = self._nuevo_token(usado_en=datetime.utcnow())
        self.tokens.documents.append(doc)
        with patch.object(conductores, "verificar_token_hash",
                          side_effect=AssertionError("no debia llamar bcrypt")):
            self.assertIsNone(conductores._buscar_token_aut(token))

        token2, doc2 = self._nuevo_token(expira=datetime.utcnow() - timedelta(hours=1))
        self.tokens.documents.append(doc2)
        self.assertIsNone(conductores._buscar_token_aut(token2))

    def test_legacy_solo_bcrypt_sigue_funcionando(self):
        token = "aut-legacy"
        self.tokens.documents.append({
            "_id": "t-2", "cedula": "2020", "token_hash": crear_hash(token),  # SIN sha
            "creado_en": datetime.utcnow(),
            "expira": datetime.utcnow() + timedelta(hours=720),
        })
        self.assertIsNotNone(conductores._buscar_token_aut(token))


class VerificarTokenHashTests(unittest.TestCase):
    """Helper de claves.py: UNA verificación, sin variantes de case."""

    def test_bcrypt_match_y_no_match(self):
        h = crear_hash("MiToken_123")
        self.assertTrue(verificar_token_hash("MiToken_123", h))
        self.assertFalse(verificar_token_hash("MiToken_124", h))
        # La variante en mayúsculas NO matchea (comportamiento token, no clave).
        self.assertFalse(verificar_token_hash("MITOKEN_123", h))

    def test_valor_claro_legacy(self):
        self.assertTrue(verificar_token_hash("tok", "tok"))
        self.assertFalse(verificar_token_hash("tok", "otro"))

    def test_hash_token_deterministico(self):
        self.assertEqual(hash_token("a"), hash_token("a"))
        self.assertNotEqual(hash_token("a"), hash_token("b"))
        self.assertEqual(len(hash_token("x")), 64)


if __name__ == "__main__":
    unittest.main()
