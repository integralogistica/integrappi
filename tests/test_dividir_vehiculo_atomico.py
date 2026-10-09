# Tests de atomicidad de /pedidos/dividir-vehiculo.
#
# Contexto (2026-10-09): la división movía los docs hacia el carro B ANTES del
# recálculo (_calc), y si _calc fallaba (p.ej. tarifa faltante para el tipo
# resultante) el carro quedaba "a medias": docs movidos, totales del vehículo
# sin repartir (kg reales/cajas del original en ambos carros → % uso absurdo,
# caso CELTA-20261006-M-2026106-EJE CAFETERO-1/1B al 199%). Ahora todo write
# se registra y _rollback() revierte movimientos, splits y clones.
import asyncio
import unittest
from unittest.mock import patch

from bson import ObjectId
from fastapi import HTTPException

from rutas import pedidos as mod
from rutas.pedidos import DividirHastaTresPayload, GrupoDivision, dividir_vehiculo


class FakeCollection:
    """MongoCollection en memoria: igualdades + _id $in + $set."""

    def __init__(self, docs=None):
        self.docs = list(docs or [])

    @staticmethod
    def _match(doc, query):
        for k, v in query.items():
            if isinstance(v, dict) and "$in" in v:
                if doc.get(k) not in v["$in"]:
                    return False
            elif doc.get(k) != v:
                return False
        return True

    def find(self, query=None, projection=None):
        query = query or {}
        res = [dict(d) for d in self.docs if self._match(d, query)]
        if projection:
            res = [{k: d[k] for k in projection if k in d} for d in res]
        return res

    def find_one(self, query=None):
        for d in self.docs:
            if self._match(d, query or {}):
                return d
        return None

    def insert_one(self, doc):
        # Como PyMongo: muta el doc pasado asignándole _id
        doc.setdefault("_id", ObjectId())
        self.docs.append(doc)
        return type("R", (), {"inserted_id": doc["_id"]})()

    def update_many(self, query, update):
        n = 0
        for d in self.docs:
            if self._match(d, query or {}):
                d.update(update.get("$set", {}))
                n += 1
        return type("R", (), {"modified_count": n})()

    def update_one(self, query, update):
        return self.update_many(query, update)

    def delete_one(self, query):
        antes = len(self.docs)
        self.docs = [d for d in self.docs if not self._match(d, query or {})]
        return type("R", (), {"deleted_count": antes - len(self.docs)})()


class FakeDB:
    def __init__(self, colecciones):
        self.colecciones = colecciones

    def __getitem__(self, nombre):
        return self.colecciones[nombre]


def _doc_pedido(ci, destinatario, kilos, cajas=10, flete=100000):
    return {
        "_id": ObjectId(),
        "consecutivo_vehiculo": "TEST-1",
        "consecutivo_integrapp": ci,
        "consecutivo_pedido": ci.split("-")[-1],
        "destinatario": destinatario,
        "destino": "BOGOTA",
        "destino_real": "BOGOTA",
        "origen": "FUNZA",
        "regional": "FUNZA",
        "estado": "PREAUTORIZADO",
        "num_cajas": cajas,
        "num_kilos": kilos,
        "num_kilos_sicetac": kilos,
        "valor_flete": flete,
        "cargue_descargue": 0,
        "desvio": 0,
        "punto_adicional": 0,
        "total_puntos": 0,
    }


USUARIO_ADMIN = {"usuario": "ADMIN1", "perfil": "ADMIN", "regional": "FUNZA"}

TARIFA_OK = {
    "origen": "FUNZA",
    "destino": "BOGOTA",
    "tarifas": {"NHR": 900000, "TURBO": 1200000},
    "pago_cargue_desc": "NO",
}

CONFIG_OC = {
    "tipo_vehiculo": "NHR",
    "valor_punto_adicional": 50000,
    "cargue_descargue": 100000,
}


class TestDividirVehiculoAtomico(unittest.TestCase):

    def _correr(self, colecciones, payload):
        with patch.object(mod, "db", FakeDB(colecciones)), \
             patch.object(mod, "coleccion_pedidos", colecciones["pedidos"]), \
             patch.object(mod, "coleccion_usuarios", colecciones["baseusuarios"]):
            return asyncio.run(dividir_vehiculo(payload))

    # ── 1) Si _calc falla después de mover docs, TODO vuelve a A ────────────
    def test_rollback_movimiento_si_falla_calculo(self):
        pedidos = FakeCollection([
            _doc_pedido("T-1", "X", 2000),
            _doc_pedido("T-1", "Y", 1000),
        ])
        colecciones = {
            "pedidos": pedidos,
            "baseusuarios": FakeCollection([USUARIO_ADMIN]),
            "tarifas": FakeCollection([]),          # ← sin tarifa: _calc debe fallar
            "config_otros_costos": FakeCollection([CONFIG_OC]),
        }
        payload = DividirHastaTresPayload(
            usuario="ADMIN1",
            consecutivo_origen="TEST-1",
            destino_unico="BOGOTA",
            grupo_B=GrupoDivision(destinatarios=["Y"]),
        )

        with self.assertRaises(HTTPException) as ctx:
            self._correr(colecciones, payload)
        self.assertIn("No hay tarifa", str(ctx.exception.detail))

        # Los 2 docs volvieron a A, con su CI original (sin sufijo B)
        en_a = pedidos.find({"consecutivo_vehiculo": "TEST-1"})
        self.assertEqual(len(en_a), 2)
        self.assertEqual(sorted(d["consecutivo_integrapp"] for d in en_a), ["T-1", "T-1"])
        # No existe el carro B
        self.assertEqual(pedidos.find({"consecutivo_vehiculo": "TEST-1B"}), [])

    # ── 2) Split por kilos + fallo posterior: clon borrado y fuente restaurada
    def test_rollback_split_por_kilos(self):
        original = _doc_pedido("T-1", "X", 1000, cajas=20)
        pedidos = FakeCollection([dict(original)])
        colecciones = {
            "pedidos": pedidos,
            "baseusuarios": FakeCollection([USUARIO_ADMIN]),
            "tarifas": FakeCollection([]),          # ← el fallo llega tras el split
            "config_otros_costos": FakeCollection([CONFIG_OC]),
        }
        payload = DividirHastaTresPayload(
            usuario="ADMIN1",
            consecutivo_origen="TEST-1",
            destino_unico="BOGOTA",
            grupo_B=GrupoDivision(split={"consecutivo_integrapp": "T-1", "kilos": 400}),
        )

        with self.assertRaises(HTTPException):
            self._correr(colecciones, payload)

        # El doc fuente recuperó sus valores originales
        self.assertEqual(len(pedidos.docs), 1)
        doc = pedidos.docs[0]
        self.assertEqual(doc["num_kilos"], 1000)
        self.assertEqual(doc["num_kilos_sicetac"], 1000)
        self.assertEqual(doc["num_cajas"], 20)
        # No quedó ningún clon ni carro B
        self.assertEqual(pedidos.find({"consecutivo_vehiculo": "TEST-1B"}), [])

    # ── 3) División exitosa: los totales físicos quedan repartidos por carro ─
    def test_division_exitosa_reparte_totales(self):
        pedidos = FakeCollection([
            _doc_pedido("T-1", "X", 2000, cajas=15),
            _doc_pedido("T-1", "Y", 1000, cajas=5),
        ])
        colecciones = {
            "pedidos": pedidos,
            "baseusuarios": FakeCollection([USUARIO_ADMIN]),
            "tarifas": FakeCollection([TARIFA_OK]),
            "config_otros_costos": FakeCollection([CONFIG_OC]),
        }
        payload = DividirHastaTresPayload(
            usuario="ADMIN1",
            consecutivo_origen="TEST-1",
            destino_unico="BOGOTA",
            grupo_B=GrupoDivision(destinatarios=["Y"]),
        )

        res = self._correr(colecciones, payload)
        self.assertIn("resumen", res)

        a = pedidos.find_one({"consecutivo_vehiculo": "TEST-1", "destinatario": "X"})
        b = pedidos.find_one({"consecutivo_vehiculo": "TEST-1B"})
        # Kg reales y cajas repartidos (no el total original en ambos)
        self.assertEqual(a["total_kilos_vehiculo"], 2000)
        self.assertEqual(a["total_cajas_vehiculo"], 15)
        self.assertEqual(b["total_kilos_vehiculo"], 1000)
        self.assertEqual(b["total_cajas_vehiculo"], 5)
        # El doc movido quedó con sufijo B en su CI/CP
        self.assertEqual(b["consecutivo_integrapp"], "T-1B")


if __name__ == "__main__":
    unittest.main()
