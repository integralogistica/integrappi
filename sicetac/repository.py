from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from bson.decimal128 import Decimal128
from pymongo import ASCENDING, MongoClient, UpdateOne
from pymongo.errors import PyMongoError

from .errors import MongoPersistenceError


def _bson(value):
    if isinstance(value, Decimal):
        return Decimal128(value)
    if isinstance(value, dict):
        return {k: _bson(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_bson(v) for v in value]
    return value


class SicetacRepository:
    def __init__(self, uri, database, collection, shared_client=None):
        self.client = shared_client or MongoClient(uri, serverSelectionTimeoutMS=10000, connectTimeoutMS=10000)
        self.collection = self.client[database][collection]

    def comprobar(self):
        try:
            self.client.admin.command("ping")
            self.collection.create_index([("consulta_id", ASCENDING)], unique=True, name="uq_sicetac_consulta_id")
        except PyMongoError as exc:
            raise MongoPersistenceError("No fue posible conectar o preparar MongoDB") from exc

    def upsert_many(self, documents):
        if not documents:
            return 0, 0
        now = datetime.now(timezone.utc)
        operations = []
        for document in documents:
            document = _bson(document)
            document["actualizado_en"] = now
            operations.append(UpdateOne({"consulta_id": document["consulta_id"]}, {"$set": document, "$setOnInsert": {"creado_en": now}}, upsert=True))
        try:
            result = self.collection.bulk_write(operations, ordered=False)
            inserted = result.upserted_count
            return inserted, result.matched_count
        except PyMongoError as exc:
            raise MongoPersistenceError("Falló la persistencia idempotente de resultados SICE-TAC") from exc

    def listar(self, periodo=None, limit=100):
        query = {"periodo_aplicado": periodo} if periodo else {}
        return list(self.collection.find(query, {"_id": 0}).sort("actualizado_en", -1).limit(limit))

