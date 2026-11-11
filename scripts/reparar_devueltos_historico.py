# Reparación: mover docs huérfanos de historico_otros_costos -> otros_costos
# (bug de /devolver-del-historico: quedaban en histórico con estado "devuelto").
# Mismo patrón delete-first + insert idempotente de _mover_documento (rutas/otros_costos.py).
import os
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv(Path(__file__).resolve().parents[1] / ".env")
client = MongoClient(os.getenv("MONGO_URI"), serverSelectionTimeoutMS=30000)
client.admin.command("ping")
db = client["integra"]

CONSECUTIVOS = [
    "JUAN MINA-OC-20261028-0001",
    "JUAN MINA-OC-20261028-0002",
    "JUAN MINA-OC-20261028-0003",
    "JUAN MINA-OC-20261028-0004",
]

print(f"BD: {db.name}")
for c in CONSECUTIVOS:
    doc = db.historico_otros_costos.find_one({"consecutivo": c})
    if not doc:
        print(f"[SKIP] {c}: no está en el histórico")
        continue
    if doc.get("estado") != "devuelto":
        print(f"[SKIP] {c}: estado {doc.get('estado')} != devuelto (no tocar)")
        continue
    if db.otros_costos.find_one({"consecutivo": c}):
        print(f"[SKIP] {c}: ya existe en otros_costos (posible duplicado, revisar a mano)")
        continue
    # _mover_documento: delete-first en destino + insert + delete en origen
    db.otros_costos.delete_one({"_id": doc["_id"]})
    db.otros_costos.insert_one(doc)
    db.historico_otros_costos.delete_one({"_id": doc["_id"]})
    ok_act = db.otros_costos.find_one({"consecutivo": c}, {"estado": 1})
    ok_hist = db.historico_otros_costos.find_one({"consecutivo": c})
    print(f"[OK]   {c}: otros_costos={ok_act.get('estado') if ok_act else None}, "
          f"en historico={'SÍ (¡ERROR!)' if ok_hist else 'no'}")

print("\n== Verificación extra: los 3 FUNZA con el mismo síntoma ==")
for c in ["FUNZA-OC-20260903-0004", "FUNZA-OC-20261022-0002", "FUNZA-OC-20261030-0001"]:
    en_hist = db.historico_otros_costos.find_one({"consecutivo": c}, {"estado": 1, "updated_at": 1})
    en_act = db.otros_costos.find_one({"consecutivo": c}, {"estado": 1, "updated_at": 1})
    print(f"  {c}: historico={en_hist}  activos={en_act}")
