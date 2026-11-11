# Diagnóstico: solicitudes quedadas en historico_otros_costos con estado "devuelto"
# (bug de /devolver-del-historico que no movía el doc a otros_costos). SOLO LECTURA.
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

print(f"BD: {db.name}\n")
print("== TODOS los 'devuelto' que haya en historico_otros_costos ==")
for d in db.historico_otros_costos.find({"estado": "devuelto"}, {"consecutivo": 1, "updated_at": 1}):
    print(f"  {d['consecutivo']}  (updated {d.get('updated_at')})")

print("\n== Estado de los 4 consecutivos pedidos ==")
for c in CONSECUTIVOS:
    en_hist = db.historico_otros_costos.find_one({"consecutivo": c}, {"estado": 1, "updated_at": 1})
    en_act = db.otros_costos.find_one({"consecutivo": c}, {"estado": 1, "updated_at": 1})
    print(f"  {c}:")
    print(f"    historico_otros_costos -> {en_hist}")
    print(f"    otros_costos           -> {en_act}")
