import os
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from pathlib import Path

# Cargar variables desde el .env (requiere instalar python-dotenv)
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
except Exception:
    pass

uri = os.getenv("MONGO_URI")
if not uri:
    raise RuntimeError("❌ Falta MONGO_URI en el archivo .env o en las variables de entorno")

# Conectar usando la cadena estándar (no SRV)
bd_cliente = MongoClient(uri, serverSelectionTimeoutMS=15000)

# Validar conexión al arrancar
try:
    bd_cliente.admin.command("ping")
    print("✅ Conexión exitosa a MongoDB")
except PyMongoError as e:
    raise RuntimeError(f"❌ Error conectando a MongoDB: {e}")
