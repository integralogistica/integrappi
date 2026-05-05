import os
import time
from pymongo import MongoClient
from pymongo.errors import PyMongoError, ConfigurationError
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

# Conectar con reintentos (útil para cuando el cluster está despertando)
max_retries = 3
retry_delay = 5  # segundos

for attempt in range(max_retries):
    try:
        print(f"🔌 Intentando conectar a MongoDB (intento {attempt + 1}/{max_retries})...")
        bd_cliente = MongoClient(
            uri,
            serverSelectionTimeoutMS=30000,  # 30 segundos para dar tiempo al cluster a despertar
            connectTimeoutMS=30000,
            socketTimeoutMS=30000,
            retryWrites=True,
            w="majority"
        )

        # Probar conexión
        bd_cliente.admin.command("ping")
        print("✅ Conexión exitosa a MongoDB")
        break

    except (PyMongoError, ConfigurationError) as e:
        print(f"⚠️ Intento {attempt + 1} falló: {e}")

        if attempt < max_retries - 1:
            print(f"⏳ Reintentando en {retry_delay} segundos...")
            time.sleep(retry_delay)
        else:
            raise RuntimeError(f"❌ No se pudo conectar a MongoDB después de {max_retries} intentos: {e}")
