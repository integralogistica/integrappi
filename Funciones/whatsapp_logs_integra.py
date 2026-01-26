# Funciones/whatsapp_logs_integra.py
import os
from datetime import datetime
from typing import Dict, Any, Optional
from pymongo import MongoClient

mongo_uri = os.getenv("MONGO_URI")
if not mongo_uri:
    raise ValueError("La variable de entorno MONGO_URI no está configurada.")

client = MongoClient(mongo_uri)
db = client["integra"]
coleccion_uso_whatsapp = db["uso_whatsapp"]

def log_whatsapp_event(
    phone: str,
    direction: str,  # "IN" | "OUT" | "SYSTEM"
    event: str,      # "MESSAGE_RECEIVED" | "MESSAGE_SENT" | "STATE_CHANGED" | "ERROR" | etc.
    text: Optional[str] = None,
    state: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
):
    doc = {
        "phone": phone,
        "direction": direction,
        "event": event,
        "text": text,
        "state": state,
        "context": context or {},
        "meta": meta or {},
        "created_at": datetime.utcnow(),
    }
    try:
        coleccion_uso_whatsapp.insert_one(doc)
    except Exception as e:
        # No romper el bot por logging
        print(f"⚠️ Error guardando log uso_whatsapp: {e}")
