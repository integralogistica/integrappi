# Funciones/chat_state_integra.py
from typing import Dict, Any, Optional
from datetime import datetime, timedelta, timezone

_STATE: Dict[str, Dict[str, Any]] = {}

# TTL para sesiones autenticadas (30 días)
AUTH_SESSION_DAYS = 30

# Nombre de colección para sesiones de WhatsApp
SESSIONS_COLLECTION = "whatsapp_sessions"

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def get_state(phone: str) -> Dict[str, Any]:
    # Siempre retorna updated_at (o None)
    s = _STATE.get(phone)
    if not s:
        return {"state": "START", "context": {}, "updated_at": None}

    return {
        "state": s.get("state") or "START",
        "context": s.get("context") or {},
        "updated_at": s.get("updated_at"),
    }

def set_state(phone: str, state: str, context: Dict[str, Any], updated_at: Optional[str] = None):
    _STATE[phone] = {
        "state": state,
        "context": context or {},
        "updated_at": updated_at or _utc_now_iso(),
    }

def touch_state(phone: str):
    # refresca timestamp sin cambiar state/context
    s = _STATE.get(phone)
    if not s:
        _STATE[phone] = {"state": "START", "context": {}, "updated_at": _utc_now_iso()}
        return
    s["updated_at"] = _utc_now_iso()

def reset_state(phone: str):
    _STATE.pop(phone, None)
    # Nota: NO borramos la sesión autenticada aquí, ya que persiste 30 días

# =========================
# Gestión de sesiones autenticadas (MongoDB)
# =========================

def set_auth_session(phone: str, cedula: str):
    """Guarda una sesión autenticada en MongoDB (upsert)."""
    try:
        from bd.bd_cliente import bd_cliente
        base_datos = bd_cliente.integra
        
        expires_at = datetime.now(timezone.utc) + timedelta(days=AUTH_SESSION_DAYS)
        
        # Upsert: crea o actualiza la sesión
        base_datos[SESSIONS_COLLECTION].update_one(
            {"phone": phone},
            {"$set": {
                "cedula": cedula,
                "expires_at": expires_at,
                "updated_at": datetime.now(timezone.utc)
            }},
            upsert=True
        )
        print(f"[AUTH] Sesión guardada en MongoDB: phone={phone}, cedula={cedula}")
    except Exception as e:
        print(f"[AUTH] Error guardando sesión en MongoDB: {e}")


def get_auth_session(phone: str) -> Optional[Dict[str, Any]]:
    """Retorna la sesión autenticada si existe en MongoDB y no ha expirado."""
    try:
        from bd.bd_cliente import bd_cliente
        base_datos = bd_cliente.integra
        
        session = base_datos[SESSIONS_COLLECTION].find_one({
            "phone": phone,
            "expires_at": {"$gt": datetime.now(timezone.utc)}
        })
        
        if not session:
            return None
        
        return {
            "authenticated": True,
            "cedula": session.get("cedula"),
            "expires_at": session.get("expires_at").isoformat(),
        }
    except Exception as e:
        print(f"[AUTH] Error consultando sesión en MongoDB: {e}")
        return None


def is_authenticated(phone: str) -> bool:
    """Verifica si el teléfono tiene una sesión activa en MongoDB."""
    return get_auth_session(phone) is not None


def invalidate_auth_session(phone: str):
    """Invalida manualmente una sesión autenticada de MongoDB."""
    try:
        from bd.bd_cliente import bd_cliente
        base_datos = bd_cliente.integra
        
        resultado = base_datos[SESSIONS_COLLECTION].delete_one({"phone": phone})
        print(f"[AUTH] Sesión invalidada: phone={phone}, borrados={resultado.deleted_count}")
    except Exception as e:
        print(f"[AUTH] Error invalidando sesión: {e}")


def create_session_index():
    """Crea índice único en el campo phone para la colección de sesiones."""
    try:
        from bd.bd_cliente import bd_cliente
        base_datos = bd_cliente.integra

        base_datos[SESSIONS_COLLECTION].create_index(
            [("phone", 1)],
            unique=True,
            background=True
        )
        print(f"[AUTH] Índice creado exitosamente en {SESSIONS_COLLECTION}.phone")
    except Exception as e:
        print(f"[AUTH] Error creando índice: {e}")


# =========================
# Gestión de sesiones de clientes (clave genérica, 30 días)
# =========================

CLIENT_SESSIONS_COLLECTION = "whatsapp_client_sessions"


def set_cliente_auth_session(phone: str):
    """Guarda una sesión autenticada de cliente en MongoDB (upsert, 30 días)."""
    try:
        from bd.bd_cliente import bd_cliente
        base_datos = bd_cliente.integra

        expires_at = datetime.now(timezone.utc) + timedelta(days=AUTH_SESSION_DAYS)
        base_datos[CLIENT_SESSIONS_COLLECTION].update_one(
            {"phone": phone},
            {"$set": {
                "expires_at": expires_at,
                "updated_at": datetime.now(timezone.utc),
            }},
            upsert=True,
        )
        print(f"[CLIENT_AUTH] Sesión guardada: phone={phone}")
    except Exception as e:
        print(f"[CLIENT_AUTH] Error guardando sesión: {e}")


def get_cliente_auth_session(phone: str) -> Optional[Dict[str, Any]]:
    """Retorna sesión de cliente si existe en MongoDB y no ha expirado."""
    try:
        from bd.bd_cliente import bd_cliente
        base_datos = bd_cliente.integra

        session = base_datos[CLIENT_SESSIONS_COLLECTION].find_one({
            "phone": phone,
            "expires_at": {"$gt": datetime.now(timezone.utc)},
        })
        if not session:
            return None
        return {
            "authenticated": True,
            "expires_at": session.get("expires_at").isoformat(),
        }
    except Exception as e:
        print(f"[CLIENT_AUTH] Error consultando sesión: {e}")
        return None


def is_cliente_authenticated(phone: str) -> bool:
    """Verifica si el teléfono de cliente tiene sesión activa."""
    return get_cliente_auth_session(phone) is not None


def invalidate_cliente_auth_session(phone: str):
    """Invalida manualmente la sesión de cliente."""
    try:
        from bd.bd_cliente import bd_cliente
        base_datos = bd_cliente.integra

        resultado = base_datos[CLIENT_SESSIONS_COLLECTION].delete_one({"phone": phone})
        print(f"[CLIENT_AUTH] Sesión invalidada: phone={phone}, borrados={resultado.deleted_count}")
    except Exception as e:
        print(f"[CLIENT_AUTH] Error invalidando sesión: {e}")
