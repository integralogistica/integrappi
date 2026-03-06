# Funciones/chat_state_integra.py
from typing import Dict, Any, Optional
from datetime import datetime, timedelta, timezone

_STATE: Dict[str, Dict[str, Any]] = {}

# Almacenamiento de sesiones autenticadas para transportadores
# {phone: {authenticated: True, cedula: "xxx", expires_at: "ISODate"}}
_AUTH_SESSIONS: Dict[str, Dict[str, Any]] = {}

# TTL para sesiones autenticadas (30 días)
AUTH_SESSION_DAYS = 30

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
# Gestión de sesiones autenticadas
# =========================

def set_auth_session(phone: str, cedula: str):
    """Guarda una sesión autenticada para transportador."""
    expires_at = datetime.now(timezone.utc) + timedelta(days=AUTH_SESSION_DAYS)
    _AUTH_SESSIONS[phone] = {
        "authenticated": True,
        "cedula": cedula,
        "expires_at": expires_at.isoformat(),
    }

def get_auth_session(phone: str) -> Optional[Dict[str, Any]]:
    """Retorna la sesión autenticada si existe y no ha expirado."""
    session = _AUTH_SESSIONS.get(phone)
    if not session:
        return None
    
    # Verificar expiración
    expires_at = _parse_dt_iso(session.get("expires_at", ""))
    if not expires_at or datetime.now(timezone.utc) > expires_at:
        # Sesión expirada, eliminarla
        _AUTH_SESSIONS.pop(phone, None)
        return None
    
    return session

def is_authenticated(phone: str) -> bool:
    """Verifica si el teléfono tiene una sesión activa."""
    return get_auth_session(phone) is not None

def invalidate_auth_session(phone: str):
    """Invalida manualmente una sesión autenticada."""
    _AUTH_SESSIONS.pop(phone, None)

def _parse_dt_iso(dt_str: str) -> Optional[datetime]:
    """Parsea datetime ISO string con timezone."""
    if not dt_str:
        return None
    try:
        if dt_str.endswith("Z"):
            dt_str = dt_str.replace("Z", "+00:00")
        d = datetime.fromisoformat(dt_str)
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d
    except Exception:
        return None
