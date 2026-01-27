# Funciones/chat_state_integra.py
from typing import Dict, Any, Optional
from datetime import datetime, timezone

_STATE: Dict[str, Dict[str, Any]] = {}

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
