from typing import Dict, Any

_STATE: Dict[str, Dict[str, Any]] = {}

def get_state(phone: str) -> Dict[str, Any]:
    return _STATE.get(phone, {"state": "START", "context": {}})

def set_state(phone: str, state: str, context: Dict[str, Any]):
    _STATE[phone] = {"state": state, "context": context or {}}

def reset_state(phone: str):
    _STATE.pop(phone, None)
