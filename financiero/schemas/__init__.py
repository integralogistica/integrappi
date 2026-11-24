"""Esquemas Pydantic del Módulo Financiero (puertos de app/schemas/*)."""
from typing import Optional

from pydantic import BaseModel


class AccionTransicionRequest(BaseModel):
    """# puerto de app/schemas/requisicion.py::AccionTransicionRequest."""
    accion: str  # APROBAR, RECHAZAR, ANULAR, ENTREGAR, RECIBIR_TOTAL...
    observacion: Optional[str] = None
    version: Optional[int] = None  # versión que el cliente leyó: si cambió, la API responde 409
    datos_adicionales: Optional[dict] = None
