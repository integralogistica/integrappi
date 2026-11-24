"""Esquemas de traslados.

# puerto LITERAL de app/schemas/traslado.py del módulo externo.
"""
import uuid
from datetime import datetime
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field

from financiero.models.traslado import EstadoTraslado


class TrasladoLinea(BaseModel):
    """Misma forma que usa el formulario (FORMATO - MOVIMIENTO PRESUPUESTAL).
    Acepta campos extra (mes, origen, efecto...)."""
    model_config = ConfigDict(extra="allow")

    destinoRubro: str = Field(..., min_length=1)
    monto: Decimal = Field(gt=0)
    motivo: str = Field(..., min_length=1)


class TrasladoCreate(BaseModel):
    area: Optional[str] = None            # área destino (por defecto la del usuario)
    lineas: List[TrasladoLinea] = Field(..., min_length=1)
    observaciones: Optional[str] = None
    datos_front: Optional[dict] = None


class TrasladoReenvio(BaseModel):
    lineas: List[TrasladoLinea] = Field(..., min_length=1)
    observaciones: Optional[str] = None
    datos_front: Optional[dict] = None
    version: Optional[int] = None


class TrasladoResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    consecutivo: str
    solicitante_id: uuid.UUID
    solicitante_nombre: str
    area: str
    fecha: datetime
    motivo: Optional[str] = None
    observaciones: Optional[str] = None
    datos_front: Optional[dict] = None
    lineas: List[dict]
    total: Decimal
    estado: EstadoTraslado
    aprobador_id: Optional[uuid.UUID] = None
    fecha_aprobacion: Optional[datetime] = None
    observacion_aprobacion: Optional[str] = None
    motivo_rechazo: Optional[str] = None
    aprobacion: Optional[dict] = None
    historial_cambios: List[dict]
    version: int
    created_at: datetime
    updated_at: datetime
