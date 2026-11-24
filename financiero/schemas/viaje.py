"""Esquemas de viajes.

# puerto LITERAL de app/schemas/viaje.py del módulo externo.
"""
import uuid
from datetime import datetime
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field

from financiero.models.viaje import EstadoViaje, CiudadViaje


class ViajeCreate(BaseModel):
    viajero_nombre: str = Field(..., min_length=3)
    viajero_cedula: Optional[str] = ""
    origen: CiudadViaje
    destino: CiudadViaje
    fecha_inicio: datetime
    fecha_fin: datetime
    objetivo: str = Field(..., min_length=3)
    autorizador_id: uuid.UUID
    requiere_tiquetes: bool = True
    viajero_cargo: Optional[str] = None
    categoria_solicitud: Optional[str] = None
    observaciones: Optional[str] = None
    datos_front: Optional[dict] = None


class ViajeResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    consecutivo: str
    solicitante_id: uuid.UUID
    solicitante_nombre: str
    viajero_nombre: str
    viajero_cedula: Optional[str] = None
    origen: str
    destino: str
    fecha_inicio: datetime
    fecha_fin: datetime
    objetivo: str
    autorizador_id: Optional[uuid.UUID] = None
    fecha_autorizacion: Optional[datetime] = None
    observacion_autorizacion: Optional[str] = None
    estado: EstadoViaje
    requiere_tiquetes: bool
    tiquetes_detalle: Optional[dict] = None
    categoria_tarifa: Optional[str] = None  # Enmascarado para roles no autorizados
    presupuesto_items: List[dict]
    total_presupuesto: Decimal
    anticipo_pagado: bool
    fecha_pago_anticipo: Optional[datetime] = None
    comprobante_anticipo_adjunto_id: Optional[uuid.UUID] = None
    legalizacion_detalle: Optional[dict] = None
    viajero_cargo: Optional[str] = None
    categoria_solicitud: Optional[str] = None
    observaciones: Optional[str] = None
    datos_front: Optional[dict] = None
    presupuesto_detalle: Optional[dict] = None
    anticipo_detalle: Optional[dict] = None
    firmas: Optional[dict] = None  # solicitó / autorizó / controller, a medida que se aprueba
    historial_cambios: List[dict]
    version: int
    created_at: datetime
    updated_at: datetime
