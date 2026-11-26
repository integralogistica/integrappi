"""Esquemas de terceros de Compras (proveedores nuevos)."""
import uuid
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field

from financiero.models.tercero import EstadoTercero


class TerceroCreate(BaseModel):
    razon_social: str = Field(..., min_length=2)
    nit: str = Field(..., min_length=5)
    nit_pago: Optional[str] = None
    tipo_cuenta: Optional[str] = None      # Ahorros / Corriente
    banco: Optional[str] = None
    no_cuenta: Optional[str] = None
    area: Optional[str] = None
    origen_requisicion: bool = True        # Compras también puede crearlos directo
    requisicion_id: Optional[uuid.UUID] = None
    consecutivo_req: Optional[str] = None


class TerceroUpdate(BaseModel):
    razon_social: Optional[str] = None
    nit: Optional[str] = None
    nit_pago: Optional[str] = None
    tipo_cuenta: Optional[str] = None
    banco: Optional[str] = None
    no_cuenta: Optional[str] = None
    area: Optional[str] = None


class TerceroRechazo(BaseModel):
    motivo: str = Field(..., min_length=5)


class TerceroMarcaSoftware(BaseModel):
    marcar: Optional[bool] = None  # sin valor -> alterna el estado actual


class TerceroResponse(BaseModel):
    id: uuid.UUID
    razon_social: str
    nit: str
    nit_pago: Optional[str] = None
    tipo_cuenta: Optional[str] = None
    banco: Optional[str] = None
    no_cuenta: Optional[str] = None
    area: Optional[str] = None
    origen_requisicion: bool
    requisicion_id: Optional[uuid.UUID] = None
    consecutivo_req: Optional[str] = None
    creado_por: Optional[str] = None
    creado_por_nombre: Optional[str] = None
    estado_compras: EstadoTercero
    motivo_rechazo_compras: Optional[str] = None
    aprobado_por: Optional[str] = None
    fecha_aprobacion: Optional[datetime] = None
    creado_en_software: bool
    creado_en_software_por: Optional[str] = None
    fecha_creado_en_software: Optional[datetime] = None
    historial_cambios: List[dict]
    version: int
    created_at: datetime
    updated_at: datetime
