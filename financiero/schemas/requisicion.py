"""Esquemas de requisiciones.

# puerto LITERAL de app/schemas/requisicion.py del módulo externo (la
AccionTransicionRequest vive en financiero/schemas/__init__.py).
"""
import uuid
from datetime import datetime
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from financiero.models.requisicion import EstadoRequisicion


CRITERIOS_PROVEEDOR = {"CE": 0.10, "EC": 0.10, "ES": 0.10, "SQR": 0.05, "AC": 0.05, "G": 0.10, "PFC": 0.20, "MA": 0.15, "SST": 0.15}


class CalificacionProveedor(BaseModel):
    """Seguimiento a proveedor (FORM-CPR-011): 9 criterios de 0 a 15. Se pide al confirmar la recepción."""
    notas: dict[str, float]

    @model_validator(mode="after")
    def validar(self):
        faltan = [k for k in CRITERIOS_PROVEEDOR if k not in self.notas]
        if faltan:
            raise ValueError(f"Faltan criterios: {', '.join(faltan)}")
        for k, v in self.notas.items():
            if k not in CRITERIOS_PROVEEDOR:
                raise ValueError(f"Criterio desconocido: {k}")
            if not (0 <= v <= 15):
                raise ValueError(f"El criterio {k} debe estar entre 0 y 15")
        return self

    def puntaje(self) -> float:
        return round(sum(self.notas[k] * p for k, p in CRITERIOS_PROVEEDOR.items()), 2)

    def letra(self) -> str:
        p = self.puntaje()
        return "A" if p >= 13.8 else ("B" if p >= 7.5 else "C")


class RequisicionItem(BaseModel):
    model_config = ConfigDict(extra="allow")   # conserva descuento y demás campos del formulario
    codigo: Optional[str] = None
    descripcion: str
    cantidad: Decimal = Field(gt=0)
    unidad: str = "UND"
    precio_unitario: Decimal = Field(ge=0)
    total: Decimal = Field(ge=0)
    cuenta_contable: Optional[str] = None
    centro_costo: Optional[str] = None


class RequisicionCreate(BaseModel):
    area: Optional[str] = None            # solo la usa el administrador; los demás usan la de su usuario
    lugar_entrega: str = Field(..., min_length=2)
    es_urgente: bool = False
    motivo_urgencia: Optional[str] = None
    proveedor_sugerido: Optional[str] = None
    proveedor_nit: Optional[str] = None
    items: List[RequisicionItem] = Field(..., min_length=1)
    proveedor_datos: Optional[dict] = None
    datos_front: Optional[dict] = None
    observaciones: Optional[str] = None
    subtotal: Decimal
    iva: Decimal = Decimal("0.00")
    total: Decimal


class RequisicionReenvio(RequisicionCreate):
    version: Optional[int] = None


class RequisicionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    consecutivo: str
    oc_numero: Optional[str] = None
    solicitante_id: uuid.UUID
    solicitante_nombre: str
    area: str
    lugar_entrega: str
    es_urgente: bool
    motivo_urgencia: Optional[str] = None
    proveedor_sugerido: Optional[str] = None
    proveedor_nit: Optional[str] = None
    items: List[dict]
    subtotal: Decimal
    iva: Decimal
    total: Decimal
    estado: EstadoRequisicion
    jefe_aprobador_id: Optional[uuid.UUID] = None
    fecha_aprobacion_jefe: Optional[datetime] = None
    observacion_jefe: Optional[str] = None
    compras_analista_id: Optional[uuid.UUID] = None
    fecha_compras: Optional[datetime] = None
    cotizaciones_adjuntos: Optional[List[dict]] = None
    aprobador_financiero_id: Optional[uuid.UUID] = None
    rol_aprobador_financiero: Optional[str] = None
    fecha_aprobacion_financiera: Optional[datetime] = None
    observacion_financiera: Optional[str] = None
    recibido_por_id: Optional[uuid.UUID] = None
    fecha_recibido: Optional[datetime] = None
    soporte_entrega_adjunto_id: Optional[uuid.UUID] = None
    proveedor_datos: Optional[dict] = None
    datos_front: Optional[dict] = None
    observaciones: Optional[str] = None
    pago_urgente: bool = False
    pago_fecha: Optional[datetime] = None
    pago_aprobado: Optional[bool] = None
    pago_decidido_por: Optional[str] = None
    entregado_en: Optional[datetime] = None
    entregado_por_id: Optional[uuid.UUID] = None
    calificacion_proveedor: Optional[dict] = None
    historial_cambios: List[dict]
    version: int
    created_at: datetime
    updated_at: datetime
