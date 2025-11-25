from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class ManifestoPago(BaseModel):
    id: Optional[str] = None
    Tenedor: str = "12345"
    Manifiesto: str = "982345"
    Saldo: Optional[int] = 0
    Fecha_saldo: datetime
    Deducciones: Optional[int] = 0
    causal: Optional[str] = None
