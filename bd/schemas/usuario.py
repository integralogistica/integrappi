from pydantic import BaseModel, EmailStr
from typing import Optional
from datetime import datetime

class Usuario(BaseModel):
    id: Optional[str] = None
    nombre: str = "Edwin Zarate"
    email: EmailStr = "edwin@example.com"
    telefono: Optional[str] = None
    fecha_nacimiento: Optional[datetime]
    foto_perfil: Optional[str] = "https://example.com/profile.jpg"
    metodo_pago: Optional[str] = "Visa"
    es_anfitrion: bool = False
