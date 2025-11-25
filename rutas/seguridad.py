# rutas/seguridad.py

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime, timedelta

import jwt  # PyJWT
from passlib.context import CryptContext
from bson import ObjectId

from rutas.baseusuarios import coleccion_usuarios

# --- Configuración JWT ---
SECRET_KEY = "TU_SECRETO_MUY_SEGURO"  # en producción, usar variable de entorno
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

class LoginSeguridad(BaseModel):
    usuario: str = Field(..., example="USUARIO1")
    clave: str = Field(..., example="tu-contraseña")

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"

router = APIRouter(
    prefix="/seguridad",
    tags=["Seguridad"]
)

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=15))
    to_encode.update({"exp": expire})
    token = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return token

@router.post("/login", response_model=TokenResponse)
def login_seguridad(payload: LoginSeguridad):
    usuario_norm = payload.usuario.strip().upper()
    usuario_doc = coleccion_usuarios.find_one({"usuario": usuario_norm})
    if not usuario_doc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            detail="Usuario o contraseña incorrectos")

    hashed = usuario_doc.get("clave", "")
    if not pwd_context.verify(payload.clave, hashed):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            detail="Usuario o contraseña incorrectos")

    perfil = usuario_doc.get("perfil", "").strip().upper()
    if perfil != "SEGURIDAD":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,
                            detail="No autorizado para este perfil")

    token_data = {
        "sub": str(usuario_doc["_id"]),
        "usuario": usuario_doc["usuario"],
        "perfil": perfil
    }
    access_token = create_access_token(
        data=token_data,
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )

    return {"access_token": access_token, "token_type": "bearer"}

# Dependencia para proteger rutas con JWT
from fastapi.security import OAuth2PasswordBearer

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/seguridad/login")

async def get_current_user_seguridad(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="No se pudieron validar las credenciales",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id: str = payload.get("sub")
        perfil: str = payload.get("perfil")
        if user_id is None or perfil is None:
            raise credentials_exception
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expirado")
    except jwt.InvalidTokenError:
        raise credentials_exception

    usuario = coleccion_usuarios.find_one({"_id": ObjectId(user_id)})
    if not usuario or usuario.get("perfil", "").strip().upper() != "SEGURIDAD":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado")

    return {
        "id": str(usuario["_id"]),
        "usuario": usuario["usuario"],
        "perfil": usuario["perfil"],
    }

# Ejemplo de ruta protegida
@router.get("/protegido")
def ruta_protegida(user = Depends(get_current_user_seguridad)):
    return {"message": "Acceso concedido", "usuario": user}
