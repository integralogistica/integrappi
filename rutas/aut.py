from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, EmailStr
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import jwt, JWTError, ExpiredSignatureError
from passlib.context import CryptContext
from datetime import datetime, timedelta, timezone

ALGORITHM = "HS256"
SECRET_KEY = "una_clave_secreta_muy_larga_y_compleja"
TOKEN_DURATION_MINUTES = 1

ruta_aut = APIRouter(
    tags=['login'],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}}
)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")
crypt = CryptContext(schemes=["bcrypt"])

class Usuario(BaseModel):
    nombre: str 
    email: EmailStr

class UsuarioDB(Usuario):
    password: str 

usuarios_bd = {
    "edwin": {
        "nombre": "edwin",
        "email": "edwin@gmail.com",
        "password": "$2a$12$uwdg/oXY4edLxQwxwSForOkZqxG7u3iD8hH01WyG5g6giHRwGwNzO"
    },
    "nestor": {
        "nombre": "nestor",
        "email": "nestor@gmail.com",
        "password": "$2a$12$EuelWt5IDJZwIPpNi1IXs.W7gOCA5jRygsuPlCQ9owIWFK5LIm7vy"
    }
}

def buscarUsuarioDB(nombre: str):
    if nombre in usuarios_bd:
        return UsuarioDB(**usuarios_bd[nombre])

def buscarUsuario(nombre: str):
    if nombre in usuarios_bd:
        return Usuario(**usuarios_bd[nombre])
    
async def aut_usuario(token: str = Depends(oauth2_scheme)):
    excepcion = HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Credenciales inválidas")
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=ALGORITHM)
        nombre = payload.get("sub")
        if nombre is None:
            raise excepcion
    except ExpiredSignatureError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="El token ha expirado")
    except JWTError:
        raise excepcion
    return buscarUsuario(nombre)

async def current_user(usuario: Usuario = Depends(aut_usuario)):
    return usuario


@ruta_aut.post("/login")
async def login(form: OAuth2PasswordRequestForm = Depends()):
    usuario_db = usuarios_bd.get(form.username)
    if not usuario_db:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="El usuario no existe")
    
    usuario = buscarUsuarioDB(form.username)

    if not crypt.verify(form.password, usuario.password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Clave incorrecta")
    
    expiracion = datetime.now(timezone.utc) + timedelta(minutes=TOKEN_DURATION_MINUTES)
    access_token = {"sub": usuario.nombre, "exp": expiracion}

    return {
        "access_token": jwt.encode(access_token, SECRET_KEY, algorithm=ALGORITHM),
        "token_type": "bearer"
    }

@ruta_aut.get("/autenticacion/me")
async def me(usuario: Usuario = Depends(current_user)):
    return usuario
