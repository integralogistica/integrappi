from fastapi import APIRouter, HTTPException, status, Body, BackgroundTasks, Depends
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pymongo import MongoClient
from bson import ObjectId
from pydantic import BaseModel
from typing import List, Optional
import os
import random
import resend 
import requests
import re
import jwt
from datetime import datetime, timedelta, timezone

from Funciones.claves import crear_hash, verificar_clave, es_hash
from Funciones.utilidades_texto import norm_cliente_oc

# ==============================================================================
# 🔗 CONFIGURACIÓN DE BASE DE DATOS
# ==============================================================================
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
from bd.bd_cliente import bd_cliente
ConexionMongo = bd_cliente
base_datos = ConexionMongo["integra"]
coleccion_usuarios = base_datos["baseusuarios"]

BASEUSUARIOS_JWT_SECRET = os.getenv("JWT_SECRET", "cambia_esta_clave_por_una_bien_larga_y_aleatoria")
BASEUSUARIOS_JWT_ALGORITHM = "HS256"
BASEUSUARIOS_TOKEN_MINUTES = int(os.getenv("JWT_EXPIRE_MINUTES", "20"))
baseusuarios_oauth2 = OAuth2PasswordBearer(
    tokenUrl="baseusuarios/token",
    scheme_name="BaseUsuariosOAuth2",
)

# Índices
try:
    coleccion_usuarios.create_index("usuario", unique=True)
    coleccion_usuarios.create_index("perfil")
except Exception:
    pass


# ==============================================================================
# 🚦 CONFIGURACIÓN DEL ROUTER
# ==============================================================================
ruta_baseusuarios = APIRouter(
    prefix="/baseusuarios",
    tags=["BaseUsuarios"],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}},
)


# ==============================================================================
# 🔑 CONFIGURACIÓN RESEND
# ==============================================================================
resend.api_key = os.getenv("RESEND_API_KEY", "re_TuApiKeyAqui...") 
MAIL_FROM = os.getenv("MAIL_FROM", "no-reply@integralogistica.com")


# ==============================================================================
# 📌 ESQUEMAS DE DATOS
# ==============================================================================
class BaseUsuario(BaseModel):
    nombre: str
    correo: Optional[str] = None
    regional: str
    celular: Optional[str] = None
    perfil: str
    usuario: str
    clave: str
    clientes: Optional[List[str]] = None
    # (2026-10-15) Perfiles ADICIONALES: un usuario conserva su perfil
    # principal (p. ej. OPERATIVO) y gana capacidades extra. Hoy solo PADRINO
    # (entrada al portal de Seguridad en modo simplificado).
    perfiles_adicionales: Optional[List[str]] = None
    # Módulo Estudios de Seguridad: solo aplican con perfil SEGURIDAD.
    empresa_id_seguridad: Optional[str] = None  # _id de empresas_seguridad
    rol_seguridad: Optional[str] = None  # CONSULTADOR | ADMIN_EMPRESA
    # (2026-10-29) Clientes asignables desde el catálogo `clientes_siscore`
    # (NITs en MAYÚSCULAS). Fuente de verdad de "qué clientes puede ver" —
    # de aquí se DERIVA el campo `clientes` (portales) vía PORTALES_POR_NIT.
    clientes_siscore: Optional[List[str]] = None

class ActualizarClientesInput(BaseModel):
    clientes: List[str]

class ActualizarClientesSiscoreInput(BaseModel):
    """Clientes del catálogo clientes_siscore (NITs) que el usuario puede ver.
    Asignable a CUALQUIER perfil. [] = ninguno (para perfil CLIENTE significa
    que aún no tiene tablero asignado)."""
    clientes_siscore: List[str] = []

class ActualizarClientesAprobacionInput(BaseModel):
    """Alcance de aprobación por cliente en Otros Costos (perfil COORDINADOR/CONTROL).
    Lista vacía = TODOS los clientes (valor canónico; "TODOS" también colapsa a [])."""
    clientes_aprobacion: List[str] = []

class ActualizarDatosInput(BaseModel):
    nombre: str
    correo: Optional[str] = None
    regional: str
    celular: Optional[str] = None
    clave: Optional[str] = None  # solo si quiere cambiarla
    usuario: Optional[str] = None  # nombre de usuario

class UsuarioLite(BaseModel):
    id: str
    nombre: str
    usuario: str

class VerificarInput(BaseModel):
    usuario: str
    perfil: str

class ValidarCodigoInput(BaseModel):
    usuario: str
    codigo: str
    perfil: str

class CambioClaveInput(BaseModel):
    usuario: str
    nuevaClave: str
    codigo: str
    perfil: str

class RecuperarBaseInput(BaseModel):
    correo: str

class ConfirmarBaseInput(BaseModel):
    correo: str
    codigo: str
    nuevaClave: str


# ==============================================================================
# 🛠️ FUNCIONES AUXILIARES
# ==============================================================================

def modelo_usuario(u) -> dict:
    return {
        "id": str(u["_id"]),
        "nombre": u["nombre"],
        "correo": u.get("correo"),
        "regional": u["regional"],
        "celular": u.get("celular"),
        "perfil": u["perfil"],
        "usuario": u["usuario"],
        "clientes": u["clientes"] if isinstance(u.get("clientes"), list) else ["KABI"],
        # (2026-10-29) Clientes asignados del catálogo clientes_siscore (NITs).
        # None = usuario creado antes del campo (los toggles KABI/MEDICAL_CARE
        # viejos siguen mandando en `clientes`).
        "clientes_siscore": u.get("clientes_siscore") if isinstance(u.get("clientes_siscore"), list) else None,
        # Alcance de aprobación por cliente en Otros Costos ([] = TODOS)
        "clientes_aprobacion": u.get("clientes_aprobacion") if isinstance(u.get("clientes_aprobacion"), list) else [],
        # Perfiles adicionales (2026-10-15): p. ej. OPERATIVO que también es Padrino.
        "perfiles_adicionales": u.get("perfiles_adicionales") or [],
        "activo": u.get("activo", True),
        "notificaciones_mc": u.get("notificaciones_mc") or [],
        # En BD puede ser ObjectId (scripts viejos) o string: normalizar.
        "empresa_id_seguridad": str(u["empresa_id"]) if u.get("empresa_id") else None,
        "rol_seguridad": u.get("rol_seguridad"),
    }


# Perfiles que pueden asignarse como ADICIONALES (2026-10-15). Por ahora solo
# PADRINO: no gobierna ningún portal, solo la vista simplificada de /revision
# (cookie seguridadPerfil), así que puede sumarse a un perfil principal sin
# tocar ningún otro módulo.
PERFILES_ADICIONALES_VALIDOS = {"PADRINO"}


def _normalizar_perfiles_adicionales(perfil: str, adicionales: Optional[List[str]]) -> list:
    """Sanitiza la lista: mayúsculas, dedupe, solo válidos, y SIN redundancias
    (un SEGURIDAD/ADMIN/PADRINO principal no necesita el PADRINO adicional:
    los dos primeros tienen más permisos y el tercero ya es padrino)."""
    principal = str(perfil or "").upper()
    if principal in ("SEGURIDAD", "ADMIN", "PADRINO"):
        return []
    salida = []
    for p in adicionales or []:
        norm = str(p or "").strip().upper()
        if norm in PERFILES_ADICIONALES_VALIDOS and norm not in salida:
            salida.append(norm)
    return salida


def _campos_seguridad(data: BaseUsuario) -> dict:
    """Campos de empresa/rol del módulo de Estudios de Seguridad.

    Solo aplican al perfil CLIENTE_ESTUDIOS (cliente externo del portal):
    empresa_id (ObjectId validado de empresas_seguridad) + rol_seguridad
    (CONSULTADOR | ADMIN_EMPRESA). El perfil SEGURIDAD es personal interno
    de Integra (aprobación de placas /revision) y NO lleva empresa aquí.
    Devuelve {} para cualquier otro perfil (ignora los campos).
    """
    if data.perfil.upper() != "CLIENTE_ESTUDIOS":
        return {}
    empresa_id = (data.empresa_id_seguridad or "").strip()
    if not empresa_id:
        raise HTTPException(
            status_code=422,
            detail="El perfil CLIENTE_ESTUDIOS requiere seleccionar la empresa (módulo de Estudios de Seguridad)",
        )
    try:
        oid = ObjectId(empresa_id)
    except Exception:
        raise HTTPException(status_code=422, detail="empresa_id_seguridad inválido")
    empresas_seg = base_datos["empresas_seguridad"]
    if not empresas_seg.find_one({"_id": oid, "activo": True}):
        raise HTTPException(status_code=422, detail="La empresa de seguridad no existe o está inactiva")
    rol = (data.rol_seguridad or "CONSULTADOR").strip().upper()
    if rol not in ("CONSULTADOR", "ADMIN_EMPRESA"):
        raise HTTPException(status_code=422, detail="rol_seguridad debe ser CONSULTADOR o ADMIN_EMPRESA")
    return {"empresa_id": empresa_id, "rol_seguridad": rol}


def _buscar_baseusuario_activo(usuario: str, clave: str):
    """
    Login por CORREO (estricto). El campo del body sigue llamándose `usuario`
    para no romper el front desplegado, pero se espera un correo.
    """
    valor = usuario.strip()
    clave_ingresada = clave.strip()
    if "@" not in valor:
        return None
    query_correo = {"correo": {"$regex": f"^{re.escape(valor.lower())}$", "$options": "i"}}
    encontrados = list(coleccion_usuarios.find(query_correo).limit(5))
    if not encontrados:
        return None
    if len(encontrados) > 1:
        # Sin índice único en `correo` puede haber duplicados; determinístico:
        # el primero activo. El script de migración los reporta para sanear.
        print(f"⚠️ baseusuarios: correo duplicado en login → {valor.lower()}")
        encontrados.sort(key=lambda d: d.get("activo", True), reverse=True)
    encontrado = encontrados[0]
    if not encontrado.get("activo", True):
        raise HTTPException(status_code=403, detail="Usuario inactivo. Contacta al administrador.")
    clave_almacenada = str(encontrado.get("clave", "")).strip()
    if not verificar_clave(clave_ingresada, clave_almacenada):
        return None
    return encontrado


def _crear_token_baseusuario(usuario: dict) -> str:
    ahora = datetime.now(timezone.utc)
    payload = {
        "sub": str(usuario["_id"]),
        "usuario": usuario.get("usuario", ""),
        "perfil": str(usuario.get("perfil", "")).upper(),
        "auth_source": "baseusuarios",
        "iat": ahora,
        "exp": ahora + timedelta(minutes=BASEUSUARIOS_TOKEN_MINUTES),
    }
    return jwt.encode(payload, BASEUSUARIOS_JWT_SECRET, algorithm=BASEUSUARIOS_JWT_ALGORITHM)


async def obtener_baseusuario_actual(token: str = Depends(baseusuarios_oauth2)):
    error = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="No se pudo validar las credenciales",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, BASEUSUARIOS_JWT_SECRET, algorithms=[BASEUSUARIOS_JWT_ALGORITHM])
        if payload.get("auth_source") != "baseusuarios":
            raise error
        usuario_id = payload.get("sub")
        if not usuario_id:
            raise error
        usuario = coleccion_usuarios.find_one({"_id": ObjectId(usuario_id)})
    except HTTPException:
        raise
    except Exception as exc:
        raise error from exc
    if not usuario or not usuario.get("activo", True):
        raise error
    return modelo_usuario(usuario)

def normalizar_clientes(clientes: Optional[List[str]]) -> List[str]:
    clientes_validos = {"KABI", "MEDICAL_CARE"}
    clientes_normalizados = []
    for cliente in clientes or ["KABI"]:
        cliente_norm = str(cliente).upper().strip()
        if cliente_norm not in clientes_validos:
            raise HTTPException(status_code=400, detail=f"Cliente no reconocido: {cliente}")
        if cliente_norm not in clientes_normalizados:
            clientes_normalizados.append(cliente_norm)
    if not clientes_normalizados:
        raise HTTPException(status_code=400, detail="Debe seleccionar al menos un cliente")
    return clientes_normalizados


# (2026-10-29) Portales clásicos derivados de los NITs de clientes_siscore.
# `clientes` (KABI/MEDICAL_CARE) deja de editarse directo en GestionUsuarios:
# la asignación se hace por NIT y el portal se deriva de aquí.
PORTALES_POR_NIT = {
    "900402080": "KABI",        # Fresenius Kabi
    "901689684": "MEDICAL_CARE" # Fresenius Medical Care
}

# Perfiles que NUNCA reciben portales clásicos, tengan o no clientes asignados
# (externos: su único acceso es su portal propio).
PERFILES_SIN_PORTALES = {"CLIENTE", "CLIENTE_FMC"}


def _normalizar_clientes_siscore(nits: List[str]) -> List[str]:
    """NITs asignados del catálogo `clientes_siscore`: MAYÚSCULAS, strip,
    dedupe y validación de existencia (422 si alguno no está en el catálogo)."""
    norm: List[str] = []
    for nit in nits or []:
        n = str(nit or "").strip().upper()
        if n and n not in norm:
            norm.append(n)
    if not norm:
        return []
    encontrados = {
        d.get("nit") for d in base_datos["clientes_siscore"].find({"nit": {"$in": norm}}, {"nit": 1})
    }
    faltantes = [n for n in norm if n not in encontrados]
    if faltantes:
        raise HTTPException(
            status_code=422,
            detail=f"NITs no existentes en el catálogo clientes_siscore: {', '.join(faltantes)}",
        )
    return norm


def _portales_derivados(usuario: dict, nits: List[str]) -> List[str]:
    """`clientes` (portales) que corresponden a los NITs asignados. Los
    perfiles externos (CLIENTE/CLIENTE_FMC) no reciben portales clásicos.
    ADMIN siempre ve todo: todos los portales, tenga o no asignación."""
    perfil = str(usuario.get("perfil", "")).upper()
    if perfil in PERFILES_SIN_PORTALES:
        return []
    if perfil == "ADMIN":
        return list(PORTALES_POR_NIT.values())
    salida = []
    for n in nits:
        portal = PORTALES_POR_NIT.get(n)
        if portal and portal not in salida:
            salida.append(portal)
    return salida


def _normalizar_clientes_aprobacion(clientes: List[str]) -> List[str]:
    """Alcance de aprobación por cliente (Otros Costos). NO es lo mismo que
    `normalizar_clientes` (acceso a portales KABI/MEDICAL_CARE): aquí no hay
    whitelist fija, se valida contra el catálogo `clientes_otros_costos`
    (editable en Mongo). Canónico: [] = TODOS; "TODOS" en la lista colapsa a [].
    Mismas reglas de normalización que usa otros_costos.py al leer (norm_cliente_oc)."""
    norm: List[str] = []
    for cliente in clientes:
        c = norm_cliente_oc(cliente)
        if c and c not in norm:
            norm.append(c)
    if not norm or "TODOS" in norm:
        return []
    catalogo = {
        norm_cliente_oc(d.get("nombre", ""))
        for d in base_datos["clientes_otros_costos"].find({}, {"nombre": 1})
    }
    catalogo.discard("")
    faltantes = [c for c in norm if c not in catalogo]
    if faltantes:
        raise HTTPException(
            status_code=422,
            detail=f"Clientes no existentes en el catálogo de Otros Costos: {', '.join(faltantes)}",
        )
    return norm

def enviar_correo_codigo(destinatario: str, codigo: str):
    """Envía el código de verificación usando Resend de forma silenciosa."""
    
    if not resend.api_key or "TuApiKeyAqui" in resend.api_key:
         print("⚠️ ERROR: Falta API KEY de Resend.")
         return

    html_simple = f"""
    <p>Hola,</p>
    <p>Tu código de verificación es: <strong>{codigo}</strong></p>
    <p><small>Si no solicitaste este código, ignora este mensaje.</small></p>
    """

    try:
        params = {
            "from": MAIL_FROM,
            "to": [destinatario],
            "subject": f"Código de verificación: {codigo}",
            "html": html_simple,
        }
        resend.Emails.send(params)
        
    except Exception as e:
        print(f"❌ Error crítico enviando correo: {e}")


# ==============================================================================
# 🔐 RUTAS DE RECUPERACIÓN
# ==============================================================================

# 1. Verificar Usuario
@ruta_baseusuarios.post("/verificarRecuperacion")
async def verificar_recuperacion(data: VerificarInput, background_tasks: BackgroundTasks):
    usuario_norm = data.usuario.strip().upper()
    
    usuario = coleccion_usuarios.find_one({
        "usuario": usuario_norm,
        "perfil": data.perfil.upper()
    })
    
    if not usuario:
        return {"existe": False}

    codigo = str(random.randint(1000, 9999))
    
    coleccion_usuarios.update_one(
        {"_id": usuario["_id"]},
        {"$set": {"recovery_code": codigo}}
    )
    
    correo_destino = usuario.get("correo") or usuario_norm

    background_tasks.add_task(enviar_correo_codigo, correo_destino, codigo)

    return {"existe": True, "mensaje": "Código generado"}


# 2. Validar Código
@ruta_baseusuarios.post("/validarCodigoRecuperacion")
async def validar_codigo_recuperacion(data: ValidarCodigoInput):
    usuario_norm = data.usuario.strip().upper()
    
    usuario = coleccion_usuarios.find_one({
        "usuario": usuario_norm, 
        "perfil": data.perfil.upper()
    })
    
    if not usuario: 
        return {"valido": False}
    
    codigo_guardado = usuario.get("recovery_code")
    es_valido = (codigo_guardado is not None) and (codigo_guardado == data.codigo)
    
    return {"valido": es_valido}


# 3. Cambiar Contraseña
@ruta_baseusuarios.post("/cambiarClaveConductor")
async def cambiar_clave_conductor(data: CambioClaveInput):
    usuario_norm = data.usuario.strip().upper()
    
    usuario = coleccion_usuarios.find_one({
        "usuario": usuario_norm, 
        "perfil": data.perfil.upper()
    })
    
    if not usuario: 
        raise HTTPException(status_code=404, detail="Usuario no encontrado")

    codigo_guardado = usuario.get("recovery_code")
    if not codigo_guardado or codigo_guardado != data.codigo:
         raise HTTPException(status_code=403, detail="Código inválido o expirado")

    coleccion_usuarios.update_one(
        {"_id": usuario["_id"]},
        {
            "$set": {"clave": crear_hash(data.nuevaClave.strip())},
            "$unset": {"recovery_code": ""}
        }
    )
    return {"mensaje": "Clave actualizada correctamente"}


# ==============================================================================
# 🔓 RECUPERACIÓN DE CONTRASEÑA (sin perfil, para usuarios Torre de Control)
# ==============================================================================

@ruta_baseusuarios.post("/recuperar/solicitar")
async def recuperar_base_solicitar(data: RecuperarBaseInput, background_tasks: BackgroundTasks):
    correo_norm = data.correo.strip().lower()

    doc = coleccion_usuarios.find_one({"correo": {"$regex": f"^{correo_norm}$", "$options": "i"}})
    if not doc:
        print(f"⚠️ Recuperación: correo NO encontrado en BD → {correo_norm}")
        return {"mensaje": "Si el correo está registrado, se enviará un código de verificación."}

    print(f"✅ Recuperación: correo encontrado → {correo_norm} | usuario: {doc.get('usuario')} | correo en BD: {doc.get('correo')}")
    print(f"🔑 Código generado: para {correo_norm}")
    print(f"📧 RESEND_API_KEY configurada: {'Sí' if resend.api_key and 'TuApiKeyAqui' not in resend.api_key else 'NO'}")
    print(f"📧 MAIL_FROM: {MAIL_FROM}")

    codigo = str(random.randint(1000, 9999))
    coleccion_usuarios.update_one(
        {"_id": doc["_id"]},
        {"$set": {"recovery_code": codigo}}
    )

    background_tasks.add_task(enviar_correo_codigo, correo_norm, codigo)

    return {"mensaje": "Si el correo está registrado, se enviará un código de verificación."}


@ruta_baseusuarios.post("/recuperar/confirmar")
async def recuperar_base_confirmar(data: ConfirmarBaseInput):
    correo_norm = data.correo.strip().lower()

    doc = coleccion_usuarios.find_one({"correo": {"$regex": f"^{correo_norm}$", "$options": "i"}})
    if not doc:
        raise HTTPException(status_code=400, detail="Correo no encontrado")

    codigo_guardado = doc.get("recovery_code")
    if not codigo_guardado or codigo_guardado != data.codigo:
        raise HTTPException(status_code=403, detail="Código inválido o expirado")

    coleccion_usuarios.update_one(
        {"_id": doc["_id"]},
        {
            "$set": {"clave": crear_hash(data.nuevaClave.strip())},
            "$unset": {"recovery_code": ""}
        }
    )
    return {"mensaje": "Contraseña actualizada correctamente"}


# ==============================================================================
# 👤 RUTAS CRUD DE USUARIOS
# ==============================================================================

@ruta_baseusuarios.post("/", response_model=dict)
async def crear_baseusuario(data: BaseUsuario):
    if coleccion_usuarios.find_one({"usuario": data.usuario.upper()}):
        raise HTTPException(status_code=400, detail="El usuario ya existe")

    # (2026-10-29) Asignación por NIT (catálogo clientes_siscore): si viene,
    # valida y DERIVA los portales clásicos (los perfiles externos no reciben).
    # Si no viene, se usa el comportamiento histórico (clientes directo).
    perfil_nuevo = data.perfil.upper()
    nits = _normalizar_clientes_siscore(data.clientes_siscore) if data.clientes_siscore is not None else None
    if perfil_nuevo in ("CLIENTE_FMC", "CLIENTE"):
        clientes_val = []
    elif nits is not None:
        clientes_val = _portales_derivados({"perfil": perfil_nuevo}, nits)
    else:
        clientes_val = normalizar_clientes(data.clientes)

    nuevo = {
        "nombre": data.nombre.upper(),
        "correo": data.correo.upper() if data.correo else None,
        "regional": data.regional.upper(),
        "celular": data.celular.upper() if data.celular else None,
        "perfil": perfil_nuevo,
        "perfiles_adicionales": _normalizar_perfiles_adicionales(data.perfil, data.perfiles_adicionales),
        "usuario": data.usuario.upper(),
        "clave": crear_hash(data.clave.strip()),
        "clientes": clientes_val,
        **_campos_seguridad(data),
    }
    if nits is not None:
        nuevo["clientes_siscore"] = nits

    id_insertado = coleccion_usuarios.insert_one(nuevo).inserted_id
    usuario_insertado = coleccion_usuarios.find_one({"_id": id_insertado})
    return {"mensaje": "Usuario creado", "usuario": modelo_usuario(usuario_insertado)}


@ruta_baseusuarios.get("/", response_model=List[dict])
async def obtener_baseusuarios():
    usuarios = coleccion_usuarios.find()
    return [modelo_usuario(u) for u in usuarios]


# (2026-10-14) PADRINO: como Seguridad pero SOLO impersonar placas para
# cargar documentos/datos — sin autorizar estados ni gestionar conductores
# (guards en vehiculos.actualizar-estado y conductores.invitar/desvincular).
PERFILES_VALIDOS = ['ADMIN', 'ANALISTA', 'CLIENTE', 'CLIENTE_ESTUDIOS', 'CLIENTE_FMC', 'CONDUCTOR', 'CONTROL', 'COORDINADOR', 'DESPACHADOR', 'FINANCIERO', 'OPERADOR', 'OPERATIVO', 'PADRINO', 'SEGURIDAD', 'VISUALIZADOR']

@ruta_baseusuarios.get("/perfiles-disponibles", response_model=List[str])
async def obtener_perfiles_disponibles():
    return PERFILES_VALIDOS


@ruta_baseusuarios.get("/despachadores", response_model=List[UsuarioLite])
async def listar_despachadores():
    cursor = coleccion_usuarios.find(
        {"perfil": "DESPACHADOR"}, 
        {"nombre": 1, "usuario": 1}
    ).sort("nombre", 1)
    
    return [{"id": str(u["_id"]), "nombre": u["nombre"], "usuario": u["usuario"]} for u in cursor]
    

@ruta_baseusuarios.get("/{usuario_id}", response_model=dict)
async def obtener_baseusuario(usuario_id: str):
    try:
        oid = ObjectId(usuario_id)
    except Exception:
        raise HTTPException(status_code=400, detail="ID inválido")
    
    usuario = coleccion_usuarios.find_one({"_id": oid})
    if not usuario:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
        
    return modelo_usuario(usuario)


@ruta_baseusuarios.put("/{usuario_id}", response_model=dict)
async def actualizar_baseusuario(usuario_id: str, data: BaseUsuario):
    try:
        oid = ObjectId(usuario_id)
    except Exception:
        raise HTTPException(status_code=400, detail="ID inválido")
        
    # (2026-10-29) clientes_siscore: solo se toca si viene explícito (el PUT
    # completo no debe borrar la asignación de un form que no la envía).
    perfil_actualiza = data.perfil.upper()
    nits = _normalizar_clientes_siscore(data.clientes_siscore) if data.clientes_siscore is not None else None
    if perfil_actualiza in ("CLIENTE_FMC", "CLIENTE"):
        clientes_val = []
    elif nits is not None:
        clientes_val = _portales_derivados({"perfil": perfil_actualiza}, nits)
    else:
        clientes_val = normalizar_clientes(data.clientes)

    actualiza = {
        "nombre": data.nombre.upper(),
        "correo": data.correo.upper() if data.correo else None,
        "regional": data.regional.upper(),
        "celular": data.celular.upper() if data.celular else None,
        "perfil": perfil_actualiza,
        "perfiles_adicionales": _normalizar_perfiles_adicionales(data.perfil, data.perfiles_adicionales),
        "usuario": data.usuario.upper(),
        "clientes": clientes_val,
    }
    if nits is not None:
        actualiza["clientes_siscore"] = nits
    # Seguridad: si es SEGURIDAD setea empresa/rol (validados); si dejó de
    # serlo limpia los campos para no dejar acceso huérfano al portal.
    campos_seg = _campos_seguridad(data)
    if campos_seg:
        actualiza.update(campos_seg)
    else:
        actualiza["empresa_id"] = None
        actualiza["rol_seguridad"] = None
    # Solo pisar la clave si viene una clave NUEVA en claro; si el cliente
    # reenvía el hash (que nunca debería tener) se conserva la existente.
    if data.clave and data.clave.strip() and not es_hash(data.clave.strip()):
        actualiza["clave"] = crear_hash(data.clave.strip())

    result = coleccion_usuarios.update_one({"_id": oid}, {"$set": actualiza})
    
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
        
    actualizado = coleccion_usuarios.find_one({"_id": oid})
    return {"mensaje": "Usuario actualizado", "usuario": modelo_usuario(actualizado)}


@ruta_baseusuarios.delete("/{usuario_id}", response_model=dict)
async def eliminar_baseusuario(usuario_id: str):
    try:
        oid = ObjectId(usuario_id)
    except Exception:
        raise HTTPException(status_code=400, detail="ID inválido")
        
    result = coleccion_usuarios.delete_one({"_id": oid})
    
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
        
    return {"mensaje": "Usuario eliminado"}


# ==============================================================================
# 🚪 RUTAS DE LOGIN
# ==============================================================================

def _detalle_login_invalido(usuario: str) -> str:
    """(2026-10-16) Motivo ESPECÍFICO del rechazo (pedido del usuario): la
    cuenta no existe es distinto de clave errónea — antes todo era el genérico
    «Usuario o clave incorrectos». Solo se consulta en el camino de FALLO del
    login, así que no cuesta nada en el caso feliz."""
    valor = usuario.strip()
    if "@" not in valor:
        return ("Ingresa tu CORREO institucional (ej: nombre@empresa.com) — "
                "el usuario de texto ya no se usa para entrar.")
    query_correo = {"correo": {"$regex": f"^{re.escape(valor.lower())}$", "$options": "i"}}
    if coleccion_usuarios.find_one(query_correo, {"_id": 1}) is None:
        return (f"No existe una cuenta con el correo {valor}. "
                "Verifícalo o pide al administrador que la cree.")
    return "El correo es correcto pero la contraseña NO lo es. Verifícala e inténtalo de nuevo."


@ruta_baseusuarios.post("/login", response_model=dict)
async def login_baseusuario(usuario: str = Body(..., embed=True), clave: str = Body(..., embed=True)):
    encontrado = _buscar_baseusuario_activo(usuario, clave)
    if not encontrado:
        raise HTTPException(status_code=401, detail=_detalle_login_invalido(usuario))
    return {
        "mensaje": "Login exitoso",
        "access_token": _crear_token_baseusuario(encontrado),
        "token_type": "bearer",
        "usuario": {
            "id": str(encontrado["_id"]),
            "usuario": encontrado["usuario"],
            "nombre": encontrado.get("nombre", ""),
            "correo": encontrado.get("correo") or "",
            "perfil": encontrado["perfil"],
            "perfiles_adicionales": encontrado.get("perfiles_adicionales") or [],
            "regional": encontrado["regional"],
            "clientes": encontrado.get("clientes") or ["KABI"],
            "clientes_siscore": encontrado.get("clientes_siscore") if isinstance(encontrado.get("clientes_siscore"), list) else None,
        }
    }


@ruta_baseusuarios.post("/token", tags=["Autenticación"])
async def token_baseusuario(form_data: OAuth2PasswordRequestForm = Depends()):
    """Entrega el Bearer JWT usado por Swagger y los endpoints administrativos."""
    encontrado = _buscar_baseusuario_activo(form_data.username, form_data.password)
    if not encontrado:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuario o clave incorrectos",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return {
        "access_token": _crear_token_baseusuario(encontrado),
        "token_type": "bearer",
        "usuario": encontrado.get("usuario", ""),
        "perfil": str(encontrado.get("perfil", "")).upper(),
    }


@ruta_baseusuarios.patch("/{id}/activo", response_model=dict)
async def toggle_activo_usuario(id: str, activo: bool = Body(..., embed=True)):
    try:
        oid = ObjectId(id)
    except Exception:
        raise HTTPException(status_code=400, detail="ID inválido")
    result = coleccion_usuarios.update_one({"_id": oid}, {"$set": {"activo": activo}})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    return {"mensaje": "Estado actualizado", "activo": activo}


_NOTIF_MC_VALIDAS = {'retraso_operacion', 'sin_cruce'}

@ruta_baseusuarios.patch("/{id}/notificaciones_mc", response_model=dict)
async def actualizar_notificaciones_mc(id: str, notificaciones_mc: List[str] = Body(..., embed=True)):
    try:
        oid = ObjectId(id)
    except Exception:
        raise HTTPException(status_code=400, detail="ID inválido")
    invalidas = [n for n in notificaciones_mc if n not in _NOTIF_MC_VALIDAS]
    if invalidas:
        raise HTTPException(status_code=400, detail=f"Notificaciones inválidas: {invalidas}")
    result = coleccion_usuarios.update_one({"_id": oid}, {"$set": {"notificaciones_mc": notificaciones_mc}})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    return {"mensaje": "Notificaciones actualizadas", "notificaciones_mc": notificaciones_mc}


@ruta_baseusuarios.patch("/{id}/datos", response_model=dict)
async def actualizar_datos_usuario(id: str, data: ActualizarDatosInput):
    try:
        oid = ObjectId(id)
    except Exception:
        raise HTTPException(status_code=400, detail="ID inválido")

    actualiza: dict = {
        "nombre":   data.nombre.strip().upper(),
        "correo":   data.correo.strip().upper() if data.correo and data.correo.strip() else None,
        "regional": data.regional.strip().upper(),
        "celular":  data.celular.strip().upper() if data.celular and data.celular.strip() else None,
    }

    # Verificar si se está cambiando el usuario
    if data.usuario and data.usuario.strip():
        nuevo_usuario = data.usuario.strip().upper()
        # Buscar si ya existe otro usuario con ese nombre
        usuario_existente = coleccion_usuarios.find_one({
            "usuario": nuevo_usuario,
            "_id": {"$ne": oid}
        })
        if usuario_existente:
            raise HTTPException(status_code=400, detail="El nombre de usuario ya está en uso")
        actualiza["usuario"] = nuevo_usuario

    if data.clave and data.clave.strip() and not es_hash(data.clave.strip()):
        actualiza["clave"] = crear_hash(data.clave.strip())

    result = coleccion_usuarios.update_one({"_id": oid}, {"$set": actualiza})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    actualizado = coleccion_usuarios.find_one({"_id": oid})
    return {"mensaje": "Datos actualizados", "usuario": modelo_usuario(actualizado)}


@ruta_baseusuarios.patch("/{id}/clientes", response_model=dict)
async def actualizar_clientes_usuario(id: str, data: ActualizarClientesInput):
    try:
        oid = ObjectId(id)
    except Exception:
        raise HTTPException(status_code=400, detail="ID inválido")

    clientes = normalizar_clientes(data.clientes)

    result = coleccion_usuarios.update_one(
        {"_id": oid},
        {"$set": {"clientes": clientes}}
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")

    return {"mensaje": "Clientes actualizados", "clientes": clientes}


@ruta_baseusuarios.patch("/{id}/clientes-aprobacion", response_model=dict)
async def actualizar_clientes_aprobacion_usuario(id: str, data: ActualizarClientesAprobacionInput):
    """Alcance de aprobación por cliente en Otros Costos (sólo tiene efecto en
    perfiles COORDINADOR/CONTROL): [] = TODOS los clientes."""
    try:
        oid = ObjectId(id)
    except Exception:
        raise HTTPException(status_code=400, detail="ID inválido")

    clientes = _normalizar_clientes_aprobacion(data.clientes_aprobacion)

    result = coleccion_usuarios.update_one(
        {"_id": oid},
        {"$set": {"clientes_aprobacion": clientes}}
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")

    return {
        "mensaje": "Alcance de aprobación actualizado",
        "clientes_aprobacion": clientes,
        "todos": not clientes,
    }


@ruta_baseusuarios.patch("/{id}/clientes-siscore", response_model=dict)
async def actualizar_clientes_siscore_usuario(id: str, data: ActualizarClientesSiscoreInput):
    """(2026-10-29) Clientes que el usuario puede ver, tomados del catálogo
    `clientes_siscore` (NITs). Asignable a CUALQUIER perfil, sin importar el
    rango. Es la fuente de verdad: el campo `clientes` (portales KABI /
    MEDICAL_CARE del selector de login) se DERIVA de aquí con PORTALES_POR_NIT
    (los perfiles externos CLIENTE/CLIENTE_FMC no reciben portales clásicos).
    [] = sin clientes asignados (perfil CLIENTE sin tablero todavía)."""
    try:
        oid = ObjectId(id)
    except Exception:
        raise HTTPException(status_code=400, detail="ID inválido")

    usuario = coleccion_usuarios.find_one({"_id": oid})
    if not usuario:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")

    nits = _normalizar_clientes_siscore(data.clientes_siscore)
    portales = _portales_derivados(usuario, nits)

    coleccion_usuarios.update_one(
        {"_id": oid},
        {"$set": {"clientes_siscore": nits, "clientes": portales}}
    )
    return {
        "mensaje": "Clientes asignados actualizados",
        "clientes_siscore": nits,
        "clientes": portales,
    }


class ActualizarPerfilInput(BaseModel):
    perfil: str

@ruta_baseusuarios.patch("/{id}/perfil", response_model=dict)
async def actualizar_perfil_usuario(id: str, data: ActualizarPerfilInput):
    try:
        oid = ObjectId(id)
    except Exception:
        raise HTTPException(status_code=400, detail="ID inválido")

    perfil_norm = data.perfil.strip().upper()
    if not perfil_norm:
        raise HTTPException(status_code=400, detail="El perfil no puede estar vacío")

    result = coleccion_usuarios.update_one(
        {"_id": oid},
        {"$set": {"perfil": perfil_norm}}
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")

    return {"mensaje": "Perfil actualizado", "perfil": perfil_norm}


class ActualizarPerfilesAdicionalesInput(BaseModel):
    perfiles_adicionales: List[str] = []


@ruta_baseusuarios.patch("/{id}/perfiles-adicionales", response_model=dict)
async def actualizar_perfiles_adicionales(id: str, data: ActualizarPerfilesAdicionalesInput):
    """(2026-10-15) Perfiles ADICIONALES del usuario (hoy solo PADRINO): el
    perfil principal NO cambia — p. ej. un OPERATIVO que además entra al
    portal de Seguridad en modo Padrino. Si el perfil principal pasa a
    SEGURIDAD/ADMIN/PADRINO, la lista se colapsa a [] (redundante)."""
    try:
        oid = ObjectId(id)
    except Exception:
        raise HTTPException(status_code=400, detail="ID inválido")

    usuario = coleccion_usuarios.find_one({"_id": oid})
    if not usuario:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")

    adicionales = _normalizar_perfiles_adicionales(usuario.get("perfil"), data.perfiles_adicionales)
    coleccion_usuarios.update_one({"_id": oid}, {"$set": {"perfiles_adicionales": adicionales}})
    return {"mensaje": "Perfiles adicionales actualizados", "perfiles_adicionales": adicionales}


@ruta_baseusuarios.post("/loginseguridad", response_model=dict)
async def login_seguridad(correo: str = Body(..., embed=True), clave: str = Body(..., embed=True)):

    correo_limpio = correo.strip()
    clave_ingresada = clave.strip()
    encontrado = coleccion_usuarios.find_one({
        "correo": {"$regex": f"^{correo_limpio}$", "$options": "i"}
    })
    
    if not encontrado:
        raise HTTPException(status_code=401, detail="Correo o clave incorrectos")
        
    clave_almacenada = str(encontrado.get("clave", "")).strip()
    if not verificar_clave(clave_ingresada, clave_almacenada):
        raise HTTPException(status_code=401, detail="Correo o clave incorrectos")

    perfil = encontrado.get("perfil", "").strip().upper()
    if perfil not in ["SEGURIDAD", "ADMIN"]:
        raise HTTPException(status_code=403, detail="No tiene permisos de Seguridad")
        
    return {
        "mensaje": "Login seguridad exitoso", 
        "usuario": {
            "id": str(encontrado["_id"]), 
            "nombre": encontrado.get("nombre", "Usuario"), 
            "usuario": encontrado.get("usuario", ""), 
            "correo": encontrado.get("correo", ""),
            "perfil": perfil
        }
    }
#login conductor

@ruta_baseusuarios.post("/loginConductor", response_model=dict)
async def login_Conductor(usuario: str = Body(..., embed=True), clave: str = Body(..., embed=True)):
    
    usuario_ingresado = usuario.strip()
    clave_ingresada = clave.strip()
    encontrado = coleccion_usuarios.find_one({
        "correo": {"$regex": f"^{re.escape(usuario_ingresado)}$", "$options": "i"}
    })

    if not encontrado:
        raise HTTPException(status_code=401, detail="Usuario o clave incorrectos")

    clave_almacenada = str(encontrado.get("clave", "")).strip()
    if not verificar_clave(clave_ingresada, clave_almacenada):
        raise HTTPException(status_code=401, detail="Usuario o clave incorrectos")
        
    perfil = encontrado.get("perfil", "").strip().upper()
    if perfil not in ["CONDUCTOR", "ADMIN"]:
        raise HTTPException(status_code=403, detail="No tiene permisos de Seguridad")
        
    nombre_completo = encontrado.get("nombre", "").strip() 
    primer_nombre = nombre_completo.split(' ')[0] 
        
    return {
        "mensaje": "Login Conductor exitoso", 
        "usuario": {
            "id": str(encontrado["_id"]), 
            "usuario": encontrado["usuario"], 
            "perfil": perfil,
            "primerNombre": primer_nombre 
        }
    }
