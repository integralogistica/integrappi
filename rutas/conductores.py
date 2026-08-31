import os
import random
import re
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional

import resend
from fastapi import APIRouter, BackgroundTasks, Body, HTTPException, Request, status
from pydantic import BaseModel

from bd.bd_cliente import bd_cliente
from Funciones.claves import crear_hash, verificar_clave

# ==============================================================================
# 🔗 CONFIGURACIÓN DE BASE DE DATOS
# ==============================================================================
# Los conductores viven en `conductores`. `baseusuarios` solo se consulta para
# permitir el acceso de ADMIN al portal de conductores.
bd = bd_cliente["integra"]
coleccion_conductores = bd["conductores"]
coleccion_baseusuarios = bd["baseusuarios"]
# Vehículos: se escriben desde aquí (invitación/vinculación) para evitar import
# circular con rutas/vehiculos.py, que importa Mongo por su cuenta.
coleccion_vehiculos = bd["vehiculos"]
# Políticas de tratamiento de datos (Habeas Data): versionadas en `politicas_datos`
# (una sola activa) y evidencia append-only de aceptaciones en `aceptaciones_politica`.
coleccion_politicas = bd["politicas_datos"]
coleccion_aceptaciones = bd["aceptaciones_politica"]

# Índice único (el correo se guarda en MAYÚSCULAS → unicidad case-insensitive).
try:
    coleccion_conductores.create_index("correo", unique=True)
    coleccion_politicas.create_index("version", unique=True)
    coleccion_aceptaciones.create_index([("conductor_id", 1), ("version", 1)])
    # Cédula: sparse (cuentas legacy no la tienen); NO unique hasta sanear datos.
    coleccion_conductores.create_index("cedula", sparse=True)
    coleccion_vehiculos.create_index("idConductor")
except Exception:
    pass


# ==============================================================================
# 🚦 CONFIGURACIÓN DEL ROUTER
# ==============================================================================
ruta_conductores = APIRouter(
    prefix="/conductores",
    tags=["Conductores"],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}},
)


# ==============================================================================
# 🔑 CONFIGURACIÓN RESEND
# ==============================================================================
resend.api_key = os.getenv("RESEND_API_KEY", "re_TuApiKeyAqui...")
MAIL_FROM = os.getenv("MAIL_FROM", "no-reply@integralogistica.com")
FRONTEND_URL_VERIFICAR = os.getenv(
    "FRONTEND_URL_VERIFICAR",
    "https://integralogistica.com/integrapp/VerificarCorreo",
)
# Página de aceptación de invitación de conductor (la crea el tenedor).
FRONTEND_URL_INVITACION = os.getenv(
    "FRONTEND_URL_INVITACION",
    "https://integralogistica.com/integrapp/AceptarInvitacion",
)
EXPIRA_HORAS_VERIFICACION = int(os.getenv("VERIFICACION_EXPIRE_HORAS", "48"))


# ==============================================================================
# 📜 POLÍTICAS DE TRATAMIENTO DE DATOS
# ==============================================================================
# Primera versión sembrada automáticamente si `politicas_datos` está vacía
# (patrón de /tipos-costo en otros_costos.py). Editable desde Mongo o desde los
# endpoints de admin sin redeploy; nuevas versiones van con version+1.
POLITICA_DATOS_TITULO_V1 = "Política de Tratamiento de Datos Personales — Habeas Data"
POLITICA_DATOS_V1_HTML = """
<p><strong>Responsable del tratamiento:</strong> Integra Cadena de Servicios S.A.S.
(nit 901.442.833-5), en adelante <em>Integra</em>.</p>
<p>En cumplimiento de la <strong>Ley Estatutaria 1581 de 2012</strong>, el
<strong>Decreto 1074 de 2015 (art. 2.2.4.2)</strong> y demás normas concordantes
de protección de datos personales en Colombia, Integra informa lo siguiente:</p>

<h4>1. Datos que se recolectan</h4>
<p>Nombre completo, número de cédula de ciudadanía, número de celular, correo
electrónico, regional, información del vehículo (placa, línea, documentos
soporte) y documentos personales asociados a la hoja de vida del conductor.</p>

<h4>2. Finalidad del tratamiento</h4>
<ul>
  <li>Registro, verificación y aprobación de conductores y vehículos para la
      prestación del servicio de transporte.</li>
  <li>Verificación de seguridad y documental del conductor y su vehículo.</li>
  <li>Gestión contractual, operativa y de pagos del servicio prestado.</li>
  <li>Comunicaciones asociadas al servicio y a la plataforma IntegrApp.</li>
</ul>

<h4>3. Derechos del titular (ARCO)</h4>
<p>Como titular de los datos usted tiene derecho a conocer, actualizar,
rectificar y suprimir sus datos personales, así como a revocar la autorización
otorgada, en los términos de la Ley 1581 de 2012. Estos derechos pueden
ejercerse escribiendo al correo del responsable de tratamiento de datos.</p>

<h4>4. Autorización</h4>
<p>Al marcar la casilla de aceptación, el titular autoriza de forma previa,
expresa e inequívoca el tratamiento de sus datos personales para las
finalidades descritas anteriormente.</p>

<h4>5. Vigencia</h4>
<p>La presente política rige desde su publicación y puede ser actualizada;
cada actualización genera una nueva versión que le será notificada cuando
sea requerido por la ley.</p>
"""

# ── v2: DECLARACIONES DE VINCULACIÓN (2026-08-27) ─────────────────────────────
# Cada declaración se acepta INDIVIDUALMENTE (checkbox por declaración) y deja
# evidencia propia en `aceptaciones_politica` (una entrada por declaración).
# Si la política activa no tiene `declaraciones`, se auto-publica esta versión.
DECLARACIONES_V2 = [
    {
        "id": "origen_fondos",
        "titulo": "Declaración 1 — Origen de Fondos",
        "texto_html": (
            "<p>Declaro que los recursos que entrego y/o recibiré en desarrollo de mi "
            "vinculación con ORION TRANSPORTADORA DE CARGA S.A.S. provienen de actividades "
            "lícitas y que no me encuentro incluido en listas vinculantes o restrictivas "
            "relacionadas con el lavado de activos, la financiación del terrorismo u otros "
            "delitos asociados.</p>"
        ),
    },
    {
        "id": "sarlaft",
        "titulo": "Declaración 2 — SARLAFT",
        "texto_html": (
            "<p>Declaro que he sido informado(a) sobre las políticas y lineamientos del "
            "Sistema de Administración del Riesgo de Lavado de Activos y de la Financiación "
            "del Terrorismo (SARLAFT) adoptados por ORION TRANSPORTADORA DE CARGA S.A.S., y "
            "me comprometo a cumplir las disposiciones que me sean aplicables y a reportar "
            "cualquier situación inusual o sospechosa de la que tenga conocimiento en el "
            "desarrollo de mis actividades.</p>"
        ),
    },
    {
        "id": "ptee",
        "titulo": "Declaración 3 — PTEE",
        "texto_html": (
            "<p>Declaro que he leído la Política del Programa de Transparencia y Ética "
            "Empresarial (PTEE) de ORION TRANSPORTADORA DE CARGA S.A.S. y me comprometo a "
            "cumplir sus lineamientos, actuando con integridad y reportando cualquier "
            "situación que pueda constituir fraude, corrupción, soborno o cualquier conducta "
            "contraria a la ley o a las políticas de la organización.</p>"
        ),
    },
    {
        "id": "informacion_veraz",
        "titulo": "Declaración 4 — Información Veraz",
        "texto_html": (
            "<p>Declaro que la información suministrada es veraz y autorizo su verificación "
            "ante cualquier entidad pública o privada y me comprometo a actualizar los datos "
            "y documentos entregados.</p>"
        ),
    },
    {
        "id": "tratamiento_datos",
        "titulo": "Declaración 5 — Tratamiento de Datos Personales",
        "texto_html": (
            "<p>Autorizo de manera voluntaria, previa, expresa, informada e inequívoca a "
            "ORION TRANSPORTADORA DE CARGA S.A.S., identificada con NIT 800047876, para "
            "recolectar, almacenar, usar, procesar, actualizar, transferir, transmitir, "
            "circular y, en general, tratar mis datos personales de conformidad con la "
            "Ley 1581 de 2012, el Decreto 1377 de 2013 y demás normas que los modifiquen, "
            "adicionen o sustituyan, así como con la Política de Protección de Datos "
            "Personales de la organización.</p>"
            "<p>Declaro que he sido informado(a) de mis derechos como titular de los datos "
            "personales, entre ellos: conocer, actualizar y rectificar mis datos; solicitar "
            "prueba de la autorización otorgada; conocer el uso dado a mis datos; revocar la "
            "autorización y/o solicitar la supresión de los datos cuando sea procedente; "
            "acceder gratuitamente a mis datos personales; y ejercer los demás derechos "
            "consagrados en el artículo 8 de la Ley 1581 de 2012.</p>"
            "<p>Entiendo que mis datos personales podrán ser tratados para fines relacionados "
            "con la actualización de información, conocimiento de contrapartes, validación de "
            "identidad, verificación de antecedentes legales, penales y financieros, procesos "
            "de debida diligencia y consultas en bases de datos públicas y privadas, así como "
            "para las demás finalidades descritas en la Política de Protección de Datos "
            "Personales.</p>"
            "<p>Asimismo, manifiesto que conozco que puedo ejercer mis derechos mediante "
            "comunicación dirigida a la Carrera 68A No. 19-80 o al correo electrónico "
            "oficialdecumplimiento@transorion.com.co, y que la Política de Protección de "
            "Datos Personales se encuentra disponible para consulta en el siguiente enlace: "
            "Política de Protección de Datos Personales, documento que declaro conocer y me "
            "comprometo a consultar.</p>"
        ),
    },
    {
        "id": "seguridad_salud",
        "titulo": "Declaración 6 — Seguridad y Salud",
        "texto_html": (
            "<p>Me comprometo a cumplir las normas de Seguridad y Salud en el Trabajo y a "
            "reportar de manera inmediata cualquier acto o condición insegura, incidente, "
            "accidente, novedad en mi estado de salud o situación que pueda poner en riesgo "
            "mi integridad o la de terceros.</p>"
        ),
    },
    {
        "id": "pesv",
        "titulo": "Declaración 7 — PESV",
        "texto_html": (
            "<p>Me comprometo a realizar las inspecciones preoperacionales del vehículo, "
            "reportar de manera inmediata cualquier falla o condición que afecte su "
            "operación segura, así como cualquier novedad en mi estado de salud, condición "
            "de fatiga o situación que pueda poner en riesgo mi seguridad, la de los demás "
            "actores viales o la integridad de la carga.</p>"
        ),
    },
]

POLITICA_DATOS_TITULO_V2 = "Declaraciones de Vinculación y Autorización de Tratamiento de Datos Personales"

# Declaraciones que NO se exigen para activar la cuenta (2026-08-31, orden del
# usuario): si el titular no las marca, el flujo continúa igual; si las marca,
# la evidencia se registra como con las demás. En la UI NO se comunican como
# opcionales (se ven iguales a las exigidas).
DECLARACIONES_NO_EXIGIDAS = {"tratamiento_datos"}


def _politica_vigente() -> Optional[dict]:
    """
    Política activa. Auto-siembra la v1 si la colección está vacía y, si la
    activa no tiene `declaraciones` (modelo v1 de política única), auto-publica
    la v2 con las 7 declaraciones de vinculación individuales.
    """
    if coleccion_politicas.count_documents({}) == 0:
        coleccion_politicas.insert_one({
            "version": 1,
            "titulo": POLITICA_DATOS_TITULO_V1,
            "texto_html": POLITICA_DATOS_V1_HTML,
            "activo": True,
            "publicado_en": datetime.now(timezone.utc),
            "publicado_por": "SISTEMA",
        })
    politica = coleccion_politicas.find_one({"activo": True}, sort=[("version", -1)])
    if politica and not politica.get("declaraciones"):
        # Upgrade a v2: declaraciones individuales (Origen de Fondos, SARLAFT,
        # PTEE, Información Veraz, Tratamiento de Datos, SST, PESV).
        ultima = coleccion_politicas.find_one({}, sort=[("version", -1)])
        nueva_version = (ultima or {}).get("version", 1) + 1
        coleccion_politicas.update_many({"activo": True}, {"$set": {"activo": False}})
        coleccion_politicas.insert_one({
            "version": nueva_version,
            "titulo": POLITICA_DATOS_TITULO_V2,
            "declaraciones": DECLARACIONES_V2,
            "activo": True,
            "publicado_en": datetime.now(timezone.utc),
            "publicado_por": "SISTEMA",
            "auto_upgrade": True,
        })
        politica = coleccion_politicas.find_one({"activo": True}, sort=[("version", -1)])
    return politica


def _politica_publica(doc: dict) -> dict:
    """Proyección de la política para respuestas públicas (sin metadatos internos)."""
    return {
        "version": doc.get("version"),
        "titulo": doc.get("titulo", ""),
        "texto_html": doc.get("texto_html", ""),
        "declaraciones": doc.get("declaraciones", []),
        "publicado_en": doc.get("publicado_en"),
    }


# ==============================================================================
# 📌 ESQUEMAS DE DATOS
# ==============================================================================
class RegistrarConductorInput(BaseModel):
    # `usuario` se conserva por compatibilidad con el front desplegado; el
    # identificador real del conductor es `correo` (login solo por correo).
    nombre: str
    usuario: Optional[str] = None  # ignorado; se usa `correo`
    correo: str
    clave: str
    cedula: Optional[str] = None
    celular: Optional[str] = None
    regional: Optional[str] = None
    perfil: Optional[str] = None  # CONDUCTOR (default) o TENEDOR (dueño del vehículo)


class VerificarInput(BaseModel):
    usuario: str  # contiene el correo (compatibilidad de body con el front)
    perfil: Optional[str] = None


class ValidarCodigoInput(BaseModel):
    usuario: str  # contiene el correo
    codigo: str
    perfil: Optional[str] = None


class CambioClaveInput(BaseModel):
    usuario: str  # contiene el correo
    nuevaClave: str
    codigo: str
    perfil: Optional[str] = None


# ==============================================================================
# 🛠️ HELPERS
# ==============================================================================
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
        resend.Emails.send({
            "from": MAIL_FROM,
            "to": [destinatario],
            "subject": f"Código de verificación: {codigo}",
            "html": html_simple,
        })
    except Exception as e:
        print(f"❌ Error crítico enviando correo: {e}")


def _existe_correo(correo: str) -> bool:
    if not correo:
        return False
    patron = {"$regex": f"^{re.escape(correo.strip())}$", "$options": "i"}
    return coleccion_conductores.find_one({"correo": patron}) is not None


def _buscar_por_usuario(usuario_o_correo: str):
    """Conductor por correo exacto (case-insensitive), para recuperación de clave."""
    if not usuario_o_correo:
        return None
    patron = {"$regex": f"^{re.escape(usuario_o_correo.strip())}$", "$options": "i"}
    return coleccion_conductores.find_one({"correo": patron})


def _generar_token_verificacion(doc_id) -> str:
    """Token plano de un solo uso; en BD queda solo su hash (patrón de aut2.py)."""
    token_plano = secrets.token_urlsafe(32)
    coleccion_conductores.update_one(
        {"_id": doc_id},
        {"$set": {
            "verificacion_token_hash": crear_hash(token_plano),
            "verificacion_expira": datetime.now(timezone.utc) + timedelta(hours=EXPIRA_HORAS_VERIFICACION),
        }},
    )
    return token_plano


def _verificar_token_verificacion(doc: dict, token_plano: str) -> bool:
    token_hash = doc.get("verificacion_token_hash")
    expira = doc.get("verificacion_expira")
    if not token_hash or not expira:
        return False
    # Fechas Mongo naive = UTC (convención del proyecto).
    if expira.tzinfo is None:
        expira = expira.replace(tzinfo=timezone.utc)
    if datetime.now(timezone.utc) > expira:
        return False
    return verificar_clave(token_plano, token_hash)


def _buscar_conductor_por_token(token_plano: str) -> Optional[dict]:
    """
    Conductor cuyo token de verificación coincide (hash + expiración).
    Comparte el bucle entre /verificar-correo (GET) y /aceptar-politica (POST).
    """
    if not token_plano:
        return None
    for doc in coleccion_conductores.find(
        {"verificacion_token_hash": {"$exists": True}},
    ).limit(200):
        if _verificar_token_verificacion(doc, token_plano):
            return doc
    return None


def enviar_correo_verificacion(destinatario: str, enlace: str, nombre: str):
    """Envía el correo con el enlace de verificación usando Resend (fire-and-forget)."""
    if not resend.api_key or "TuApiKeyAqui" in resend.api_key:
        print("⚠️ ERROR: Falta API KEY de Resend; no se envió el correo de verificación.")
        return
    html = f"""
    <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 520px; margin: 0 auto;">
      <h2 style="color: #0f1928;">¡Bienvenido a IntegrApp, {nombre}!</h2>
      <p>Para activar tu cuenta de conductor y continuar con el registro de tu vehículo,
      confirma tu correo electrónico con el siguiente enlace:</p>
      <p>Al verificar tu correo deberás leer y aceptar nuestras Políticas de
      Tratamiento de Datos Personales (Habeas Data).</p>
      <p style="text-align: center; margin: 28px 0;">
        <a href="{enlace}"
           style="background: #0f1928; color: #fff; padding: 12px 28px; border-radius: 10px;
                  text-decoration: none; font-weight: bold;">
          Verificar mi correo
        </a>
      </p>
      <p>O copia y pega este enlace en tu navegador:</p>
      <p><a href="{enlace}">{enlace}</a></p>
      <p><small>El enlace vence en {EXPIRA_HORAS_VERIFICACION} horas. Si no solicitaste esta
      cuenta, ignora este mensaje.</small></p>
    </div>
    """
    try:
        resend.Emails.send({
            "from": MAIL_FROM,
            "to": [destinatario],
            "subject": "Verifica tu correo — IntegrApp Conductores",
            "html": html,
        })
        print(f"📧 Correo de verificación enviado a {destinatario}")
    except Exception as e:
        print(f"❌ Error enviando correo de verificación: {e}")


# ==============================================================================
# 📝 REGISTRO
# ==============================================================================
@ruta_conductores.post("/registrar", response_model=dict)
async def registrar_conductor(data: RegistrarConductorInput, background_tasks: BackgroundTasks):
    correo_norm = (data.correo or "").strip()
    clave_plana = (data.clave or "").strip()

    if _existe_correo(correo_norm):
        raise HTTPException(status_code=400, detail="El usuario ya existe")

    if len(clave_plana) < 6:
        raise HTTPException(status_code=400, detail="La clave debe tener al menos 6 caracteres")

    try:
        clave_hash = crear_hash(clave_plana)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Perfil: CONDUCTOR (default) o TENEDOR (dueño del vehículo con flota).
    perfil_solicitado = (data.perfil or "CONDUCTOR").strip().upper()
    if perfil_solicitado not in ("CONDUCTOR", "TENEDOR"):
        perfil_solicitado = "CONDUCTOR"

    cedula = re.sub(r"\D", "", data.cedula or "") or None

    nuevo = {
        "nombre": (data.nombre or "").upper(),
        "correo": correo_norm.upper() if correo_norm else None,
        "cedula": cedula,
        "regional": (data.regional or "N/A").upper(),
        "celular": (data.celular or "").upper() if data.celular else None,
        "perfil": perfil_solicitado,
        "clave": clave_hash,
        "clientes": [],
        "activo": True,
        # El correo se verifica con el enlace enviado al registrarse.
        "correo_verificado": False,
    }

    insertado = coleccion_conductores.insert_one(nuevo).inserted_id

    # Enviar correo de verificación (token hasheado en BD, plano solo en el enlace).
    token = _generar_token_verificacion(insertado)
    enlace = f"{FRONTEND_URL_VERIFICAR}?token={token}"
    background_tasks.add_task(enviar_correo_verificacion, correo_norm, enlace, (data.nombre or "").strip())

    return {
        "mensaje": "Conductor registrado. Revisa tu correo para verificar la cuenta.",
        "usuario": {"id": str(insertado), "correo": nuevo["correo"], "perfil": "CONDUCTOR"},
    }


# ==============================================================================
# 🔓 LOGIN
# ==============================================================================
@ruta_conductores.post("/login", response_model=dict)
async def login_conductor(usuario: str = Body(..., embed=True), clave: str = Body(..., embed=True)):
    usuario_ingresado = usuario.strip()
    clave_ingresada = clave.strip()
    # Regex anclado (^...$): antes matcheaba por subcadena y un correo
    # podía autenticarse como prefijo de otro.
    query_correo = {"correo": {"$regex": f"^{re.escape(usuario_ingresado)}$", "$options": "i"}}

    # 1. Conductor o Tenedor en la colección conductores.
    encontrado = coleccion_conductores.find_one(query_correo)
    perfil = (encontrado.get("perfil") or "CONDUCTOR").upper() if encontrado else "CONDUCTOR"

    # 2. Si no es conductor/tenedor, intentar ADMIN en baseusuarios (acceso de soporte al portal).
    if not encontrado:
        encontrado = coleccion_baseusuarios.find_one({**query_correo, "perfil": "ADMIN"})
        perfil = "ADMIN"

    if not encontrado:
        raise HTTPException(status_code=401, detail="Usuario o clave incorrectos")

    clave_almacenada = str(encontrado.get("clave", "")).strip()
    if not verificar_clave(clave_ingresada, clave_almacenada):
        raise HTTPException(status_code=401, detail="Usuario o clave incorrectos")

    # Cuentas stub de invitación pendiente (sin aceptar el enlace del tenedor).
    if perfil in ("CONDUCTOR", "TENEDOR") and not encontrado.get("activo", True):
        raise HTTPException(
            status_code=403,
            detail="Tu cuenta está pendiente de activación: abre el enlace que te enviamos por correo para elegir tu contraseña.",
        )

    # Exigir correo verificado a conductores y tenedores (los ADMIN de soporte no).
    if perfil in ("CONDUCTOR", "TENEDOR") and not encontrado.get("correo_verificado", False):
        raise HTTPException(
            status_code=403,
            detail="Tu correo aún no está verificado. Revisa tu bandeja de entrada (y spam) y abre el enlace de verificación.",
        )

    nombre_completo = encontrado.get("nombre", "").strip()
    primer_nombre = nombre_completo.split(" ")[0]

    return {
        "mensaje": "Login Conductor exitoso",
        "usuario": {
            "id": str(encontrado["_id"]),
            "correo": encontrado.get("correo", ""),
            "perfil": perfil,
            "primerNombre": primer_nombre,
        },
    }


# ==============================================================================
# ✉️ VERIFICACIÓN DE CORREO
# ==============================================================================
@ruta_conductores.get("/verificar-correo", response_model=dict)
async def verificar_correo(token: str = ""):
    """
    Valida el token del enlace del correo SIN efectos en BD (idempotente):
    la cuenta solo se habilita en POST /aceptar-politica, tras aceptar la
    política de tratamiento de datos vigente.
    """
    token_plano = token.strip()
    if not token_plano:
        raise HTTPException(status_code=400, detail="Token de verificación vacío")

    doc = _buscar_conductor_por_token(token_plano)
    if not doc:
        raise HTTPException(
            status_code=400,
            detail="Enlace de verificación inválido, expirado o ya utilizado. Puedes reenviarlo desde el login.",
        )

    correo = doc.get("correo", "")
    if doc.get("correo_verificado"):
        return {
            "estado": "ya_verificado",
            "mensaje": "Tu correo ya fue verificado. Puedes iniciar sesión.",
            "correo": correo,
        }

    politica = _politica_vigente()
    if not politica:
        raise HTTPException(
            status_code=503,
            detail="No hay política de tratamiento de datos vigente configurada. Intenta más tarde.",
        )
    return {
        "estado": "pendiente_aceptacion",
        "correo": correo,
        "politica": _politica_publica(politica),
    }


class AceptarPoliticaInput(BaseModel):
    token: str
    version_politica: int
    acepta: bool
    # Modelo declaraciones: ids de las declaraciones marcadas como aceptadas.
    # Deben ser todas las EXIGIDAS de la política vigente (validado en el
    # endpoint; las de DECLARACIONES_NO_EXIGIDAS no bloquean).
    declaraciones_aceptadas: Optional[list] = None


def _validar_declaraciones_completas(politica: dict, declaraciones_aceptadas: Optional[list]):
    """
    Con el modelo de declaraciones (v2+): exige que el usuario haya aceptado
    todas las declaraciones EXIGIDAS de la política vigente (las de
    DECLARACIONES_NO_EXIGIDAS no bloquean). Retorna la lista saneada de ids
    MARCADOS (solo las realmente aceptadas, para evidencia honesta), o None
    si la política no usa declaraciones (modelo v1).
    """
    declaraciones = politica.get("declaraciones") or []
    if not declaraciones:
        return None
    ids_todas = [d["id"] for d in declaraciones]
    ids_exigidas = [i for i in ids_todas if i not in DECLARACIONES_NO_EXIGIDAS]
    marcadas = set(declaraciones_aceptadas or [])
    faltantes = [i for i in ids_exigidas if i not in marcadas]
    if faltantes:
        raise HTTPException(
            status_code=400,
            detail="Debes aceptar todas las declaraciones para continuar. Faltan: " + ", ".join(faltantes),
        )
    return [i for i in ids_todas if i in marcadas]


def _registrar_aceptacion(doc: dict, politica: dict, request: Request, ahora, canal: str, declaraciones_aceptadas: Optional[list] = None):
    """
    Registra la evidencia append-only de aceptación de política y habilita la
    cuenta (correo_verificado + activo). Compartido entre /aceptar-politica
    (registro propio) y /aceptar-invitacion (cuenta creada por un tenedor).

    Con el modelo de declaraciones (v2+): una entrada de evidencia POR
    declaración aceptada (declaracion_id + titulo) y en el conductor queda
    `declaraciones_aceptadas: [ids]` además del resumen de aceptación.
    """
    declaraciones = politica.get("declaraciones") or []
    ip = request.client.host if request.client else ""
    user_agent = (request.headers.get("user-agent") or "")[:300]

    if declaraciones:
        aceptacion_ids = []
        for decl in declaraciones:
            # Solo las que el usuario marcó; por diseño el endpoint valida que
            # estén todas las EXIGIDAS antes de llegar aquí (las no exigidas
            # solo dejan evidencia si se marcaron).
            if declaraciones_aceptadas is not None and decl["id"] not in declaraciones_aceptadas:
                continue
            evidencia = {
                "conductor_id": doc["_id"],
                "conductor_usuario": doc.get("correo", ""),
                "conductor_correo": doc.get("correo", ""),
                "conductor_nombre": doc.get("nombre", ""),
                "politica_id": politica["_id"],
                "version": politica.get("version"),
                "declaracion_id": decl["id"],
                "declaracion_titulo": decl.get("titulo", ""),
                "aceptado_en": ahora,
                "canal": canal,
                "ip": ip,
                "user_agent": user_agent,
            }
            aceptacion_ids.append(coleccion_aceptaciones.insert_one(evidencia).inserted_id)

        coleccion_conductores.update_one(
            {"_id": doc["_id"]},
            {"$set": {
                "correo_verificado": True,
                "activo": True,
                "aceptacion_politica": {
                    "version": politica.get("version"),
                    "politica_id": politica["_id"],
                    "aceptado_en": ahora,
                    "declaraciones_aceptadas": declaraciones_aceptadas or [d["id"] for d in declaraciones],
                },
                "declaraciones_aceptadas": declaraciones_aceptadas or [d["id"] for d in declaraciones],
            }},
        )
        return

    # Modelo v1 (política única, sin declaraciones): comportamiento original.
    aceptacion = {
        "conductor_id": doc["_id"],
        "conductor_usuario": doc.get("correo", ""),
        "conductor_correo": doc.get("correo", ""),
        "conductor_nombre": doc.get("nombre", ""),
        "politica_id": politica["_id"],
        "version": politica.get("version"),
        "aceptado_en": ahora,
        "canal": canal,
        "ip": ip,
        "user_agent": user_agent,
    }
    aceptacion_id = coleccion_aceptaciones.insert_one(aceptacion).inserted_id

    coleccion_conductores.update_one(
        {"_id": doc["_id"]},
        {"$set": {
            "correo_verificado": True,
            "activo": True,
            "aceptacion_politica": {
                "version": politica.get("version"),
                "politica_id": politica["_id"],
                "aceptado_en": ahora,
                "aceptacion_id": aceptacion_id,
            },
        }},
    )


@ruta_conductores.post("/aceptar-politica", response_model=dict)
async def aceptar_politica(data: AceptarPoliticaInput, request: Request):
    """
    Registra la aceptación de la política de datos (evidencia trazable) y
    habilita la cuenta. Único punto que escribe la verificación.
    """
    token_plano = (data.token or "").strip()
    if not token_plano:
        raise HTTPException(status_code=400, detail="Token de verificación vacío")

    doc = _buscar_conductor_por_token(token_plano)
    if not doc:
        raise HTTPException(
            status_code=400,
            detail="Enlace de verificación inválido, expirado o ya utilizado. Puedes reenviarlo desde el login.",
        )

    correo = doc.get("correo", "")

    # Consentimiento afirmado en el servidor, no solo en el front.
    if data.acepta is not True:
        raise HTTPException(
            status_code=400,
            detail="Debes aceptar las políticas de tratamiento de datos para continuar.",
        )

    # Idempotente: token ya aceptado no genera segunda evidencia.
    if doc.get("correo_verificado") and doc.get("aceptacion_politica"):
        return {"estado": "ya_verificado", "mensaje": "Tu correo ya fue verificado.", "correo": correo}

    politica = _politica_vigente()
    if not politica:
        raise HTTPException(
            status_code=503,
            detail="No hay política de tratamiento de datos vigente configurada. Intenta más tarde.",
        )

    # La política cambió entre el GET y este POST → devolver la nueva para re-aceptar.
    if data.version_politica != politica.get("version"):
        raise HTTPException(
            status_code=400,
            detail={
                "mensaje": "La política fue actualizada. Revísala y acéptala nuevamente.",
                "politica": _politica_publica(politica),
            },
        )

    # Modelo declaraciones: TODAS las de la política vigente deben venir marcadas.
    ids_declaraciones = _validar_declaraciones_completas(politica, data.declaraciones_aceptadas)

    ahora = datetime.now(timezone.utc)
    _registrar_aceptacion(
        doc, politica, request, ahora, canal="verificacion_correo",
        declaraciones_aceptadas=ids_declaraciones,
    )

    return {
        "estado": "verificado",
        "mensaje": "Correo verificado y declaraciones aceptadas. Ya puedes iniciar sesión.",
        "correo": correo,
        "version_politica": politica.get("version"),
    }


class ReenviarVerificacionInput(BaseModel):
    correo: str


@ruta_conductores.post("/reenviar-verificacion", response_model=dict)
async def reenviar_verificacion(data: ReenviarVerificacionInput, background_tasks: BackgroundTasks):
    """
    Reenvía el correo de verificación. Respuesta neutra: no revela si la cuenta
    existe ni si ya está verificada (evita enumeración de correos).
    """
    correo_norm = (data.correo or "").strip()
    patron = {"$regex": f"^{re.escape(correo_norm)}$", "$options": "i"}
    doc = coleccion_conductores.find_one(patron)

    if doc and not doc.get("correo_verificado", False):
        # Regenerar token (invalida el anterior) y reenviar.
        token = _generar_token_verificacion(doc["_id"])
        enlace = f"{FRONTEND_URL_VERIFICAR}?token={token}"
        correo_destino = doc.get("correo") or correo_norm
        background_tasks.add_task(
            enviar_correo_verificacion, correo_destino, enlace, (doc.get("nombre") or "").strip()
        )

    return {"mensaje": "Si el correo está pendiente de verificación, se envió un nuevo enlace."}


# ==============================================================================
# 👥 INVITACIÓN DE CONDUCTOR POR PARTE DEL TENEDOR
# ==============================================================================
class InvitarConductorInput(BaseModel):
    id_tenedor: str
    placa: str
    correo_conductor: str
    nombre_conductor: Optional[str] = None


def _correo_patron(correo: str) -> dict:
    """Query de correo exacto case-insensitive (anclada, con el campo incluido)."""
    return {"correo": {"$regex": f"^{re.escape((correo or '').strip())}$", "$options": "i"}}


def enviar_correo_invitacion(destinatario: str, enlace: str, nombre: str, placa: str, tenedor: str):
    """Correo al conductor invitado: link para activar cuenta y aceptar política."""
    if not resend.api_key or "TuApiKeyAqui" in resend.api_key:
        print("⚠️ ERROR: Falta API KEY de Resend; no se envió la invitación.")
        return
    html = f"""
    <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 520px; margin: 0 auto;">
      <h2 style="color: #0f1928;">¡Hola{(', ' + nombre) if nombre else ''}!</h2>
      <p><strong>{tenedor}</strong> te invitó a operar el vehículo de placa
      <strong>{placa}</strong> en IntegrApp.</p>
      <p>Para activar tu cuenta de conductor, elige tu contraseña y acepta nuestras
      Políticas de Tratamiento de Datos Personales (Habeas Data):</p>
      <p style="text-align: center; margin: 28px 0;">
        <a href="{enlace}"
           style="background: #0f1928; color: #fff; padding: 12px 28px; border-radius: 10px;
                  text-decoration: none; font-weight: bold;">
          Activar mi cuenta
        </a>
      </p>
      <p>O copia y pega este enlace en tu navegador:</p>
      <p><a href="{enlace}">{enlace}</a></p>
      <p><small>El enlace vence en {EXPIRA_HORAS_VERIFICACION} horas. Si no esperabas esta
      invitación, ignora este mensaje.</small></p>
    </div>
    """
    try:
        resend.Emails.send({
            "from": MAIL_FROM,
            "to": [destinatario],
            "subject": f"Te invitaron a conducir el vehículo {placa} — IntegrApp",
            "html": html,
        })
        print(f"📧 Correo de invitación enviado a {destinatario}")
    except Exception as e:
        print(f"❌ Error enviando invitación: {e}")


@ruta_conductores.post("/invitar-conductor", response_model=dict)
async def invitar_conductor(data: InvitarConductorInput, background_tasks: BackgroundTasks):
    """
    El tenedor invita a un conductor para su placa:
    - Si ya tiene cuenta CONDUCTOR activa → se vincula directo.
    - Si no → se crea cuenta stub (inactiva, sin clave usable) y se envía
      el correo de activación; al aceptar queda vinculada.
    """
    placa = (data.placa or "").strip().upper()
    correo_invitado = (data.correo_conductor or "").strip()
    if not correo_invitado or "@" not in correo_invitado:
        raise HTTPException(status_code=400, detail="Correo del conductor inválido.")

    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")
    if str(vehiculo.get("idUsuario", "")) != str(data.id_tenedor):
        raise HTTPException(status_code=403, detail="El vehículo no pertenece a este tenedor.")

    existente = coleccion_conductores.find_one(_correo_patron(correo_invitado))

    if existente:
        perfil_existente = (existente.get("perfil") or "CONDUCTOR").upper()
        if perfil_existente == "TENEDOR":
            raise HTTPException(status_code=400, detail="Ese correo pertenece a un tenedor; no puede ser conductor.")
        if existente.get("activo", True) and existente.get("correo_verificado", False):
            # Cuenta viva → vinculación directa.
            coleccion_vehiculos.update_one(
                {"placa": placa},
                {"$set": {
                    "idConductor": str(existente["_id"]),
                    "invitacionConductor": {
                        "correo": existente.get("correo", ""),
                        "estado": "aceptada",
                        "creado_en": datetime.now(timezone.utc),
                    },
                }},
            )
            return {
                "estado": "vinculado",
                "mensaje": "El conductor ya tenía cuenta: quedó vinculado al vehículo.",
            }
        # Cuenta stub previa de otra invitación → regenerar token y reenviar.

    # Crear (o reusar) cuenta stub: sin clave usable hasta que el conductor
    # la elija en la página de aceptación.
    ahora = datetime.now(timezone.utc)
    if not existente:
        doc_stub = {
            "nombre": (data.nombre_conductor or correo_invitado.split("@")[0]).upper(),
            "correo": correo_invitado.upper(),
            "cedula": None,
            "regional": "N/A",
            "celular": None,
            "perfil": "CONDUCTOR",
            "clave": crear_hash(secrets.token_urlsafe(24)),  # aleatoria: nadie la conoce
            "clientes": [],
            "activo": False,          # login bloqueado hasta aceptar
            "correo_verificado": False,
            "invitado_por": str(data.id_tenedor),
        }
        stub_id = coleccion_conductores.insert_one(doc_stub).inserted_id
    else:
        stub_id = existente["_id"]
        coleccion_conductores.update_one(
            {"_id": stub_id},
            {"$set": {"invitado_por": str(data.id_tenedor)}},
        )

    token = _generar_token_verificacion(stub_id)
    enlace = f"{FRONTEND_URL_INVITACION}?token={token}&placa={placa}"

    # Nombre del tenedor para el correo (si existe su cuenta).
    doc_tenedor = None
    try:
        from bson import ObjectId as _ObjectId
        doc_tenedor = coleccion_conductores.find_one({"_id": _ObjectId(data.id_tenedor)})
    except Exception:
        doc_tenedor = coleccion_conductores.find_one({"_id": data.id_tenedor})
    nombre_tenedor = (doc_tenedor or {}).get("nombre", "") or "Integra"

    coleccion_vehiculos.update_one(
        {"placa": placa},
        {"$set": {
            "invitacionConductor": {
                "correo": correo_invitado.upper(),
                "estado": "pendiente",
                "creado_en": ahora,
                "expira": datetime.now(timezone.utc) + timedelta(hours=EXPIRA_HORAS_VERIFICACION),
            },
        }},
    )

    background_tasks.add_task(
        enviar_correo_invitacion, correo_invitado, enlace,
        (data.nombre_conductor or "").strip(), placa, nombre_tenedor,
    )
    return {
        "estado": "invitado",
        "mensaje": f"Invitación enviada a {correo_invitado}. El conductor quedará vinculado al aceptar.",
    }


class ReenviarInvitacionInput(BaseModel):
    id_tenedor: str
    placa: str


@ruta_conductores.post("/reenviar-invitacion", response_model=dict)
async def reenviar_invitacion(data: ReenviarInvitacionInput, background_tasks: BackgroundTasks):
    """Regenera el token de la invitación pendiente de una placa y reenvía el correo."""
    placa = (data.placa or "").strip().upper()
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")
    if str(vehiculo.get("idUsuario", "")) != str(data.id_tenedor):
        raise HTTPException(status_code=403, detail="El vehículo no pertenece a este tenedor.")

    invitacion = vehiculo.get("invitacionConductor") or {}
    if vehiculo.get("idConductor"):
        raise HTTPException(status_code=400, detail="Esa placa ya tiene un conductor vinculado.")
    if not invitacion.get("correo"):
        raise HTTPException(status_code=400, detail="No hay invitación pendiente para esta placa.")

    stub = coleccion_conductores.find_one(_correo_patron(invitacion["correo"]))
    if not stub:
        raise HTTPException(status_code=404, detail="No se encontró la cuenta del conductor invitado.")

    token = _generar_token_verificacion(stub["_id"])
    enlace = f"{FRONTEND_URL_INVITACION}?token={token}&placa={placa}"
    coleccion_vehiculos.update_one(
        {"placa": placa},
        {"$set": {"invitacionConductor.expira": datetime.now(timezone.utc) + timedelta(hours=EXPIRA_HORAS_VERIFICACION)}},
    )
    background_tasks.add_task(
        enviar_correo_invitacion, invitacion["correo"], enlace, "", placa, "Integra",
    )
    return {"mensaje": "Invitación reenviada."}


class AceptarInvitacionInput(BaseModel):
    token: str
    placa: str
    clave: str
    version_politica: int
    acepta: bool
    declaraciones_aceptadas: Optional[list] = None
    celular: Optional[str] = None
    cedula: Optional[str] = None


@ruta_conductores.post("/aceptar-invitacion", response_model=dict)
async def aceptar_invitacion(data: AceptarInvitacionInput, request: Request):
    """
    El conductor invitado activa su cuenta: valida el token, registra la
    aceptación de política (evidencia), fija su clave y queda vinculado a la placa.
    """
    token_plano = (data.token or "").strip()
    placa = (data.placa or "").strip().upper()
    if not token_plano:
        raise HTTPException(status_code=400, detail="Token de invitación vacío")

    doc = _buscar_conductor_por_token(token_plano)
    if not doc:
        raise HTTPException(
            status_code=400,
            detail="Enlace de invitación inválido o expirado. Pide al tenedor que te reenvíe la invitación.",
        )

    if data.acepta is not True:
        raise HTTPException(status_code=400, detail="Debes aceptar las políticas de tratamiento de datos para continuar.")

    clave_plana = (data.clave or "").strip()
    if len(clave_plana) < 6:
        raise HTTPException(status_code=400, detail="La clave debe tener al menos 6 caracteres")

    politica = _politica_vigente()
    if not politica:
        raise HTTPException(status_code=503, detail="No hay política de tratamiento de datos vigente configurada.")
    if data.version_politica != politica.get("version"):
        raise HTTPException(
            status_code=400,
            detail={
                "mensaje": "La política fue actualizada. Revísala y acéptala nuevamente.",
                "politica": _politica_publica(politica),
            },
        )

    # Modelo declaraciones: TODAS las de la política vigente deben venir marcadas.
    ids_declaraciones = _validar_declaraciones_completas(politica, data.declaraciones_aceptadas)

    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")

    ahora = datetime.now(timezone.utc)

    # Evidencia de política + cuenta habilitada.
    _registrar_aceptacion(
        doc, politica, request, ahora, canal="invitacion_tenedor",
        declaraciones_aceptadas=ids_declaraciones,
    )

    # Clave elegida por el conductor + datos adicionales.
    updates: dict = {"clave": crear_hash(clave_plana)}
    if data.celular:
        updates["celular"] = re.sub(r"\D", "", data.celular)
    if data.cedula:
        updates["cedula"] = re.sub(r"\D", "", data.cedula)
    coleccion_conductores.update_one({"_id": doc["_id"]}, {"$set": updates})

    # Vinculación al vehículo.
    coleccion_vehiculos.update_one(
        {"placa": placa},
        {"$set": {
            "idConductor": str(doc["_id"]),
            "invitacionConductor": {
                "correo": doc.get("correo", ""),
                "estado": "aceptada",
                "creado_en": (vehiculo.get("invitacionConductor") or {}).get("creado_en", ahora),
                "aceptada_en": ahora,
            },
        }},
    )

    return {
        "estado": "aceptada",
        "mensaje": "Cuenta activada y vehículo vinculado. Ya puedes iniciar sesión.",
        "correo": doc.get("correo", ""),
    }


class DesvincularConductorInput(BaseModel):
    id_tenedor: str
    placa: str


@ruta_conductores.put("/desvincular-conductor", response_model=dict)
async def desvincular_conductor(data: DesvincularConductorInput):
    """El tenedor quita al conductor (o la invitación pendiente) de una placa."""
    placa = (data.placa or "").strip().upper()
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")
    if str(vehiculo.get("idUsuario", "")) != str(data.id_tenedor):
        raise HTTPException(status_code=403, detail="El vehículo no pertenece a este tenedor.")
    if not vehiculo.get("idConductor") and not (vehiculo.get("invitacionConductor") or {}).get("correo"):
        raise HTTPException(status_code=400, detail="Esa placa no tiene conductor ni invitación.")

    coleccion_vehiculos.update_one(
        {"placa": placa},
        {"$set": {"idConductor": None, "invitacionConductor": None}},
    )
    return {"mensaje": "Conductor desvinculado del vehículo."}


# ==============================================================================
# 📜 POLÍTICA DE DATOS — CONSULTA PÚBLICA Y ADMINISTRACIÓN
# ==============================================================================
@ruta_conductores.get("/politica-datos", response_model=dict)
async def obtener_politica_datos():
    """Política vigente (auto-siembra la v1 si la colección está vacía)."""
    politica = _politica_vigente()
    if not politica:
        raise HTTPException(status_code=503, detail="No hay política de datos vigente configurada")
    return _politica_publica(politica)


def _requiere_admin(usuario: str) -> dict:
    """Valida que `usuario` exista en baseusuarios con perfil ADMIN (patrón del proyecto)."""
    user = coleccion_baseusuarios.find_one({"usuario": (usuario or "").strip().upper()})
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    if (user.get("perfil") or "").upper() != "ADMIN":
        raise HTTPException(
            status_code=403,
            detail=f"Su perfil ({user.get('perfil')}) no tiene permiso para administrar políticas.",
        )
    return user


class NuevaPoliticaInput(BaseModel):
    usuario: str
    titulo: str
    texto_html: str


@ruta_conductores.post("/politica-datos", response_model=dict, status_code=201)
async def crear_politica_datos(data: NuevaPoliticaInput):
    """Publica una nueva versión (version = max+1) y la deja como vigente."""
    _requiere_admin(data.usuario)

    titulo = (data.titulo or "").strip()
    texto = (data.texto_html or "").strip()
    if not titulo or not texto:
        raise HTTPException(status_code=400, detail="titulo y texto_html son obligatorios")

    ultima = coleccion_politicas.find_one(sort=[("version", -1)])
    nueva_version = (ultima.get("version", 0) or 0) + 1 if ultima else 1

    # Invariante: una sola versión activa.
    coleccion_politicas.update_many({"activo": True}, {"$set": {"activo": False}})
    doc = {
        "version": nueva_version,
        "titulo": titulo,
        "texto_html": texto,
        "activo": True,
        "publicado_en": datetime.now(timezone.utc),
        "publicado_por": (data.usuario or "").strip().upper(),
    }
    coleccion_politicas.insert_one(doc)
    return {"version": nueva_version, "titulo": titulo, "activo": True}


@ruta_conductores.get("/politica-datos/historial", response_model=list)
async def historial_politicas(usuario: str = ""):
    """Versiones publicadas (sin texto_html, es pesado). Solo ADMIN."""
    _requiere_admin(usuario)
    docs = coleccion_politicas.find({}, {"texto_html": 0}).sort("version", -1)
    return [
        {
            "version": d.get("version"),
            "titulo": d.get("titulo", ""),
            "activo": d.get("activo", False),
            "publicado_en": d.get("publicado_en"),
            "publicado_por": d.get("publicado_por", ""),
        }
        for d in docs
    ]


class ActivarPoliticaInput(BaseModel):
    usuario: str
    version: int


@ruta_conductores.put("/politica-datos/activar", response_model=dict)
async def activar_politica_datos(data: ActivarPoliticaInput):
    """Reactiva una versión histórica (desactivando la vigente). Solo ADMIN."""
    _requiere_admin(data.usuario)

    doc = coleccion_politicas.find_one({"version": data.version})
    if not doc:
        raise HTTPException(status_code=404, detail=f"No existe la versión {data.version}")

    coleccion_politicas.update_many({"activo": True}, {"$set": {"activo": False}})
    coleccion_politicas.update_one({"_id": doc["_id"]}, {"$set": {"activo": True}})
    return {"version": data.version, "activo": True}



@ruta_conductores.post("/recuperar/verificar", response_model=dict)
async def recuperar_verificar(data: VerificarInput, background_tasks: BackgroundTasks):
    # El front envía el correo en el campo `usuario` (compatibilidad de body).
    doc = _buscar_por_usuario(data.usuario)
    if not doc:
        return {"existe": False}

    codigo = str(random.randint(1000, 9999))
    coleccion_conductores.update_one({"_id": doc["_id"]}, {"$set": {"recovery_code": codigo}})

    correo_destino = doc.get("correo") or data.usuario.strip()
    background_tasks.add_task(enviar_correo_codigo, correo_destino, codigo)

    return {"existe": True, "mensaje": "Código generado"}


@ruta_conductores.post("/recuperar/validar", response_model=dict)
async def recuperar_validar(data: ValidarCodigoInput):
    doc = _buscar_por_usuario(data.usuario)
    if not doc:
        return {"valido": False}

    codigo_guardado = doc.get("recovery_code")
    es_valido = (codigo_guardado is not None) and (codigo_guardado == data.codigo)
    return {"valido": es_valido}


@ruta_conductores.post("/recuperar/cambiar", response_model=dict)
async def recuperar_cambiar(data: CambioClaveInput):
    doc = _buscar_por_usuario(data.usuario)
    if not doc:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")

    codigo_guardado = doc.get("recovery_code")
    if not codigo_guardado or codigo_guardado != data.codigo:
        raise HTTPException(status_code=403, detail="Código inválido o expirado")

    coleccion_conductores.update_one(
        {"_id": doc["_id"]},
        {"$set": {"clave": crear_hash(data.nuevaClave.strip())}, "$unset": {"recovery_code": ""}},
    )
    return {"mensaje": "Clave actualizada correctamente"}
